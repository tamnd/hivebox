//! Wiring pooled namespaces the way the comb does, then checking a cell in one reaches the DNS VIP
//! and nothing else. It needs root, `ip`, `python3` and Linux 6.6 or newer, and passes without
//! doing anything otherwise. It prints how long wiring and attaching take.

#![cfg(target_os = "linux")]

use std::net::{Ipv4Addr, UdpSocket};
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

use hive_guard::link::Netlink;
use hive_guard::wire::{self, VIP_DEVICE};
use hive_guard::{CellNet, DNS_VIP, Guard, MIRRORS_VIP, Profile};

const N: u64 = 64;

fn ip(args: &[&str]) -> bool {
    Command::new("ip").args(args).output().is_ok_and(|o| o.status.success())
}

struct Cleanup {
    base: u64,
    nss: Vec<String>,
    pins: PathBuf,
    made_vips: bool,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        for i in 0..N {
            ip(&["link", "del", &wire::host_name(self.base + i)]);
            let _ = Command::new("iptables")
                .args(["-D", "INPUT", "-i", &wire::host_name(self.base + i), "-j", "ACCEPT"])
                .output();
        }
        for ns in &self.nss {
            ip(&["netns", "del", ns]);
        }
        if self.made_vips {
            ip(&["link", "del", VIP_DEVICE]);
        }
        let _ = std::fs::remove_dir_all(&self.pins);
    }
}

#[test]
fn pooled_namespaces_are_wired_and_guarded() {
    let root = rustix::process::geteuid().is_root();
    let tools =
        ip(&["-V"]) && Command::new("python3").arg("-V").output().is_ok_and(|o| o.status.success());
    if !root || !tools {
        eprintln!("skipped: needs root, ip and python3");
        return;
    }
    let pid = u64::from(std::process::id());
    let base = pid * 1000;
    let mut c = Cleanup {
        base,
        nss: (0..N).map(|i| format!("hbw{pid}-{i}")).collect(),
        pins: format!("/sys/fs/bpf/hive-wire-{pid}").into(),
        made_vips: !std::path::Path::new("/sys/class/net").join(VIP_DEVICE).exists(),
    };
    for ns in &c.nss {
        assert!(ip(&["netns", "add", ns]));
    }
    let mut guard = match Guard::open(&c.pins) {
        Ok(g) => g,
        Err(e) if e.kind() == std::io::ErrorKind::Unsupported => {
            eprintln!("skipped: {e}");
            return;
        }
        Err(e) => panic!("{e}"),
    };
    guard.set_profile(Profile::NONE, &Profile::NONE.builtin_rules()).unwrap();
    let mut nl = Netlink::open().unwrap();
    wire::vips(&mut nl).unwrap();
    wire::vips(&mut nl).unwrap();

    let (mut wiring, mut attaching) = (Duration::ZERO, Duration::ZERO);
    let mut veths = Vec::new();
    for (i, ns) in c.nss.iter().enumerate() {
        let n = base + i as u64;
        let t = Instant::now();
        let v = wire::wire(
            &mut nl,
            &PathBuf::from("/run/netns").join(ns),
            n,
            Ipv4Addr::new(100, 64, 1, 1 + i as u8),
        )
        .unwrap();
        wiring += t.elapsed();
        let t = Instant::now();
        assert_eq!(guard.attach(&v.host).unwrap(), v.ifindex);
        attaching += t.elapsed();
        let _ =
            Command::new("iptables").args(["-I", "INPUT", "-i", &v.host, "-j", "ACCEPT"]).output();
        veths.push(v);
    }
    println!(
        "wired {N} in {wiring:.2?}, {:.2?} each, attached in {attaching:.2?}, {:.2?} each",
        wiring / N as u32,
        attaching / N as u32
    );

    let t = Instant::now();
    for (i, v) in veths.iter().enumerate() {
        let cell =
            CellNet { idx: 5000 + i as u32, ip: v.ip, mac: Some(v.mac), profile: Profile::NONE };
        guard.set_cell(v.ifindex, &cell).unwrap();
    }
    println!("gave {N} interfaces a cell in {:.2?}", t.elapsed());

    let dns = UdpSocket::bind((DNS_VIP, 53)).unwrap();
    dns.set_read_timeout(Some(Duration::from_millis(500))).unwrap();
    let mirror = UdpSocket::bind((MIRRORS_VIP, 53)).unwrap();
    mirror.set_read_timeout(Some(Duration::from_millis(500))).unwrap();
    let send = |ns: &str, to: Ipv4Addr| {
        let script = format!(
            "import socket; s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); s.sendto(b'hi', ('{to}', 53))"
        );
        assert!(
            Command::new("ip")
                .args(["netns", "exec", ns, "python3", "-c", &script])
                .status()
                .unwrap()
                .success()
        );
    };
    let mut buf = [0u8; 8];
    for (i, ns) in [0, N as usize - 1].map(|i| (i, &c.nss[i])) {
        send(ns, DNS_VIP);
        let (_, from) = dns.recv_from(&mut buf).expect("the cell reaches the DNS VIP");
        assert_eq!(from.ip(), std::net::IpAddr::V4(veths[i].ip), "from the cell's own address");
        send(ns, MIRRORS_VIP);
        assert!(mirror.recv_from(&mut buf).is_err(), "none has no mirrors");
    }
    let out =
        Command::new("ip").args(["netns", "exec", &c.nss[0], "ip", "-o", "addr"]).output().unwrap();
    let addrs = String::from_utf8_lossy(&out.stdout);
    assert!(
        !addrs.contains("inet6")
            || addrs.lines().all(|l| !l.contains("eth0") || !l.contains("inet6")),
        "no IPv6 on eth0: {addrs}"
    );

    let t = Instant::now();
    for v in &veths {
        guard.remove_cell(v.ifindex).unwrap();
        guard.detach(v.ifindex).unwrap();
        wire::unwire(&mut nl, v).unwrap();
    }
    println!("unwired {N} in {:.2?}", t.elapsed());
    assert!(!std::path::Path::new("/sys/class/net").join(&veths[0].host).exists());
    wire::unwire(&mut nl, &veths[0]).unwrap();
    guard.remove_cell(veths[0].ifindex).unwrap();
    assert_eq!(std::fs::read_dir(c.pins.join("links")).unwrap().count(), 0);
    c.made_vips &= true;
}
