//! The program on a real veth, with a cell in its own network namespace sending to addresses on
//! the host. It needs root, `ip`, `python3` and a kernel with tcx links (6.6 or newer), and passes
//! without doing anything otherwise.

#![cfg(target_os = "linux")]

use std::net::{Ipv4Addr, UdpSocket};
use std::process::Command;
use std::time::{Duration, Instant};

use hive_guard::{CellNet, DNS_VIP, GATEWAY, Guard, Profile, Proto, Reason, Rule};

const CELL: Ipv4Addr = Ipv4Addr::new(100, 64, 0, 2);
const OTHER: Ipv4Addr = Ipv4Addr::new(100, 64, 0, 3);
const DENIED: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 1);
const RESOLVED: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 2);

fn ip(args: &str) -> bool {
    Command::new("ip").args(args.split(' ')).status().is_ok_and(|s| s.success())
}

fn must(args: &str) {
    assert!(ip(args), "ip {args}");
}

fn mac(text: &str) -> [u8; 6] {
    let mut mac = [0u8; 6];
    for (b, part) in mac.iter_mut().zip(text.trim().split(':')) {
        *b = u8::from_str_radix(part, 16).unwrap();
    }
    mac
}

/// A host firewall with a drop policy for input, as ufw has, would hide what the program
/// passes, so the test lets in whatever arrives on its own interface while it runs.
fn firewall(op: &str, iface: &str) {
    let _ = Command::new("iptables").args([op, "INPUT", "-i", iface, "-j", "ACCEPT"]).output();
}

/// Everything the test makes on the host, removed however it ends.
struct Net {
    ns: String,
    host: String,
    dummy: String,
    pins: std::path::PathBuf,
}

impl Drop for Net {
    fn drop(&mut self) {
        firewall("-D", &self.host);
        ip(&format!("netns del {}", self.ns));
        ip(&format!("link del {}", self.host));
        ip(&format!("link del {}", self.dummy));
        let _ = std::fs::remove_dir_all(&self.pins);
    }
}

impl Net {
    fn inside(&self, cmd: &[&str]) -> std::process::Output {
        let out = Command::new("ip").args(["netns", "exec", &self.ns]).args(cmd).output().unwrap();
        assert!(out.status.success(), "{cmd:?}: {}", String::from_utf8_lossy(&out.stderr));
        out
    }

    /// Sends one datagram from inside the cell, from `src`.
    fn send(&self, src: Ipv4Addr, to: Ipv4Addr, port: u16) {
        let script = format!(
            "import socket; s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); s.bind(('{src}', 0)); s.sendto(b'hi', ('{to}', {port}))"
        );
        self.inside(&["python3", "-c", &script]);
    }
}

fn got(sock: &UdpSocket) -> bool {
    let mut buf = [0u8; 16];
    sock.recv_from(&mut buf).is_ok()
}

fn listen(ip: Ipv4Addr, port: u16) -> UdpSocket {
    let s = UdpSocket::bind((ip, port)).unwrap();
    s.set_read_timeout(Some(Duration::from_millis(500))).unwrap();
    s
}

#[test]
fn a_cell_reaches_only_what_its_profile_and_dns_allow() {
    let tools =
        ip("-V") && Command::new("python3").arg("-V").output().is_ok_and(|o| o.status.success());
    if !rustix::process::geteuid().is_root() || !tools {
        eprintln!("skipped: needs root, ip and python3");
        return;
    }
    let id = std::process::id() % 100_000;
    let net = Net {
        ns: format!("hbg{id}"),
        host: format!("hbg{id}h"),
        dummy: format!("hbg{id}d"),
        pins: format!("/sys/fs/bpf/hive-test-{id}").into(),
    };
    must(&format!("netns add {}", net.ns));
    must(&format!("link add {} type veth peer name eth0 netns {}", net.host, net.ns));
    must(&format!("link add {} type dummy", net.dummy));
    for a in [DNS_VIP, DENIED, RESOLVED] {
        must(&format!("addr add {a}/32 dev {}", net.dummy));
    }
    must(&format!("link set {} up", net.dummy));
    must(&format!("addr add {GATEWAY}/32 dev {}", net.host));
    must(&format!("link set {} up", net.host));
    firewall("-I", &net.host);
    must(&format!("route add {CELL}/32 dev {}", net.host));
    must(&format!("route add {OTHER}/32 dev {}", net.host));
    let host_mac = std::fs::read_to_string(format!("/sys/class/net/{}/address", net.host)).unwrap();
    for cmd in [
        format!("addr add {CELL}/32 dev eth0"),
        format!("addr add {OTHER}/32 dev eth0"),
        "link set lo up".into(),
        "link set eth0 up".into(),
        format!("neigh add {GATEWAY} lladdr {} dev eth0 nud permanent", host_mac.trim()),
        format!("route add default via {GATEWAY} dev eth0 onlink src {CELL}"),
    ] {
        let mut args = vec!["ip"];
        args.extend(cmd.split(' '));
        net.inside(&args);
    }
    let cell_mac = mac(&String::from_utf8(
        net.inside(&["cat", "/sys/class/net/eth0/address"]).stdout,
    )
    .unwrap());

    let t = Instant::now();
    let mut guard = match Guard::open(&net.pins) {
        Ok(g) => g,
        Err(e) if e.kind() == std::io::ErrorKind::Unsupported => {
            eprintln!("skipped: {e}");
            return;
        }
        Err(e) => panic!("{e}"),
    };
    println!("loaded in {:.2?}", t.elapsed());
    for p in [Profile::NONE, Profile::MIRRORS] {
        guard.set_profile(p, &p.builtin_rules()).unwrap();
    }

    let dns = listen(DNS_VIP, 53);
    let denied = listen(DENIED, 9);
    let resolved = listen(RESOLVED, 9);

    // Attached with no cell, the interface passes nothing.
    let t = Instant::now();
    let ifindex = guard.attach(&net.host).unwrap();
    println!("attached in {:.2?}", t.elapsed());
    net.send(CELL, DNS_VIP, 53);
    assert!(!got(&dns));
    assert!(guard.stats().unwrap().get(Reason::NoCell) >= 1);

    let cell = CellNet { idx: 41, ip: CELL, mac: Some(cell_mac), profile: Profile::NONE };
    guard.set_cell(ifindex, &cell).unwrap();
    assert_eq!(guard.cell(ifindex).unwrap(), Some(cell));
    net.send(CELL, DNS_VIP, 53);
    assert!(got(&dns), "the DNS proxy is in every profile: {:?}", guard.stats().unwrap());
    net.send(CELL, DENIED, 9);
    assert!(!got(&denied));
    let deny = guard.denies().into_iter().find(|d| d.ip == DENIED).expect("the drop was reported");
    assert_eq!((deny.cell, deny.reason, deny.port, deny.proto), (41, Reason::Policy, 9, 17));

    // What the DNS proxy resolved is reachable until it expires.
    guard.allow(41, RESOLVED, Duration::from_secs(60)).unwrap();
    guard.allow(41, DENIED, Duration::ZERO).unwrap();
    net.send(CELL, RESOLVED, 9);
    assert!(got(&resolved));
    net.send(CELL, DENIED, 9);
    assert!(!got(&denied), "an expired answer allows nothing");
    // A later cell on the same interface does not inherit it.
    guard.set_cell(ifindex, &CellNet { idx: 42, ..cell }).unwrap();
    net.send(CELL, RESOLVED, 9);
    assert!(!got(&resolved));
    guard.set_cell(ifindex, &cell).unwrap();

    // Another source address is not the cell's, and neither is another MAC.
    let before = guard.stats().unwrap().get(Reason::Spoof);
    net.send(OTHER, RESOLVED, 9);
    assert!(!got(&resolved));
    assert_eq!(guard.stats().unwrap().get(Reason::Spoof), before + 1);
    guard.set_cell(ifindex, &CellNet { mac: Some([2, 0, 0, 0, 0, 1]), ..cell }).unwrap();
    net.send(CELL, RESOLVED, 9);
    assert!(!got(&resolved));
    assert_eq!(guard.stats().unwrap().get(Reason::Spoof), before + 2);
    guard.set_cell(ifindex, &cell).unwrap();

    // A profile change takes effect on the next packet.
    let rule = Rule { profile: Profile(7), ip: DENIED, proto: Proto::Udp, port: 9 };
    guard.set_profile(Profile(7), &[rule]).unwrap();
    guard.set_cell(ifindex, &CellNet { profile: Profile(7), ..cell }).unwrap();
    net.send(CELL, DENIED, 9);
    assert!(got(&denied));
    net.send(CELL, DNS_VIP, 53);
    assert!(!got(&dns), "profile 7 has no DNS");
    guard.set_profile(Profile(7), &[]).unwrap();
    net.send(CELL, DENIED, 9);
    assert!(!got(&denied));
    guard.set_cell(ifindex, &cell).unwrap();

    // The policy outlives the process that set it, and a new one takes over without a gap.
    drop(guard);
    net.send(CELL, DENIED, 9);
    assert!(!got(&denied), "still enforced with no process holding the program");
    net.send(CELL, RESOLVED, 9);
    assert!(got(&resolved));
    let mut guard = Guard::open(&net.pins).unwrap();
    assert_eq!(guard.cell(ifindex).unwrap(), Some(cell));
    guard.attach(&net.host).unwrap();
    let links = std::fs::read_dir(net.pins.join("links")).unwrap().count();
    assert_eq!(links, 1, "the old link went once the new one was in");
    net.send(CELL, DENIED, 9);
    assert!(!got(&denied));

    // What a create and a DNS answer cost the node.
    let t = Instant::now();
    for i in 0..1000 {
        guard.set_cell(ifindex, &CellNet { idx: 1000 + i, ..cell }).unwrap();
    }
    println!("set_cell takes {:.2?}", t.elapsed() / 1000);
    guard.set_cell(ifindex, &cell).unwrap();
    let t = Instant::now();
    for i in 0..1000u32 {
        guard.allow(41, Ipv4Addr::from(0x0A00_0000 + i), Duration::from_secs(60)).unwrap();
    }
    println!("allow takes {:.2?}", t.elapsed() / 1000);

    // A quarantined cell reaches nothing, not the DNS proxy and not what it resolved before.
    let mut allowed = guard.dns_allow().unwrap();
    guard.set_cell(ifindex, &CellNet { profile: Profile::QUARANTINE, ..cell }).unwrap();
    let t = Instant::now();
    assert_eq!(allowed.forget(41).unwrap(), 1002);
    println!("forgetting 1002 answers takes {:.2?}", t.elapsed());
    net.send(CELL, RESOLVED, 9);
    assert!(!got(&resolved));
    net.send(CELL, DNS_VIP, 53);
    assert!(!got(&dns));
    assert_eq!(allowed.forget(41).unwrap(), 0);
    guard.set_cell(ifindex, &cell).unwrap();

    let stats = guard.stats().unwrap();
    println!("passed {} dropped {}: {stats:?}", stats.passed(), stats.dropped());

    // Detached, the interface is not filtered at all.
    guard.detach(ifindex).unwrap();
    net.send(CELL, DENIED, 9);
    assert!(got(&denied));
    guard.attach(&net.host).unwrap();
    must(&format!("link del {}", net.host));
    assert_eq!(guard.sweep().unwrap(), 1, "the link of a deleted interface is swept");
}
