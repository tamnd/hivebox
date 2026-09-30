//! Real containers. These need root, cgroup v2, an image imported with `hive-oci import` and a
//! static drone, and they skip when `HIVE_OCI_IMAGE` and `HIVE_OCI_DRONE` do not say where those
//! are:
//!
//! ```text
//! docker export $(docker create python:3.12-slim) | hive-oci import /var/tmp/python
//! HIVE_OCI_IMAGE=/var/tmp/python HIVE_OCI_DRONE=target/x86_64-unknown-linux-musl/release/hive-drone \
//!     cargo test -p hive-cell-oci --test oci
//! ```

#![cfg(target_os = "linux")]

use hive_cell::{
    CellDriver, CellHandle, GuestChannel, Liveness, PauseMode, RootfsPlan, Slot, cgroup, netns,
};
use hive_cell_oci::{Config, OciDriver};
use hive_drone::Client;
use hive_proto::drone::api::{Command, RunRequest};
use hive_types::{Backend, CellId, CellSpec, Resources, Source};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const BASE: u32 = 1_000_000;

struct Env {
    image: PathBuf,
    root: PathBuf,
    driver: OciDriver,
}

fn env(name: &str, workers: usize) -> Option<Env> {
    let image = PathBuf::from(std::env::var_os("HIVE_OCI_IMAGE")?);
    let drone = PathBuf::from(std::env::var_os("HIVE_OCI_DRONE")?);
    let root = PathBuf::from(format!("/run/hive-oci-test/{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("netns")).unwrap();
    // cgroup v2 takes controllers from the top down, so the test slice turns them on for its
    // children the way the comb does for hive.slice.
    let slice = PathBuf::from("/sys/fs/cgroup/hive-oci-test.slice");
    std::fs::create_dir_all(&slice).unwrap();
    cgroup::write(&slice, "cgroup.subtree_control", "+cpu +memory +pids").unwrap();
    let driver = OciDriver::new(Config {
        worker: vec![env!("CARGO_BIN_EXE_hive-oci").into(), "worker".into()],
        workers,
        drone: std::fs::canonicalize(drone).unwrap(),
        state_dir: root.join("oci"),
        uid_base: BASE,
        uid_count: 65536,
    })
    .unwrap();
    Some(Env { image, root, driver })
}

static SEQ: AtomicU64 = AtomicU64::new(1);

struct Cell {
    h: CellHandle,
    client: Client,
    slot: Slot,
}

impl Env {
    fn slot(&self, id: CellId, n: u64, r: &Resources) -> Slot {
        let dir = self.root.join("cells").join(id.to_string());
        std::fs::create_dir_all(&dir).unwrap();
        let cg = PathBuf::from("/sys/fs/cgroup/hive-oci-test.slice").join(id.to_string());
        std::fs::create_dir_all(&cg).unwrap();
        cgroup::limit(&cg, r).unwrap();
        let ns = self.root.join("netns").join(id.to_string());
        netns::create(std::slice::from_ref(&ns)).pop().unwrap().unwrap();
        let secret: [u8; 32] = std::array::from_fn(|i| (n as u8).wrapping_mul(31) ^ (i as u8));
        Slot { cgroup: cg, netns: Some(ns), nameserver: None, dir, secret }
    }

    /// Makes a cell and connects to its drone, and returns how long each step took.
    async fn create(&self, spec: &CellSpec) -> (Cell, [Duration; 3]) {
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let id = CellId::new(1, 1, 1, n, 7).unwrap();
        let slot = self.slot(id, n, &spec.resources);
        let rootfs = RootfsPlan { lowers: vec![self.image.clone()], upper: slot.dir.join("upper") };
        let t = Instant::now();
        let mut h = self.driver.prepare(id, spec, &rootfs, &slot).await.unwrap();
        let prepared = t.elapsed();
        self.driver.start(&mut h).await.unwrap();
        let started = t.elapsed();
        let GuestChannel::Unix(sock) = &h.channel else { panic!("not a unix channel") };
        let until = Instant::now() + Duration::from_secs(10);
        let client = loop {
            if let Ok(s) = tokio::net::UnixStream::connect(sock).await {
                let nonce: [u8; 32] = std::array::from_fn(|i| (n as u8) ^ (i as u8));
                if let Ok(c) = Client::connect(s, &slot.secret, 0, nonce).await {
                    break c;
                }
            }
            if Instant::now() > until {
                let log = std::fs::read_to_string(
                    self.root.join("oci").join(id.to_string()).join("drone.log"),
                );
                panic!("the drone never answered: {log:?}");
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        };
        let connected = t.elapsed();
        (Cell { h, client, slot }, [prepared, started - prepared, connected - started])
    }

    async fn stop(&self, c: Cell, grace: Duration) -> hive_cell::ExitInfo {
        drop(c.client);
        let exit = self.driver.stop(&c.h, grace).await.unwrap();
        tidy(&c.slot);
        exit
    }
}

fn tidy(slot: &Slot) {
    let _ = std::fs::remove_dir_all(&slot.dir);
    let _ = cgroup::kill(&slot.cgroup);
    for _ in 0..500 {
        if !slot.cgroup.exists() || std::fs::remove_dir(&slot.cgroup).is_ok() {
            break;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    if let Some(ns) = &slot.netns {
        let _ = netns::remove(ns);
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn spec() -> CellSpec {
    let mut s = CellSpec::new(Source::Image("python".into()), Backend::Container);
    s.resources.mem_mib = 256;
    s.env.insert("HIVE_TEST".into(), "yes".into());
    s
}

async fn sh(c: &Cell, script: &str) -> (i32, String) {
    let r = c
        .client
        .run(&RunRequest {
            command: Some(Command { shell: script.into(), ..Command::default() }),
            ..Default::default()
        })
        .await
        .unwrap();
    let text =
        format!("{}{}", String::from_utf8_lossy(&r.stdout), String::from_utf8_lossy(&r.stderr));
    (r.exit_code, text)
}

async fn ok(c: &Cell, script: &str) -> String {
    let (code, text) = sh(c, script).await;
    assert_eq!(code, 0, "{script}: {text}");
    text
}

async fn refused(c: &Cell, script: &str) {
    let (code, text) = sh(c, script).await;
    assert_ne!(code, 0, "{script} worked: {text}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_container_cell_runs_python_and_keeps_to_itself() {
    let Some(e) = env("run", 2) else { return };
    let (c, _) = e.create(&spec()).await;

    assert_eq!(ok(&c, "python3 -c 'print(6 * 7)'").await, "42\n");
    assert_eq!(ok(&c, "echo $HIVE_TEST $HOME").await, "yes /root\n");
    assert_eq!(ok(&c, "id -u; hostname").await, "0\ncell\n");
    // Root in the cell is someone else on the host.
    let map = ok(&c, "cat /proc/self/uid_map").await;
    assert_eq!(map.split_whitespace().collect::<Vec<_>>(), ["0", &BASE.to_string(), "65536"]);
    // The image's files belong to the cell's root, and the cell can write where it likes.
    assert_eq!(ok(&c, "stat -c %u /usr/bin /etc/passwd").await, "0\n0\n");
    ok(&c, "mkdir /workspace && echo made > /workspace/f && cat /workspace/f && pip --version")
        .await;
    // Its own network, with nothing but loopback, and its own processes.
    let links = ok(&c, "cat /proc/net/dev").await;
    assert!(links.contains("lo:") && links.lines().count() == 3, "{links}");
    // localhost resolves, which test suites that bind to it need, and the cell can add names.
    let bound =
        "import socket; s = socket.socket(); s.bind(('localhost', 0)); print(s.getsockname()[0])";
    assert_eq!(ok(&c, &format!("python3 -c \"{bound}\"")).await, "127.0.0.1\n");
    ok(&c, "echo '10.9.9.9 extra' >> /etc/hosts && getent hosts extra").await;
    let procs = ok(&c, "ls /proc | grep -c '^[0-9]'").await;
    assert!(procs.trim().parse::<u32>().unwrap() < 10, "{procs} processes");
    // Its cgroup, read only, with its limit on it.
    assert_eq!(ok(&c, "cat /sys/fs/cgroup/memory.max").await, format!("{}\n", 256 << 20));
    refused(&c, "echo max > /sys/fs/cgroup/memory.max").await;

    // The drone is there read only, its socket is not there at all, and neither is its secret.
    assert_eq!(ok(&c, "ls -A /.hive").await, "drone\n");
    refused(&c, "echo x > /.hive/drone").await;
    refused(&c, "rm -f /.hive/drone").await;
    let fds = ok(&c, "ls /proc/self/fd").await;
    assert_eq!(fds.split_whitespace().collect::<Vec<_>>(), ["0", "1", "2", "3"], "{fds}");
    // Neither init nor the drone can be traced or read by what runs in the cell.
    refused(&c, "cat /proc/1/environ").await;
    refused(&c, "cat /proc/$(cut -d' ' -f1 /proc/1/task/1/children)/environ").await;
    // No way out through namespaces, mounts or the kernel's odd corners.
    refused(&c, "unshare -U true").await;
    refused(&c, "mount -t tmpfs none /mnt").await;
    assert_eq!(ok(&c, "wc -c < /proc/kcore").await, "0\n");
    refused(&c, "echo 1 > /proc/sys/kernel/sysrq").await;

    // Freeze and thaw.
    e.driver.pause(&c.h, PauseMode::Freeze).await.unwrap();
    assert_eq!(e.driver.check(&c.h).await.unwrap(), Liveness::Paused);
    e.driver.resume(&c.h).await.unwrap();
    assert_eq!(e.driver.check(&c.h).await.unwrap(), Liveness::Alive);
    assert_eq!(ok(&c, "echo back").await, "back\n");

    let h = c.h.clone();
    let dir = e.root.join("oci").join(h.id.to_string());
    let exit = e.stop(c, Duration::from_secs(5)).await;
    assert_eq!(exit.code, Some(128 + 15), "{exit:?}");
    assert!(!exit.oom);
    assert!(!dir.exists(), "the cell's state is still there");
    assert!(matches!(e.driver.check(&h).await.unwrap(), Liveness::Gone(_)));
    // A second stop is fine.
    e.driver.stop(&h, Duration::ZERO).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cell_that_runs_out_of_memory_is_gone_and_says_so() {
    let Some(e) = env("oom", 1) else { return };
    let mut s = spec();
    s.resources.mem_mib = 64;
    let (c, _) = e.create(&s).await;
    // The whole cgroup goes, the drone with it, so the call ends with a hang up and not an answer.
    let hog = "python3 -c 'b = bytearray(300 << 20); print(len(b))'";
    let command = Some(Command { shell: hog.into(), ..Command::default() });
    let _ = c.client.run(&RunRequest { command, ..Default::default() }).await;
    let until = Instant::now() + Duration::from_secs(10);
    let gone = loop {
        if let Liveness::Gone(exit) = e.driver.check(&c.h).await.unwrap() {
            break exit;
        }
        assert!(Instant::now() < until, "the cell outlived its memory");
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert!(gone.oom, "{gone:?}");
    let exit = e.stop(c, Duration::ZERO).await;
    assert!(exit.oom && exit.signal == Some(9), "{exit:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_prepare_or_start_leaves_nothing() {
    let Some(e) = env("bad", 1) else { return };
    let s = spec();
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let id = CellId::new(1, 1, 1, n, 7).unwrap();
    let slot = e.slot(id, n, &s.resources);
    let missing =
        RootfsPlan { lowers: vec![e.root.join("nothing")], upper: slot.dir.join("upper") };
    assert!(e.driver.prepare(id, &s, &missing, &slot).await.is_err());
    assert!(!e.root.join("oci").join(id.to_string()).exists());

    // A drone that is not there fails inside libcontainer, when it mounts it.
    let broken = OciDriver::new(Config {
        worker: vec![env!("CARGO_BIN_EXE_hive-oci").into(), "worker".into()],
        workers: 1,
        drone: PathBuf::from("/nonexistent/hive-drone"),
        state_dir: e.root.join("oci"),
        uid_base: BASE,
        uid_count: 65536,
    })
    .unwrap();
    let rootfs = RootfsPlan { lowers: vec![e.image.clone()], upper: slot.dir.join("upper") };
    let mut h = broken.prepare(id, &s, &rootfs, &slot).await.unwrap();
    let err = broken.start(&mut h).await.unwrap_err();
    assert!(err.to_string().contains("making the container"), "{err}");
    broken.stop(&h, Duration::ZERO).await.unwrap();
    assert!(!e.root.join("oci").join(id.to_string()).exists());
    // libcontainer removes the cgroup when a create fails, and the comb takes that as done.
    let left = slot.cgroup.exists() && cgroup::populated(&slot.cgroup).unwrap();
    assert!(!left, "a process was left in the cgroup");
    tidy(&slot);
}

/// What a container cell costs. Run it with
/// `cargo test --release -p hive-cell-oci --test oci -- --ignored --nocapture`, with `CELLS` and
/// `WORKERS` to change the sizes.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, not a test"]
async fn oci_cost() {
    let workers = std::env::var("WORKERS").ok().and_then(|v| v.parse().ok()).unwrap_or(8);
    let cells: usize = std::env::var("CELLS").ok().and_then(|v| v.parse().ok()).unwrap_or(64);
    let Some(e) = env("cost", workers) else { return };
    let e = std::sync::Arc::new(e);
    let s = spec();

    // One at a time: each step of a create, then a command, then the stop.
    let mut steps = vec![Vec::new(); 5];
    for _ in 0..cells.min(40) {
        let (c, t) = e.create(&s).await;
        let r = Instant::now();
        ok(&c, "true").await;
        let ran = r.elapsed();
        let t0 = Instant::now();
        e.stop(c, Duration::from_secs(5)).await;
        for (v, d) in steps.iter_mut().zip([t[0], t[1], t[2], ran, t0.elapsed()]) {
            v.push(d);
        }
    }
    for (name, mut v) in
        ["prepare", "start", "handshake", "run true", "stop"].into_iter().zip(steps)
    {
        v.sort();
        println!("serial {name}: p50 {:?} p99 {:?}", v[v.len() / 2], v[v.len() * 99 / 100]);
    }

    // All at once, then every one of them idle, then all stopped at once.
    let t = Instant::now();
    let made = futures::future::join_all((0..cells).map(|_| {
        let e = e.clone();
        let s = s.clone();
        tokio::spawn(async move {
            let t = Instant::now();
            let (c, _) = e.create(&s).await;
            (c, t.elapsed())
        })
    }))
    .await;
    let wall = t.elapsed();
    let (live, mut times): (Vec<Cell>, Vec<Duration>) =
        made.into_iter().map(Result::unwrap).unzip();
    times.sort();
    println!(
        "{cells} at once with {workers} workers: all up in {wall:?}, {:.0} cells/s, create p50 {:?} p99 {:?}",
        cells as f64 / wall.as_secs_f64(),
        times[cells / 2],
        times[cells * 99 / 100]
    );
    tokio::time::sleep(Duration::from_secs(1)).await;
    let mem: Vec<u64> = live.iter().map(|c| cgroup::metrics(&c.h.cgroup).mem_bytes).collect();
    println!(
        "idle cell memory: mean {} KiB, max {} KiB",
        (mem.iter().sum::<u64>() / mem.len() as u64) >> 10,
        mem.iter().max().unwrap() >> 10
    );
    let t = Instant::now();
    futures::future::join_all(live.into_iter().map(|c| {
        let e = e.clone();
        tokio::spawn(async move { e.stop(c, Duration::from_secs(5)).await })
    }))
    .await;
    println!("{cells} stopped at once in {:?}", t.elapsed());
}
