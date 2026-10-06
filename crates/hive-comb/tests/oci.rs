//! Container cells through the whole comb: admission, the cgroup and network namespace pools, the
//! WAL, the OCI driver with its workers started as `hive-comb --oci-worker`, and the drone inside.
//! Needs root, cgroup v2, a static drone in `HIVE_OCI_DRONE`, and an image: either one made by
//! `hive-oci import` in `HIVE_OCI_IMAGE`, or a `hive-nectar` store in `HIVE_NECTAR_STORE` and the id
//! of an image in it in `HIVE_NECTAR_IMAGE`. Passes without doing anything when one is missing.
//! The snapshot test also needs the store and `mkfs.erofs` in `HIVE_MKFS_EROFS`.

#![cfg(target_os = "linux")]

mod common;

use bytes::Bytes;
use common::*;
use hive_comb::Llm;
use hive_comb::api::Api;
use hive_nectar::upper::Scrub;
use hive_proto::v1;
use hive_proto::v1::llm_server::Llm as _;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};

/// A comb with only the container backend, on pools and a data directory of its own.
struct Node {
    cfg: Config,
    drone: PathBuf,
    // Dropped in this order: the comb before its cgroups, its pins and its directory.
    comb: Comb,
    guard: Option<Guarded>,
    tree: Tree,
    _scratch: Scratch,
    // Last, so the next guarded node waits until this one is all gone.
    _one: Option<tokio::sync::MutexGuard<'static, ()>>,
}

/// Guarded nodes share the guard's addresses on the host, so one runs at a time.
static GUARDED: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// What a node with `hive-guard` adds to the host, removed however the test ends.
struct Guarded {
    pins: PathBuf,
    made_vips: bool,
}

impl Guarded {
    const RULE: [&str; 6] = ["INPUT", "-i", "hv+", "-j", "ACCEPT", "-w"];

    fn new() -> Self {
        // A host firewall that drops input, as ufw does, would hide what the guard passes.
        let _ = std::process::Command::new("iptables").arg("-I").args(Self::RULE).output();
        Self {
            pins: format!("/sys/fs/bpf/hive-comb-test-{}", std::process::id()).into(),
            made_vips: !Path::new("/sys/class/net/hive0").exists(),
        }
    }
}

impl Drop for Guarded {
    fn drop(&mut self) {
        let _ = std::process::Command::new("iptables").arg("-D").args(Self::RULE).output();
        if self.made_vips {
            let _ = std::process::Command::new("ip").args(["link", "del", "hive0"]).output();
        }
        let _ = std::fs::remove_dir_all(&self.pins);
    }
}

impl Node {
    async fn new(depth: usize) -> Option<Self> {
        Self::with(depth, false).await
    }

    async fn with(depth: usize, guard: bool) -> Option<Self> {
        Self::build(depth, guard.then(Llm::default)).await
    }

    /// A node with the guard, and its LLM gateway set up as `llm` says.
    async fn build(depth: usize, llm: Option<Llm>) -> Option<Self> {
        let Some(drone) = std::env::var_os("HIVE_OCI_DRONE") else {
            eprintln!("skipped: set HIVE_OCI_DRONE to run it");
            return None;
        };
        let nectar =
            std::env::var_os("HIVE_NECTAR_STORE").zip(std::env::var_os("HIVE_NECTAR_IMAGE"));
        let image = std::env::var_os("HIVE_OCI_IMAGE");
        if nectar.is_none() && image.is_none() {
            eprintln!(
                "skipped: set HIVE_OCI_IMAGE, or HIVE_NECTAR_STORE and HIVE_NECTAR_IMAGE, to run it"
            );
            return None;
        }
        let tree = Tree::new()?;
        let scratch = Scratch::new();
        // Scratch makes an empty python image, and this one is the real thing.
        let python = scratch.0.join("images").join("python");
        std::fs::remove_dir_all(&python).unwrap();
        let mut images = Images::default();
        match (nectar, image) {
            (Some((store, id)), _) => {
                std::fs::write(&python, id.as_encoded_bytes()).unwrap();
                images = Images {
                    store: Some(store.into()),
                    cache_dir: scratch.0.join("cache"),
                    layers_dir: scratch.0.join("layers"),
                    mkfs: std::env::var_os("HIVE_MKFS_EROFS").map_or(images.mkfs, PathBuf::from),
                    ..images
                };
            }
            (None, Some(image)) => std::os::unix::fs::symlink(image, &python).unwrap(),
            (None, None) => unreachable!(),
        }
        let one = match llm {
            Some(_) => Some(GUARDED.lock().await),
            None => None,
        };
        let guard = llm.is_some().then(Guarded::new);
        let network = match (&guard, llm) {
            (Some(g), Some(llm)) => Network {
                guard: true,
                cells: (std::net::Ipv4Addr::new(100, 64, 240, 0), 24),
                pin_dir: g.pins.clone(),
                upstream: vec![upstream().await],
                profiles: [("lookup".into(), vec!["ok.test".into(), "*.example.test".into()])]
                    .into(),
                llm,
            },
            _ => Network { guard: false, ..Network::default() },
        };
        let cfg = Config {
            network,
            cgroup_root: Some(tree.0.clone()),
            cgroup_depth: depth,
            netns_dir: Some(scratch.0.join("netns")),
            netns_depth: depth,
            create_deadline: Duration::from_secs(30),
            stop_grace: Duration::from_secs(2),
            // Admission counts memory the cells may use, and idle ones use under a MiB.
            mem_mib: Some(1 << 20),
            images,
            ..config(&scratch.0)
        };
        let drone = PathBuf::from(drone);
        let comb = open_oci(&cfg, &drone).await;
        Some(Self { cfg, drone, comb, guard, tree, _scratch: scratch, _one: one })
    }

    /// Shuts the comb down, which leaves its cells running, and opens a new one on the same data.
    async fn restart(&mut self) {
        self.comb.shutdown().await;
        self.comb = open_oci(&self.cfg, &self.drone).await;
    }
}

/// A resolver for the DNS proxy to ask. It knows `ok.test`, at an address the test puts on the
/// host, and `a.example.test`, which also has a private address, and nothing else.
async fn upstream() -> std::net::SocketAddr {
    let sock = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = sock.local_addr().unwrap();
    tokio::spawn(async move {
        let mut buf = [0u8; 512];
        while let Ok((n, from)) = sock.recv_from(&mut buf).await {
            let q = &buf[..n];
            let (mut at, mut labels) = (12, Vec::new());
            while at < n && q[at] != 0 {
                let len = usize::from(q[at]);
                labels.push(String::from_utf8_lossy(&q[(at + 1).min(n)..(at + 1 + len).min(n)]));
                at += 1 + len;
            }
            if at + 5 > n {
                continue;
            }
            let ips: &[[u8; 4]] = match labels.join(".").to_ascii_lowercase().as_str() {
                "ok.test" => &[[198, 51, 100, 7]],
                "a.example.test" => &[[10, 1, 2, 3], [198, 51, 100, 7]],
                _ => &[],
            };
            let mut r = q[..2].to_vec();
            r.extend([0x81, 0x80, 0, 1, 0, ips.len() as u8, 0, 0, 0, 0]);
            r.extend(&q[12..at + 5]);
            for ip in ips {
                r.extend([0xc0, 12, 0, 1, 0, 1, 0, 0, 0, 60, 0, 4]);
                r.extend(ip);
            }
            let _ = sock.send_to(&r, from).await;
        }
    });
    addr
}

/// A dummy interface with an address on it, for a cell to reach once it has looked it up.
struct Dummy(String);

impl Dummy {
    fn new(ip: &str) -> Self {
        let name = format!("hbt{}", std::process::id());
        let ip_cmd = |args: &[&str]| std::process::Command::new("ip").args(args).status().unwrap();
        assert!(ip_cmd(&["link", "add", &name, "type", "dummy"]).success());
        assert!(ip_cmd(&["addr", "add", &format!("{ip}/32"), "dev", &name]).success());
        assert!(ip_cmd(&["link", "set", &name, "up"]).success());
        Self(name)
    }
}

impl Drop for Dummy {
    fn drop(&mut self) {
        let _ = std::process::Command::new("ip").args(["link", "del", &self.0]).output();
    }
}

/// What the cell's own resolver makes of each name: its addresses, or `-` when it has none.
async fn lookup(comb: &Comb, id: CellId, names: &[&str]) -> String {
    let script = format!(
        "python3 -c 'import socket\nfor n in {names:?}:\n    try:\n        print(\",\".join(sorted(socket.gethostbyname_ex(n)[2])))\n    except OSError:\n        print(\"-\")'"
    );
    sh(comb, id, &script).await
}

/// The response code the proxy gives the cell for a query of type `kind`.
async fn rcode(comb: &Comb, id: CellId, name: &str, kind: u16) -> u8 {
    let script = format!(
        "python3 -c 'import socket, struct\nq = b\"\\x12\\x34\\x01\\x00\\x00\\x01\" + bytes(6)\nfor l in \"{name}\".split(\".\"):\n    q += bytes([len(l)]) + l.encode()\nq += b\"\\x00\" + struct.pack(\">HH\", {kind}, 1)\ns = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)\ns.settimeout(3)\ns.sendto(q, (\"169.254.77.53\", 53))\nprint(s.recv(512)[3] & 15)'"
    );
    sh(comb, id, &script).await.trim().parse().unwrap()
}

async fn open_oci(cfg: &Config, drone: &Path) -> Comb {
    let oci = hive_cell_oci::Config {
        worker: vec![env!("CARGO_BIN_EXE_hive-comb").into(), "--oci-worker".into()],
        drone: drone.to_path_buf(),
        state_dir: cfg.data_dir.join("oci"),
        ..hive_cell_oci::Config::default()
    };
    let mut drivers = DriverRegistry::new();
    drivers.add(Arc::new(hive_cell_oci::OciDriver::new(oci).unwrap()));
    Comb::open(cfg.clone(), drivers).await.unwrap()
}

async fn sh(comb: &Comb, id: CellId, script: &str) -> String {
    // Right after a restart the comb is still dialling the drone again.
    let until = Instant::now() + Duration::from_secs(5);
    let drone = loop {
        match comb.drone(id).await {
            Ok(d) => break d,
            Err(e) if e.reason == Reason::DroneUnreachable && Instant::now() < until => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(e) => panic!("no drone for {id}: {e}"),
        }
    };
    let req = RunRequest {
        command: Some(Command { shell: script.into(), ..Command::default() }),
        ..RunRequest::default()
    };
    let r = drone.run(&req).await.unwrap();
    assert_eq!(r.exit_code, 0, "{script}: {}", String::from_utf8_lossy(&r.stderr));
    String::from_utf8(r.stdout.to_vec()).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_container_cell_goes_through_the_comb_and_outlives_it() {
    let Some(mut node) = Node::new(4).await else { return };
    let id = node.comb.create(request(spec("python"))).await.unwrap().id;
    assert_eq!(node.comb.get(id).unwrap().status.state, CellState::Running);
    assert_eq!(sh(&node.comb, id, "python3 -c 'print(6 * 7)'").await, "42\n");
    // Its own cgroup namespace, rooted at the cell's cgroup, and a network with only loopback.
    assert_eq!(sh(&node.comb, id, "cat /proc/self/cgroup").await, "0::/\n");
    let links = sh(&node.comb, id, "tail -n +3 /proc/net/dev | cut -d: -f1 | tr -d ' '").await;
    assert_eq!(links, "lo\n");

    assert_eq!(node.comb.pause(id).await.unwrap().status.state, CellState::Paused);
    assert_eq!(node.comb.resume(id).await.unwrap().status.state, CellState::Running);
    sh(&node.comb, id, "echo kept > /root/note").await;

    node.restart().await;
    assert_eq!(node.comb.get(id).unwrap().status.state, CellState::Running);
    assert_eq!(sh(&node.comb, id, "cat /root/note").await, "kept\n");

    let info = node.comb.stop(id, None).await.unwrap();
    assert_eq!(info.status.state, CellState::Stopped);
    assert!(!node.cfg.data_dir.join("oci").join(id.to_string()).exists());
    node.comb.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_paused_cell_is_reclaimed_and_then_stopped() {
    let Some(mut node) = Node::new(4).await else { return };
    node.cfg.reclaim_after = Duration::from_millis(500);
    node.cfg.pause_ttl = Duration::from_secs(4);
    node.restart().await;
    let mem = || {
        let text = std::fs::read_to_string(node.tree.0.join("memory.current")).unwrap();
        text.trim().parse::<u64>().unwrap() >> 20
    };
    let id = node.comb.create(request(spec("python"))).await.unwrap().id;
    // A cell that has done some work: a page cache full of the image and the files it wrote, and
    // a process holding memory of its own.
    sh(&node.comb, id, "python3 -c 'import asyncio, json, sqlite3, ssl, decimal, unittest'").await;
    sh(&node.comb, id, "head -c 64M /dev/urandom > /tmp/blob && cat /tmp/blob > /dev/null").await;
    let hold =
        "python3 -c 'import time; x = bytearray(96 << 20); time.sleep(3600)' >/dev/null 2>&1 &";
    sh(&node.comb, id, hold).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let before = mem();

    // Frozen and thawed, with an exec right after as a user would send.
    let t = Instant::now();
    assert_eq!(node.comb.pause(id).await.unwrap().status.state, CellState::Paused);
    let froze = t.elapsed();
    let t = Instant::now();
    sh(&node.comb, id, "true").await;
    let thawed = t.elapsed();

    // Paused long enough to be reclaimed, then woken by an exec alone.
    node.comb.pause(id).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let reclaimed = mem();
    let t = Instant::now();
    sh(&node.comb, id, "python3 -c 'import json'").await;
    let woke = t.elapsed();
    let after = mem();
    println!(
        "{before} MiB before, {reclaimed} MiB once reclaimed, {after} MiB after a python start; \
         freeze {froze:.2?}, exec after a freeze {thawed:.2?}, python start after a reclaim {woke:.2?}"
    );
    assert!(reclaimed < before, "{reclaimed} MiB reclaimed from {before} MiB");

    // The longer TTL takes, and then a paused cell runs out of its pause.
    let info = node.comb.extend_ttl(id, Some(Duration::from_secs(3600)), None).await.unwrap();
    assert!(info.spec.hard_ttl.unwrap() >= Duration::from_secs(3600));
    node.comb.pause(id).await.unwrap();
    let t = Instant::now();
    let ended = loop {
        let s = node.comb.get(id).unwrap().status;
        if s.state.is_terminal() {
            break s;
        }
        assert!(t.elapsed() < Duration::from_secs(30), "still {} after 30s", s.state);
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!((ended.state, ended.cause), (CellState::Stopped, Some(Cause::Idle)));
    assert!(t.elapsed() >= Duration::from_secs(3), "stopped after {:.2?}", t.elapsed());
    node.comb.shutdown().await;
}

/// A cell made from snapshot `snap` of another.
async fn restore(comb: &Comb, snap: hive_nectar::BlobId) -> CellId {
    let mut s = spec("python");
    s.source = Source::Snapshot(snap.to_string());
    comb.create(request(s)).await.unwrap().id
}

#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_restores_and_commits_what_the_cell_wrote() {
    if std::env::var_os("HIVE_NECTAR_STORE").is_none()
        || std::env::var_os("HIVE_MKFS_EROFS").is_none()
    {
        eprintln!("skipped: set HIVE_NECTAR_STORE and HIVE_MKFS_EROFS to run it");
        return;
    }
    let Some(mut node) = Node::new(4).await else { return };
    let store = hive_nectar::PosixStore::open(node.cfg.images.store.clone().unwrap()).unwrap();
    let id = node.comb.create(request(spec("python"))).await.unwrap().id;
    let pem = r"-----BEGIN RSA PRIVATE KEY-----\nMIIB\n-----END RSA PRIVATE KEY-----\n";
    let work = format!(
        "mkdir -p /srv/app/tests && echo one > /root/note && echo 'print(1)' > /srv/app/main.py \
         && printf '%b' '{pem}' > /srv/app/tests/key.pem && echo 'export TOKEN=x' > /root/.bash_history \
         && rm /etc/issue && head -c 32M /dev/urandom > /srv/app/blob"
    );
    sh(&node.comb, id, &work).await;
    sh(&node.comb, id, "sleep 3600 >/dev/null 2>&1 &").await;

    // A running cell is frozen only while its changes are read out, and goes on as it was. Execs
    // sent back to back meanwhile show how long it stood still.
    let ended = std::sync::atomic::AtomicBool::new(false);
    let t = Instant::now();
    let (first, (worst, execs)) = tokio::join!(
        async {
            let snap = node.comb.snapshot(id, None).await.unwrap();
            ended.store(true, Ordering::Relaxed);
            snap
        },
        async {
            let (mut worst, mut n) = (Duration::ZERO, 0);
            while !ended.load(Ordering::Relaxed) {
                let t = Instant::now();
                sh(&node.comb, id, "true").await;
                (worst, n) = (worst.max(t.elapsed()), n + 1);
            }
            (worst, n)
        }
    );
    let took = t.elapsed();
    assert_eq!(sh(&node.comb, id, "cat /proc/[0-9]*/comm | grep -c ^sleep$").await, "1\n");
    assert_eq!(node.comb.get(id).unwrap().status.state, CellState::Running);
    let m = hive_nectar::oci::load_manifest(&store, first).await.unwrap();
    let top = m.layers.last().unwrap();
    let text = node.comb.metrics().registry().render();
    let stage = |name: &str| {
        let line = format!("hive_snapshot_seconds_sum{{stage=\"{name}\"}} ");
        let v = text.lines().find_map(|l| l.strip_prefix(line.as_str())).unwrap();
        Duration::from_secs_f64(v.parse().unwrap())
    };
    println!(
        "snapshot of a 32 MiB write in {took:.2?}: frozen {:.2?} while it was read out, then \
         {:.2?} to build; {execs} execs meanwhile, the slowest in {worst:.2?}; new layer {} KiB \
         of metadata and {} KiB of data",
        stage("read"),
        stage("build"),
        top.meta_size >> 10,
        top.data_size >> 10
    );
    assert!(m.provenance.as_ref().is_some_and(|p| p.scrubbed.is_none()));

    let copy = restore(&node.comb, first).await;
    let seen = "cat /root/note /srv/app/main.py; test -e /etc/issue || echo gone; \
                head -c 14 /srv/app/tests/key.pem; echo; python3 -c 'print(6 * 7)'";
    let want = "one\nprint(1)\ngone\n-----BEGIN RSA\n42\n";
    assert_eq!(sh(&node.comb, copy, seen).await, want);

    // A restored cell snapshots on top of its snapshot, and the record keeps that over a restart.
    sh(&node.comb, copy, "echo three > /root/third").await;
    node.restart().await;
    let chained = node.comb.snapshot(copy, None).await.unwrap();
    let again = restore(&node.comb, chained).await;
    assert_eq!(sh(&node.comb, again, "cat /root/note /root/third").await, "one\nthree\n");
    assert_eq!(
        hive_nectar::oci::load_manifest(&store, chained).await.unwrap().layers.len(),
        m.layers.len() + 1
    );

    // A paused cell stays paused.
    node.comb.pause(id).await.unwrap();
    node.comb.snapshot(id, None).await.unwrap();
    assert_eq!(node.comb.get(id).unwrap().status.state, CellState::Paused);

    // Scrubbing refuses the key until its directory is allowed, and only that commits.
    let e = node.comb.snapshot(id, Some(Scrub::default())).await.unwrap_err();
    assert_eq!(e.reason, Reason::PolicyDenied, "{e}");
    assert!(e.message.contains("srv/app/tests/key.pem:1"), "{e}");
    assert!(!e.message.contains("MIIB"), "{e}");
    let e = node.comb.commit("p", first, "mine").await.unwrap_err();
    assert_eq!(e.reason, Reason::PolicyDenied, "{e}");
    let allow = Scrub { allow: vec!["srv/app/tests".into()] };
    let clean = node.comb.snapshot(id, Some(allow)).await.unwrap();
    assert_eq!(node.comb.get(id).unwrap().status.state, CellState::Paused);
    let e = node.comb.commit("p", clean, "../mine").await.unwrap_err();
    assert_eq!(e.reason, Reason::InvalidArgument, "{e}");
    node.comb.commit("p", clean, "mine").await.unwrap();
    let named = node.comb.create(request(spec("mine"))).await.unwrap().id;
    let seen = "cat /root/note; test -e /root/.bash_history || echo gone; head -c 14 /srv/app/tests/key.pem";
    assert_eq!(sh(&node.comb, named, seen).await, "one\ngone\n-----BEGIN RSA");

    for c in [id, copy, again, named] {
        node.comb.stop(c, None).await.unwrap();
    }
    let e = node.comb.snapshot(id, None).await.unwrap_err();
    assert_eq!(e.reason, Reason::CellNotRunning, "{e}");
    node.comb.shutdown().await;
}

/// Whether a TCP connect from the cell got an answer from `ip`, refused or not, within a second.
async fn reaches(comb: &Comb, id: CellId, ip: &str, port: u16) -> bool {
    let script = format!(
        "python3 -c 'import socket\ns = socket.socket()\ns.settimeout(1)\ntry:\n    s.connect((\"{ip}\", {port}))\nexcept ConnectionRefusedError:\n    pass\nexcept OSError:\n    print(\"no\")'"
    );
    sh(comb, id, &script).await.is_empty()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_guarded_cell_reaches_the_node_resolver_and_nothing_else() {
    if !Path::new("/sys/fs/bpf").exists() {
        eprintln!("skipped: no bpf filesystem");
        return;
    }
    let Some(mut node) = Node::with(4, true).await else { return };
    let comb = node.comb.clone();
    let id = comb.create(request(spec("python"))).await.unwrap().id;
    let links = sh(&comb, id, "tail -n +3 /proc/net/dev | cut -d: -f1 | tr -d ' ' | sort").await;
    assert_eq!(links, "eth0\nlo\n", "the guard did not load, see the comb's output");
    let addr = sh(&comb, id, "python3 -c 'import socket; s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); s.connect((\"169.254.77.53\", 53)); print(s.getsockname()[0])'").await;
    assert!(addr.starts_with("100.64.240."), "{addr}");
    assert_eq!(
        sh(&comb, id, "cat /etc/resolv.conf").await.lines().next(),
        Some("nameserver 169.254.77.53")
    );
    assert!(reaches(&comb, id, "169.254.77.53", 53).await, "every profile has the resolver");
    assert!(!reaches(&comb, id, "169.254.77.80", 80).await, "none has no mirrors");
    assert!(!reaches(&comb, id, "1.1.1.1", 443).await);
    assert!(!reaches(&comb, id, "169.254.77.1", 22).await, "nor the node itself");
    // Writing its own resolver changes nothing, since the file is read only.
    assert!(
        sh(&comb, id, "echo nameserver 1.1.1.1 > /etc/resolv.conf 2>/dev/null || echo ro").await
            == "ro\n"
    );

    // With none, every name is unknown, and nothing is asked upstream.
    let _host = Dummy::new("198.51.100.7");
    assert_eq!(lookup(&comb, id, &["ok.test"]).await, "-\n");
    assert!(!reaches(&comb, id, "198.51.100.7", 9).await);

    // After a restart the comb still knows the cell's interface, and frees it on stop.
    node.restart().await;
    assert!(reaches(&node.comb, id, "169.254.77.53", 53).await);
    let mut lookup_spec = spec("python");
    lookup_spec.network_profile = "lookup".into();
    let finder = node.comb.create(request(lookup_spec)).await.unwrap().id;
    assert!(!reaches(&node.comb, finder, "198.51.100.7", 9).await, "not before it is looked up");
    let t = Instant::now();
    let found = lookup(
        &node.comb,
        finder,
        &["ok.test", "a.example.test", "example.test", "b.ok.test", "other.test"],
    )
    .await;
    println!("five lookups from a cell, with python starting, took {:.2?}", t.elapsed());
    assert_eq!(found, "198.51.100.7\n198.51.100.7\n-\n-\n-\n", "the private address is gone");
    assert!(reaches(&node.comb, finder, "198.51.100.7", 9).await, "reachable once looked up");
    assert!(!reaches(&node.comb, id, "198.51.100.7", 9).await, "but only by the cell that did");
    assert!(!reaches(&node.comb, finder, "10.1.2.3", 9).await);
    assert_eq!(rcode(&node.comb, finder, "ok.test", 16).await, 5, "TXT is refused");
    assert_eq!(rcode(&node.comb, finder, "ok.test", 28).await, 0, "AAAA is empty");
    assert_eq!(rcode(&node.comb, finder, "nope.test", 1).await, 3);
    node.comb.stop(finder, None).await.unwrap();
    let mirrors = {
        let mut s = spec("python");
        s.network_profile = "mirrors".into();
        s
    };
    let other = node.comb.create(request(mirrors)).await.unwrap().id;
    assert!(reaches(&node.comb, other, "169.254.77.80", 80).await, "mirrors has the mirror proxy");
    let mut open = spec("python");
    open.network_profile = "open".into();
    let e = node.comb.create(request(open)).await.unwrap_err();
    assert_eq!(e.reason, Reason::CapacityUnavailable);
    for id in [id, other] {
        node.comb.stop(id, None).await.unwrap();
    }
    // Only the spares keep a program. The pool refills once it is under half full, so it may
    // stay at 3.
    let dir = node.guard.as_ref().unwrap().pins.join("links");
    let until = Instant::now() + Duration::from_secs(20);
    while Some(std::fs::read_dir(&dir).unwrap().count()) != node.comb.spare_netns() {
        if Instant::now() > until {
            let names = |d: &Path| {
                let mut v: Vec<String> = std::fs::read_dir(d)
                    .unwrap()
                    .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
                    .collect();
                v.sort();
                v
            };
            let hv: Vec<String> = names(Path::new("/sys/class/net"))
                .into_iter()
                .filter(|n| n.starts_with("hv"))
                .map(|n| {
                    let i = std::fs::read_to_string(format!("/sys/class/net/{n}/ifindex")).unwrap();
                    format!("{n}={}", i.trim())
                })
                .collect();
            panic!(
                "links {:?}, spares {:?}, interfaces {hv:?}",
                names(&dir),
                node.comb.spare_netns()
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    node.comb.shutdown().await;
}

/// What the engine was sent: the Authorization header and the body of each call.
type Seen = Arc<Mutex<Vec<(String, Value)>>>;

/// An inference engine that answers chat completions the way vLLM does, streamed or not. The
/// prompt's tokens are the bytes of the last message, the answer is always "hi", the tokens 104 and
/// 105, and a model named `slow` takes a second and a half to give it.
async fn engine() -> (std::net::SocketAddr, Seen) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Seen::default();
    let log = seen.clone();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let log = log.clone();
            tokio::spawn(async move {
                let svc = hyper::service::service_fn(move |req: hyper::Request<Incoming>| {
                    let log = log.clone();
                    async move { Ok::<_, std::convert::Infallible>(answer(req, &log).await) }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), svc)
                    .await;
            });
        }
    });
    (addr, seen)
}

async fn answer(req: hyper::Request<Incoming>, log: &Seen) -> hyper::Response<Full<Bytes>> {
    if req.uri().path() != "/v1/chat/completions" {
        let r = hyper::Response::builder().status(404);
        return r.body(Full::new(req.uri().path().to_string().into())).unwrap();
    }
    let auth = req.headers().get("authorization").map_or("", |v| v.to_str().unwrap()).to_string();
    let v: Value =
        serde_json::from_slice(&req.into_body().collect().await.unwrap().to_bytes()).unwrap();
    log.lock().unwrap().push((auth, v.clone()));
    if v["model"] == "slow" {
        tokio::time::sleep(Duration::from_millis(1500)).await;
    }
    let last = v["messages"].as_array().unwrap().last().unwrap();
    let prompt: Vec<u32> = last["content"].as_str().unwrap().bytes().map(u32::from).collect();
    let (ids, lp) = (v["return_token_ids"] == true, v["logprobs"] == true);
    let usage = json!({"prompt_tokens": prompt.len(), "completion_tokens": 2});
    let (kind, body) = if v["stream"] == true {
        let mut first = json!({"model": "m", "choices": [{"index": 0, "delta": {"role": "assistant", "content": ""}}]});
        if ids {
            first["prompt_token_ids"] = json!(prompt);
        }
        let mut events = vec![first];
        for (t, text) in [(104, "h"), (105, "i")] {
            let mut c = json!({"index": 0, "delta": {"content": text}});
            if ids {
                c["token_ids"] = json!([t]);
            }
            if lp {
                c["logprobs"] = json!({"content": [{"token": text, "logprob": -0.5}]});
            }
            events.push(json!({"model": "m", "choices": [c]}));
        }
        events.push(
            json!({"model": "m", "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]}),
        );
        events.push(json!({"model": "m", "choices": [], "usage": usage}));
        let mut s: String = events.iter().map(|e| format!("data: {e}\n\n")).collect();
        s += "data: [DONE]\n\n";
        ("text/event-stream", s)
    } else {
        let mut c = json!({"index": 0, "message": {"role": "assistant", "content": "hi"}, "finish_reason": "stop", "logprobs": null});
        if ids {
            c["token_ids"] = json!([104, 105]);
        }
        if lp {
            c["logprobs"] = json!({"content": [{"token": "h", "logprob": -0.5}, {"token": "i", "logprob": -0.5}]});
        }
        let mut r = json!({"model": "m", "choices": [c], "usage": usage});
        if ids {
            r["prompt_token_ids"] = json!(prompt);
        }
        ("application/json", r.to_string())
    };
    hyper::Response::builder().header("content-type", kind).body(Full::new(body.into())).unwrap()
}

/// A chat completion from the cell to `llm.hive.internal` with its own key: the status, the
/// Retry-After header or `-`, and the body.
async fn chat(comb: &Comb, id: CellId, body: &Value) -> (u16, String, String) {
    let script = format!(
        "python3 - '{body}' <<'P'
import sys, urllib.request, urllib.error
req = urllib.request.Request('http://llm.hive.internal/v1/chat/completions', data=sys.argv[1].encode(), headers={{'Content-Type': 'application/json', 'Authorization': 'Bearer cell-secret'}})
try:
    r = urllib.request.urlopen(req, timeout=30)
except urllib.error.HTTPError as e:
    r = e
print(r.status, r.headers.get('Retry-After') or '-')
sys.stdout.write(r.read().decode())
P"
    );
    let out = sh(comb, id, &script).await;
    let (head, body) = out.split_once('\n').unwrap();
    let (code, retry) = head.split_once(' ').unwrap();
    (code.parse().unwrap(), retry.to_string(), body.to_string())
}

fn msg(model: &str, text: &str, extra: &Value) -> Value {
    let mut v = json!({"model": model, "messages": [{"role": "user", "content": text}]});
    for (k, x) in extra.as_object().unwrap() {
        v[k] = x.clone();
    }
    v
}

fn call<T>(m: T) -> tonic::Request<T> {
    let mut r = tonic::Request::new(m);
    r.metadata_mut().insert("x-hive-project", "p".parse().unwrap());
    r
}

fn hold(retry_s: u64, drain_s: u64) -> tonic::Request<v1::LlmHoldRequest> {
    call(v1::LlmHoldRequest {
        retry_after: Some(Duration::from_secs(retry_s).try_into().unwrap()),
        ttl: Some(Duration::from_secs(60).try_into().unwrap()),
        drain: Some(Duration::from_secs(drain_s).try_into().unwrap()),
        ..v1::LlmHoldRequest::default()
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cell_with_the_llm_profile_calls_the_engine_through_the_gateway() {
    if !Path::new("/sys/fs/bpf").exists() {
        eprintln!("skipped: no bpf filesystem");
        return;
    }
    let llm = Llm { logprobs: true, ..Llm::default() };
    let Some(node) = Node::build(4, Some(llm)).await else { return };
    let comb = node.comb.clone();
    let api = Api::new(comb.clone(), tokio_util::sync::CancellationToken::new());
    let (engine, seen) = engine().await;
    let mut s = spec("python");
    s.network_profile = "llm".into();
    s.labels.insert("rollout_id".into(), "r7".into());
    let id = comb.create(request(s)).await.unwrap().id;
    let plain = comb.create(request(spec("python"))).await.unwrap().id;
    assert_eq!(lookup(&comb, id, &["llm.hive.internal"]).await, "169.254.77.81\n");
    assert_eq!(lookup(&comb, plain, &["llm.hive.internal"]).await, "-\n");
    assert!(!reaches(&comb, plain, "169.254.77.81", 80).await, "none has no gateway");

    // With no route yet the gateway asks the agent to come back.
    let (code, retry, _) = chat(&comb, id, &msg("m", "hello", &json!({}))).await;
    assert_eq!((code, retry.as_str()), (503, "5"));
    let route = v1::LlmRoute { upstream: format!("http://{engine}"), api_key: "sk-engine".into() };
    api.set_route(call(route)).await.unwrap();

    // The cell gets the answer it asked for, and the engine got the gateway's key and was asked
    // for the token ids and log probabilities.
    let (code, _, body) = chat(&comb, id, &msg("m", "hello", &json!({}))).await;
    assert_eq!(code, 200, "{body}");
    let v: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["choices"][0]["message"]["content"], "hi");
    assert_eq!(v["choices"][0]["logprobs"], Value::Null);
    assert!(!body.contains("token_ids"), "{body}");
    let (auth, sent) = seen.lock().unwrap().last().cloned().unwrap();
    assert_eq!(auth, "Bearer sk-engine");
    assert_eq!(
        (sent["return_token_ids"].clone(), sent["logprobs"].clone()),
        (json!(true), json!(true))
    );

    let (code, _, body) = chat(&comb, id, &msg("m", "again", &json!({"stream": true}))).await;
    assert_eq!(code, 200, "{body}");
    assert!(!body.contains("token_ids") && body.ends_with("data: [DONE]\n\n"), "{body}");
    let text: String = body
        .lines()
        .filter_map(|l| l.strip_prefix("data: "))
        .filter_map(|d| serde_json::from_str::<Value>(d).ok())
        .filter_map(|e| e["choices"][0]["delta"]["content"].as_str().map(String::from))
        .collect();
    assert_eq!(text, "hi");
    // What the cell asked for itself stays in its answer.
    let (_, _, body) = chat(&comb, id, &msg("m", "x", &json!({"return_token_ids": true}))).await;
    assert!(body.contains(r#""token_ids":[104,105]"#), "{body}");

    let turns = |take| v1::LlmTurnsRequest { rollout_id: "r7".into(), take, ..Default::default() };
    let got = api.turns(call(turns(true))).await.unwrap().into_inner();
    assert_eq!(got.turns.len(), 3);
    for (i, (t, prompt)) in got.turns.iter().zip(["hello", "again", "x"]).enumerate() {
        assert_eq!((t.seq, t.stream, t.status), (i as u64, i == 1, 200));
        assert_eq!((t.cell_id.as_str(), t.rollout_id.as_str()), (id.to_string().as_str(), "r7"));
        assert_eq!(t.prompt_ids, prompt.bytes().map(u32::from).collect::<Vec<_>>());
        assert_eq!(t.choices[0].output_ids, [104, 105]);
        assert_eq!(t.choices[0].logprobs, [-0.5, -0.5]);
        assert_eq!((t.prompt_tokens, t.completion_tokens), (prompt.len() as u64, 2));
        assert_eq!(t.path, "/v1/chat/completions");
    }
    assert!(api.turns(call(turns(false))).await.unwrap().into_inner().turns.is_empty());

    // A hold that does not wait reports the call in flight, one that does waits for it, and the
    // call ends well. New calls get 503 until the hold ends.
    let slow = tokio::spawn({
        let comb = comb.clone();
        async move { chat(&comb, id, &msg("slow", "slow", &json!({}))).await }
    });
    while !seen.lock().unwrap().iter().any(|(_, v)| v["model"] == "slow") {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(api.hold(hold(2, 0)).await.unwrap().into_inner().in_flight, 1);
    let t = Instant::now();
    assert_eq!(api.hold(hold(2, 10)).await.unwrap().into_inner().in_flight, 0);
    println!("the hold waited {:.2?} for the call in flight", t.elapsed());
    assert_eq!(slow.await.unwrap().0, 200);
    let (code, retry, body) = chat(&comb, id, &msg("m", "held", &json!({}))).await;
    assert_eq!((code, retry.as_str()), (503, "2"), "{body}");
    let release = v1::LlmHoldRequest { release: true, ..Default::default() };
    api.hold(call(release)).await.unwrap();
    assert_eq!(chat(&comb, id, &msg("m", "after", &json!({}))).await.0, 200);
    let got = api.turns(call(turns(true))).await.unwrap().into_inner();
    let seqs: Vec<_> = got.turns.iter().map(|t| (t.seq, t.prompt_ids.len())).collect();
    assert_eq!(seqs, [(3, 4), (4, 5)], "the numbering goes on and held calls are not kept");

    // Another project's trainer sees none of it.
    let mut other = tonic::Request::new(turns(false));
    other.metadata_mut().insert("x-hive-project", "q".parse().unwrap());
    let route = v1::LlmRoute::default();
    api.set_route(call(route)).await.unwrap();
    assert!(api.turns(other).await.unwrap().into_inner().turns.is_empty());
    for id in [id, plain] {
        comb.stop(id, None).await.unwrap();
    }
    comb.shutdown().await;
}

/// What a container cell costs through the comb. Run it with
/// `cargo test --release -p hive-comb --test oci -- --ignored --nocapture`, with `CELLS` to change
/// how many are made at once and `GUARD` set to wire every cell's interface with the guard on.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, not a test"]
async fn oci_through_the_comb() {
    let cells: usize = std::env::var("CELLS").ok().and_then(|v| v.parse().ok()).unwrap_or(64);
    let guard = std::env::var_os("GUARD").is_some();
    let t = Instant::now();
    let Some(node) = Node::with(cells, guard).await else { return };
    let comb = node.comb.clone();
    // Starts from full pools, as a node that has been up a while would.
    while comb.spare_netns().is_some_and(|d| d < cells)
        || comb.spare_cgroups().is_some_and(|d| d[1] < cells)
    {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    println!("guard {guard}: pools of {cells} full in {:?}", t.elapsed());

    let mut one = Vec::new();
    for _ in 0..20 {
        let t = Instant::now();
        let id = comb.create(request(spec("python"))).await.unwrap().id;
        one.push(t.elapsed());
        comb.stop(id, None).await.unwrap();
    }
    one.sort();
    println!("one at a time: create p50 {:?} max {:?}", one[one.len() / 2], one[one.len() - 1]);

    let t = Instant::now();
    let made = futures::future::join_all((0..cells).map(|_| {
        let comb = comb.clone();
        tokio::spawn(async move {
            let t = Instant::now();
            let id = comb.create(request(spec("python"))).await.unwrap().id;
            (id, t.elapsed())
        })
    }))
    .await;
    let wall = t.elapsed();
    let (ids, mut times): (Vec<CellId>, Vec<Duration>) =
        made.into_iter().map(Result::unwrap).unzip();
    times.sort();
    println!(
        "{cells} at once: all running in {wall:?}, {:.0} cells/s, create p50 {:?} p99 {:?}",
        cells as f64 / wall.as_secs_f64(),
        times[cells / 2],
        times[cells * 99 / 100]
    );
    let t = Instant::now();
    futures::future::join_all(ids.into_iter().map(|id| {
        let comb = comb.clone();
        tokio::spawn(async move { comb.stop(id, None).await.unwrap() })
    }))
    .await;
    println!("{cells} stopped at once in {:?}", t.elapsed());
    comb.shutdown().await;
}
