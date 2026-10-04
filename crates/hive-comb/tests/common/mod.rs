//! The fake driver and the helpers the comb tests share. The fake's cells are a real drone behind
//! a Unix socket in the cell's directory, so everything but the sandbox is real.

// Each test file uses its own part of this.
#![allow(dead_code, unreachable_pub, unused_imports)]

pub use futures::future::BoxFuture;
pub use hive_cell::{
    CellDriver, CellHandle, DriverCaps, DriverRegistry, ExitInfo, GuestChannel, Liveness, NodeFit,
    PauseMode, RootfsPlan, Slot,
};
pub use hive_comb::{CellInfo, Comb, Config, CreateRequest, Images, Network};
pub use hive_drone::Drone;
pub use hive_proto::drone::api::{Command, RunRequest};
pub use hive_types::{
    Backend, Cause, CellId, CellSpec, CellState, Error, IdleAction, Qos, Reason, Source,
};
pub use std::collections::HashMap;
pub use std::path::{Path, PathBuf};
pub use std::sync::atomic::{AtomicU64, Ordering};
pub use std::sync::{Arc, Mutex};
pub use std::time::{Duration, Instant};
pub use tokio::net::UnixListener;
pub use tokio::task::AbortHandle;

/// A cell of the fake driver: a drone and the tasks that serve it.
pub struct Guest {
    pub drone: Option<Arc<Drone>>,
    pub listener: Option<AbortHandle>,
    // A second handle on each accepted socket, to hang up on the comb whatever the drone's tasks
    // are doing.
    pub links: Arc<Mutex<Vec<std::os::unix::net::UnixStream>>>,
    pub paused: bool,
    pub reclaimed: bool,
    pub exit: Option<ExitInfo>,
}

impl Guest {
    pub fn cut_links(&mut self) {
        for l in self.links.lock().unwrap().drain(..) {
            let _ = l.shutdown(std::net::Shutdown::Both);
        }
    }

    pub fn end(&mut self, exit: ExitInfo) {
        if let Some(l) = self.listener.take() {
            l.abort();
        }
        self.cut_links();
        self.exit = Some(exit);
    }
}

#[derive(Default)]
pub struct Fake {
    pub guests: Mutex<HashMap<CellId, Guest>>,
    pub stops: AtomicU64,
    // A real process per cell, put in the cell's cgroup when it has one.
    pub workers: Mutex<HashMap<CellId, std::process::Child>>,
    // How long each start takes, in milliseconds, before the cell is up.
    pub start_ms: AtomicU64,
}

impl Fake {
    pub fn with<T>(&self, id: CellId, f: impl FnOnce(&mut Guest) -> T) -> T {
        f(self.guests.lock().unwrap().get_mut(&id).expect("a guest"))
    }

    /// The workload exits on its own, as if the cell's init died.
    pub fn kill(&self, id: CellId) {
        self.with(id, |g| g.end(ExitInfo { code: Some(1), ..ExitInfo::default() }));
    }

    /// The channel drops but the cell lives on.
    pub fn cut(&self, id: CellId) {
        self.with(id, Guest::cut_links);
    }

    pub fn live(&self) -> usize {
        self.guests.lock().unwrap().values().filter(|g| g.exit.is_none()).count()
    }
}

pub fn socket(h: &CellHandle) -> PathBuf {
    match &h.channel {
        GuestChannel::Unix(p) => p.clone(),
        GuestChannel::Vsock { .. } => unreachable!("the fake driver only makes Unix channels"),
    }
}

impl CellDriver for Fake {
    fn backend(&self) -> Backend {
        Backend::Container
    }

    fn caps(&self) -> DriverCaps {
        DriverCaps { pause: true, ..DriverCaps::default() }
    }

    fn probe(&self) -> BoxFuture<'_, Result<NodeFit, Error>> {
        Box::pin(async { Ok(NodeFit { ready: true, notes: Vec::new() }) })
    }

    fn prepare<'a>(
        &'a self,
        id: CellId,
        _spec: &'a CellSpec,
        rootfs: &'a RootfsPlan,
        slot: &'a Slot,
    ) -> BoxFuture<'a, Result<CellHandle, Error>> {
        Box::pin(async move {
            let image = rootfs.lowers[0].file_name().unwrap().to_str().unwrap();
            // The "crash" image never gets its guest agent up.
            let drone =
                (image != "crash").then(|| Drone::new(hive_drone::Config::default(), slot.secret));
            let guest = Guest {
                drone,
                listener: None,
                links: Arc::default(),
                paused: false,
                reclaimed: false,
                exit: None,
            };
            self.guests.lock().unwrap().insert(id, guest);
            Ok(CellHandle {
                id,
                backend: Backend::Container,
                pid: None,
                channel: GuestChannel::Unix(slot.dir.join("drone.sock")),
                cgroup: slot.cgroup.clone(),
                netns: slot.netns.clone(),
                extra: Default::default(),
            })
        })
    }

    fn start<'a>(&'a self, h: &'a mut CellHandle) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            let slow = self.start_ms.load(Ordering::Relaxed);
            if slow > 0 {
                tokio::time::sleep(Duration::from_millis(slow)).await;
            }
            let (drone, links) = self.with(h.id, |g| (g.drone.clone(), g.links.clone()));
            let Some(drone) = drone else {
                self.with(h.id, |g| g.end(ExitInfo { code: Some(2), ..ExitInfo::default() }));
                return Ok(());
            };
            let listener = UnixListener::bind(socket(h)).map_err(|e| {
                Error::new(Reason::Internal, format!("binding the guest socket: {e}"))
            })?;
            let task = tokio::spawn(async move {
                while let Ok((s, _)) = listener.accept().await {
                    let s = s.into_std().unwrap();
                    links.lock().unwrap().push(s.try_clone().unwrap());
                    let s = tokio::net::UnixStream::from_std(s).unwrap();
                    tokio::spawn(drone.clone().serve(s));
                }
            });
            self.with(h.id, |g| g.listener = Some(task.abort_handle()));
            if !h.cgroup.as_os_str().is_empty() || h.netns.is_some() {
                let mut cmd = match &h.netns {
                    Some(ns) => {
                        let mut c = std::process::Command::new("nsenter");
                        c.arg(format!("--net={}", ns.display())).args(["--", "sleep"]);
                        c
                    }
                    None => std::process::Command::new("sleep"),
                };
                let child = cmd.arg("600").spawn().unwrap();
                let pid = child.id();
                // Kept before it is moved, so the test reaps it whatever happens here.
                self.workers.lock().unwrap().insert(h.id, child);
                if !h.cgroup.as_os_str().is_empty() {
                    hive_cell::cgroup::write(&h.cgroup, "cgroup.procs", &pid.to_string())
                        .map_err(|e| Error::new(Reason::Internal, e.to_string()))?;
                }
                if let Some(ns) = &h.netns {
                    // A real runtime has the cell in its namespace before start returns. nsenter
                    // gets there a little later, and a cell stopped straight away would lose its
                    // namespace first.
                    let want = inode(ns);
                    let until = Instant::now() + Duration::from_secs(10);
                    while std::fs::read_link(format!("/proc/{pid}/ns/net"))
                        .is_ok_and(|l| l.to_str() != Some(&format!("net:[{want}]")))
                    {
                        assert!(Instant::now() < until, "nsenter never joined {}", ns.display());
                        tokio::time::sleep(Duration::from_micros(200)).await;
                    }
                }
            }
            h.pid = Some(std::process::id());
            Ok(())
        })
    }

    fn pause<'a>(&'a self, h: &'a CellHandle, mode: PauseMode) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            self.with(h.id, |g| {
                g.paused = true;
                g.reclaimed |= mode == PauseMode::Reclaim;
            });
            Ok(())
        })
    }

    fn resume<'a>(&'a self, h: &'a CellHandle) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            self.with(h.id, |g| (g.paused, g.reclaimed) = (false, false));
            Ok(())
        })
    }

    fn stop<'a>(
        &'a self,
        h: &'a CellHandle,
        _: Duration,
    ) -> BoxFuture<'a, Result<ExitInfo, Error>> {
        Box::pin(async move {
            self.stops.fetch_add(1, Ordering::Relaxed);
            let exit = self.guests.lock().unwrap().remove(&h.id).map(|mut g| {
                let exit = g.exit.unwrap_or(ExitInfo { signal: Some(9), ..ExitInfo::default() });
                g.end(exit);
                exit
            });
            let _ = std::fs::remove_file(socket(h));
            Ok(exit.unwrap_or_default())
        })
    }

    fn check<'a>(&'a self, h: &'a CellHandle) -> BoxFuture<'a, Result<Liveness, Error>> {
        Box::pin(async move {
            Ok(match self.guests.lock().unwrap().get(&h.id) {
                None => Liveness::Gone(ExitInfo::default()),
                Some(g) => match (g.exit, g.paused) {
                    (Some(exit), _) => Liveness::Gone(exit),
                    (None, true) => Liveness::Paused,
                    (None, false) => Liveness::Alive,
                },
            })
        })
    }
}

/// A data directory that is removed at the end, with images `python` and `crash`.
pub struct Scratch(pub PathBuf);

impl Scratch {
    pub fn new() -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("hive-comb-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        for image in ["python", "crash"] {
            std::fs::create_dir_all(dir.join("images").join(image)).unwrap();
        }
        Self(dir)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        // Namespace files are mount points, which have to be unmounted before they can go.
        if let Ok(entries) = std::fs::read_dir(self.0.join("netns")) {
            for e in entries.flatten() {
                let _ = hive_cell::netns::remove(&e.path());
            }
        }
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub fn config(dir: &Path) -> Config {
    Config {
        data_dir: dir.to_path_buf(),
        mem_mib: Some(4096),
        create_deadline: Duration::from_secs(5),
        stop_grace: Duration::from_millis(100),
        keep_ended: Duration::from_secs(60),
        cgroup_root: None,
        netns_dir: None,
        network: Network { guard: false, ..Network::default() },
        ..Config::default()
    }
}

pub async fn open(cfg: Config, fake: &Arc<Fake>) -> Comb {
    let mut drivers = DriverRegistry::new();
    drivers.add(fake.clone());
    Comb::open(cfg, drivers).await.unwrap()
}

pub fn spec(image: &str) -> CellSpec {
    let mut s = CellSpec::new(Source::Image(image.into()), Backend::Container);
    s.resources.mem_mib = 256;
    s
}

pub fn request(s: CellSpec) -> CreateRequest {
    CreateRequest { spec: s, project: "p".into(), idem_key: None, anyway: false }
}

pub async fn echo(comb: &Comb, id: CellId, text: &str) -> String {
    let drone = comb.drone(id).await.unwrap();
    let req = RunRequest {
        command: Some(Command { shell: format!("echo {text}"), ..Command::default() }),
        ..RunRequest::default()
    };
    let r = drone.run(&req).await.unwrap();
    String::from_utf8(r.stdout.to_vec()).unwrap()
}

/// Waits for the cell to reach `state`, and fails the test after five seconds.
pub async fn reaches(comb: &Comb, id: CellId, state: CellState) -> CellInfo {
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        let info = comb.get(id).unwrap();
        if info.status.state == state {
            return info;
        }
        assert!(Instant::now() < until, "{id} is {} and never got to {state}", info.status.state);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub fn inode(p: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(p).unwrap().ino()
}

/// A cgroup tree of the test's own, or `None` when not running as root on cgroup v2.
pub struct Tree(pub PathBuf);

impl Tree {
    pub fn new() -> Option<Self> {
        let status = std::fs::read_to_string("/proc/self/status").unwrap();
        let uid = status.lines().find_map(|l| l.strip_prefix("Uid:")).unwrap();
        if uid.split_whitespace().nth(1) != Some("0")
            || !Path::new("/sys/fs/cgroup/cgroup.controllers").exists()
        {
            eprintln!("skipped: needs root and cgroup v2");
            return None;
        }
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let name = format!("hive-comb-{}-{n}.slice", std::process::id());
        Some(Self(Path::new("/sys/fs/cgroup").join(name)))
    }
}

impl Drop for Tree {
    fn drop(&mut self) {
        fn clear(dir: &Path) {
            // A refill batch still running on a blocking thread can add leaves after they were
            // listed, so they are listed again on every try.
            for _ in 0..1000 {
                if let Ok(entries) = std::fs::read_dir(dir) {
                    for e in entries.flatten() {
                        if e.file_type().is_ok_and(|t| t.is_dir()) {
                            clear(&e.path());
                        }
                    }
                }
                let _ = std::fs::write(dir.join("cgroup.kill"), "1");
                if std::fs::remove_dir(dir).is_ok() || !dir.exists() {
                    return;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        clear(&self.0);
    }
}
