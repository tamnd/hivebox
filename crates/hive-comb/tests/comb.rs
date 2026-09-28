//! The comb end to end, with a fake driver whose cells are a real drone behind a Unix socket in the
//! cell's directory. Everything but the sandbox is real: the WAL, admission, the handshake, the
//! lifecycle actors and recovery after a restart.

#![cfg(target_os = "linux")]

use futures::future::BoxFuture;
use hive_cell::{
    CellDriver, CellHandle, DriverCaps, DriverRegistry, ExitInfo, GuestChannel, Liveness, NodeFit,
    PauseMode, RootfsPlan, Slot,
};
use hive_comb::{CellInfo, Comb, Config, CreateRequest};
use hive_drone::Drone;
use hive_proto::drone::api::{Command, RunRequest};
use hive_types::{
    Backend, Cause, CellId, CellSpec, CellState, Error, IdleAction, Qos, Reason, Source,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::UnixListener;
use tokio::task::AbortHandle;

/// A cell of the fake driver: a drone and the tasks that serve it.
struct Guest {
    drone: Option<Arc<Drone>>,
    listener: Option<AbortHandle>,
    // A second handle on each accepted socket, to hang up on the comb whatever the drone's tasks
    // are doing.
    links: Arc<Mutex<Vec<std::os::unix::net::UnixStream>>>,
    paused: bool,
    exit: Option<ExitInfo>,
}

impl Guest {
    fn cut_links(&mut self) {
        for l in self.links.lock().unwrap().drain(..) {
            let _ = l.shutdown(std::net::Shutdown::Both);
        }
    }

    fn end(&mut self, exit: ExitInfo) {
        if let Some(l) = self.listener.take() {
            l.abort();
        }
        self.cut_links();
        self.exit = Some(exit);
    }
}

#[derive(Default)]
struct Fake {
    guests: Mutex<HashMap<CellId, Guest>>,
    stops: AtomicU64,
}

impl Fake {
    fn with<T>(&self, id: CellId, f: impl FnOnce(&mut Guest) -> T) -> T {
        f(self.guests.lock().unwrap().get_mut(&id).expect("a guest"))
    }

    /// The workload exits on its own, as if the cell's init died.
    fn kill(&self, id: CellId) {
        self.with(id, |g| g.end(ExitInfo { code: Some(1), ..ExitInfo::default() }));
    }

    /// The channel drops but the cell lives on.
    fn cut(&self, id: CellId) {
        self.with(id, Guest::cut_links);
    }

    fn live(&self) -> usize {
        self.guests.lock().unwrap().values().filter(|g| g.exit.is_none()).count()
    }
}

fn socket(h: &CellHandle) -> PathBuf {
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
            let guest =
                Guest { drone, listener: None, links: Arc::default(), paused: false, exit: None };
            self.guests.lock().unwrap().insert(id, guest);
            Ok(CellHandle {
                id,
                backend: Backend::Container,
                pid: None,
                channel: GuestChannel::Unix(slot.dir.join("drone.sock")),
                cgroup: slot.cgroup.clone(),
                extra: Default::default(),
            })
        })
    }

    fn start<'a>(&'a self, h: &'a mut CellHandle) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
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
            h.pid = Some(std::process::id());
            Ok(())
        })
    }

    fn pause<'a>(&'a self, h: &'a CellHandle, _: PauseMode) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            self.with(h.id, |g| g.paused = true);
            Ok(())
        })
    }

    fn resume<'a>(&'a self, h: &'a CellHandle) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            self.with(h.id, |g| g.paused = false);
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
struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
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
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn config(dir: &Path) -> Config {
    Config {
        data_dir: dir.to_path_buf(),
        mem_mib: Some(4096),
        create_deadline: Duration::from_secs(5),
        stop_grace: Duration::from_millis(100),
        keep_ended: Duration::from_secs(60),
        ..Config::default()
    }
}

async fn open(cfg: Config, fake: &Arc<Fake>) -> Comb {
    let mut drivers = DriverRegistry::new();
    drivers.add(fake.clone());
    Comb::open(cfg, drivers).await.unwrap()
}

fn spec(image: &str) -> CellSpec {
    let mut s = CellSpec::new(Source::Image(image.into()), Backend::Container);
    s.resources.mem_mib = 256;
    s
}

fn request(s: CellSpec) -> CreateRequest {
    CreateRequest { spec: s, project: "p".into(), idem_key: None }
}

async fn echo(comb: &Comb, id: CellId, text: &str) -> String {
    let drone = comb.drone(id).await.unwrap();
    let req = RunRequest {
        command: Some(Command { shell: format!("echo {text}"), ..Command::default() }),
        ..RunRequest::default()
    };
    let r = drone.run(&req).await.unwrap();
    String::from_utf8(r.stdout.to_vec()).unwrap()
}

/// Waits for the cell to reach `state`, and fails the test after five seconds.
async fn reaches(comb: &Comb, id: CellId, state: CellState) -> CellInfo {
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

#[tokio::test(flavor = "multi_thread")]
async fn a_cell_runs_commands_and_stops() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let comb = open(config(&s.0), &fake).await;
    let mut events = comb.subscribe();

    let cell = comb.create(request(spec("python"))).await.unwrap();
    assert_eq!(cell.status.state, CellState::Running);
    assert_eq!(comb.committed(), (1, 256 << 20));
    assert_eq!(echo(&comb, cell.id, "hello").await, "hello\n");
    assert!(s.0.join("cells").join(cell.id.to_string()).is_dir());

    let ended = comb.stop(cell.id, None).await.unwrap();
    assert_eq!(ended.status.state, CellState::Stopped);
    assert_eq!(ended.status.cause, Some(Cause::Requested));
    assert_eq!(comb.committed(), (0, 0));
    assert_eq!(fake.live(), 0);
    assert!(!s.0.join("cells").join(cell.id.to_string()).exists());
    // A stopped cell stays visible, and stopping it again is not an error.
    assert_eq!(comb.list().len(), 1);
    comb.stop(cell.id, None).await.unwrap();
    assert!(comb.drone(cell.id).await.unwrap_err().reason == Reason::CellNotRunning);

    let mut seen = Vec::new();
    while let Ok(e) = events.try_recv() {
        seen.push(e.status.state);
    }
    let want = [
        CellState::Preparing,
        CellState::Starting,
        CellState::Running,
        CellState::Stopping,
        CellState::Stopped,
    ];
    assert_eq!(seen, want);
    comb.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn one_idempotency_key_makes_one_cell() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let comb = Arc::new(open(config(&s.0), &fake).await);
    let creates: Vec<_> = (0..16)
        .map(|_| {
            let comb = comb.clone();
            tokio::spawn(async move {
                let req = CreateRequest { idem_key: Some("k1".into()), ..request(spec("python")) };
                comb.create(req).await.unwrap().id
            })
        })
        .collect();
    let mut ids = Vec::new();
    for c in creates {
        ids.push(c.await.unwrap());
    }
    ids.dedup();
    assert_eq!(ids.len(), 1);
    assert_eq!(comb.list().len(), 1);
    assert_eq!(comb.committed().0, 1);
    // The same key in another project is another cell.
    let other = CreateRequest {
        project: "q".into(),
        idem_key: Some("k1".into()),
        ..request(spec("python"))
    };
    assert_ne!(comb.create(other).await.unwrap().id, ids[0]);
    comb.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn admission_and_bad_requests_leave_nothing_behind() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let comb = open(Config { mem_mib: Some(1024), ..config(&s.0) }, &fake).await;

    let e = comb.create(request(spec("no-such-image"))).await.unwrap_err();
    assert_eq!(e.reason, Reason::ImageUnavailable);
    let e = comb.create(request(spec("../images"))).await.unwrap_err();
    assert_eq!(e.reason, Reason::InvalidArgument);
    let microvm = CellSpec::new(Source::Image("python".into()), Backend::Microvm);
    let e = comb.create(request(microvm)).await.unwrap_err();
    assert_eq!(e.reason, Reason::CapacityUnavailable);

    let mut big = spec("python");
    big.resources.mem_mib = 768;
    big.qos = Qos::Latency;
    comb.create(request(big.clone())).await.unwrap();
    let e = comb.create(request(big.clone())).await.unwrap_err();
    assert_eq!(e.reason, Reason::CapacityUnavailable);
    // Best effort cells may overcommit.
    big.qos = Qos::BestEffort;
    comb.create(request(big)).await.unwrap();

    assert_eq!(comb.committed(), (2, 1536 << 20));
    let failed: Vec<_> =
        comb.list().into_iter().filter(|c| c.status.state == CellState::Failed).collect();
    assert_eq!(failed.len(), 1, "the missing image fails after it has an id");
    assert_eq!(failed[0].status.cause, Some(Cause::StartFailed));
    assert!(!s.0.join("cells").join(failed[0].id.to_string()).exists());
    comb.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cell_that_dies_while_starting_fails_and_is_cleaned_up() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let comb = open(config(&s.0), &fake).await;
    let started = Instant::now();
    let e = comb.create(request(spec("crash"))).await.unwrap_err();
    assert_eq!(e.reason, Reason::DroneUnreachable, "{e}");
    assert!(started.elapsed() < Duration::from_secs(2), "the death was noticed quickly");
    let cell = &comb.list()[0];
    assert_eq!(cell.status.state, CellState::Failed);
    assert_eq!(fake.stops.load(Ordering::Relaxed), 1);
    assert_eq!(fake.live(), 0);
    assert_eq!(comb.committed(), (0, 0));
    comb.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_channel_reconnects_and_a_dead_cell_is_stopped() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let comb = open(config(&s.0), &fake).await;
    let a = comb.create(request(spec("python"))).await.unwrap().id;
    let b = comb.create(request(spec("python"))).await.unwrap().id;

    // Cut the channel a few times: each reconnect uses the secret from the last handshake.
    for round in 0..3 {
        let before = comb.drone(a).await.unwrap();
        fake.cut(a);
        before.closed().await;
        let until = Instant::now() + Duration::from_secs(5);
        while let Err(e) = comb.drone(a).await {
            assert_eq!(e.reason, Reason::DroneUnreachable);
            assert!(Instant::now() < until, "never reconnected");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(echo(&comb, a, &format!("round{round}")).await, format!("round{round}\n"));
    }
    assert_eq!(comb.get(a).unwrap().status.state, CellState::Running);

    fake.kill(b);
    let ended = reaches(&comb, b, CellState::Stopped).await;
    assert_eq!(ended.status.cause, Some(Cause::Exited));
    assert_eq!(comb.committed().0, 1);
    comb.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn timers_expire_and_pause_cells() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let comb = open(config(&s.0), &fake).await;

    let mut hard = spec("python");
    hard.hard_ttl = Some(Duration::from_millis(300));
    let hard = comb.create(request(hard)).await.unwrap().id;

    let mut idle = spec("python");
    idle.idle_ttl = Some(Duration::from_millis(300));
    idle.idle_action = IdleAction::Pause;
    let idle = comb.create(request(idle)).await.unwrap().id;

    let mut idle_stop = spec("python");
    idle_stop.idle_ttl = Some(Duration::from_millis(300));
    idle_stop.idle_action = IdleAction::Stop;
    let idle_stop = comb.create(request(idle_stop)).await.unwrap().id;

    // Keep one idle cell busy past its ttl.
    let busy_until = Instant::now() + Duration::from_millis(600);
    while Instant::now() < busy_until {
        echo(&comb, idle, "busy").await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(comb.get(idle).unwrap().status.state, CellState::Running);

    let expired = reaches(&comb, hard, CellState::Expired).await;
    assert_eq!(expired.status.cause, Some(Cause::HardTtl));
    let stopped = reaches(&comb, idle_stop, CellState::Stopped).await;
    assert_eq!(stopped.status.cause, Some(Cause::Idle));
    reaches(&comb, idle, CellState::Paused).await;
    assert!(fake.with(idle, |g| g.paused));

    // A request wakes it.
    assert_eq!(echo(&comb, idle, "awake").await, "awake\n");
    assert_eq!(comb.get(idle).unwrap().status.state, CellState::Running);
    assert!(!fake.with(idle, |g| g.paused));
    comb.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn pause_and_resume_by_hand() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let comb = open(config(&s.0), &fake).await;
    let id = comb.create(request(spec("python"))).await.unwrap().id;
    assert_eq!(comb.pause(id).await.unwrap().status.state, CellState::Paused);
    assert_eq!(comb.pause(id).await.unwrap().status.state, CellState::Paused);
    assert_eq!(comb.resume(id).await.unwrap().status.state, CellState::Running);
    assert_eq!(comb.resume(id).await.unwrap().status.state, CellState::Running);
    comb.stop(id, None).await.unwrap();
    assert_eq!(comb.pause(id).await.unwrap_err().reason, Reason::CellNotRunning);
    comb.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_comb_picks_up_every_cell() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let comb = open(config(&s.0), &fake).await;
    let mut ids = Vec::new();
    for _ in 0..6 {
        ids.push(comb.create(request(spec("python"))).await.unwrap().id);
    }
    comb.pause(ids[1]).await.unwrap();
    comb.stop(ids[2], None).await.unwrap();
    comb.shutdown().await;
    drop(comb);
    assert_eq!(fake.live(), 5, "a comb going away leaves its cells running");

    // While no comb is watching, one cell dies.
    fake.kill(ids[3]);

    let reopened = Instant::now();
    let comb = open(config(&s.0), &fake).await;
    assert!(reopened.elapsed() < Duration::from_secs(2), "took {:?}", reopened.elapsed());
    assert_eq!(comb.list().len(), 6);
    assert_eq!(comb.get(ids[0]).unwrap().status.state, CellState::Running);
    assert_eq!(comb.get(ids[1]).unwrap().status.state, CellState::Paused);
    assert_eq!(comb.get(ids[2]).unwrap().status.state, CellState::Stopped);
    let dead = reaches(&comb, ids[3], CellState::Stopped).await;
    assert_eq!(dead.status.cause, Some(Cause::Exited));
    assert_eq!(comb.committed(), (4, 4 * (256 << 20)));

    // The secrets came back from the WAL, so the channels work.
    assert_eq!(echo(&comb, ids[0], "again").await, "again\n");
    assert_eq!(echo(&comb, ids[1], "woken").await, "woken\n");
    // New ids never repeat old ones.
    let new = comb.create(request(spec("python"))).await.unwrap().id;
    assert!(!ids.contains(&new));
    assert!(new.seq() > ids[5].seq());

    // And once more, to be sure a recovered comb writes a WAL the next one reads.
    comb.shutdown().await;
    drop(comb);
    let comb = open(config(&s.0), &fake).await;
    assert_eq!(comb.list().len(), 7);
    assert_eq!(echo(&comb, new, "third").await, "third\n");
    comb.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ended_cells_are_forgotten_after_a_while() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let cfg = Config { keep_ended: Duration::from_millis(200), ..config(&s.0) };
    let comb = open(cfg.clone(), &fake).await;
    let id = comb.create(request(spec("python"))).await.unwrap().id;
    comb.stop(id, None).await.unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    while comb.get(id).is_ok() {
        assert!(Instant::now() < until, "never forgotten");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(comb.get(id).unwrap_err().reason, Reason::CellNotFound);
    comb.shutdown().await;
    drop(comb);
    let comb = open(cfg, &fake).await;
    assert!(comb.list().is_empty());
    comb.shutdown().await;
}

/// Creates and stops cells as fast as the comb allows, for the numbers in the PR. Run with
/// `cargo test --release -p hive-comb --test comb -- --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, not a test"]
async fn churn() {
    for concurrency in
        std::env::var("HB_C").map_or(vec![1usize, 16, 64, 256], |v| vec![v.parse().unwrap()])
    {
        let s = Scratch::new();
        let fake = Arc::new(Fake::default());
        let comb = Arc::new(open(Config { mem_mib: Some(1 << 20), ..config(&s.0) }, &fake).await);
        let total =
            std::env::var("HB_N").map_or(2048usize.max(concurrency * 8), |v| v.parse().unwrap());
        let started = Instant::now();
        let workers: Vec<_> = (0..concurrency)
            .map(|_| {
                let comb = comb.clone();
                tokio::spawn(async move {
                    let mut creates = Vec::new();
                    for _ in 0..total / concurrency {
                        let t = Instant::now();
                        let id = comb.create(request(spec("python"))).await.unwrap().id;
                        creates.push(t.elapsed());
                        comb.stop(id, Some(Duration::ZERO)).await.unwrap();
                    }
                    creates
                })
            })
            .collect();
        let mut creates = Vec::new();
        for w in workers {
            creates.extend(w.await.unwrap());
        }
        let took = started.elapsed();
        creates.sort();
        let p = |q: f64| creates[((creates.len() - 1) as f64 * q) as usize];
        let wal = comb.wal_stats();
        println!(
            "concurrency {concurrency:>3}: {total} cells made and stopped in {took:?}, {:.0} per second, create p50 {:?} p99 {:?}, {} WAL writes in {} syncs",
            total as f64 / took.as_secs_f64(),
            p(0.5),
            p(0.99),
            wal.writes,
            wal.syncs,
        );
        comb.shutdown().await;
    }
}
