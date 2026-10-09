//! One cell: the state everyone can read, and the actor that owns its transitions.
//!
//! Each cell has its own task. Every transition goes through it, so two requests for the same
//! cell never race, and a slow create or stop holds up nothing but its own cell. The actor writes
//! each new state to the WAL before it acts on it. Reads such as `get` and `list` don't go through
//! the actor at all. They read [`Cell::status`], which the actor updates after each transition.

use crate::admit::Reservation;
use crate::comb::Inner;
use crate::record::{Handle, Record, now_ms, time};
use hive_cell::{
    CellDriver, CellHandle, GuestChannel, Liveness, PauseMode, RootfsPlan, Slot, cgroup,
};
#[cfg(target_os = "linux")]
use hive_cell::tree::Precopy;
use hive_drone::Client;
use hive_guard::wire::Veth;
use hive_nectar::BlobId;
use hive_nectar::oci::Staged;
use hive_nectar::upper::Scrub;
use hive_types::{Backend, Cause, CellId, CellSpec, CellState, Error, IdleAction, Qos, Reason};
use prost::Message;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::{Duration, Instant, SystemTime};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::{mpsc, oneshot, watch};

/// How long a cell that is still there gets to answer on a new channel before it counts as lost.
const RECONNECT_PATIENCE: Duration = Duration::from_secs(5);
/// The tries at removing an ended cell's cgroup stop once the wait between them, which doubles from
/// 100 ms, reaches this. That is about 100 s in all.
const REMOVE_PATIENCE: Duration = Duration::from_secs(60);
/// Longest one connection attempt with its handshake may take. A local one takes well under a
/// millisecond.
const ATTEMPT: Duration = Duration::from_secs(2);
/// The longest a cell's trim wait backs off to, in multiples of `trim_idle`.
const TRIM_BACKOFF: u32 = 16;

/// Where a cell is, as its actor last left it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Status {
    /// Its state.
    pub state: CellState,
    /// Why it ended, once it has.
    pub cause: Option<Cause>,
    /// What went wrong, if something did.
    pub message: String,
    /// When the state last changed.
    pub changed: SystemTime,
    /// Frozen and cut off from the network for good, so it stays paused until it is stopped.
    pub quarantined: bool,
}

/// A cell as the API shows it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CellInfo {
    /// Its id.
    pub id: CellId,
    /// The project that owns it.
    pub project: String,
    /// What it was created with.
    pub spec: CellSpec,
    /// Where it is now.
    pub status: Status,
    /// When it was created.
    pub created: SystemTime,
}

pub(crate) enum Cmd {
    Stop {
        cause: Cause,
        grace: Duration,
        done: oneshot::Sender<()>,
    },
    Pause {
        done: oneshot::Sender<Result<(), Error>>,
    },
    Resume {
        done: oneshot::Sender<Result<(), Error>>,
    },
    /// New timers, each left as it was when `None`. The hard TTL counts from now.
    ExtendTtl {
        hard: Option<Duration>,
        idle: Option<Duration>,
        done: oneshot::Sender<Result<(), Error>>,
    },
    /// The setup is done, so the setup boost ends.
    Ready {
        done: oneshot::Sender<Result<(), Error>>,
    },
    /// Freezes the cell for good and cuts it off the network, answered with what was cut.
    Quarantine {
        done: oneshot::Sender<Result<Cutoff, Error>>,
    },
    /// A disk snapshot, answered with what was read out of the cell, for the caller to build.
    Snapshot {
        scrub: Option<Scrub>,
        done: oneshot::Sender<Result<Staged, Error>>,
    },
    /// Copies what the cell wrote into a new directory for a fork, answered with the cell's spec
    /// and the id of its image when that is in the store.
    Fork {
        into: PathBuf,
        done: oneshot::Sender<Result<(CellSpec, Option<BlobId>), Error>>,
    },
}

/// What a quarantine did to a cell's network.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cutoff {
    /// The cell's interface lets nothing through any more.
    Cut,
    /// The cell had only loopback, so there was nothing to cut.
    Loopback,
    /// The node does not give cells namespaces of their own, so nothing could be cut.
    Unmanaged,
}

impl Cutoff {
    /// The name used on the wire and in the audit log.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cut => "cut",
            Self::Loopback => "loopback",
            Self::Unmanaged => "unmanaged",
        }
    }
}

/// A cell's timers as they are now, which `ExtendTtl` can change after the create.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Ttls {
    /// From the create to the stop, whatever the cell does.
    pub(crate) hard: Option<Duration>,
    /// From the last use to the pause or stop.
    pub(crate) idle: Option<Duration>,
}

/// The part of a cell anyone may read. The actor holds the rest.
#[derive(Debug)]
pub(crate) struct Cell {
    pub(crate) id: CellId,
    pub(crate) project: String,
    pub(crate) idem_key: String,
    pub(crate) spec: CellSpec,
    pub(crate) created: SystemTime,
    pub(crate) status: watch::Sender<Status>,
    drone: RwLock<Option<Client>>,
    last_active_ms: AtomicU64,
    ttls: RwLock<Ttls>,
    tx: mpsc::Sender<Cmd>,
}

impl Cell {
    pub(crate) fn new(
        id: CellId,
        project: String,
        idem_key: String,
        spec: CellSpec,
        created: SystemTime,
        status: Status,
    ) -> (Arc<Self>, mpsc::Receiver<Cmd>) {
        let (tx, rx) = mpsc::channel(16);
        let cell = Self {
            id,
            project,
            idem_key,
            created,
            status: watch::Sender::new(status),
            drone: RwLock::new(None),
            last_active_ms: AtomicU64::new(now_ms()),
            ttls: RwLock::new(Ttls { hard: spec.hard_ttl, idle: spec.idle_ttl }),
            spec,
            tx,
        };
        (Arc::new(cell), rx)
    }

    pub(crate) fn info(&self) -> CellInfo {
        let mut spec = self.spec.clone();
        let ttls = self.ttls();
        (spec.hard_ttl, spec.idle_ttl) = (ttls.hard, ttls.idle);
        CellInfo {
            id: self.id,
            project: self.project.clone(),
            spec,
            status: self.status.borrow().clone(),
            created: self.created,
        }
    }

    pub(crate) fn state(&self) -> CellState {
        self.status.borrow().state
    }

    /// The guest agent's client, if the cell is running and connected. Counts as activity for
    /// the idle timer.
    pub(crate) fn drone(&self) -> Option<Client> {
        self.touch();
        self.drone.read().unwrap_or_else(PoisonError::into_inner).clone()
    }

    pub(crate) fn ttls(&self) -> Ttls {
        *self.ttls.read().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn touch(&self) {
        self.last_active_ms.fetch_max(now_ms(), Ordering::Relaxed);
    }

    /// How long since anything used the cell.
    pub(crate) fn quiet_for(&self) -> Duration {
        Duration::from_millis(now_ms().saturating_sub(self.last_active_ms.load(Ordering::Relaxed)))
    }

    fn set_drone(&self, c: Option<Client>) {
        *self.drone.write().unwrap_or_else(PoisonError::into_inner) = c;
    }

    pub(crate) async fn send(&self, cmd: Cmd) -> bool {
        self.tx.send(cmd).await.is_ok()
    }
}

/// How an actor starts.
pub(crate) enum Start {
    /// A new cell. The answer goes to `done` once it is running or has failed.
    /// With a seed, the cell's writable layer starts with what that directory holds.
    Create {
        secret: [u8; 32],
        seed: Option<PathBuf>,
        done: oneshot::Sender<Result<(), Error>>,
    },
    /// A cell from before a restart, found with this record. `done` is told once the cell is
    /// running again, or has been cleaned up.
    Recover { record: Box<Record>, done: oneshot::Sender<()> },
}

pub(crate) struct Actor {
    pub(crate) inner: Arc<Inner>,
    pub(crate) cell: Arc<Cell>,
    pub(crate) driver: Arc<dyn CellDriver>,
    pub(crate) rx: mpsc::Receiver<Cmd>,
    pub(crate) reservation: Option<Reservation>,
    record: Record,
    handle: Option<CellHandle>,
    /// The cell's own cgroup, from the moment it is taken until the cell is over.
    cgroup: Option<PathBuf>,
    /// The cell's own network namespace, the same way.
    netns: Option<PathBuf>,
    /// The cell's interface in that namespace, when the node wires them.
    veth: Option<Veth>,
    /// Whether the paused cell's memory has been reclaimed, so the reclaim timer is done.
    reclaimed: bool,
    /// When the running cell last gave back its cold page cache.
    trimmed: Option<SystemTime>,
    /// How long the running cell waits idle before a trim, once it has read back most of what a
    /// trim gave away. `None` is `trim_idle`.
    trim_wait: Option<Duration>,
    /// The cell's refaulted bytes and the bytes it gave back, right after its last trim.
    trim_mark: Option<(u64, u64)>,
    /// When the setup boost ends, while the cell has one.
    boost_until: Option<SystemTime>,
}

impl Actor {
    pub(crate) fn new(
        inner: Arc<Inner>,
        cell: Arc<Cell>,
        driver: Arc<dyn CellDriver>,
        rx: mpsc::Receiver<Cmd>,
        reservation: Option<Reservation>,
        record: Record,
    ) -> Self {
        Self {
            inner,
            cell,
            driver,
            rx,
            reservation,
            record,
            handle: None,
            cgroup: None,
            netns: None,
            veth: None,
            reclaimed: false,
            trimmed: None,
            trim_wait: None,
            trim_mark: None,
            boost_until: None,
        }
    }

    pub(crate) async fn run(mut self, start: Start) {
        let shutdown = self.inner.shutdown.clone();
        tokio::select! {
            () = self.life(start) => {}
            // The comb is going away. The cell is left exactly as it is, for the next comb to find.
            () = shutdown.cancelled() => {}
        }
    }

    async fn life(&mut self, start: Start) {
        match start {
            Start::Create { secret, seed, done } => {
                let result = self.create(secret, seed).await;
                let failed = result.is_err();
                let _ = done.send(result);
                if failed {
                    return self.linger().await;
                }
            }
            Start::Recover { record, done } => {
                self.record = *record;
                let lives = self.recover().await;
                let _ = done.send(());
                if !lives {
                    return self.linger().await;
                }
            }
        }
        self.serve().await;
        self.linger().await;
    }

    fn id(&self) -> u128 {
        self.cell.id.to_bits()
    }

    /// Writes the new state to the WAL, then shows it. Nothing is done about the new state until
    /// this returns.
    async fn commit(
        &mut self,
        state: CellState,
        cause: Option<Cause>,
        message: &str,
    ) -> Result<(), Error> {
        let from = self.cell.state();
        debug_assert!(from == state || from.can_become(state), "{from} to {state}");
        self.record.set_cell_state(state);
        if let Some(c) = cause {
            self.record.set_cell_cause(c);
        }
        message.clone_into(&mut self.record.message);
        self.record.handle = self.handle.as_ref().map(Handle::from_cell);
        let bytes = self.record.encode_to_vec().into();
        self.inner.wal.put(self.id(), bytes).await.map_err(|e| wal_error(&e))?;
        self.show(state, cause, message);
        Ok(())
    }

    /// Shows a new state without writing it down. Only for states a restart has nothing to do
    /// about.
    fn show(&mut self, state: CellState, cause: Option<Cause>, message: &str) {
        let status = Status {
            state,
            cause,
            message: message.to_owned(),
            changed: SystemTime::now(),
            quarantined: self.record.quarantined,
        };
        self.cell.status.send_replace(status);
        self.inner.changed(&self.cell);
    }

    async fn create(&mut self, secret: [u8; 32], seed: Option<PathBuf>) -> Result<(), Error> {
        let deadline = self.inner.cfg.create_deadline;
        let started = Instant::now();
        let permit = match tokio::time::timeout(
            deadline,
            self.inner.admission.create_permit(self.cell.spec.backend),
        )
        .await
        {
            Ok(Some(p)) => p,
            Ok(None) => {
                return self.fail_start(Error::new(Reason::Internal, "no create permit")).await;
            }
            Err(_) => {
                self.inner
                    .metrics
                    .created(self.cell.spec.backend, Reason::CapacityUnavailable.as_str());
                let e = Error::new(
                    Reason::CapacityUnavailable,
                    "the node was too busy making other cells to start this one in time",
                );
                return self.fail_start(e).await;
            }
        };
        let backend = self.cell.spec.backend;
        self.inner.metrics.stage(backend, "admit", started.elapsed());
        // The bring up gets the whole deadline again, so a create that queued behind a burst is
        // not failed for the time the cells ahead of it took.
        self.record.secret = secret.to_vec();
        // Not written down: a restart that finds no record for a cell cleans up its directory, which
        // is all a preparing cell has. The first record is the one with the driver's handle in it.
        self.show(CellState::Preparing, None, "");
        let result = match tokio::time::timeout(deadline, self.bring_up(secret, seed)).await {
            Ok(r) => r,
            Err(_) => Err(Error::new(
                Reason::DroneUnreachable,
                "the guest agent did not answer within the create deadline",
            )),
        };
        drop(permit);
        let m = &self.inner.metrics;
        match result {
            Ok(()) => {
                m.stage(backend, "total", started.elapsed());
                m.created(backend, "ok");
                Ok(())
            }
            Err(e) => {
                m.created(backend, e.reason.as_str());
                self.fail_start(e).await
            }
        }
    }

    async fn bring_up(&mut self, secret: [u8; 32], seed: Option<PathBuf>) -> Result<(), Error> {
        let inner = self.inner.clone();
        let backend = self.cell.spec.backend;
        let mut t = Instant::now();
        let mut lap = |stage: &str| {
            inner.metrics.stage(backend, stage, t.elapsed());
            t = Instant::now();
        };
        let slot = self.slot(secret).await?;
        lap("pool");
        // A wasm cell's image names its program, which its driver finds, and it has no rootfs.
        let (mut rootfs, image) = if backend == Backend::Fncall {
            (RootfsPlan::default(), None)
        } else {
            self.inner.rootfs(&self.cell.spec, &self.cell.project, &slot).await?
        };
        rootfs.seed = seed;
        self.record.image = image.map(|i| i.as_bytes().to_vec()).unwrap_or_default();
        lap("rootfs");
        let handle = self.driver.prepare(self.cell.id, &self.cell.spec, &rootfs, &slot).await?;
        self.handle = Some(handle);
        lap("prepare");
        self.commit(CellState::Starting, None, "").await?;
        lap("wal");
        let driver = self.driver.clone();
        let handle = self.handle.as_mut().expect("set just above");
        driver.start(handle).await?;
        lap("start");
        if let (Some(core), Some(cgroup)) = (&inner.core, &self.cgroup) {
            // Before the drone answers, so nothing has run in the cell yet that could fork
            // without the cookie.
            if let Err(e) = core.give(self.cell.spec.qos, cgroup).await {
                eprintln!(
                    "hive-comb: {} runs without its core scheduling cookie: {e}",
                    self.cell.id
                );
            }
        }
        let client = tokio::select! {
            c = connect(&handle.channel, &secret, None) => c?,
            e = died(&*driver, handle) => return Err(e),
        };
        lap("handshake");
        self.record.secret = client.established().next_secret.to_vec();
        self.cell.set_drone(Some(client));
        self.commit(CellState::Running, None, "").await?;
        lap("wal");
        Ok(())
    }

    async fn slot(&mut self, secret: [u8; 32]) -> Result<Slot, Error> {
        let dir = self.inner.cell_dir(self.cell.id);
        std::fs::create_dir_all(&dir).map_err(|e| io_error("making the cell directory", &e))?;
        let mut cgroup = PathBuf::new();
        // A wasm cell runs in the comb's own threads, so it has no cgroup or namespace of its
        // own, and its driver holds it to its memory.
        let own = self.cell.spec.backend != Backend::Fncall;
        if let Some(pool) = self.inner.cgroups.clone().filter(|_| own) {
            let (qos, mut r) = (self.cell.spec.qos, self.cell.spec.resources);
            let asked = r.vcpu_milli;
            r.vcpu_milli = pool.quota(qos, asked);
            let factor = self.inner.cfg.setup_boost;
            if let Some(d) = self.cell.spec.burst_until_ready.filter(|_| factor > 1) {
                // The leaf is set to the boosted quota from the start, which costs nothing over
                // setting the steady one. A boost below the burst cap changes nothing.
                r.vcpu_milli = r.vcpu_milli.max(asked.saturating_mul(factor));
                self.boost_until = Some(self.cell.created + d);
            }
            let taken = tokio::task::spawn_blocking(move || pool.take(qos, &r))
                .await
                .map_err(|e| Error::new(Reason::Internal, e.to_string()))?;
            cgroup = taken.map_err(|e| io_error("setting up the cell's cgroup", &e))?;
            self.cgroup = Some(cgroup.clone());
        }
        let mut nameserver = None;
        if let Some(pool) = self.inner.netns.clone().filter(|_| own) {
            let profile = self.inner.profile(&self.cell.spec.network_profile)?;
            let taken = tokio::task::spawn_blocking(move || pool.take())
                .await
                .map_err(|e| Error::new(Reason::Internal, e.to_string()))?;
            let spare = taken.map_err(|e| io_error("making the cell's network namespace", &e))?;
            self.netns = Some(spare.path);
            self.veth = spare.veth;
            if let (Some(veth), Some(net), Some(profile)) =
                (&self.veth, self.inner.netns.as_ref().and_then(|p| p.net()), profile)
            {
                net.assign(veth, self.cell.id, profile)
                    .map_err(|e| io_error("putting the cell on its interface", &e))?;
                nameserver = self.inner.netns.as_ref().and_then(|p| p.nameserver());
            }
        }
        Ok(Slot { cgroup, netns: self.netns.clone(), nameserver, dir, secret })
    }

    /// Undoes whatever a failed create got as far as, and records the failure.
    async fn fail_start(&mut self, e: Error) -> Result<(), Error> {
        self.cell.set_drone(None);
        if let Some(h) = &self.handle {
            let _ = self.driver.stop(h, Duration::ZERO).await;
        }
        self.release().await;
        let message = e.to_string();
        let _ = self.commit(CellState::Failed, Some(Cause::StartFailed), &message).await;
        Err(e)
    }

    async fn recover(&mut self) -> bool {
        let backend = self.cell.spec.backend;
        self.handle = self.record.handle(self.cell.id, backend);
        self.cgroup =
            self.handle.as_ref().map(|h| h.cgroup.clone()).filter(|c| !c.as_os_str().is_empty());
        self.netns = self.handle.as_ref().and_then(|h| h.netns.clone());
        if let (Some(ns), Some(pool)) = (&self.netns, &self.inner.netns) {
            self.veth = pool.recover(ns, self.cell.id);
        }
        let state = self.record.cell_state().unwrap_or(CellState::Failed);
        if state.is_terminal() {
            // Already over and cleaned up. It only stays for others to see how it ended.
            return false;
        }
        let Some(handle) = self.handle.clone() else {
            // It never got as far as the driver, so there is nothing to find or undo.
            self.release().await;
            let _ = self
                .commit(
                    CellState::Failed,
                    Some(Cause::Recovery),
                    "the node restarted while it was being made",
                )
                .await;
            return false;
        };
        match state {
            CellState::Running | CellState::Paused | CellState::Pausing => {}
            CellState::Stopping => {
                let cause = self.record.cell_cause().unwrap_or(Cause::Requested);
                self.stop(cause, self.inner.cfg.stop_grace).await;
                return false;
            }
            _ => {
                self.stop(Cause::Recovery, Duration::ZERO).await;
                return false;
            }
        }
        let live = self.driver.check(&handle).await;
        if self.record.quarantined && matches!(live, Ok(Liveness::Alive | Liveness::Paused)) {
            // Frozen and cut off again, in case the last comb ended before it was done.
            if matches!(live, Ok(Liveness::Alive))
                && self.driver.pause(&handle, PauseMode::Freeze).await.is_err()
            {
                self.stop(Cause::Recovery, Duration::ZERO).await;
                return false;
            }
            if self.isolate().is_err() {
                self.stop(Cause::Recovery, Duration::ZERO).await;
                return false;
            }
            return self.commit(CellState::Paused, None, "quarantined").await.is_ok();
        }
        match live {
            Ok(Liveness::Gone(exit)) => {
                let cause = if exit.oom { Cause::Oom } else { Cause::Exited };
                self.stop(cause, Duration::ZERO).await;
                return false;
            }
            Ok(Liveness::Paused) if state == CellState::Running => {
                // Frozen for a snapshot when the last comb ended, so it is thawed again.
                if self.driver.resume(&handle).await.is_err() {
                    self.stop(Cause::Recovery, Duration::ZERO).await;
                    return false;
                }
            }
            Ok(Liveness::Alive | Liveness::Paused) => {}
            Err(_) => {
                self.stop(Cause::Recovery, Duration::ZERO).await;
                return false;
            }
        }
        // The boost is back on until its time is up. A cell whose time is up drops to its steady
        // quota at once, in case the last comb ended before it could do that.
        if self.cgroup.is_some() {
            self.boost_until = self.cell.spec.burst_until_ready.map(|d| self.cell.created + d);
        }
        if state == CellState::Paused {
            // A frozen guest agent cannot answer, so the channel waits for the resume.
            return true;
        }
        if self.reconnect(&handle).await.is_err() {
            self.stop(Cause::DroneLost, Duration::ZERO).await;
            return false;
        }
        if state == CellState::Pausing {
            // Whether the pause took is unknown, so it is done again. If it fails the cell is
            // running, which is fine too.
            let _ = self.pause_now().await;
            return !self.cell.state().is_terminal();
        }
        // Written again so the new secret is on disk.
        self.commit(state, None, "").await.is_ok()
    }

    /// Serves a live cell until it ends.
    async fn serve(&mut self) {
        let mut pressure = self.inner.pressure.subscribe();
        loop {
            let drone = self.cell.drone.read().unwrap_or_else(PoisonError::into_inner).clone();
            let lost = async {
                match &drone {
                    Some(c) => c.closed().await,
                    None => std::future::pending().await,
                }
            };
            let timer = self.next_timer();
            let wake = async {
                match timer {
                    Some((at, _)) => tokio::time::sleep_until(at.into()).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                cmd = self.rx.recv() => match cmd {
                    Some(Cmd::Stop { cause, grace, done }) => {
                        self.stop(cause, grace).await;
                        let _ = done.send(());
                        return;
                    }
                    Some(Cmd::Pause { done }) => {
                        let r = self.pause().await;
                        let ended = self.cell.state().is_terminal();
                        let _ = done.send(r);
                        if ended {
                            return;
                        }
                    }
                    Some(Cmd::Resume { done }) => {
                        let r = self.resume().await;
                        let ended = self.cell.state().is_terminal();
                        let _ = done.send(r);
                        if ended {
                            return;
                        }
                    }
                    Some(Cmd::ExtendTtl { hard, idle, done }) => {
                        let _ = done.send(self.extend_ttl(hard, idle).await);
                    }
                    Some(Cmd::Ready { done }) => {
                        let _ = done.send(self.ready().await);
                    }
                    Some(Cmd::Quarantine { done }) => {
                        let r = self.quarantine().await;
                        let ended = self.cell.state().is_terminal();
                        let _ = done.send(r);
                        if ended {
                            return;
                        }
                    }
                    Some(Cmd::Snapshot { scrub, done }) => {
                        let _ = done.send(self.snapshot(scrub).await);
                    }
                    Some(Cmd::Fork { into, done }) => {
                        let _ = done.send(self.fork(into).await);
                    }
                    None => return,
                },
                () = lost => {
                    if !self.drone_lost().await {
                        return;
                    }
                }
                // The brake went on or off, which moves the timers.
                Ok(()) = pressure.changed() => {}
                () = wake => {
                    let (_, what) = timer.expect("only wakes with a timer");
                    match what {
                        Timer::Hard => return self.stop(Cause::HardTtl, self.inner.cfg.stop_grace).await,
                        Timer::Boost => self.end_boost(),
                        Timer::PauseTtl => {
                            if self.next_timer().is_some_and(|(at, _)| at > Instant::now()) {
                                continue;
                            }
                            return self.stop(Cause::Idle, self.inner.cfg.stop_grace).await;
                        }
                        Timer::Reclaim => {
                            if self.next_timer().is_some_and(|(at, _)| at > Instant::now()) {
                                continue;
                            }
                            self.reclaim().await;
                        }
                        Timer::Trim => {
                            if self.next_timer().is_some_and(|(at, _)| at > Instant::now()) {
                                continue;
                            }
                            self.trim().await;
                        }
                        Timer::Squeeze => {
                            if !*self.inner.pressure.borrow()
                                || self.next_timer().is_some_and(|(at, _)| at > Instant::now())
                            {
                                continue;
                            }
                            if self.pause().await.is_ok() {
                                self.inner.metrics.squeezed();
                            } else {
                                // Counts as use, so a pause that failed waits its turn again
                                // instead of being tried in a loop.
                                self.cell.touch();
                            }
                            if self.cell.state().is_terminal() {
                                return;
                            }
                        }
                        Timer::Idle => {
                            // The cell was used while the timer slept, so it is not idle after all.
                            if self.next_timer().is_some_and(|(at, _)| at > Instant::now()) {
                                continue;
                            }
                            if self.cell.spec.idle_action == IdleAction::Pause && self.driver.caps().pause {
                                let _ = self.pause().await;
                                if self.cell.state().is_terminal() {
                                    return;
                                }
                            } else {
                                return self.stop(Cause::Idle, self.inner.cfg.stop_grace).await;
                            }
                        }
                    }
                }
            }
        }
    }

    fn next_timer(&self) -> Option<(Instant, Timer)> {
        // A quarantined cell is kept as it is until someone stops it, whatever its timers say.
        if self.record.quarantined {
            return None;
        }
        let now_sys = SystemTime::now();
        let now = Instant::now();
        let at = |t: SystemTime| now + t.duration_since(now_sys).unwrap_or_default();
        let ttls = self.cell.ttls();
        let hard = ttls.hard.map(|ttl| (at(self.cell.created + ttl), Timer::Hard));
        let cfg = &self.inner.cfg;
        let pressed = *self.inner.pressure.borrow();
        let (idle, squeeze, trim, reclaim, pause_ttl) = match self.cell.state() {
            CellState::Running => {
                let last = time(self.cell.last_active_ms.load(Ordering::Relaxed));
                let idle = ttls.idle.map(|ttl| (at(last + ttl), Timer::Idle));
                // Under pressure an idle cell is paused early, but only one a request would
                // resume anyway, and never a latency cell, which pays for being kept warm.
                let squeeze = (pressed
                    && !cfg.pressure_idle.is_zero()
                    && self.cell.spec.idle_action == IdleAction::Pause
                    && self.cell.spec.qos != Qos::Latency
                    && self.driver.caps().pause)
                    .then(|| (at(last + cfg.pressure_idle), Timer::Squeeze));
                // Latency cells keep their page cache warm too.
                let trim = (!cfg.trim_idle.is_zero()
                    && self.cell.spec.qos != Qos::Latency
                    && self.driver.caps().trim)
                    .then(|| {
                        let since = self.trimmed.map_or(last, |t| t.max(last));
                        (at(since + self.trim_wait.unwrap_or(cfg.trim_idle)), Timer::Trim)
                    });
                (idle, squeeze, trim, None, None)
            }
            CellState::Paused => {
                let since = self.cell.status.borrow().changed;
                let wait = if pressed && !cfg.pressure_idle.is_zero() {
                    Duration::ZERO
                } else {
                    cfg.reclaim_after
                };
                let reclaim = (!self.reclaimed && self.driver.caps().pause)
                    .then(|| (at(since + wait), Timer::Reclaim));
                (None, None, None, reclaim, Some((at(since + cfg.pause_ttl), Timer::PauseTtl)))
            }
            _ => (None, None, None, None, None),
        };
        let boost = self.boost_until.map(|t| (at(t), Timer::Boost));
        [hard, idle, squeeze, trim, reclaim, pause_ttl, boost]
            .into_iter()
            .flatten()
            .min_by_key(|(at, _)| *at)
    }

    /// Swaps a paused cell's memory out. A failed reclaim leaves the cell frozen as it was, and is
    /// not tried again until the next pause.
    async fn reclaim(&mut self) {
        self.reclaimed = true;
        let handle = self.handle.clone().expect("a live cell has a handle");
        let _ = self.driver.pause(&handle, PauseMode::Reclaim).await;
    }

    /// Has the driver give back the page cache the idle cell has not used lately. The cell runs on
    /// either way, and a trim that failed waits its turn again like one that worked.
    ///
    /// A cell that was used since its last trim and had to read back at least half of what that
    /// trim gave away needed those pages, the way an agent that rereads its files after each wait
    /// for its model does. Its next trim waits twice as long, up to [`TRIM_BACKOFF`] times
    /// `trim_idle`, and a trim the cell did not read back halves the wait again.
    async fn trim(&mut self) {
        let base = self.inner.cfg.trim_idle;
        let last = time(self.cell.last_active_ms.load(Ordering::Relaxed));
        if let (Some(trimmed), Some((mark, given))) = (self.trimmed, self.trim_mark)
            && last > trimmed
            && let Some(now) = self.cgroup.as_deref().and_then(cgroup::refaulted)
        {
            let wait = self.trim_wait.unwrap_or(base);
            self.trim_wait = Some(backoff(wait, base, given, now.saturating_sub(mark)));
        }
        self.trimmed = Some(SystemTime::now());
        let handle = self.handle.clone().expect("a live cell has a handle");
        if let Ok(bytes) = self.driver.trim(&handle).await {
            self.inner.metrics.trimmed(bytes);
            let refaulted = self.cgroup.as_deref().and_then(cgroup::refaulted);
            self.trim_mark = refaulted.map(|r| (r, bytes));
        }
    }

    async fn extend_ttl(
        &mut self,
        hard: Option<Duration>,
        idle: Option<Duration>,
    ) -> Result<(), Error> {
        let state = self.cell.state();
        if state.is_terminal() {
            return Err(not_running(state));
        }
        let mut ttls = self.cell.ttls();
        if let Some(h) = hard {
            let lived = SystemTime::now().duration_since(self.cell.created).unwrap_or_default();
            ttls.hard = Some(lived + h);
        }
        if idle.is_some() {
            ttls.idle = idle;
        }
        if let Some(spec) = &mut self.record.spec {
            spec.hard_ttl = ttls.hard.map(hive_proto::convert::duration_to_v1);
            spec.idle_ttl = ttls.idle.map(hive_proto::convert::duration_to_v1);
        }
        let message = self.cell.status.borrow().message.clone();
        let cause = self.cell.status.borrow().cause;
        self.commit(state, cause, &message).await?;
        *self.cell.ttls.write().unwrap_or_else(PoisonError::into_inner) = ttls;
        // Counts as use, so a cell given a longer idle TTL is not paused on the old clock.
        self.cell.touch();
        Ok(())
    }

    /// Drops the cell to its steady CPU quota if it is on the setup boost.
    fn end_boost(&mut self) {
        if self.boost_until.take().is_none() {
            return;
        }
        let (Some(dir), Some(pool)) = (&self.cgroup, &self.inner.cgroups) else { return };
        let spec = &self.cell.spec;
        let steady = cgroup::cpu_max(pool.quota(spec.qos, spec.resources.vcpu_milli));
        if let Err(e) = cgroup::write(dir, "cpu.max", &steady) {
            eprintln!("hive-comb: {} keeps its setup boost: {e}", self.cell.id);
        }
    }

    /// Ends the setup boost and writes that down, so a comb that starts over does not put it back.
    async fn ready(&mut self) -> Result<(), Error> {
        let state = self.cell.state();
        if state.is_terminal() {
            return Err(not_running(state));
        }
        if self.boost_until.is_none() {
            return Ok(());
        }
        self.end_boost();
        if let Some(spec) = &mut self.record.spec {
            spec.burst_until_ready = None;
        }
        let message = self.cell.status.borrow().message.clone();
        let cause = self.cell.status.borrow().cause;
        self.commit(state, cause, &message).await
    }

    /// The guest agent's connection dropped. Returns whether the cell lives on.
    async fn drone_lost(&mut self) -> bool {
        let handle = self.handle.clone().expect("a live cell has a handle");
        match self.driver.check(&handle).await {
            Ok(Liveness::Gone(exit)) => {
                let cause = if exit.oom { Cause::Oom } else { Cause::Exited };
                self.stop(cause, Duration::ZERO).await;
                return false;
            }
            Ok(_) => {}
            Err(_) => {
                self.stop(Cause::DroneLost, Duration::ZERO).await;
                return false;
            }
        }
        let state = self.cell.state();
        if state == CellState::Paused {
            // Frozen, so it cannot answer. The resume connects again.
            self.cell.set_drone(None);
            return true;
        }
        // The cell is there but the channel dropped, so try again before giving up on it.
        if self.reconnect(&handle).await.is_err() {
            self.stop(Cause::DroneLost, Duration::ZERO).await;
            return false;
        }
        self.commit(state, None, "").await.is_ok()
    }

    async fn pause(&mut self) -> Result<(), Error> {
        match self.cell.state() {
            CellState::Paused => return Ok(()),
            CellState::Running => {}
            s => return Err(not_running(s)),
        }
        if !self.driver.caps().pause {
            return Err(Error::new(Reason::PolicyDenied, "this backend cannot pause"));
        }
        self.commit(CellState::Pausing, None, "").await?;
        self.pause_now().await
    }

    async fn pause_now(&mut self) -> Result<(), Error> {
        let handle = self.handle.clone().expect("a live cell has a handle");
        match self.driver.pause(&handle, PauseMode::Freeze).await {
            Ok(()) => self.commit(CellState::Paused, None, "").await,
            Err(e) => {
                // The pause did not take, so the cell is still running.
                self.commit(CellState::Running, None, &e.to_string()).await?;
                Err(e)
            }
        }
    }

    /// Connects to the guest agent again with the secret from the last handshake, and keeps the
    /// next one in memory. The caller commits, which writes it to disk.
    async fn reconnect(&mut self, handle: &CellHandle) -> Result<(), Error> {
        let secret = self.record.secret().unwrap_or_default();
        let client = connect(&handle.channel, &secret, Some(RECONNECT_PATIENCE)).await?;
        self.record.secret = client.established().next_secret.to_vec();
        self.cell.set_drone(Some(client));
        Ok(())
    }

    async fn resume(&mut self) -> Result<(), Error> {
        if self.record.quarantined {
            return Err(Error::new(
                Reason::PolicyDenied,
                "the cell is quarantined, so it stays frozen until it is stopped",
            ));
        }
        match self.cell.state() {
            CellState::Running => return Ok(()),
            CellState::Paused => {}
            s => return Err(not_running(s)),
        }
        self.commit(CellState::Running, None, "").await?;
        let handle = self.handle.clone().expect("a live cell has a handle");
        self.cell.touch();
        self.reclaimed = false;
        if let Err(e) = self.driver.resume(&handle).await {
            // Still frozen, so it goes back to paused, the only way there being through pausing.
            self.commit(CellState::Pausing, None, "").await?;
            self.commit(CellState::Paused, None, &e.to_string()).await?;
            return Err(e);
        }
        if self.cell.drone.read().unwrap_or_else(PoisonError::into_inner).is_none() {
            // Paused when the comb restarted, so this is the first time it can answer.
            if let Err(e) = self.reconnect(&handle).await {
                self.stop(Cause::DroneLost, Duration::ZERO).await;
                return Err(e);
            }
            self.commit(CellState::Running, None, "").await?;
        }
        Ok(())
    }

    /// Freezes the cell and cuts it off the network, for good: it cannot be resumed, its timers
    /// stop, and only a stop ends it. The quarantine is written down before anything is done, so
    /// a comb that ends partway through finishes it when it comes back.
    async fn quarantine(&mut self) -> Result<Cutoff, Error> {
        let state = self.cell.state();
        if !matches!(state, CellState::Running | CellState::Paused) {
            return Err(not_running(state));
        }
        if !self.driver.caps().pause {
            return Err(Error::new(
                Reason::PolicyDenied,
                "this backend cannot freeze a cell, so stop it instead",
            ));
        }
        let handle = self.handle.clone().expect("a live cell has a handle");
        let was = self.record.quarantined;
        self.record.quarantined = true;
        if state == CellState::Running {
            self.commit(CellState::Pausing, None, "").await?;
            if let Err(e) = self.driver.pause(&handle, PauseMode::Freeze).await {
                // The freeze did not take, so the cell is still running and not quarantined.
                self.record.quarantined = was;
                self.commit(CellState::Running, None, &e.to_string()).await?;
                return Err(e);
            }
        }
        // Frozen first, so nothing in the cell sees the network go and acts on it.
        let net = self.isolate()?;
        self.commit(CellState::Paused, None, "quarantined").await?;
        Ok(net)
    }

    /// Takes away the cell's network, if the node gave it one.
    fn isolate(&self) -> Result<Cutoff, Error> {
        if self.netns.is_none() {
            return Ok(Cutoff::Unmanaged);
        }
        let net = self.inner.netns.as_ref().and_then(|p| p.net());
        let (Some(veth), Some(net)) = (&self.veth, net) else { return Ok(Cutoff::Loopback) };
        net.isolate(veth, self.cell.id)
            .map_err(|e| io_error("cutting the cell off the network", &e))?;
        Ok(Cutoff::Cut)
    }

    /// Builds what the cell wrote into a layer on its image and stores that as a new image. A
    /// running cell is frozen meanwhile, without telling anyone, since it is thawed again before
    /// the actor does anything else. A comb that ends in between thaws it when it comes back.
    async fn snapshot(&mut self, scrub: Option<Scrub>) -> Result<Staged, Error> {
        let running = match self.cell.state() {
            CellState::Running => true,
            CellState::Paused => false,
            s => return Err(not_running(s)),
        };
        if self.cell.spec.backend != Backend::Container {
            return Err(Error::new(
                Reason::PolicyDenied,
                "only container cells have snapshots yet",
            ));
        }
        let Some(base) = self.record.image() else {
            return Err(Error::new(
                Reason::PolicyDenied,
                "the cell's image is not from the image store, so it has no snapshots",
            ));
        };
        let handle = self.handle.clone().expect("a live cell has a handle");
        let started = Instant::now();
        if running {
            self.driver.pause(&handle, PauseMode::Freeze).await?;
        }
        let upper = self.inner.cell_dir(self.cell.id).join("upper");
        let staged = self.inner.stage(&upper, base, scrub, self.cell.id.to_string()).await;
        if running && let Err(e) = self.driver.resume(&handle).await {
            // Still frozen, so it shows as paused, the only way there being through pausing.
            let _ = self.commit(CellState::Pausing, None, "").await;
            let _ = self.commit(CellState::Paused, None, &e.to_string()).await;
        }
        self.inner.metrics.snapshot("read", started.elapsed());
        self.cell.touch();
        staged
    }

    /// Copies what the cell wrote into `into`, for a fork's children to start from. A running cell
    /// is frozen for the copy without telling anyone, the same as for a snapshot.
    async fn fork(&mut self, into: PathBuf) -> Result<(CellSpec, Option<BlobId>), Error> {
        let running = match self.cell.state() {
            CellState::Running => true,
            CellState::Paused => false,
            s => return Err(not_running(s)),
        };
        if self.cell.spec.backend != Backend::Container {
            return Err(Error::new(Reason::PolicyDenied, "only container cells fork yet"));
        }
        let handle = self.handle.clone().expect("a live cell has a handle");
        let upper = self.inner.cell_dir(self.cell.id).join("upper");
        let started = Instant::now();
        if !running {
            copy_once(upper, into).await?;
            self.inner.metrics.snapshot("fork_copy", started.elapsed());
            self.cell.touch();
            return Ok((self.cell.spec.clone(), self.record.image()));
        }
        // Most of the copy is made while the cell runs, and it is frozen only to bring the copy
        // up to date, which redoes what changed in the meantime.
        let pre = precopy(upper, into).await?;
        let frozen = Instant::now();
        self.driver.pause(&handle, PauseMode::Freeze).await?;
        let copied = finish(pre).await;
        if let Err(e) = self.driver.resume(&handle).await {
            // Still frozen, so it shows as paused, the only way there being through pausing.
            let _ = self.commit(CellState::Pausing, None, "").await;
            let _ = self.commit(CellState::Paused, None, &e.to_string()).await;
        }
        self.inner.metrics.snapshot("fork_frozen", frozen.elapsed());
        self.inner.metrics.snapshot("fork_copy", started.elapsed());
        self.cell.touch();
        copied?;
        Ok((self.cell.spec.clone(), self.record.image()))
    }

    /// Stops the cell and records how it ended. Never fails: whatever goes wrong, the cell ends
    /// up in a terminal state.
    async fn stop(&mut self, cause: Cause, grace: Duration) {
        if self.cell.state().is_terminal() {
            return;
        }
        let inner = self.inner.clone();
        let backend = self.cell.spec.backend;
        let started = Instant::now();
        let mut t = started;
        let mut lap = |stage: &str| {
            inner.metrics.stopped(backend, stage, t.elapsed());
            t = Instant::now();
        };
        let _ = self.commit(CellState::Stopping, Some(cause), "").await;
        lap("wal");
        self.cell.set_drone(None);
        let mut cause = cause;
        let mut message = String::new();
        if let Some(h) = &self.handle {
            match self.driver.stop(h, grace).await {
                // The OOM killer takes the drone with the rest of the cell, so a cell lost that
                // way ran out of memory.
                Ok(exit) if exit.oom && matches!(cause, Cause::Exited | Cause::DroneLost) => {
                    cause = Cause::Oom;
                }
                Ok(_) => {}
                Err(e) => message = format!("stopping it failed: {e}"),
            }
        }
        lap("driver");
        self.release().await;
        lap("release");
        let state = match cause {
            Cause::HardTtl => CellState::Expired,
            c if c.is_infra() => CellState::Failed,
            _ => CellState::Stopped,
        };
        let _ = self.commit(state, Some(cause), &message).await;
        lap("wal");
        self.inner.metrics.stopped(backend, "total", started.elapsed());
    }

    /// Gives back the cell's share of the node and removes its directory, on the blocking pool so a
    /// bulk stop does not hold up the runtime.
    async fn release(&mut self) {
        self.reservation = None;
        let dir = self.inner.cell_dir(self.cell.id);
        let _ = tokio::task::spawn_blocking(move || std::fs::remove_dir_all(dir)).await;
        if let Some(dir) = self.cgroup.take() {
            // Kills anything the driver left behind. A failure is often passing, like running out
            // of open files under load, and giving up would leave the cell's processes running, so
            // it tries again for a while. A leaf that still cannot go is swept by the next comb.
            tokio::spawn(async move {
                let mut wait = Duration::from_millis(100);
                loop {
                    match crate::cgroups::remove(dir.clone()).await {
                        Ok(()) => break,
                        Err(_) if wait < REMOVE_PATIENCE => {
                            tokio::time::sleep(wait).await;
                            wait *= 2;
                        }
                        Err(e) => {
                            eprintln!("hive-comb: removing a cell's cgroup: {e}");
                            break;
                        }
                    }
                }
            });
        }
        if let (Some(ns), Some(pool)) = (self.netns.take(), self.inner.netns.clone()) {
            if let (Some(veth), Some(net)) = (self.veth.take(), pool.net()) {
                net.release(&veth);
            }
            // The kernel frees the namespace itself once no mount and no process holds it, and
            // the cell's interface with it.
            tokio::spawn(async move {
                if let Err(e) = pool.remove(ns).await {
                    eprintln!("hive-comb: removing a cell's network namespace: {e}");
                }
            });
        }
    }

    /// Keeps an ended cell visible for a while, then forgets it.
    async fn linger(&mut self) {
        let ended = self.cell.status.borrow().changed;
        let until = ended + self.inner.cfg.keep_ended;
        let left = until.duration_since(SystemTime::now()).unwrap_or_default();
        let sleep = tokio::time::sleep(left);
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                () = &mut sleep => break,
                cmd = self.rx.recv() => match cmd {
                    Some(Cmd::Stop { done, .. }) => { let _ = done.send(()); }
                    Some(
                        Cmd::Pause { done }
                        | Cmd::Resume { done }
                        | Cmd::ExtendTtl { done, .. }
                        | Cmd::Ready { done },
                    ) => {
                        let _ = done.send(Err(not_running(self.cell.state())));
                    }
                    Some(Cmd::Snapshot { done, .. }) => {
                        let _ = done.send(Err(not_running(self.cell.state())));
                    }
                    Some(Cmd::Fork { done, .. }) => {
                        let _ = done.send(Err(not_running(self.cell.state())));
                    }
                    Some(Cmd::Quarantine { done }) => {
                        let _ = done.send(Err(not_running(self.cell.state())));
                    }
                    None => return,
                },
            }
        }
        if self.inner.wal.delete(self.id()).await.is_ok() {
            self.inner.forget(&self.cell);
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Timer {
    Hard,
    /// The setup boost is over.
    Boost,
    Idle,
    /// Idle for `pressure_idle` while the brake is on.
    Squeeze,
    /// Idle for `trim_idle` since it was last used or trimmed.
    Trim,
    Reclaim,
    PauseTtl,
}

/// Connects to a cell's guest agent and runs the handshake. With no `patience` it keeps trying
/// until the caller gives up, since a just started cell may not be listening yet.
pub(crate) async fn connect(
    channel: &GuestChannel,
    secret: &[u8; 32],
    patience: Option<Duration>,
) -> Result<Client, Error> {
    let started = Instant::now();
    let mut wait = Duration::from_millis(1);
    loop {
        // A guest agent that takes the connection and then never answers would otherwise hold
        // this up for good.
        let attempt = tokio::time::timeout(ATTEMPT, try_connect(channel, secret)).await;
        match attempt.unwrap_or_else(|_| Err(io::Error::from(io::ErrorKind::TimedOut))) {
            Ok(c) => return Ok(c),
            Err(e) => {
                if patience.is_some_and(|p| started.elapsed() >= p) {
                    return Err(Error::new(
                        Reason::DroneUnreachable,
                        format!("the guest agent did not answer: {e}"),
                    ));
                }
            }
        }
        tokio::time::sleep(wait).await;
        wait = (wait * 2).min(Duration::from_millis(50));
    }
}

async fn try_connect(channel: &GuestChannel, secret: &[u8; 32]) -> io::Result<Client> {
    let stream = match channel {
        GuestChannel::Unix(path) => UnixStream::connect(path).await?,
        GuestChannel::Vsock { uds, port } => {
            // Firecracker's host side of vsock: say which port, then read its OK line.
            let mut s = UnixStream::connect(uds).await?;
            s.write_all(format!("CONNECT {port}\n").as_bytes()).await?;
            let mut reader = BufReader::new(s);
            let mut line = String::new();
            reader.read_line(&mut line).await?;
            if !line.starts_with("OK ") {
                return Err(io::Error::other(format!("vsock said {:?}", line.trim())));
            }
            reader.into_inner()
        }
    };
    let nonce = crate::comb::random();
    let clock = now_ms().saturating_mul(1_000_000);
    Client::connect(stream, secret, clock, nonce).await
}

/// Checks now and then whether a starting cell is still there, and returns once it is not.
async fn died(driver: &dyn CellDriver, h: &CellHandle) -> Error {
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        match driver.check(h).await {
            Ok(Liveness::Gone(exit)) => {
                return Error::new(
                    Reason::DroneUnreachable,
                    format!("the cell ended before its guest agent answered: {exit:?}"),
                );
            }
            Ok(_) => {}
            Err(e) => return e,
        }
    }
}

fn not_running(s: CellState) -> Error {
    Error::new(Reason::CellNotRunning, format!("the cell is {s}"))
}

pub(crate) fn wal_error(e: &io::Error) -> Error {
    Error::new(Reason::Internal, format!("the node could not write its WAL: {e}"))
}

fn io_error(what: &str, e: &io::Error) -> Error {
    Error::new(Reason::Internal, format!("{what}: {e}"))
}

/// The wait before a cell's next trim, from the one before it: doubled up to [`TRIM_BACKOFF`] times
/// `base` when the cell read back at least half of the `given` bytes its last trim gave away, and
/// halved down to `base` when it did not. A trim that gave nothing away says nothing either way.
fn backoff(wait: Duration, base: Duration, given: u64, refaulted: u64) -> Duration {
    if given == 0 {
        wait
    } else if refaulted >= given / 2 {
        (wait * 2).min(base * TRIM_BACKOFF)
    } else {
        (wait / 2).max(base)
    }
}

/// Copies the writable layer `upper` of a cell that is not running into the new directory `into`.
#[cfg(target_os = "linux")]
async fn copy_once(upper: PathBuf, into: PathBuf) -> Result<(), Error> {
    blocking(move || {
        std::fs::create_dir_all(&into)?;
        hive_cell::tree::copy(&upper, &into).map(drop)
    })
    .await
}

/// Copies the writable layer `upper` of a running cell into the new directory `into`, for
/// [`finish`] to bring up to date once the cell is frozen.
#[cfg(target_os = "linux")]
async fn precopy(upper: PathBuf, into: PathBuf) -> Result<Precopy, Error> {
    blocking(move || {
        std::fs::create_dir_all(&into)?;
        Precopy::start(&upper, &into)
    })
    .await
}

#[cfg(target_os = "linux")]
async fn finish(pre: Precopy) -> Result<(), Error> {
    blocking(move || pre.finish().map(drop)).await
}

#[cfg(target_os = "linux")]
async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> Result<T, Error> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|_| Error::new(Reason::Internal, "the copy panicked"))?
        .map_err(|e| Error::new(Reason::Internal, format!("copying what the cell wrote: {e}")))
}

#[cfg(not(target_os = "linux"))]
#[derive(Debug)]
struct Precopy;

#[cfg(not(target_os = "linux"))]
async fn copy_once(_upper: PathBuf, _into: PathBuf) -> Result<(), Error> {
    Err(Error::new(Reason::PolicyDenied, "forks need a Linux node"))
}

#[cfg(not(target_os = "linux"))]
async fn precopy(_upper: PathBuf, _into: PathBuf) -> Result<Precopy, Error> {
    Err(Error::new(Reason::PolicyDenied, "forks need a Linux node"))
}

#[cfg(not(target_os = "linux"))]
async fn finish(_pre: Precopy) -> Result<(), Error> {
    Err(Error::new(Reason::PolicyDenied, "forks need a Linux node"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cell_that_reads_back_its_trims_is_trimmed_less_often() {
        let base = Duration::from_secs(30);
        let mib = 1 << 20;
        // Read back 40 of 64 MiB: the wait doubles, and stops at 16 times the base.
        let mut wait = base;
        for want in [60, 120, 240, 480, 480] {
            wait = backoff(wait, base, 64 * mib, 40 * mib);
            assert_eq!(wait, Duration::from_secs(want));
        }
        // Read back 1 of 64 MiB: it halves, and stops at the base.
        for want in [240, 120, 60, 30, 30] {
            wait = backoff(wait, base, 64 * mib, mib);
            assert_eq!(wait, Duration::from_secs(want));
        }
        assert_eq!(backoff(Duration::from_secs(120), base, 0, 5 * mib), Duration::from_secs(120));
    }
}
