//! One cell: the state everyone can read, and the actor that owns its transitions.
//!
//! Each cell has its own task. Every transition goes through it, so two requests for the same
//! cell never race, and a slow create or stop holds up nothing but its own cell. The actor writes
//! each new state to the WAL before it acts on it. Reads such as `get` and `list` don't go through
//! the actor at all. They read [`Cell::status`], which the actor updates after each transition.

use crate::admit::Reservation;
use crate::comb::Inner;
use crate::record::{Handle, Record, now_ms, time};
use hive_cell::{CellDriver, CellHandle, GuestChannel, Liveness, PauseMode, Slot};
use hive_drone::Client;
use hive_guard::wire::Veth;
use hive_types::{Cause, CellId, CellSpec, CellState, Error, IdleAction, Reason};
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
    Create { secret: [u8; 32], done: oneshot::Sender<Result<(), Error>> },
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
            Start::Create { secret, done } => {
                let result = self.create(secret).await;
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
        let status =
            Status { state, cause, message: message.to_owned(), changed: SystemTime::now() };
        self.cell.status.send_replace(status);
        self.inner.changed(&self.cell);
    }

    async fn create(&mut self, secret: [u8; 32]) -> Result<(), Error> {
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
        let result = match tokio::time::timeout(deadline, self.bring_up(secret)).await {
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

    async fn bring_up(&mut self, secret: [u8; 32]) -> Result<(), Error> {
        let inner = self.inner.clone();
        let backend = self.cell.spec.backend;
        let mut t = Instant::now();
        let mut lap = |stage: &str| {
            inner.metrics.stage(backend, stage, t.elapsed());
            t = Instant::now();
        };
        let slot = self.slot(secret).await?;
        lap("pool");
        let rootfs = self.inner.rootfs(&self.cell.spec, &slot).await?;
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
        if let Some(pool) = self.inner.cgroups.clone() {
            let (qos, r) = (self.cell.spec.qos, self.cell.spec.resources);
            let taken = tokio::task::spawn_blocking(move || pool.take(qos, &r))
                .await
                .map_err(|e| Error::new(Reason::Internal, e.to_string()))?;
            cgroup = taken.map_err(|e| io_error("setting up the cell's cgroup", &e))?;
            self.cgroup = Some(cgroup.clone());
        }
        let mut nameserver = None;
        if let Some(pool) = self.inner.netns.clone() {
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
                // The low bits of the sequence number are unique among every cell the node has at
                // once, and never go back to one that just ended.
                net.assign(veth, self.cell.id.seq() as u32, profile)
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
            self.veth = pool.recover(ns);
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
        match self.driver.check(&handle).await {
            Ok(Liveness::Gone(exit)) => {
                let cause = if exit.oom { Cause::Oom } else { Cause::Exited };
                self.stop(cause, Duration::ZERO).await;
                return false;
            }
            Ok(Liveness::Alive | Liveness::Paused) => {}
            Err(_) => {
                self.stop(Cause::Recovery, Duration::ZERO).await;
                return false;
            }
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
                    None => return,
                },
                () = lost => {
                    if !self.drone_lost().await {
                        return;
                    }
                }
                () = wake => {
                    let (_, what) = timer.expect("only wakes with a timer");
                    match what {
                        Timer::Hard => return self.stop(Cause::HardTtl, self.inner.cfg.stop_grace).await,
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
        let now_sys = SystemTime::now();
        let now = Instant::now();
        let at = |t: SystemTime| now + t.duration_since(now_sys).unwrap_or_default();
        let ttls = self.cell.ttls();
        let hard = ttls.hard.map(|ttl| (at(self.cell.created + ttl), Timer::Hard));
        let cfg = &self.inner.cfg;
        let (idle, reclaim, pause_ttl) = match (self.cell.state(), ttls.idle) {
            (CellState::Running, Some(ttl)) => {
                let last = time(self.cell.last_active_ms.load(Ordering::Relaxed));
                (Some((at(last + ttl), Timer::Idle)), None, None)
            }
            (CellState::Paused, _) => {
                let since = self.cell.status.borrow().changed;
                let reclaim = (!self.reclaimed && self.driver.caps().pause)
                    .then(|| (at(since + cfg.reclaim_after), Timer::Reclaim));
                (None, reclaim, Some((at(since + cfg.pause_ttl), Timer::PauseTtl)))
            }
            _ => (None, None, None),
        };
        [hard, idle, reclaim, pause_ttl].into_iter().flatten().min_by_key(|(at, _)| *at)
    }

    /// Swaps a paused cell's memory out. A failed reclaim leaves the cell frozen as it was, and is
    /// not tried again until the next pause.
    async fn reclaim(&mut self) {
        self.reclaimed = true;
        let handle = self.handle.clone().expect("a live cell has a handle");
        let _ = self.driver.pause(&handle, PauseMode::Reclaim).await;
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
                Ok(exit) if exit.oom && cause == Cause::Exited => cause = Cause::Oom,
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
                    Some(Cmd::Pause { done } | Cmd::Resume { done } | Cmd::ExtendTtl { done, .. }) => {
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
    Idle,
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
