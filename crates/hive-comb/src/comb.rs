//! The comb: the table of cells, the way into each cell's actor, and recovery after a restart.

use crate::admit::{self, Admission};
use crate::cell::{Actor, Cell, CellInfo, Cmd, Start, Status};
use crate::cgroups::Cgroups;
use crate::config::Config;
use crate::core_sched::CoreSched;
use crate::llm::Gateway;
use crate::metrics::Metrics;
use crate::net::Net;
use crate::netns::Namespaces;
use crate::pressure::Brake;
use crate::record::{Record, SEQ_KEY, Seq, time};
use crate::wal::Wal;
use hive_cell::{DriverRegistry, RootfsPlan, Slot};
use hive_drone::Client;
use hive_guard::Profile;
use hive_nectar::mount::{IdMap, Layers};
use hive_nectar::oci::load_manifest;
use hive_nectar::{BlobId, BlobStore, Cache, PosixStore, S3Config, S3Store};
use hive_proto::convert;
use hive_rt::{OsRng, Rng};
use hive_types::{Backend, Cause, CellId, CellSpec, CellState, Error, Reason, Source, is_name};
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{broadcast, oneshot, watch};
use tokio_util::sync::CancellationToken;

const SHARDS: usize = 64;
/// Sequence numbers are reserved in the WAL this many at a time, so most creates don't write
/// the counter.
const SEQ_BLOCK: u64 = 4096;
/// A key turned away for room is turned away again for between half this and this.
const TURNED_FOR: Duration = Duration::from_secs(600);
/// The most keys `Turned` holds in one generation.
const TURNED_MAX: usize = 65_536;

/// A request for a new cell.
#[derive(Clone, Debug)]
pub struct CreateRequest {
    /// What to make.
    pub spec: CellSpec,
    /// Who owns it.
    pub project: String,
    /// A retry with the same key in the same project gets the same cell back instead of a new one.
    pub idem_key: Option<String>,
    /// Make the cell if there is room even when this node turned the key away lately. A gate
    /// sets it when every node in the key's order turned the cell away.
    pub anyway: bool,
}

/// What the WAL has done since the comb opened. Writes over syncs is how many transitions each disk flush carried.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WalStats {
    /// Records written.
    pub writes: u64,
    /// Flushes to disk.
    pub syncs: u64,
    /// Times the log was rewritten down to its live records.
    pub compactions: u64,
}

/// A change to a cell, as sent to watchers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CellEvent {
    /// The cell.
    pub id: CellId,
    /// Where it is now.
    pub status: Status,
}

/// The node agent. Cloning it is cheap, and every clone is the same comb.
#[derive(Clone, Debug)]
pub struct Comb {
    pub(crate) inner: Arc<Inner>,
}

pub(crate) struct Inner {
    pub(crate) cfg: Config,
    pub(crate) wal: Wal,
    pub(crate) drivers: DriverRegistry,
    pub(crate) admission: Arc<Admission>,
    pub(crate) shutdown: CancellationToken,
    pub(crate) cgroups: Option<Arc<Cgroups>>,
    /// The core scheduling cookies, when the host takes them and the cells have cgroups.
    pub(crate) core: Option<CoreSched>,
    pub(crate) netns: Option<Arc<Namespaces>>,
    /// The LLM gateway, when cells have the guard's network.
    pub(crate) llm: Option<Arc<Gateway>>,
    images: Option<Nectar>,
    pub(crate) metrics: Metrics,
    /// Whether the pressure brake is on, which pauses idle cells and reclaims paused ones early.
    pub(crate) pressure: watch::Sender<bool>,
    pub(crate) shards: Vec<RwLock<HashMap<CellId, Arc<Cell>>>>,
    idem: Mutex<HashMap<(String, String), CellId>>,
    turned: Mutex<Turned>,
    seq: tokio::sync::Mutex<SeqBlock>,
    events: broadcast::Sender<CellEvent>,
}

fn at(what: &str, path: &std::path::Path) -> impl FnOnce(io::Error) -> io::Error {
    let what = format!("{what} {}", path.display());
    move |e| io::Error::new(e.kind(), format!("{what}: {e}"))
}

/// Images from a `hive-nectar` store, and the layers of them this node has mounted.
struct Nectar {
    store: Arc<dyn BlobStore>,
    layers: Layers,
}

impl std::fmt::Debug for Nectar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Nectar").field("layers", &self.layers).finish_non_exhaustive()
    }
}

impl Nectar {
    fn open(cfg: &Config) -> io::Result<Option<Self>> {
        let Some(dir) = &cfg.images.store else { return Ok(None) };
        let store: Arc<dyn BlobStore> = match dir.to_str().filter(|d| d.starts_with("http://")) {
            Some(url) => Arc::new(
                S3Config::from_url_and_env(url)
                    .and_then(S3Store::new)
                    .map_err(at("opening the image bucket", dir))?,
            ),
            None => Arc::new(PosixStore::open(dir).map_err(at("opening the image store", dir))?),
        };
        let i = &cfg.images;
        let cache = Cache::open(&i.cache_dir, i.cache_bytes)
            .map_err(at("opening the image cache", &i.cache_dir))?;
        // Every cell has the same ids, so one map serves every layer.
        let c = &cfg.container;
        let idmap = IdMap::new(c.uid_base, c.uid_count)
            .map_err(|e| io::Error::new(e.kind(), format!("making the id map for layers: {e}")))?;
        let mut layers = Layers::new(&i.layers_dir, cache, Some(idmap))
            .map_err(at("clearing the layer mounts in", &i.layers_dir))?;
        if i.lazy {
            layers = layers.lazily();
        }
        Ok(Some(Self { store, layers }))
    }
}

#[derive(Debug)]
struct SeqBlock {
    next: u64,
    reserved: u64,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Comb")
            .field("node", &self.cfg.node)
            .field("drivers", &self.drivers)
            .finish()
    }
}

impl Comb {
    /// Opens the WAL in `cfg.data_dir`, finds every cell it knows about and takes them back:
    /// running cells get their guest agent reconnected, half made ones are cleaned up, and ended
    /// ones stay visible until their time is up. Returns once every cell has been looked at.
    pub async fn open(cfg: Config, drivers: DriverRegistry) -> io::Result<Self> {
        let (wal, replay) = Wal::open(&cfg.data_dir.join("wal"))?;
        if replay.cut > 0 {
            eprintln!("hive-comb: cut {} damaged bytes off the end of the WAL", replay.cut);
        }
        let mem = cfg
            .mem_mib
            .map(|m| m << 20)
            .or_else(|| admit::mem_total().map(|t| t.saturating_sub(cfg.reserved_mem_mib << 20)))
            .unwrap_or(0);
        let admission = Arc::new(Admission::new(mem, cfg.max_cells, &cfg.create_limit));
        let cgroups = match &cfg.cgroup_root {
            Some(root) => Some(Arc::new(Cgroups::init(root, cfg.cgroup_depth).map_err(|e| {
                io::Error::new(e.kind(), format!("setting up cgroups in {}: {e}", root.display()))
            })?)),
            None => None,
        };
        let core = (cfg.core_scheduling && cgroups.is_some())
            .then(|| {
                CoreSched::new()
                    .map_err(|e| eprintln!("hive-comb: cells run without core scheduling: {e}"))
                    .ok()
            })
            .flatten();
        let netns = match &cfg.netns_dir {
            Some(dir) => {
                let net = cfg.network.guard.then(|| Net::open(&cfg.network)).and_then(|n| {
                    n.map_err(|e| {
                        eprintln!(
                            "hive-comb: cells get loopback only, the guard did not load: {e}"
                        );
                    })
                    .ok()
                });
                Some(Arc::new(Namespaces::init(dir, cfg.netns_depth, net.map(Arc::new)).map_err(
                    |e| io::Error::new(e.kind(), format!("setting up {}: {e}", dir.display())),
                )?))
            }
            None => None,
        };
        let llm = match netns.as_ref().and_then(|p| p.net()) {
            Some(_) => Some(Arc::new(Gateway::new(&cfg.network.llm)?)),
            None => None,
        };
        let images = Nectar::open(&cfg)?;
        let mut records = replay.records;
        let next = records.remove(&SEQ_KEY).and_then(|b| Seq::decode(b).ok()).map_or(1, |s| s.next);
        let inner = Arc::new(Inner {
            cfg,
            wal,
            drivers,
            admission,
            shutdown: CancellationToken::new(),
            cgroups,
            core,
            netns,
            llm,
            images,
            metrics: Metrics::default(),
            pressure: watch::Sender::new(false),
            shards: (0..SHARDS).map(|_| RwLock::default()).collect(),
            idem: Mutex::default(),
            turned: Mutex::default(),
            // Whatever was reserved before the restart may have been handed out, so the new block
            // starts past all of it.
            seq: tokio::sync::Mutex::new(SeqBlock { next, reserved: next }),
            events: broadcast::channel(4096).0,
        });
        std::fs::create_dir_all(inner.cfg.data_dir.join("cells"))?;
        let comb = Self { inner };
        let (claimed, claimed_netns) = comb.recover(records).await;
        comb.fence().await;
        if let Some(pool) = &comb.inner.netns {
            let swept = pool.sweep(&claimed_netns).await?;
            if swept > 0 {
                eprintln!("hive-comb: removed {swept} network namespaces no cell claims");
            }
            tokio::spawn(pool.clone().refill(comb.inner.shutdown.clone()));
            if let Some(net) = pool.net() {
                let (serve, stop) = (net.dns(), comb.inner.shutdown.clone());
                tokio::spawn(async move {
                    tokio::select! {
                        () = serve => {}
                        () = stop.cancelled() => {}
                    }
                });
                if let Some(llm) = &comb.inner.llm {
                    let stop = comb.inner.shutdown.clone();
                    tokio::spawn(llm.clone().serve(comb.inner.clone(), net.clone(), stop));
                }
            }
        }
        if let Some(pool) = &comb.inner.cgroups {
            let swept = pool.sweep(&claimed).await?;
            if swept > 0 {
                eprintln!("hive-comb: removed {swept} cgroups no cell claims");
            }
            tokio::spawn(pool.clone().refill(comb.inner.shutdown.clone()));
        }
        let inner = &comb.inner;
        if inner.cfg.psi_stop_admit > 0.0 {
            let brake = Brake::new(
                inner.cfg.psi_source.as_deref(),
                inner.cfg.cgroup_root.as_deref(),
                inner.cfg.psi_stop_admit * 100.0,
            );
            tokio::spawn(brake.run(
                inner.admission.clone(),
                inner.pressure.clone(),
                inner.metrics.clone(),
                inner.shutdown.clone(),
            ));
        }
        Ok(comb)
    }

    /// Starts an actor for every cell in the WAL. Returns the cgroups and network namespaces the
    /// live ones hold.
    async fn recover(
        &self,
        records: HashMap<u128, bytes::Bytes>,
    ) -> (HashSet<PathBuf>, HashSet<PathBuf>) {
        let inner = &self.inner;
        let mut waits = Vec::new();
        let mut known = HashSet::new();
        let mut claimed = HashSet::new();
        let mut claimed_netns = HashSet::new();
        for (key, bytes) in records {
            let id = CellId::from_bits(key);
            known.insert(inner.cell_dir(id));
            let Ok(record) = Record::decode(bytes) else {
                eprintln!("hive-comb: dropping an unreadable WAL record for {id}");
                let _ = inner.wal.delete(key).await;
                continue;
            };
            let Ok(spec) = record.spec() else {
                eprintln!("hive-comb: dropping a WAL record with a bad spec for {id}");
                let _ = inner.wal.delete(key).await;
                continue;
            };
            let state = record.cell_state().unwrap_or(CellState::Failed);
            if let Some(h) = record.handle.as_ref()
                && !state.is_terminal()
            {
                if !h.cgroup.is_empty() {
                    claimed.insert(PathBuf::from(&h.cgroup));
                }
                if !h.netns.is_empty() {
                    claimed_netns.insert(PathBuf::from(&h.netns));
                }
            }
            let status = Status {
                state,
                cause: record.cell_cause(),
                message: record.message.clone(),
                changed: time(record.changed_ms),
            };
            let (cell, rx) = Cell::new(
                id,
                record.project.clone(),
                record.idem_key.clone(),
                spec,
                time(record.created_ms),
                status,
            );
            inner.insert(&cell);
            let Some(driver) = inner.drivers.get(cell.spec.backend).cloned() else {
                eprintln!(
                    "hive-comb: no {} driver for {id}, which stays as it is",
                    cell.spec.backend
                );
                continue;
            };
            let reservation = (!state.is_terminal()).then(|| inner.admission.adopt(&cell.spec));
            let actor =
                Actor::new(inner.clone(), cell.clone(), driver, rx, reservation, record.clone());
            let (done, recovered) = oneshot::channel();
            tokio::spawn(actor.run(Start::Recover { record: Box::new(record), done }));
            waits.push(recovered);
        }
        let _ =
            tokio::time::timeout(Duration::from_secs(30), futures::future::join_all(waits)).await;
        // A directory with no record is left from a cell whose record was dropped.
        if let Ok(dirs) = std::fs::read_dir(inner.cfg.data_dir.join("cells")) {
            for d in dirs.flatten() {
                if !known.contains(&d.path()) {
                    let _ = std::fs::remove_dir_all(d.path());
                }
            }
        }
        (claimed, claimed_netns)
    }

    /// Stops the cells left from an older epoch of this node. The node registered again since,
    /// so callers were told those cells are lost, and they must not go on running.
    async fn fence(&self) {
        let epoch = self.inner.cfg.epoch;
        let n = self.end(|c| c.id.epoch() < epoch).await;
        if n > 0 {
            eprintln!("hive-comb: stopped {n} cells from before epoch {epoch}");
        }
    }

    /// Stops every cell on the node because it lost its lease. Keeper may already have told
    /// callers the node is gone, and a gate may be making their keyed cells again elsewhere, so
    /// none of these may go on running until the next start fences them.
    pub async fn lose(&self) {
        let n = self.end(|_| true).await;
        eprintln!("hive-comb: lost the lease, stopped {n} cells");
    }

    /// Stops the cells that `pick` picks and have not ended, as lost with the node, and returns
    /// how many. Gives up waiting on them after 30 seconds.
    async fn end(&self, pick: impl Fn(&Cell) -> bool) -> usize {
        let cells: Vec<Arc<Cell>> = self
            .inner
            .shards
            .iter()
            .flat_map(|s| {
                let s = s.read().unwrap_or_else(PoisonError::into_inner);
                s.values().filter(|c| pick(c)).cloned().collect::<Vec<_>>()
            })
            .filter(|c| !c.state().is_terminal())
            .collect();
        let stops = cells.iter().map(|cell| async move {
            let (done, wait) = oneshot::channel();
            let cmd = Cmd::Stop { cause: Cause::NodeLost, grace: Duration::ZERO, done };
            if cell.send(cmd).await {
                let _ = wait.await;
            }
        });
        let _ =
            tokio::time::timeout(Duration::from_secs(30), futures::future::join_all(stops)).await;
        cells.len()
    }

    /// Makes a cell and returns once it is running, or once it has failed.
    pub async fn create(&self, req: CreateRequest) -> Result<CellInfo, Error> {
        let inner = &self.inner;
        req.spec.validate().map_err(|e| Error::new(Reason::InvalidArgument, e.to_string()))?;
        if req.spec.backend == Backend::Auto {
            return Err(Error::new(
                Reason::InvalidArgument,
                "pick a backend, auto is not supported yet",
            ));
        }
        image_name(&req.spec)?;
        inner.profile(&req.spec.network_profile)?;
        let driver = inner.drivers.get(req.spec.backend).cloned().ok_or_else(|| {
            Error::new(
                Reason::CapacityUnavailable,
                format!("this node has no {} backend", req.spec.backend),
            )
        })?;
        let idem = req.idem_key.filter(|k| !k.is_empty());
        if let Some(key) = &idem {
            let existing = inner.lock_idem().get(&(req.project.clone(), key.clone())).copied();
            if let Some(id) = existing
                && let Some(cell) = inner.cell(id)
            {
                return wait_started(&cell).await;
            }
            if !req.anyway && inner.lock_turned().has(&req.project, key, Instant::now()) {
                return Err(Error::new(
                    Reason::CapacityUnavailable,
                    "this node turned the key away for room lately, so it goes where it went then",
                ));
            }
        }
        let reservation = match inner.admission.reserve(&req.spec) {
            Ok(r) => r,
            Err(e) => {
                if let Some(key) = &idem
                    && e.reason == Reason::CapacityUnavailable
                {
                    inner.lock_turned().add(&req.project, key, Instant::now());
                }
                return Err(e);
            }
        };
        let id = inner.next_id().await?;
        let secret = random();
        let now = SystemTime::now();
        let status =
            Status { state: CellState::Pending, cause: None, message: String::new(), changed: now };
        let idem_key = idem.clone().unwrap_or_default();
        let (cell, rx) =
            Cell::new(id, req.project.clone(), idem_key.clone(), req.spec.clone(), now, status);
        let record = Record {
            spec: Some(convert::spec_to_v1(&req.spec)),
            created_ms: crate::record::ms(now),
            project: req.project.clone(),
            idem_key,
            ..Record::default()
        };
        if let Some(key) = idem {
            // Two creates with one key at once: the first to get here wins, the other waits on it.
            let winner = {
                let mut map = inner.lock_idem();
                let other =
                    map.get(&(req.project.clone(), key.clone())).and_then(|&o| inner.cell(o));
                if other.is_none() {
                    inner.lock_turned().remove(&req.project, &key);
                    map.insert((req.project, key), id);
                }
                other
            };
            if let Some(cell) = winner {
                return wait_started(&cell).await;
            }
        }
        inner.insert(&cell);
        let actor = Actor::new(inner.clone(), cell.clone(), driver, rx, Some(reservation), record);
        let (done, started) = oneshot::channel();
        tokio::spawn(actor.run(Start::Create { secret, done }));
        match started.await {
            Ok(Ok(())) => Ok(cell.info()),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(Error::new(Reason::Internal, "the node is shutting down")),
        }
    }

    /// The cell with `id`.
    pub fn get(&self, id: CellId) -> Result<CellInfo, Error> {
        self.inner.find(id).map(|c| c.info())
    }

    /// Every cell this node knows about, live or recently ended.
    #[must_use]
    pub fn list(&self) -> Vec<CellInfo> {
        let mut all = Vec::new();
        for shard in &self.inner.shards {
            let shard = shard.read().unwrap_or_else(PoisonError::into_inner);
            all.extend(shard.values().map(|c| c.info()));
        }
        all.sort_by_key(|c| c.id);
        all
    }

    /// Stops a cell and returns once it has ended. `grace` defaults to the configured one.
    pub async fn stop(&self, id: CellId, grace: Option<Duration>) -> Result<CellInfo, Error> {
        let cell = self.inner.find(id)?;
        if cell.state().is_terminal() {
            return Ok(cell.info());
        }
        let grace = grace.unwrap_or(self.inner.cfg.stop_grace);
        let (done, wait) = oneshot::channel();
        // A cell still starting gets the stop once its create is over, since the actor only reads
        // its queue once the cell is up.
        if cell.send(Cmd::Stop { cause: Cause::Requested, grace, done }).await {
            let _ = wait.await;
        }
        Ok(cell.info())
    }

    /// Pauses a running cell.
    pub async fn pause(&self, id: CellId) -> Result<CellInfo, Error> {
        let cell = self.inner.find(id)?;
        let (done, wait) = oneshot::channel();
        if !cell.send(Cmd::Pause { done }).await {
            return Err(Error::new(Reason::Internal, "the node is shutting down"));
        }
        wait.await.map_err(|_| Error::new(Reason::Internal, "the cell's actor went away"))??;
        Ok(cell.info())
    }

    /// Resumes a paused cell.
    pub async fn resume(&self, id: CellId) -> Result<CellInfo, Error> {
        let cell = self.inner.find(id)?;
        let (done, wait) = oneshot::channel();
        if !cell.send(Cmd::Resume { done }).await {
            return Err(Error::new(Reason::Internal, "the node is shutting down"));
        }
        wait.await.map_err(|_| Error::new(Reason::Internal, "the cell's actor went away"))??;
        Ok(cell.info())
    }

    /// Changes a live cell's timers. `hard` is how long from now until the cell is stopped, and
    /// `idle` replaces its idle TTL. Either is left as it was when `None`.
    pub async fn extend_ttl(
        &self,
        id: CellId,
        hard: Option<Duration>,
        idle: Option<Duration>,
    ) -> Result<CellInfo, Error> {
        let cell = self.inner.find(id)?;
        let (done, wait) = oneshot::channel();
        if !cell.send(Cmd::ExtendTtl { hard, idle, done }).await {
            return Err(Error::new(Reason::Internal, "the node is shutting down"));
        }
        wait.await.map_err(|_| Error::new(Reason::Internal, "the cell's actor went away"))??;
        Ok(cell.info())
    }

    /// Ends a live cell's setup boost, so it drops to its steady CPU quota. A cell without one is
    /// left as it is.
    pub async fn ready(&self, id: CellId) -> Result<CellInfo, Error> {
        let cell = self.inner.find(id)?;
        let (done, wait) = oneshot::channel();
        if !cell.send(Cmd::Ready { done }).await {
            return Err(Error::new(Reason::Internal, "the node is shutting down"));
        }
        wait.await.map_err(|_| Error::new(Reason::Internal, "the cell's actor went away"))??;
        Ok(cell.info())
    }

    /// The guest agent client for a running cell, for exec and file calls. A paused cell is
    /// resumed first, since a request is what wakes it, waiting at most `resume_timeout`.
    pub async fn drone(&self, id: CellId) -> Result<Client, Error> {
        let cell = self.inner.find(id)?;
        if cell.state() == CellState::Paused {
            let wait = self.inner.cfg.resume_timeout;
            tokio::time::timeout(wait, self.resume(id)).await.map_err(|_| {
                Error::new(
                    Reason::CellNotRunning,
                    format!("the cell did not resume within {wait:?}"),
                )
            })??;
        }
        match (cell.state(), cell.drone()) {
            (CellState::Running, Some(c)) if !c.is_closed() => Ok(c),
            (CellState::Running, _) => Err(Error::new(
                Reason::DroneUnreachable,
                "the channel to the guest agent dropped and is being made again",
            )),
            (s, _) => Err(Error::new(Reason::CellNotRunning, format!("the cell is {s}"))),
        }
    }

    /// Every change to every cell from now on. A watcher that falls more than 4096 changes
    /// behind misses some and gets told how many.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<CellEvent> {
        self.inner.events.subscribe()
    }

    /// Cells and bytes of memory committed right now.
    #[must_use]
    pub fn committed(&self) -> (usize, u64) {
        self.inner.admission.committed()
    }

    /// The comb's metrics, for the `/metrics` endpoint.
    #[must_use]
    pub fn metrics(&self) -> &Metrics {
        &self.inner.metrics
    }

    /// What the WAL has done since the comb opened.
    #[must_use]
    pub fn wal_stats(&self) -> WalStats {
        let stats = self.inner.wal.stats();
        WalStats {
            writes: stats.writes.load(Ordering::Relaxed),
            syncs: stats.syncs.load(Ordering::Relaxed),
            compactions: stats.compactions.load(Ordering::Relaxed),
        }
    }

    /// Empty cgroups ready for new cells, per QoS class, latency first. `None` when the comb runs
    /// without cgroups.
    #[must_use]
    pub fn spare_cgroups(&self) -> Option<[usize; 3]> {
        self.inner.cgroups.as_ref().map(|c| c.depths())
    }

    /// Network namespaces ready for new cells. `None` when cells run in the host's network.
    #[must_use]
    pub fn spare_netns(&self) -> Option<usize> {
        self.inner.netns.as_ref().map(|n| n.depth())
    }

    /// Stops every actor and leaves every cell exactly as it is, for the next comb to take over.
    /// Returns once the WAL is closed, so a new comb can open it straight away.
    pub async fn shutdown(&self) {
        self.inner.shutdown.cancel();
        self.inner.wal.close().await;
    }
}

impl Inner {
    /// The names of the image layers mounted on this node.
    pub(crate) fn mounted_layers(&self) -> Vec<[u8; 32]> {
        self.images
            .as_ref()
            .map_or_else(Vec::new, |n| n.layers.mounted().iter().map(|d| *d.as_bytes()).collect())
    }

    /// The guard's profile for a spec's `network_profile`, or `None` when cells have loopback only,
    /// which serves `none` and nothing else.
    pub(crate) fn profile(&self, name: &str) -> Result<Option<Profile>, Error> {
        let net = self.netns.as_ref().and_then(|p| p.net());
        match (net, net.and_then(|n| n.profile(name))) {
            (Some(_), Some(p)) => Ok(Some(p)),
            (None, _) if name == "none" => Ok(None),
            _ => Err(Error::new(
                Reason::CapacityUnavailable,
                format!("this node has no {name:?} network profile"),
            )),
        }
    }

    fn shard(&self, id: CellId) -> &RwLock<HashMap<CellId, Arc<Cell>>> {
        // The low bits are the tag, which is random, so cells spread evenly.
        &self.shards[(id.to_bits() as usize) % SHARDS]
    }

    fn cell(&self, id: CellId) -> Option<Arc<Cell>> {
        self.shard(id).read().unwrap_or_else(PoisonError::into_inner).get(&id).cloned()
    }

    /// The cell with `id`, or why there is none. An id from an older epoch of this node names a
    /// cell that was fenced when the node registered again, so it is lost rather than not found.
    pub(crate) fn find(&self, id: CellId) -> Result<Arc<Cell>, Error> {
        if let Some(cell) = self.cell(id) {
            return Ok(cell);
        }
        if id.node() == self.cfg.node && id.epoch() < self.cfg.epoch {
            return Err(Error::new(
                Reason::CellLost,
                format!(
                    "cell {id} is from epoch {}, and this node is in {}",
                    id.epoch(),
                    self.cfg.epoch
                ),
            ));
        }
        Err(not_found(id))
    }

    fn insert(&self, cell: &Arc<Cell>) {
        self.shard(cell.id)
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(cell.id, cell.clone());
        if !cell.idem_key.is_empty() {
            self.lock_idem().insert((cell.project.clone(), cell.idem_key.clone()), cell.id);
        }
    }

    pub(crate) fn forget(&self, cell: &Cell) {
        self.shard(cell.id).write().unwrap_or_else(PoisonError::into_inner).remove(&cell.id);
        if !cell.idem_key.is_empty() {
            let mut map = self.lock_idem();
            let key = (cell.project.clone(), cell.idem_key.clone());
            if map.get(&key) == Some(&cell.id) {
                map.remove(&key);
            }
        }
    }

    fn lock_idem(&self) -> std::sync::MutexGuard<'_, HashMap<(String, String), CellId>> {
        self.idem.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn lock_turned(&self) -> std::sync::MutexGuard<'_, Turned> {
        self.turned.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn changed(&self, cell: &Cell) {
        let status = cell.status.borrow().clone();
        let _ = self.events.send(CellEvent { id: cell.id, status });
    }

    pub(crate) fn cell_dir(&self, id: CellId) -> PathBuf {
        self.cfg.data_dir.join("cells").join(id.to_string())
    }

    /// Where a cell's root filesystem comes from. An image or template name is looked up in
    /// `data_dir/images`, where a directory is an unpacked root filesystem and a file holds the id
    /// of an image in the store, whose layers are fetched and mounted the first time a cell needs
    /// them.
    pub(crate) async fn rootfs(&self, spec: &CellSpec, slot: &Slot) -> Result<RootfsPlan, Error> {
        let name = image_name(spec)?;
        let path = self.cfg.data_dir.join("images").join(name);
        let upper = slot.dir.join("upper");
        let missing = || Error::new(Reason::ImageUnavailable, format!("no image named {name}"));
        match std::fs::metadata(&path) {
            Ok(m) if m.is_dir() => return Ok(RootfsPlan { lowers: vec![path], upper }),
            Ok(m) if m.is_file() => {}
            _ => return Err(missing()),
        }
        let Some(nectar) = &self.images else {
            return Err(Error::new(
                Reason::ImageUnavailable,
                format!("{name} is in an image store, and this node has none set up"),
            ));
        };
        let unavailable =
            |e: String| Error::new(Reason::ImageUnavailable, format!("image {name}: {e}"));
        let text =
            tokio::fs::read_to_string(&path).await.map_err(|e| unavailable(e.to_string()))?;
        let id: BlobId =
            text.trim().parse().map_err(|e: hive_nectar::BadBlobId| unavailable(e.to_string()))?;
        let manifest =
            load_manifest(&*nectar.store, id).await.map_err(|e| unavailable(e.to_string()))?;
        let lowers = nectar
            .layers
            .mount(&nectar.store, &manifest)
            .await
            .map_err(|e| unavailable(e.to_string()))?;
        Ok(RootfsPlan { lowers, upper })
    }

    async fn next_id(&self) -> Result<CellId, Error> {
        let mut seq = self.seq.lock().await;
        if seq.next >= seq.reserved {
            let reserved = seq.next + SEQ_BLOCK;
            let bytes = Seq { next: reserved }.encode_to_vec().into();
            self.wal.put(SEQ_KEY, bytes).await.map_err(|e| crate::cell::wal_error(&e))?;
            seq.reserved = reserved;
        }
        let n = seq.next;
        seq.next += 1;
        drop(seq);
        // The tag will be a MAC under the unit's key once the gate checks it. For now it is
        // random, which still makes ids hard to guess.
        let tag = u64::from_le_bytes(random()[..8].try_into().expect("eight bytes")) >> 16;
        let c = &self.cfg;
        CellId::new(c.unit, c.node, c.epoch, n, tag)
            .ok_or_else(|| Error::new(Reason::Internal, "ran out of cell sequence numbers"))
    }
}

/// Waits for a cell someone else is creating to be running, or to fail.
async fn wait_started(cell: &Cell) -> Result<CellInfo, Error> {
    let mut rx = cell.status.subscribe();
    let status = rx
        .wait_for(|s| {
            !matches!(s.state, CellState::Pending | CellState::Preparing | CellState::Starting)
        })
        .await
        .map_err(|_| Error::new(Reason::Internal, "the node is shutting down"))?
        .clone();
    if status.state == CellState::Failed && status.cause == Some(Cause::StartFailed) {
        return Err(Error::new(Reason::Internal, status.message));
    }
    Ok(cell.info())
}

/// The image a cell starts from, if its name is one the comb can look up.
fn image_name(spec: &CellSpec) -> Result<&str, Error> {
    let name = match &spec.source {
        Source::Image(n) | Source::Template(n) => n,
        Source::Snapshot(_) => {
            return Err(Error::new(Reason::InvalidArgument, "snapshots are not supported yet"));
        }
    };
    if !is_name(name) || name.split('/').any(|p| p.is_empty() || p == "." || p == "..") {
        return Err(Error::new(Reason::InvalidArgument, format!("{name:?} is not an image name")));
    }
    Ok(name)
}

fn not_found(id: CellId) -> Error {
    Error::new(Reason::CellNotFound, format!("no cell {id} on this node"))
}

pub(crate) fn random() -> [u8; 32] {
    OsRng.secret()
}

/// The keys this node turned away for room lately. A gate sends a keyed cell to the nodes in
/// its key's order, top first, so when a retry comes back here after the cell was made further
/// down, turning it away again takes it to that cell rather than making a second one. Two
/// generations of half of `TURNED_FOR` each keep it small without a timer.
#[derive(Default)]
struct Turned {
    new: HashSet<(String, String)>,
    old: HashSet<(String, String)>,
    since: Option<Instant>,
}

impl Turned {
    fn roll(&mut self, now: Instant) {
        let age = self.since.map(|s| now.saturating_duration_since(s));
        match age {
            Some(a) if a < TURNED_FOR / 2 => return,
            Some(a) if a < TURNED_FOR => self.old = std::mem::take(&mut self.new),
            _ => {
                self.old.clear();
                self.new.clear();
            }
        }
        self.since = Some(now);
    }

    fn add(&mut self, project: &str, key: &str, now: Instant) {
        self.roll(now);
        if self.new.len() < TURNED_MAX {
            self.new.insert((project.to_owned(), key.to_owned()));
        }
    }

    fn has(&mut self, project: &str, key: &str, now: Instant) -> bool {
        self.roll(now);
        let k = (project.to_owned(), key.to_owned());
        self.new.contains(&k) || self.old.contains(&k)
    }

    fn remove(&mut self, project: &str, key: &str) {
        let k = (project.to_owned(), key.to_owned());
        self.new.remove(&k);
        self.old.remove(&k);
    }
}
