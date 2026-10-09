//! The driver: the engine every cell shares, and each cell's drone on its socket.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures::future::BoxFuture;
use hive_cell::{
    CellDriver, CellHandle, CellMetrics, DriverCaps, ExitInfo, GuestChannel, Liveness, NodeFit,
    Result, RootfsPlan, Slot,
};
use hive_drone::Drone;
use hive_types::{Backend, CellId, CellSpec, Error, Reason, Source};
use tokio::net::UnixListener;
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};
use wasmtime::{Engine, InstanceAllocationStrategy, Linker, PoolingAllocationConfig};

use crate::run::{CellRunner, Ctx, Missing, Shared, Usage, is_program_name};

/// How often the engine's epoch moves on, which is how often a running program gives its thread
/// back and how close to its timeout it is cut off.
pub const TICK: Duration = Duration::from_millis(10);

/// How the node runs wasm cells.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    /// Where the programs are, each a WASI command module named `<name>.wasm`. A file that
    /// changes is compiled again on its next run.
    pub modules: PathBuf,
    /// Host directories every cell sees read only, by the absolute path they have in the cell,
    /// such as a language's standard library.
    pub mounts: BTreeMap<String, PathBuf>,
    /// How many programs may run at once over all cells. The pooling allocator keeps this many
    /// instances, memories and stacks ready, as address space and not memory.
    pub instances: u32,
    /// The most memory a program may have, in MiB. A cell's own `mem_mib` is its limit, and it
    /// may not ask for more than this.
    pub max_mem_mib: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            modules: PathBuf::from("/var/lib/hivebox/wasm"),
            mounts: BTreeMap::new(),
            instances: 1000,
            max_mem_mib: 4096,
        }
    }
}

/// The tier 0 driver. Programs run on wasmtime inside the node agent, and each cell's drone runs
/// there too, serving the cell's socket like the drone in any other cell.
#[derive(Debug)]
pub struct WasmDriver {
    shared: Arc<Shared>,
    max_mem_mib: u64,
    cells: Mutex<HashMap<CellId, Cell>>,
}

#[derive(Debug)]
struct Cell {
    drone: Arc<Drone>,
    serving: Option<JoinHandle<()>>,
    stop: watch::Sender<bool>,
    usage: Arc<Usage>,
}

impl WasmDriver {
    /// A driver with `cfg`. It makes the engine and its pool, and a thread that moves the epoch
    /// on every [`TICK`] for as long as the engine is in use.
    ///
    /// # Errors
    ///
    /// When `cfg` asks for something wasmtime cannot do, such as memories larger than 4 GiB.
    pub fn new(cfg: Config) -> Result<Self> {
        let bad = |what: &str, e: wasmtime::Error| {
            Error::new(Reason::InvalidArgument, format!("wasm cells: {what}: {e:#}"))
        };
        if cfg.max_mem_mib == 0 || cfg.max_mem_mib > 4096 {
            return Err(Error::new(
                Reason::InvalidArgument,
                "wasm cells: max_mem_mib has to be from 1 to 4096",
            ));
        }
        for (guest, host) in &cfg.mounts {
            if !guest.starts_with('/') || guest == "/" || !host.is_absolute() {
                return Err(Error::new(
                    Reason::InvalidArgument,
                    format!(
                        "wasm cells: mount {guest:?} has to be an absolute path below /, from an absolute one"
                    ),
                ));
            }
        }
        let n = cfg.instances.max(1);
        let mut pool = PoolingAllocationConfig::default();
        pool.total_core_instances(n)
            .total_memories(n)
            .total_tables(n)
            .total_stacks(n)
            .max_memory_size(usize::try_from(cfg.max_mem_mib << 20).unwrap_or(usize::MAX));
        let mut wc = wasmtime::Config::new();
        wc.async_support(true)
            .epoch_interruption(true)
            .allocation_strategy(InstanceAllocationStrategy::Pooling(pool));
        let engine = Engine::new(&wc).map_err(|e| bad("the engine", e))?;
        let mut linker = Linker::new(&engine);
        wasmtime_wasi::preview1::add_to_linker_async(&mut linker, |c: &mut Ctx| c.wasi())
            .map_err(|e| bad("WASI", e))?;
        let weak = engine.weak();
        std::thread::Builder::new()
            .name("hive-wasm-epoch".into())
            .spawn(move || {
                while let Some(engine) = weak.upgrade() {
                    engine.increment_epoch();
                    drop(engine);
                    std::thread::sleep(TICK);
                }
            })
            .map_err(|e| Error::new(Reason::Internal, format!("the epoch thread: {e}")))?;
        let mounts = cfg.mounts.into_iter().collect();
        Ok(Self {
            shared: Arc::new(Shared::new(engine, linker, cfg.modules, mounts)),
            max_mem_mib: cfg.max_mem_mib,
            cells: Mutex::new(HashMap::new()),
        })
    }

    /// Compiles the program called `name` now, so the first cell that runs it does not wait.
    ///
    /// # Errors
    ///
    /// `IMAGE_UNAVAILABLE` when there is no such program or it does not compile.
    pub async fn warm(&self, name: &str) -> Result<()> {
        self.shared.program(name).await.map(drop).map_err(|e| self.missing(name, e))
    }

    /// Compiles every program in the modules directory, one after another, and gives how long
    /// each took or why it did not compile. The node agent runs this when it starts.
    pub async fn warm_all(&self) -> Vec<(String, Result<Duration>)> {
        let mut names: Vec<String> = std::fs::read_dir(&self.shared.modules)
            .into_iter()
            .flatten()
            .filter_map(|e| {
                let name = e.ok()?.file_name().into_string().ok()?;
                name.strip_suffix(".wasm").filter(|n| is_program_name(n)).map(str::to_owned)
            })
            .collect();
        names.sort();
        let mut out = Vec::with_capacity(names.len());
        for name in names {
            let t = std::time::Instant::now();
            let r = self.warm(&name).await.map(|()| t.elapsed());
            out.push((name, r));
        }
        out
    }

    fn missing(&self, name: &str, e: Missing) -> Error {
        let message = match e {
            Missing::NotFound => {
                format!("no wasm program {name}.wasm in {}", self.shared.modules.display())
            }
            Missing::Bad(e) => format!("wasm program {name}: {e}"),
        };
        Error::new(Reason::ImageUnavailable, message)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<CellId, Cell>> {
        self.cells.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl CellDriver for WasmDriver {
    fn backend(&self) -> Backend {
        Backend::Fncall
    }

    fn caps(&self) -> DriverCaps {
        DriverCaps::default()
    }

    fn probe(&self) -> BoxFuture<'_, Result<NodeFit>> {
        Box::pin(async move {
            let dir = &self.shared.modules;
            let mut notes = Vec::new();
            if !dir.is_dir() {
                notes.push(format!("no programs directory at {}", dir.display()));
            }
            Ok(NodeFit { ready: notes.is_empty(), notes })
        })
    }

    fn prepare<'a>(
        &'a self,
        id: CellId,
        spec: &'a CellSpec,
        _rootfs: &'a RootfsPlan,
        slot: &'a Slot,
    ) -> BoxFuture<'a, Result<CellHandle>> {
        Box::pin(async move {
            let name = match &spec.source {
                Source::Image(n) | Source::Template(n) => n.as_str(),
                Source::Snapshot(_) => {
                    return Err(Error::new(Reason::PolicyDenied, "wasm cells have no snapshots"));
                }
            };
            if spec.network_profile != "none" {
                return Err(Error::new(
                    Reason::PolicyDenied,
                    "wasm cells have no network, so their profile has to be none",
                ));
            }
            let mem_mib = u64::from(spec.resources.mem_mib);
            if mem_mib > self.max_mem_mib {
                return Err(Error::new(
                    Reason::InvalidArgument,
                    format!("wasm cells on this node have at most {} MiB", self.max_mem_mib),
                ));
            }
            self.warm(name).await?;
            let root = slot.dir.join("root");
            std::fs::create_dir_all(root.join("tmp")).map_err(|e| {
                Error::new(Reason::Internal, format!("making the cell's directory: {e}"))
            })?;
            let (stop, stopped) = watch::channel(false);
            let usage = Arc::new(Usage::default());
            let mem = usize::try_from(mem_mib << 20).unwrap_or(usize::MAX);
            let env = spec.env.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            let runner = CellRunner::new(
                self.shared.clone(),
                root.clone(),
                env,
                mem,
                usage.clone(),
                stopped,
            );
            let cfg = hive_drone::Config {
                workdir: "/".into(),
                base_env: Vec::new(),
                roots: vec!["/".into()],
                build: concat!("hive-cell-wasm ", env!("CARGO_PKG_VERSION")).into(),
                base: Some(root),
                runner: Some(Arc::new(runner)),
                ..hive_drone::Config::default()
            };
            let drone = Drone::new(cfg, slot.secret);
            self.lock().insert(id, Cell { drone, serving: None, stop, usage });
            Ok(CellHandle {
                id,
                backend: Backend::Fncall,
                pid: None,
                channel: GuestChannel::Unix(slot.dir.join("drone.sock")),
                cgroup: slot.cgroup.clone(),
                netns: slot.netns.clone(),
                extra: BTreeMap::from([("program".into(), name.into())]),
            })
        })
    }

    fn start<'a>(&'a self, h: &'a mut CellHandle) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let GuestChannel::Unix(sock) = &h.channel else {
                return Err(Error::new(
                    Reason::Internal,
                    "a wasm cell's drone is on a Unix socket",
                ));
            };
            let _ = std::fs::remove_file(sock);
            let listener = UnixListener::bind(sock).map_err(|e| {
                Error::new(Reason::Internal, format!("the cell's socket {}: {e}", sock.display()))
            })?;
            let mut cells = self.lock();
            let Some(cell) = cells.get_mut(&h.id) else {
                return Err(Error::new(Reason::Internal, "the cell was never prepared"));
            };
            let drone = cell.drone.clone();
            cell.serving = Some(tokio::spawn(async move {
                // The connections go when this does, so stopping the cell cuts them off.
                let mut conns = JoinSet::new();
                loop {
                    tokio::select! {
                        accepted = listener.accept() => match accepted {
                            Ok((conn, _)) => {
                                conns.spawn(drone.clone().serve(conn));
                            }
                            Err(_) => tokio::time::sleep(TICK).await,
                        },
                        Some(_) = conns.join_next(), if !conns.is_empty() => {}
                    }
                }
            }));
            Ok(())
        })
    }

    fn stop<'a>(&'a self, h: &'a CellHandle, _grace: Duration) -> BoxFuture<'a, Result<ExitInfo>> {
        Box::pin(async move {
            if let Some(cell) = self.lock().remove(&h.id) {
                let _ = cell.stop.send(true);
                if let Some(task) = cell.serving {
                    task.abort();
                }
            }
            if let GuestChannel::Unix(sock) = &h.channel {
                let _ = std::fs::remove_file(sock);
            }
            Ok(ExitInfo::default())
        })
    }

    fn check<'a>(&'a self, h: &'a CellHandle) -> BoxFuture<'a, Result<Liveness>> {
        Box::pin(async move {
            // A cell lives in the node agent, so one the agent before this one made is gone.
            let alive = self
                .lock()
                .get(&h.id)
                .is_some_and(|c| c.serving.as_ref().is_none_or(|t| !t.is_finished()));
            Ok(if alive { Liveness::Alive } else { Liveness::Gone(ExitInfo::default()) })
        })
    }

    fn metrics(&self, h: &CellHandle) -> CellMetrics {
        let cells = self.lock();
        let Some(u) = cells.get(&h.id).map(|c| &c.usage) else {
            return CellMetrics::default();
        };
        CellMetrics {
            cpu_usec: u.cpu_nanos.load(Ordering::Relaxed) / 1000,
            mem_bytes: u.mem.load(Ordering::Relaxed),
            mem_peak: u.peak.load(Ordering::Relaxed),
            pids: u.running.load(Ordering::Relaxed),
        }
    }
}
