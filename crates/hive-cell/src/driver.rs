//! The driver trait from `spec/07_backends_snapshots.md`, section 2, and the registry the node
//! agent keeps them in.
//!
//! The node agent does everything that is the same for every backend: it admits the cell, takes a
//! cgroup and a network namespace from its pools, attaches the root filesystem and writes each
//! step to its WAL. A driver gets all of that handed to it in a [`Slot`] and a [`RootfsPlan`], and
//! only turns them into a running sandbox. What it hands back is a [`CellHandle`], which is plain
//! data, so the node agent can store it and give it back to the driver after a restart.

use futures::future::BoxFuture;
use hive_types::{Backend, CellId, CellSpec, Error, Reason, Resources};
use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// A driver call's result.
pub type Result<T> = std::result::Result<T, Error>;

/// What kind of snapshot a driver can take.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SnapshotCaps {
    /// None at all.
    #[default]
    None,
    /// The writable disk only.
    Disk,
    /// The disk, the memory and the machine state.
    DiskMem,
}

/// What a driver can do, which admission and API validation check before calling it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct DriverCaps {
    /// Pause with `Freeze` and resume.
    pub pause: bool,
    /// The deepest snapshot it can take.
    pub snapshot: SnapshotCaps,
    /// Fork a running cell into copies.
    pub fork: bool,
    /// Change a running cell's resources.
    pub resize: bool,
    /// Pass a GPU through.
    pub gpu: bool,
    /// Give back the page cache a running cell has not used lately.
    pub trim: bool,
}

/// Whether this node can run the driver's cells, found by [`CellDriver::probe`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeFit {
    /// False if something the driver needs is missing, such as `/dev/kvm`.
    pub ready: bool,
    /// What is missing, or what was found, for the node's labels and for an operator.
    pub notes: Vec<String>,
}

/// How deeply to pause a cell, from `spec/08_node_agent.md`, section 8.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PauseMode {
    /// Stop it running, memory kept. Resumes in milliseconds.
    Freeze,
    /// Freeze, then push its memory out to swap.
    Reclaim,
    /// Snapshot it and end the process, so it takes no memory at all.
    SnapshotKill,
}

/// What the node agent's pools hand a driver for one cell.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Slot {
    /// The cell's cgroup directory, made and limited by the node agent. The driver puts every
    /// process of the cell in it and nothing else.
    pub cgroup: PathBuf,
    /// The cell's network namespace, a file under the node's namespace directory that
    /// [`crate::netns`] made, or `None` for the host's network.
    pub netns: Option<PathBuf>,
    /// Where the cell sends DNS queries, when it has a network beyond loopback. The driver makes
    /// this the cell's only resolver.
    pub nameserver: Option<std::net::Ipv4Addr>,
    /// A directory only this cell's driver writes to, for sockets, logs and state. It is removed
    /// when the cell is gone.
    pub dir: PathBuf,
    /// The guest agent's secret, which the node agent uses for the channel handshake.
    pub secret: [u8; 32],
}

/// The root filesystem the node agent attached for a cell.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RootfsPlan {
    /// Read only layers, top first. For a container these are overlay lower directories.
    pub lowers: Vec<PathBuf>,
    /// Where writes go. The driver owns what is in it.
    pub upper: PathBuf,
    /// A directory whose contents the upper starts with, as a fork's children start from what
    /// their parent wrote. The driver copies it in with `tree::copy` once the upper is
    /// made and before the cell sees it.
    pub seed: Option<PathBuf>,
}

/// How the node agent reaches the guest agent inside a cell.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum GuestChannel {
    /// A Unix socket on the host, outside anything the cell can write.
    Unix(PathBuf),
    /// Firecracker's vsock: connect to `uds` and send `CONNECT <port>` first.
    Vsock {
        /// The host side socket.
        uds: PathBuf,
        /// The guest port.
        port: u32,
    },
}

/// A cell the driver made, as plain data. The node agent writes it to its WAL, so after a restart
/// the driver gets back exactly what it returned and has to find the cell from it alone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CellHandle {
    /// The cell.
    pub id: CellId,
    /// The backend that made it.
    pub backend: Backend,
    /// The process that holds the sandbox: the container's init or the VMM. `None` before start.
    pub pid: Option<u32>,
    /// Where the guest agent listens.
    pub channel: GuestChannel,
    /// The cgroup from its [`Slot`].
    pub cgroup: PathBuf,
    /// The network namespace from its [`Slot`].
    pub netns: Option<PathBuf>,
    /// Anything else the driver needs to find the cell again, such as an API socket.
    pub extra: BTreeMap<String, String>,
}

/// How a cell ended.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct ExitInfo {
    /// The main process's exit code, if it exited.
    pub code: Option<i32>,
    /// The signal that ended it, if one did.
    pub signal: Option<i32>,
    /// Whether the kernel's OOM killer ended it.
    pub oom: bool,
}

/// Whether a cell is still there, as [`CellDriver::check`] finds it after a restart.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Liveness {
    /// Running as it was left.
    Alive,
    /// Paused as it was left.
    Paused,
    /// Gone, with whatever is known about how.
    Gone(ExitInfo),
}

/// Cheap numbers about a running cell, read from its cgroup or its VMM.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct CellMetrics {
    /// CPU time used, in microseconds.
    pub cpu_usec: u64,
    /// Memory in use, in bytes.
    pub mem_bytes: u64,
    /// The most memory it has used, in bytes.
    pub mem_peak: u64,
    /// Processes and threads alive.
    pub pids: u64,
}

/// One isolation backend. A node has one of each kind it runs, in a [`DriverRegistry`].
///
/// Every method takes `&self` and may run for many cells at once. The node agent never calls two
/// methods for the same cell at the same time, since each cell's calls go through its own
/// lifecycle actor, so a driver doesn't have to lock per cell. Methods a driver doesn't have
/// fail with `POLICY_DENIED` by default, and [`CellDriver::caps`] says which those are, so the
/// node agent refuses the request before it ever gets here.
pub trait CellDriver: Send + Sync + 'static {
    /// Which backend this is.
    fn backend(&self) -> Backend;

    /// What it can do.
    fn caps(&self) -> DriverCaps;

    /// Checks that this node has what the driver needs. Called once at start.
    fn probe(&self) -> BoxFuture<'_, Result<NodeFit>>;

    /// Gets everything ready that does not use CPU yet: config, mounts, devices. The returned
    /// handle has no pid. If this fails, nothing is left behind.
    fn prepare<'a>(
        &'a self,
        id: CellId,
        spec: &'a CellSpec,
        rootfs: &'a RootfsPlan,
        slot: &'a Slot,
    ) -> BoxFuture<'a, Result<CellHandle>>;

    /// Starts a prepared cell. When this returns, the guest agent is starting or listening at
    /// [`CellHandle::channel`], and the node agent does the handshake.
    fn start<'a>(&'a self, h: &'a mut CellHandle) -> BoxFuture<'a, Result<()>>;

    /// Pauses a running cell.
    fn pause<'a>(&'a self, h: &'a CellHandle, mode: PauseMode) -> BoxFuture<'a, Result<()>> {
        let _ = (h, mode);
        Box::pin(async { Err(unsupported("pause")) })
    }

    /// Resumes a paused cell.
    fn resume<'a>(&'a self, h: &'a CellHandle) -> BoxFuture<'a, Result<()>> {
        let _ = h;
        Box::pin(async { Err(unsupported("resume")) })
    }

    /// Gives back the page cache a running cell has not used lately and returns how many bytes its
    /// memory went down by. The cell keeps running and keeps its own memory.
    fn trim<'a>(&'a self, h: &'a CellHandle) -> BoxFuture<'a, Result<u64>> {
        let _ = h;
        Box::pin(async { Err(unsupported("trim")) })
    }

    /// Changes a running cell's resources.
    fn resize<'a>(&'a self, h: &'a CellHandle, r: &'a Resources) -> BoxFuture<'a, Result<()>> {
        let _ = (h, r);
        Box::pin(async { Err(unsupported("resize")) })
    }

    /// Stops a cell: asks it to end, waits up to `grace`, then kills it, and removes everything
    /// the driver made for it. Stopping a cell that is already gone, or was only prepared, is
    /// fine, and so is stopping it twice.
    fn stop<'a>(&'a self, h: &'a CellHandle, grace: Duration) -> BoxFuture<'a, Result<ExitInfo>>;

    /// Finds out whether a cell from before a restart is still there, from its handle alone.
    fn check<'a>(&'a self, h: &'a CellHandle) -> BoxFuture<'a, Result<Liveness>>;

    /// Reads a running cell's numbers. This should be cheap, since it runs for every cell on
    /// every report.
    fn metrics(&self, h: &CellHandle) -> CellMetrics {
        crate::cgroup::metrics(&h.cgroup)
    }
}

fn unsupported(what: &str) -> Error {
    Error::new(Reason::PolicyDenied, format!("this backend cannot {what}"))
}

/// The drivers a node runs, one per backend.
#[derive(Clone, Default)]
pub struct DriverRegistry {
    drivers: BTreeMap<Backend, Arc<dyn CellDriver>>,
}

impl DriverRegistry {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a driver, replacing any other for the same backend.
    pub fn add(&mut self, driver: Arc<dyn CellDriver>) {
        self.drivers.insert(driver.backend(), driver);
    }

    /// The driver for `backend`, if this node has one.
    #[must_use]
    pub fn get(&self, backend: Backend) -> Option<&Arc<dyn CellDriver>> {
        self.drivers.get(&backend)
    }

    /// Every driver, in backend order.
    pub fn iter(&self) -> impl Iterator<Item = &Arc<dyn CellDriver>> {
        self.drivers.values()
    }
}

impl fmt::Debug for DriverRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.drivers.keys()).finish()
    }
}
