//! Every comb pushes a report once a second. Scout folds those into one view of the cluster and
//! hands it to the gate and to waggle.
//!
//! The design is in `spec/04_control_plane.md`, section 4. [`Scout`] keeps the last report from
//! each node and builds a versioned [`Snapshot`] from them when something changed. A node that
//! has not reported for [`STALE_AFTER`] is shown as down, and one silent for [`FORGET_AFTER`] is
//! dropped. Scout keeps nothing on disk: a new one has the whole cluster again one report
//! interval after it starts.
//!
//! [`Scout`] itself does no I/O and takes the time from the caller, so the simulator drives it
//! the same way [`Service`] does. The service takes reports over `hivebox.internal.v1.Scout`
//! and publishes a snapshot every [`TICK`], or at once after an urgent report. Gates and placers
//! follow it with [`follow`], which keeps a [`Mirror`] of the snapshot from the whole cluster
//! once and then only the nodes that changed.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use hive_waggle::{BackendSet, ClusterView, LayerBloom, NodeView};
use tokio::sync::watch;

mod mirror;
mod service;
mod wire;

pub use mirror::{Mirror, follow};
pub use service::{MIN_GAP, Service, TICK};
pub use wire::BadReport;

/// The name scout and waggle know a project by: the first 8 bytes of the BLAKE3 hash of its name.
#[must_use]
pub fn project_id(name: &str) -> u64 {
    let hash = blake3::hash(name.as_bytes());
    let mut first = [0u8; 8];
    first.copy_from_slice(&hash.as_bytes()[..8]);
    u64::from_le_bytes(first)
}

/// How long a node may go without a report before it is shown as down. Three missed reports.
pub const STALE_AFTER: Duration = Duration::from_secs(3);

/// How long a node may go without a report before scout forgets it.
pub const FORGET_AFTER: Duration = Duration::from_secs(60);

/// A change in a node's room bigger than this share of it is worth publishing at once.
const URGENT_SHARE: f64 = 0.05;

/// What a comb pushes about itself, once a second and at once after a big change.
#[derive(Debug, Clone)]
pub struct NodeReport {
    /// The comb's registered index.
    pub node: u16,
    /// The comb's registration epoch. It goes up each time the comb registers again, and a
    /// report from an older epoch is from a comb that has since been replaced.
    pub epoch: u16,
    /// Counts the comb's reports within an epoch, so one that arrives late is dropped.
    pub seq: u64,
    /// Where the gate reaches the comb.
    pub addr: Arc<str>,
    /// Whether the comb is taking creates.
    pub healthy: bool,
    /// The backends it can run now.
    pub backends: BackendSet,
    /// CPU on the node, in thousandths of a core.
    pub cpu_milli: u64,
    /// CPU its cells were given, in thousandths of a core.
    pub cpu_committed_milli: u64,
    /// The memory it admits cells up to, in MiB.
    pub mem_admit_mib: u64,
    /// Memory its cells were given, in MiB.
    pub mem_committed_mib: u64,
    /// Cells on the node now.
    pub cells: u32,
    /// Most cells it takes.
    pub max_cells: u32,
    /// Cgroups and network namespaces ready in its pools.
    pub pool_depth: u32,
    /// Creates a second lately.
    pub create_rate: f64,
    /// Most creates it takes at once.
    pub burst_cap: u32,
    /// The layers in its cache, or `None` when they have not changed since its last report.
    /// Most reports leave it out, since it is 4 KiB.
    pub layers: Option<LayerBloom>,
    /// Cells of the projects with the most cells on it, as (project, cells).
    pub top_projects: Vec<(u64, u32)>,
}

/// What became of a report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Applied {
    /// It was taken, and the next snapshot shows it.
    Taken,
    /// It was taken and changed the node enough that a snapshot should go out now rather than at
    /// the next tick: the node is new, came back, changed health, or its room moved by more
    /// than 5%.
    Urgent,
    /// It was older than what scout has and was dropped.
    Stale,
}

/// The cluster at one moment, shared by everyone who reads it.
#[derive(Debug, Default)]
pub struct Snapshot {
    /// Goes up by one with each snapshot.
    pub version: u64,
    /// The nodes for placement. A node that stopped reporting is here with `healthy` false
    /// until scout forgets it.
    pub view: ClusterView,
    /// Where to reach each node, sorted by node.
    pub addrs: Vec<(u16, Arc<str>)>,
    /// Cells a project has across the healthy nodes. Each node only reports its top projects,
    /// so for a project spread thin over many nodes this is a floor.
    pub projects: HashMap<u64, u64>,
    /// Sums over the healthy nodes, for metrics.
    pub totals: Totals,
    /// When each node in `view` last changed, so a watcher is sent only what is new to it.
    marks: Vec<Mark>,
}

/// The snapshot versions in which a node's view, and its layer filter, last changed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Mark {
    view: u64,
    layers: u64,
}

impl Snapshot {
    /// A snapshot of `nodes`, which must be sorted by node, with the sums worked out.
    fn assemble(version: u64, nodes: impl Iterator<Item = (NodeView, Arc<str>, Mark)>) -> Self {
        let mut snap = Snapshot { version, ..Snapshot::default() };
        for (view, addr, mark) in nodes {
            let t = &mut snap.totals;
            t.nodes += 1;
            if view.healthy {
                t.healthy += 1;
                t.cells += u64::from(view.cells);
                t.mem_admit_mib += view.mem_admit_mib;
                t.mem_committed_mib += view.mem_committed_mib;
                for &(project, cells) in &view.top_projects {
                    *snap.projects.entry(project).or_default() += u64::from(cells);
                }
            }
            snap.addrs.push((view.node, addr));
            snap.view.nodes.push(view);
            snap.marks.push(mark);
        }
        snap
    }

    /// Where to reach `node`.
    #[must_use]
    pub fn addr(&self, node: u16) -> Option<&Arc<str>> {
        let at = self.addrs.binary_search_by_key(&node, |(n, _)| *n).ok()?;
        Some(&self.addrs[at].1)
    }
}

/// Sums over the healthy nodes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Totals {
    /// Nodes scout knows.
    pub nodes: u32,
    /// Of those, the ones reporting and healthy.
    pub healthy: u32,
    /// Cells on the healthy nodes.
    pub cells: u64,
    /// Memory the healthy nodes admit, in MiB.
    pub mem_admit_mib: u64,
    /// Memory given to cells on them, in MiB.
    pub mem_committed_mib: u64,
}

/// One node as scout last heard from it.
#[derive(Debug)]
struct Entry {
    epoch: u16,
    seq: u64,
    seen: Duration,
    addr: Arc<str>,
    view: NodeView,
    /// Whether it reported within [`STALE_AFTER`] when last checked.
    live: bool,
    mark: Mark,
}

/// Folds node reports into snapshots.
#[derive(Debug)]
pub struct Scout {
    /// By node index, which is small and dense.
    nodes: Vec<Option<Entry>>,
    /// Reports taken, over all nodes. Each taken report gets the next number as its node's
    /// `report`, which is how waggle knows a report is newer than its overlay entry.
    taken: u64,
    version: u64,
    dirty: bool,
    tx: watch::Sender<Arc<Snapshot>>,
}

impl Default for Scout {
    fn default() -> Self {
        Self::new()
    }
}

impl Scout {
    /// A scout that knows no nodes.
    #[must_use]
    pub fn new() -> Self {
        let (tx, _) = watch::channel(Arc::new(Snapshot::default()));
        Self { nodes: Vec::new(), taken: 0, version: 0, dirty: false, tx }
    }

    /// Takes a report received at `now`.
    pub fn apply(&mut self, report: NodeReport, now: Duration) -> Applied {
        let at = usize::from(report.node);
        if self.nodes.len() <= at {
            self.nodes.resize_with(at + 1, || None);
        }
        let slot = &mut self.nodes[at];
        let next = self.version + 1;
        let mut urgent = true;
        let mut layers = LayerBloom::default();
        let mut layers_at = next;
        if let Some(e) = slot {
            if report.epoch < e.epoch || (report.epoch == e.epoch && report.seq <= e.seq) {
                return Applied::Stale;
            }
            if report.epoch == e.epoch {
                // A comb sends its filter again on every new stream, which is no news to
                // watchers when the layers are the same.
                if report.layers.as_ref().is_none_or(|l| *l == e.view.layers) {
                    layers_at = e.mark.layers;
                }
                layers = e.view.layers.clone();
                urgent = !e.live
                    || e.view.healthy != report.healthy
                    || moved(
                        e.view.mem_committed_mib,
                        report.mem_committed_mib,
                        report.mem_admit_mib,
                    )
                    || moved(
                        u64::from(e.view.cells),
                        u64::from(report.cells),
                        u64::from(report.max_cells),
                    );
            }
        }
        self.taken += 1;
        let view = NodeView {
            node: report.node,
            report: self.taken,
            healthy: report.healthy,
            backends: report.backends,
            cpu_milli: report.cpu_milli,
            cpu_committed_milli: report.cpu_committed_milli,
            mem_admit_mib: report.mem_admit_mib,
            mem_committed_mib: report.mem_committed_mib,
            cells: report.cells,
            max_cells: report.max_cells,
            pool_depth: report.pool_depth,
            create_rate: report.create_rate,
            burst_cap: report.burst_cap,
            layers: report.layers.unwrap_or(layers),
            top_projects: report.top_projects,
        };
        *slot = Some(Entry {
            epoch: report.epoch,
            seq: report.seq,
            seen: now,
            addr: report.addr,
            view,
            live: true,
            mark: Mark { view: next, layers: layers_at },
        });
        self.dirty = true;
        if urgent { Applied::Urgent } else { Applied::Taken }
    }

    /// Marks nodes that went quiet as down, forgets long silent ones, and publishes a snapshot
    /// if anything changed since the last one. Call it on a timer, and at once after an
    /// [`Applied::Urgent`] report.
    pub fn tick(&mut self, now: Duration) -> Option<Arc<Snapshot>> {
        for slot in &mut self.nodes {
            let Some(e) = slot else { continue };
            let quiet = now.saturating_sub(e.seen);
            if quiet >= FORGET_AFTER {
                *slot = None;
                self.dirty = true;
            } else if e.live && quiet >= STALE_AFTER {
                e.live = false;
                e.mark.view = self.version + 1;
                self.dirty = true;
            }
        }
        if !self.dirty {
            return None;
        }
        self.dirty = false;
        self.version += 1;
        let snap = Arc::new(self.build());
        self.tx.send_replace(snap.clone());
        Some(snap)
    }

    /// The last snapshot published.
    #[must_use]
    pub fn snapshot(&self) -> Arc<Snapshot> {
        self.tx.borrow().clone()
    }

    /// A receiver that always holds the last snapshot and wakes on each new one.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<Arc<Snapshot>> {
        self.tx.subscribe()
    }

    fn build(&self) -> Snapshot {
        Snapshot::assemble(
            self.version,
            self.nodes.iter().flatten().map(|e| {
                let mut view = e.view.clone();
                view.healthy &= e.live;
                (view, e.addr.clone(), e.mark)
            }),
        )
    }
}

/// Whether a value moved by more than [`URGENT_SHARE`] of `whole`.
fn moved(before: u64, after: u64, whole: u64) -> bool {
    before.abs_diff(after) as f64 > whole as f64 * URGENT_SHARE
}
