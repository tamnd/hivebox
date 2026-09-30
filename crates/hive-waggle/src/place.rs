//! The placement algorithm from `spec/04_control_plane.md` section 5.1: filter, sample by
//! headroom, score, then fill.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::time::Duration;

use hive_types::{Backend, Resources};

use crate::view::{ClusterView, NodeView};

/// Below this share of the cluster's memory in use, placement packs cells onto nodes that have
/// their image layers, which keeps caches warm and fewer nodes busy. Above it, it spreads them.
pub const PACK_BELOW: f64 = 0.6;

/// How long an in-flight entry lasts when no newer report from its node comes in.
pub const INFLIGHT_TTL: Duration = Duration::from_secs(3);

/// Fewest nodes scored for any batch, however small.
const MIN_SAMPLE: usize = 8;

/// What to place.
#[derive(Debug, Clone, Copy)]
pub struct PlaceReq<'a> {
    /// The backend every cell in the batch runs on.
    pub backend: Backend,
    /// What each cell is given.
    pub resources: Resources,
    /// How many cells.
    pub n: u32,
    /// The digests of the image's layers, for locality.
    pub layers: &'a [[u8; 32]],
    /// The project the cells are for, so one project does not pile onto one node.
    pub project: u64,
    /// A node to prefer, such as the one a fork's parent or a snapshot is on.
    pub affinity: Option<u16>,
    /// Nodes not to use, such as ones that just refused part of this batch.
    pub exclude: &'a [u16],
}

/// Where a batch goes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Placement {
    /// Each node and how many of the cells it gets, best first.
    pub nodes: Vec<(u16, u32)>,
    /// Cells no node had room for.
    pub unplaced: u32,
}

/// The weights of the score, one set for packing and one for spreading.
#[derive(Debug, Clone, Copy)]
struct Weights {
    mem: f64,
    cpu: f64,
    locality: f64,
    pool: f64,
    rate: f64,
    project: f64,
}

const PACK: Weights =
    Weights { mem: 0.2, cpu: 0.1, locality: 2.0, pool: 0.3, rate: 0.5, project: 0.5 };
const SPREAD: Weights =
    Weights { mem: 1.0, cpu: 0.5, locality: 0.1, pool: 0.3, rate: 0.5, project: 0.5 };
const AFFINITY: f64 = 1.0;
/// How many placements go by between sweeps of the overlay for nodes no longer in the view.
const SWEEP_EVERY: u32 = 1024;

/// Cells placed on a node that its reports do not count yet.
#[derive(Debug, Clone, Copy)]
struct Inflight {
    report: u64,
    until: Duration,
    cells: u32,
    mem_mib: u64,
    cpu_milli: u64,
}

/// Places batches of cells. It keeps an overlay of what it placed that scout does not show yet,
/// so batches in a row do not all land on the node that looked emptiest.
///
/// Time comes from the caller and randomness from a seed, so a simulation replays it exactly.
#[derive(Debug)]
pub struct Placer {
    rng: Rng,
    /// The overlay by node index. A plain vector, since node indexes are small and dense and
    /// a hash lookup per node was most of the cost of a small placement.
    inflight: Vec<Vec<Inflight>>,
    /// Placements since the last sweep of nodes that left the view.
    since_sweep: u32,
}

impl Placer {
    /// A placer whose sampling starts from `seed`. Replicas should use different seeds, so they
    /// do not herd onto the same nodes.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self { rng: Rng(seed), inflight: Vec::new(), since_sweep: 0 }
    }

    /// Picks nodes for `req.n` cells at time `now`, and counts them in the overlay until the
    /// nodes' reports do.
    pub fn place(&mut self, view: &ClusterView, req: &PlaceReq<'_>, now: Duration) -> Placement {
        let mut out = Placement { nodes: Vec::new(), unplaced: req.n };
        if req.n == 0 {
            return out;
        }
        let weights = if view.utilization() < PACK_BELOW { PACK } else { SPREAD };
        let mut feasible: Vec<(&NodeView, Load)> = Vec::new();
        for node in &view.nodes {
            if !node.healthy || !node.backends.has(req.backend) || req.exclude.contains(&node.node)
            {
                continue;
            }
            let load = self.load(node, now);
            let cap = load.cap(node, &req.resources);
            if cap > 0 {
                feasible.push((node, Load { cap, ..load }));
            }
        }
        let k = (2 * req.n as usize).clamp(MIN_SAMPLE, feasible.len().max(MIN_SAMPLE));
        let picked = self.sample(&feasible, k, req.affinity);
        let mut heap: BinaryHeap<Scored> = picked
            .iter()
            .map(|&i| {
                let (node, load) = &feasible[i];
                let fixed = fixed(node, req, weights);
                Scored { score: score(node, load, 0, req, weights) + fixed, fixed, at: i, given: 0 }
            })
            .collect();
        let mut given = vec![0u32; feasible.len()];
        let mut left = req.n;
        while left > 0 {
            let Some(mut top) = heap.pop() else { break };
            let (node, load) = &feasible[top.at];
            // Hand out a share at a time rather than one cell, so a batch of thousands costs a
            // few hundred heap operations, and still levels off across the nodes.
            let share = left.div_ceil(2 * (heap.len() as u32 + 1)).max(1);
            let take = share.min(load.cap - top.given).min(left);
            top.given += take;
            given[top.at] = top.given;
            left -= take;
            if top.given < load.cap {
                top.score = score(node, load, top.given, req, weights) + top.fixed;
                heap.push(top);
            }
        }
        let mut placed: Vec<(usize, u32)> =
            given.iter().enumerate().filter(|(_, g)| **g > 0).map(|(i, g)| (i, *g)).collect();
        placed.sort_by(|a, b| b.1.cmp(&a.1).then(feasible[a.0].0.node.cmp(&feasible[b.0].0.node)));
        for (i, cells) in placed {
            let node = feasible[i].0;
            let at = usize::from(node.node);
            if self.inflight.len() <= at {
                self.inflight.resize_with(at + 1, Vec::new);
            }
            self.inflight[at].push(Inflight {
                report: node.report,
                until: now + INFLIGHT_TTL,
                cells,
                mem_mib: u64::from(req.resources.mem_mib) * u64::from(cells),
                cpu_milli: u64::from(req.resources.vcpu_milli) * u64::from(cells),
            });
            out.nodes.push((node.node, cells));
        }
        out.unplaced = left;
        // Entries for nodes that left the view are only dropped here, once in a while.
        self.since_sweep += 1;
        if self.since_sweep >= SWEEP_EVERY {
            self.since_sweep = 0;
            for list in &mut self.inflight {
                list.retain(|e| e.until > now);
            }
        }
        out
    }

    /// Takes back cells a node refused out of the `placed` it was sent, so they stop counting
    /// against it. The comb's admission is the final word, and its refusal is fresher than any
    /// report.
    pub fn refused(&mut self, node: u16, cells: u32, resources: &Resources) {
        let Some(list) = self.inflight.get_mut(usize::from(node)) else { return };
        let mut left = cells;
        for e in list.iter_mut().rev() {
            let back = left.min(e.cells);
            e.cells -= back;
            e.mem_mib -= u64::from(resources.mem_mib) * u64::from(back);
            e.cpu_milli -= u64::from(resources.vcpu_milli) * u64::from(back);
            left -= back;
            if left == 0 {
                break;
            }
        }
        list.retain(|e| e.cells > 0);
    }

    /// Cells in the overlay, over every node.
    #[must_use]
    pub fn inflight(&self) -> u32 {
        self.inflight.iter().flatten().map(|e| e.cells).sum()
    }

    /// What `node` has on it, its report plus the overlay, dropping entries its report now
    /// counts or that are too old.
    fn load(&mut self, node: &NodeView, now: Duration) -> Load {
        let mut load = Load {
            cells: node.cells,
            mem_mib: node.mem_committed_mib,
            cpu_milli: node.cpu_committed_milli,
            fresh: 0,
            cap: 0,
        };
        if let Some(list) = self.inflight.get_mut(usize::from(node.node)) {
            list.retain(|e| e.report >= node.report && e.until > now);
            for e in list.iter() {
                load.cells += e.cells;
                load.mem_mib += e.mem_mib;
                load.cpu_milli += e.cpu_milli;
                load.fresh += e.cells;
            }
        }
        load
    }

    /// Up to `k` indexes into `feasible`, drawn without replacement with odds by headroom, with
    /// the affinity node always among them. This is Efraimidis and Spirakis: each node draws
    /// `-ln(u) / weight` and the `k` smallest win.
    fn sample(
        &mut self,
        feasible: &[(&NodeView, Load)],
        k: usize,
        affinity: Option<u16>,
    ) -> Vec<usize> {
        if feasible.len() <= k {
            return (0..feasible.len()).collect();
        }
        let mut keys: Vec<(f64, usize)> = feasible
            .iter()
            .enumerate()
            .map(|(i, (node, load))| {
                if Some(node.node) == affinity {
                    return (f64::NEG_INFINITY, i);
                }
                let u = self.rng.unit();
                (-u.ln() / f64::from(load.cap), i)
            })
            .collect();
        keys.select_nth_unstable_by(k - 1, |a, b| a.0.total_cmp(&b.0));
        keys.truncate(k);
        keys.into_iter().map(|(_, i)| i).collect()
    }
}

/// A node's load with the overlay counted, and how many more cells of a spec it takes now.
#[derive(Debug, Clone, Copy)]
struct Load {
    cells: u32,
    mem_mib: u64,
    cpu_milli: u64,
    /// Cells placed on it in the last few seconds that its reports do not show yet.
    fresh: u32,
    cap: u32,
}

impl Load {
    fn cap(&self, node: &NodeView, r: &Resources) -> u32 {
        let by_cells = node.max_cells.saturating_sub(self.cells);
        let mem_free = node.mem_admit_mib.saturating_sub(self.mem_mib);
        let by_mem = match r.mem_mib {
            0 => u32::MAX,
            m => u32::try_from(mem_free / u64::from(m)).unwrap_or(u32::MAX),
        };
        let by_burst = node.burst_cap.saturating_sub(self.fresh);
        by_cells.min(by_mem).min(by_burst)
    }
}

/// The part of a node's score that does not change as it fills: cached layers and affinity.
/// Checking the bloom filter is the dearest part of scoring, so it is done once a batch.
fn fixed(node: &NodeView, req: &PlaceReq<'_>, w: Weights) -> f64 {
    let locality = if req.layers.is_empty() {
        0.0
    } else {
        let hit = req.layers.iter().filter(|d| node.layers.contains(d)).count();
        hit as f64 / req.layers.len() as f64
    };
    let affinity = if Some(node.node) == req.affinity { AFFINITY } else { 0.0 };
    w.locality * locality + affinity
}

/// How good `node` is for one more cell after `given` from this batch, less [`fixed`]. Higher
/// is better.
fn score(node: &NodeView, load: &Load, given: u32, req: &PlaceReq<'_>, w: Weights) -> f64 {
    let r = &req.resources;
    let g = u64::from(given);
    let mem_used = load.mem_mib + g * u64::from(r.mem_mib);
    let mem = 1.0 - ratio(mem_used, node.mem_admit_mib);
    let cpu_used = load.cpu_milli + g * u64::from(r.vcpu_milli);
    let cpu = 1.0 - ratio(cpu_used, node.cpu_milli);
    let pool = ratio(u64::from(node.pool_depth.saturating_sub(given)), 64);
    let burst = f64::from(node.burst_cap.max(1));
    let rate = ((node.create_rate + f64::from(load.fresh + given)) / burst).min(1.0);
    let mine = node.project_cells(req.project) + given;
    let project = ratio(u64::from(mine), u64::from(load.cells + given).max(1));
    w.mem * mem + w.cpu * cpu + w.pool * pool - w.rate * rate - w.project * project
}

fn ratio(a: u64, b: u64) -> f64 {
    if b == 0 { 1.0 } else { (a as f64 / b as f64).min(1.0) }
}

/// A node in the fill, ordered by score.
#[derive(Debug)]
struct Scored {
    score: f64,
    fixed: f64,
    at: usize,
    given: u32,
}

impl PartialEq for Scored {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Scored {}

impl PartialOrd for Scored {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Scored {
    fn cmp(&self, other: &Self) -> Ordering {
        // Ties go to the lower index, so the same inputs give the same placement.
        self.score.total_cmp(&other.score).then(other.at.cmp(&self.at))
    }
}

/// SplitMix64, which is small, fast and good enough for sampling.
#[derive(Debug)]
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A number in (0, 1].
    fn unit(&mut self) -> f64 {
        ((self.next() >> 11) + 1) as f64 / (1u64 << 53) as f64
    }
}
