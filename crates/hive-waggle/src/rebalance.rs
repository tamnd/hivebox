//! The offline rebalancer from `spec/04_control_plane.md` section 5.2. Placement only decides
//! where new cells go, so a node can stay hot long after the rest drained, holding idle cells
//! that would be as well off elsewhere. Every [`REBALANCE_EVERY`] the rebalancer looks at the
//! whole cluster and plans two things: moving idle cells off hot nodes by pause and restore, and
//! offloading to cloud nodes when the on-prem share stays past the burst line.
//!
//! There is one kind of thing to move, idle memory, and any on-prem node can take it, so the
//! min cost flow the spec names comes down to matching the most loaded node with the emptiest
//! one until neither has anything left to give or take. That is what [`Rebalancer::plan`] does,
//! in O(n log n) for n nodes.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::time::Duration;

use crate::place::BURST_ABOVE;
use crate::view::{ClusterView, NodeView};

/// How often the rebalancer plans.
pub const REBALANCE_EVERY: Duration = Duration::from_secs(60);

/// A node with this share of its admittable memory given out is hot, and its idle cells are
/// worth moving.
pub const HOT_ABOVE: f64 = 0.9;

/// How many plans in a row the on-prem share has to be past the burst line before the
/// rebalancer says to offload, so a short spike does not.
pub const BURST_ROUNDS: u32 = 5;

/// Idle cells to move from one node to another.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Move {
    /// The hot node they are on.
    pub from: u16,
    /// The node to restore them on.
    pub to: u16,
    /// How many.
    pub cells: u32,
    /// The memory they hold, in MiB, taking each as the average idle cell on `from`.
    pub mem_mib: u64,
}

/// The on-prem nodes have been past the burst line for a while.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Burst {
    /// Plans in a row it has been past the line.
    pub rounds: u32,
    /// Memory given out past the line, in MiB, which is what would have to go for the on-prem
    /// nodes to be back at it.
    pub over_mib: u64,
    /// Memory the idle cells on the on-prem nodes hold, in MiB, the most an offload could move
    /// without stopping anything that is busy.
    pub idle_mib: u64,
    /// Memory the healthy cloud nodes have free, in MiB. Below `over_mib` the cloud needs more
    /// nodes.
    pub cloud_room_mib: u64,
}

/// One round of the rebalancer.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Plan {
    /// The share of the healthy on-prem nodes' admittable memory given out, from 0 to 1.
    pub share: f64,
    /// The on-prem nodes past [`HOT_ABOVE`].
    pub hot: u32,
    /// The moves, largest first.
    pub moves: Vec<Move>,
    /// Set when the share has been past the burst line for [`BURST_ROUNDS`] plans in a row.
    pub burst: Option<Burst>,
}

impl Plan {
    /// Cells all the moves take together.
    #[must_use]
    pub fn cells(&self) -> u32 {
        self.moves.iter().map(|m| m.cells).sum()
    }

    /// Memory all the moves take together, in MiB.
    #[must_use]
    pub fn mem_mib(&self) -> u64 {
        self.moves.iter().map(|m| m.mem_mib).sum()
    }
}

/// Plans moves round after round. It keeps how long the cluster has been past the burst line, so
/// one rebalancer has to see every round.
#[derive(Debug, Clone)]
pub struct Rebalancer {
    hot_above: f64,
    burst_above: f64,
    burst_rounds: u32,
    over: u32,
}

impl Default for Rebalancer {
    fn default() -> Self {
        Self::new()
    }
}

impl Rebalancer {
    /// A rebalancer with [`HOT_ABOVE`], [`BURST_ABOVE`] and [`BURST_ROUNDS`].
    #[must_use]
    pub fn new() -> Self {
        Self { hot_above: HOT_ABOVE, burst_above: BURST_ABOVE, burst_rounds: BURST_ROUNDS, over: 0 }
    }

    /// Takes the burst line the placer uses, so the two agree on when the cluster is full.
    #[must_use]
    pub fn with_burst_above(mut self, share: f64) -> Self {
        self.burst_above = share;
        self
    }

    /// Calls a node hot past `share` rather than [`HOT_ABOVE`].
    #[must_use]
    pub fn with_hot_above(mut self, share: f64) -> Self {
        self.hot_above = share;
        self
    }

    /// Plans one round over `view`.
    ///
    /// A hot node gives its idle cells until it is down to the share of the whole on-prem
    /// cluster, and a node below that share takes them until it is up to it, so a move never
    /// makes a node hot. Cells move whole, each taken as the average idle cell on its node.
    pub fn plan(&mut self, view: &ClusterView) -> Plan {
        let onprem: Vec<&NodeView> =
            view.nodes.iter().filter(|n| n.healthy && !n.cloud && n.mem_admit_mib > 0).collect();
        let used: u64 = onprem.iter().map(|n| n.mem_committed_mib).sum();
        let total: u64 = onprem.iter().map(|n| n.mem_admit_mib).sum();
        let share = if total == 0 { 1.0 } else { (used as f64 / total as f64).min(1.0) };
        let level = |n: &NodeView| (n.mem_admit_mib as f64 * share) as u64;

        // What each hot node should give and each cool one can take, in MiB.
        let mut give: Vec<(u64, u32, u64, u16)> = Vec::new();
        let mut take: Vec<(u64, u16)> = Vec::new();
        let mut hot = 0;
        for n in &onprem {
            let mine = n.mem_committed_mib as f64 / n.mem_admit_mib as f64;
            if mine > self.hot_above {
                hot += 1;
                let excess = n.mem_committed_mib.saturating_sub(level(n));
                if excess > 0 && n.idle_cells > 0 && n.idle_mem_mib > 0 {
                    let each = n.idle_mem_mib.div_ceil(u64::from(n.idle_cells));
                    give.push((excess, n.idle_cells, each, n.node));
                }
            } else {
                let room = level(n).saturating_sub(n.mem_committed_mib);
                if room > 0 {
                    take.push((room, n.node));
                }
            }
        }
        give.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.3.cmp(&b.3)));
        // The node with the most room first, and of equals the lowest numbered.
        let mut take: BinaryHeap<(u64, Reverse<u16>)> =
            take.into_iter().map(|(room, node)| (room, Reverse(node))).collect();

        let mut moves = Vec::new();
        for (mut excess, mut idle, each, from) in give {
            while excess > 0 && idle > 0 {
                let Some((room, Reverse(to))) = take.pop() else { break };
                let cells = excess.div_ceil(each).min(u64::from(idle)).min(room / each);
                if cells == 0 {
                    // The emptiest node has no room for one of these cells, so none has, but a
                    // node with smaller idle cells may still fit there.
                    take.push((room, Reverse(to)));
                    break;
                }
                let mem = cells * each;
                excess = excess.saturating_sub(mem);
                let cells = u32::try_from(cells).unwrap_or(u32::MAX);
                idle -= cells;
                moves.push(Move { from, to, cells, mem_mib: mem });
                if room > mem {
                    take.push((room - mem, Reverse(to)));
                }
            }
        }
        moves.sort_unstable_by(|a, b| b.mem_mib.cmp(&a.mem_mib).then(a.from.cmp(&b.from)));

        self.over = if total > 0 && share >= self.burst_above { self.over + 1 } else { 0 };
        let burst = (self.over >= self.burst_rounds).then(|| Burst {
            rounds: self.over,
            over_mib: used.saturating_sub((total as f64 * self.burst_above) as u64),
            idle_mib: onprem.iter().map(|n| n.idle_mem_mib).sum(),
            cloud_room_mib: view
                .nodes
                .iter()
                .filter(|n| n.healthy && n.cloud)
                .map(|n| n.mem_admit_mib.saturating_sub(n.mem_committed_mib))
                .sum(),
        });
        Plan { share, hot, moves, burst }
    }
}
