//! Placement. Given a batch of cells and the cluster state from scout it picks nodes, and it has
//! to do that for thousands of cells a second without becoming the bottleneck.
//!
//! The design is in `spec/04_control_plane.md`, section 5.1. A batch goes through four steps:
//!
//! - Nodes that are unhealthy, lack the backend, are excluded or have no room are dropped. Room
//!   counts what this placer sent in the last few seconds that the node's reports do not show yet.
//! - Of the rest, `clamp(2n, 8, all)` are drawn at random with odds by headroom, the batch
//!   sampling from Sparrow, so replicas do not all pick the same emptiest node.
//! - Each is scored on free memory and CPU, cached image layers, pool depth, recent creates and
//!   how many of the project's cells it has. Below 60% of the cluster in use, cached layers count
//!   most and cells pack. Above it, free room counts most and cells spread.
//! - The cells are handed out best score first, rescoring as a node fills, up to what it has room
//!   for and its burst cap. Cells past every cap then go where there is room, and the combs queue
//!   them.
//!
//! Cloud nodes are left out until the on-prem nodes have [`BURST_ABOVE`] of their memory in use,
//! counting what was just sent. Past that, a cell may go to a cloud node that has its image
//! staged, which the node says by putting [`image_digest`] of each staged image in its layer
//! filter. Keyed cells never go to the cloud, so a create sent again finds the same node.
//!
//! Placement only places new cells. The [`Rebalancer`] runs every minute over the whole cluster
//! and plans what to move: idle cells off nodes past [`HOT_ABOVE`] onto the emptiest ones, and an
//! offload to the cloud once the on-prem nodes stay past the burst line.
//!
//! The comb's admission has the final word. What it refuses goes back through
//! [`Placer::refused`] and is placed again with that node excluded.

#![forbid(unsafe_code)]

mod place;
mod rebalance;
mod view;

pub use place::{BURST_ABOVE, INFLIGHT_TTL, PACK_BELOW, PlaceReq, Placement, Placer};
pub use rebalance::{BURST_ROUNDS, Burst, HOT_ABOVE, Move, Plan, REBALANCE_EVERY, Rebalancer};
pub use view::{BackendSet, ClusterView, LayerBloom, NodeView, image_digest};
