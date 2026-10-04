//! Runs the control plane and the node agents in one process, on simulated time and a simulated
//! network, with failures injected from a seed. A failure found here is a failure that can be
//! replayed: the same seed runs the same way every time.
//!
//! What runs is as much of the real code as runs without a socket or a clock of its own: the
//! keeper's state machine (`hive_keeper::state::State`) decides registrations, leases and quota
//! slices, waggle's `Placer` places every create, and each gate spends its quota through the
//! gate's own `Share`. The combs, the gates' create path, scout and the clients are models of
//! the real ones, written from them, with the same timeouts, retries and rules: a comb renews
//! its lease the way `hive_comb::lease::keep` does, fences off cells from older epochs when it
//! starts, keeps an idempotency key per live cell and hands out cell ids from a sequence it
//! keeps on disk, and a gate places again only when the comb never saw the call.
//!
//! Each run injects faults picked from the seed: partitions between a node and the keeper or
//! the front, lost and late and duplicated messages, comb crashes, nodes that lose power, a node
//! replaced by another machine under the same name while the old one is cut off, keeper leader
//! loss with the new leader's clock off, and node clocks that run fast or slow. After the load
//! stops the faults heal and the cluster drains, and then every cell must have ended.
//!
//! The invariants, checked as the run goes and at the end, are the ones in
//! `spec/13_observability_testing_bench.md`, section 2:
//!
//! - No cell is double owned: no two combs serve the same node in the same epoch at once, and
//!   no cell id is handed out twice.
//! - Epoch fencing kills stale cells: once a comb starts in an epoch, no cell of its node from
//!   an older epoch is left running on it.
//! - Every create gets exactly one outcome, and every cell ends exactly once.
//! - Idempotency holds: two live cells never share a project and key.
//! - Quota overshoot stays within the slice bound: a project's live cells never pass its quota
//!   by more than what the keeper's even split, the cells scout could not see, and the creates
//!   gates let through without a share allow.

#![forbid(unsafe_code)]

mod check;
mod client;
mod comb;
mod gate;
mod keeper;
mod world;

pub use check::Violation;
pub use world::{Config, Faults, Report, run};
