//! The integration with RL training. So far that is the rollout worker: it takes tasks, runs
//! each one's samples in cells, checks each sample with `Verify.Run` and hands back trajectories
//! with a reward. The verl and slime adapters and an agent loop that calls a model are still to
//! come.
//!
//! The design is in `spec/11_rl_integration.md`.

#![forbid(unsafe_code)]

mod task;
mod trajectory;
mod worker;

pub use task::{Limits, Policy, Size, Task, Verify};
pub use trajectory::{Report, Timings, Trajectory, Turn, reward};
pub use worker::Worker;
