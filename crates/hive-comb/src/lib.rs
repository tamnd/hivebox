//! One comb per machine. It owns every cell on that machine and every state transition of those cells, writes each transition to its WAL before acting on it, and survives its own restart without taking the cells down with it.
//!
//! The design is in `spec/08_node_agent.md`. [`Comb`] is the node agent in standalone mode: it admits cells against the node's memory and cell cap, runs each one through its driver from `hive-cell`, connects to its guest agent, and keeps its record in a WAL. Each cell has its own actor task, so transitions for one cell never race and a slow cell holds up no other. After a restart, [`Comb::open`] reads the WAL back, reconnects to every cell that is still running and cleans up the ones that were caught halfway.

// The comb only runs on a Linux node, and the guest agent client it uses is Linux only too.
#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]

mod admit;
pub mod api;
mod cell;
mod cgroups;
mod comb;
mod config;
mod core_sched;
pub mod lease;
mod llm;
mod metrics;
mod net;
mod netns;
mod pool;
mod pressure;
mod record;
pub mod report;
mod siem;
mod wal;

pub use cell::{CellInfo, Cutoff, Status};
pub use comb::{CellEvent, Comb, CreateRequest, Quarantined, WalStats};
pub use config::{
    Config, ContainerBackend, FncallBackend, Images, KeeperLink, Llm, Network, ScoutLink, SiemLink,
};
pub use metrics::Metrics;
