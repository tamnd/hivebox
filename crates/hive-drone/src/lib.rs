//! A small static binary that runs inside each cell, as PID 1 in a microVM or as a normal process in a container. It speaks a framed protocol over a Unix socket or vsock, and it assumes the code it runs is hostile.
//!
//! The design is in `spec/09_guest_agent.md`. The wire format, the handshake and the method messages live in `hive_proto::drone`, because the node agent needs them too. This crate is the guest side, [`Drone`], plus the node side [`Client`] that talks to it. Commands run to completion with `process.run`, or streamed with `process.start`, and `health` reports on the cell. Sessions and file operations come in later changes.

#![forbid(unsafe_code)]

mod client;
mod config;
mod health;
mod process;
mod ring;
mod server;

pub use client::{Client, Output, Process};
pub use config::Config;
pub use server::Drone;
