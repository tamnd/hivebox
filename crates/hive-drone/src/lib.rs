//! A small static binary that runs inside each cell, as PID 1 in a microVM or as a normal process in a container. It speaks a framed protocol over a Unix socket or vsock, and it assumes the code it runs is hostile.
//!
//! The design is in `spec/09_guest_agent.md`. The wire format, the handshake and the method messages live in `hive_proto::drone`, because the node agent needs them too. This crate is the guest side, [`Drone`], plus the node side [`Client`] that talks to it. Commands run to completion with `process.run`, or streamed with `process.start`. Sessions keep one shell across calls, so `cd` and variables carry over, and `health` reports on the cell. The `fs.*` methods read, write, list and change files, and every path they take resolves inside the drone's configured roots, so no symlink or `..` leads out of them. Whole trees move as tar archives with `fs.upload` and `fs.download`, and `fs.watch` streams changes under a directory.

// The drone only ever runs inside a Linux cell, and it leans on openat2, inotify and /proc.
#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]

mod archive;
mod client;
mod config;
mod fs;
pub mod harden;
mod health;
pub mod init;
mod process;
mod ring;
mod server;
mod session;
mod watch;

pub use client::{
    ArchiveReader, ArchiveWriter, Client, FileReader, FileWriter, Output, Process, ProcessInput,
    ProcessOutput, Watcher,
};
pub use config::Config;
pub use server::Drone;
