//! The cheapest cell there is, for verifiers and reward functions that do not need a POSIX
//! environment: WebAssembly programs on wasmtime, inside the node agent.
//!
//! The design is in `spec/07_backends_snapshots.md`, section 3.1. A wasm cell is a directory and
//! the programs the node has, each a WASI command module named `<name>.wasm` in
//! [`Config::modules`]. A command runs the program its first word names, with the cell's
//! directory as `/`, so the files a caller writes are there for the program and the files the
//! program writes are there for the caller. Every run is a new instance from the engine's pool,
//! so nothing in memory carries over from one run to the next, and a run that goes past the
//! cell's memory fails to grow rather than taking the node's. A program is compiled once, the
//! first time a cell needs it.
//!
//! The node agent reaches a wasm cell the way it reaches any other, through a drone on the
//! cell's socket. That drone runs in the node agent and hands `process.run` to the cell's
//! programs, and its file methods work on the cell's directory. Streamed processes and sessions
//! are refused, since there is no process to stream and no shell to keep.

// The drone this leans on only builds for Linux.
#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]

mod driver;
mod run;
mod words;

pub use driver::{Config, TICK, WasmDriver};
