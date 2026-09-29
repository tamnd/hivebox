//! The Rust client for the public API. The Python SDK is the primary one, and this crate is what the CLI and the rollout worker are built on.
//!
//! The design is in `spec/05_api_sdk.md`, section 5. [`Client`] talks to a comb's local socket or to any endpoint that serves the v1 API, and [`Cell`] runs commands and moves files in one cell.

#![forbid(unsafe_code)]

mod client;

pub use client::*;
pub use hive_proto::v1;
pub use hive_types::{Backend, CellId, CellSpec, CellState, Error, Reason, Resources, Source};
