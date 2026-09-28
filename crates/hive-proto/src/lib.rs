//! Wire types for hivebox: the `hivebox.v1` public API, the conversions between it and `hive-types`, and the drone channel between the node agent and the guest agent. Nothing in here makes a decision.
//!
//! The public API is in `spec/05_api_sdk.md` and the drone channel in `spec/09_guest_agent.md`. The internal gate, comb, scout and keeper services land here as they are built.

#![forbid(unsafe_code)]

pub mod convert;
pub mod drone;
pub mod v1;
