//! The vocabulary every other hivebox crate speaks.
//!
//! No dependencies and no I/O, so that the gate, the node agent and the guest agent can agree on
//! what a cell id or a state transition means without linking each other. The design is in
//! `spec/04_control_plane.md`, section 1, and `spec/05_api_sdk.md`, section 3.

#![forbid(unsafe_code)]

mod id;
mod state;

pub use id::{CellId, ParseCellIdError};
pub use state::CellState;
