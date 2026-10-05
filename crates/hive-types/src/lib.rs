//! The vocabulary every other hivebox crate speaks.
//!
//! No I/O and no dependencies beyond an optional `serde`, so that the gate, the node agent and the
//! guest agent can agree on what a cell id, a spec or an error means without linking each other. The design is in
//! `spec/04_control_plane.md`, section 1, and `spec/05_api_sdk.md`, section 3.

#![forbid(unsafe_code)]

mod error;
mod id;
mod spec;
mod state;

pub use error::{Cause, Code, Error, Reason};
pub use id::{CellId, ParseCellIdError};
pub use spec::{
    Backend, CellSpec, IdleAction, Limits, MAX_BOOST, MAX_LABELS, Qos, Resources, Source,
    SpecError, is_name,
};
pub use state::CellState;
