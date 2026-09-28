//! Wire types for hivebox: the drone channel between the node agent and the guest agent today, and the generated code for the `hivebox.v1` public API and the internal services as they land. Nothing in here makes a decision.
//!
//! The drone channel is in `spec/09_guest_agent.md` and the public API in `spec/05_api_sdk.md`.

#![forbid(unsafe_code)]

pub mod drone;
