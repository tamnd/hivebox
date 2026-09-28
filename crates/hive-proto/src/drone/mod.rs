//! The channel between the node agent and the guest agent inside a cell.
//!
//! [`frame`] is the wire format, [`msg`] the control messages, [`handshake`] the mutual proof
//! that opens a connection and [`channel`] the streams on top. The methods called over those
//! streams live with the drone itself.

pub mod channel;
pub mod frame;
pub mod handshake;
pub mod msg;

pub use channel::{Channel, Incoming, Side, Stream};
pub use frame::{Frame, FrameCodec, Kind};
