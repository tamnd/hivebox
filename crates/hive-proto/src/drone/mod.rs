//! The channel between the node agent and the guest agent inside a cell.
//!
//! [`frame`] is the wire format, [`msg`] the control messages, [`handshake`] the mutual proof
//! that opens a connection, [`channel`] the streams on top and [`api`] the methods called over
//! them. The drone serves those methods and the node agent calls them.

pub mod api;
pub mod channel;
pub mod frame;
pub mod handshake;
pub mod msg;

pub use channel::{Channel, Incoming, Side, Stream};
pub use frame::{Frame, FrameCodec, Kind};
