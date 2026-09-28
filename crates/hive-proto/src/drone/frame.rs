//! The frame format on the channel between the node agent and the guest agent.
//!
//! Every frame is a nine byte header followed by its payload:
//!
//! ```text
//! stream_id: u32 big endian | kind: u8 | flags: u8 | len: u24 big endian | payload: len bytes
//! ```
//!
//! Stream 0 carries the handshake and pings. Every other stream is one call. Control payloads are
//! protobuf and data payloads are raw bytes, so output passes through without being re-encoded.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::fmt;
use std::io;
use tokio_util::codec::{Decoder, Encoder};

/// The header size in bytes.
pub const HEADER_LEN: usize = 9;
/// The largest payload one frame may carry. Bigger writes are split.
pub const MAX_PAYLOAD: usize = 64 * 1024;

/// What a frame is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Kind {
    /// Guest to node, stream 0: the guest agent's versions and capabilities.
    Hello = 0,
    /// Node to guest, stream 0: the chosen version and the node's proof.
    Welcome = 1,
    /// Guest to node, stream 0: the guest agent's proof. Nothing else is accepted before it.
    Proof = 2,
    /// Opens a stream. The payload is an `Open` message naming the method.
    Open = 3,
    /// Bytes on an open stream. With [`FLAG_END`] the sender will send no more.
    Data = 4,
    /// More room to send on a stream. The payload is a u32 byte count.
    Credit = 5,
    /// Ends a stream in both directions. The payload is a `Status`.
    Reset = 6,
    /// Stream 0, either way. The payload is echoed in a [`Kind::Pong`].
    Ping = 7,
    /// The answer to a [`Kind::Ping`].
    Pong = 8,
}

impl Kind {
    fn from_u8(b: u8) -> Option<Self> {
        Some(match b {
            0 => Self::Hello,
            1 => Self::Welcome,
            2 => Self::Proof,
            3 => Self::Open,
            4 => Self::Data,
            5 => Self::Credit,
            6 => Self::Reset,
            7 => Self::Ping,
            8 => Self::Pong,
            _ => return None,
        })
    }
}

/// On [`Kind::Data`]: the last frame the sender will send on this stream.
pub const FLAG_END: u8 = 1;

/// One frame.
#[derive(Clone, PartialEq, Eq)]
pub struct Frame {
    /// The stream it belongs to. 0 is the connection itself.
    pub stream: u32,
    /// What it is.
    pub kind: Kind,
    /// Bit flags, see [`FLAG_END`].
    pub flags: u8,
    /// The payload, at most [`MAX_PAYLOAD`] bytes.
    pub payload: Bytes,
}

impl Frame {
    /// A frame with no flags.
    pub fn new(stream: u32, kind: Kind, payload: impl Into<Bytes>) -> Self {
        Self { stream, kind, flags: 0, payload: payload.into() }
    }

    /// Whether [`FLAG_END`] is set.
    #[must_use]
    pub fn is_end(&self) -> bool {
        self.flags & FLAG_END != 0
    }
}

impl fmt::Debug for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Frame")
            .field("stream", &self.stream)
            .field("kind", &self.kind)
            .field("flags", &self.flags)
            .field("len", &self.payload.len())
            .finish()
    }
}

/// Encodes and decodes frames on a byte stream.
#[derive(Clone, Copy, Debug, Default)]
pub struct FrameCodec;

impl Decoder for FrameCodec {
    type Item = Frame;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<Frame>> {
        if src.len() < HEADER_LEN {
            src.reserve(HEADER_LEN - src.len());
            return Ok(None);
        }
        let len = usize::from(src[6]) << 16 | usize::from(src[7]) << 8 | usize::from(src[8]);
        if len > MAX_PAYLOAD {
            return Err(invalid(format!("a frame of {len} bytes, over the {MAX_PAYLOAD} limit")));
        }
        let kind =
            Kind::from_u8(src[4]).ok_or_else(|| invalid(format!("frame kind {}", src[4])))?;
        if src.len() < HEADER_LEN + len {
            src.reserve(HEADER_LEN + len - src.len());
            return Ok(None);
        }
        let stream = src.get_u32();
        let _kind = src.get_u8();
        let flags = src.get_u8();
        src.advance(3);
        let payload = src.split_to(len).freeze();
        Ok(Some(Frame { stream, kind, flags, payload }))
    }
}

impl Encoder<Frame> for FrameCodec {
    type Error = io::Error;

    fn encode(&mut self, frame: Frame, dst: &mut BytesMut) -> io::Result<()> {
        let len = frame.payload.len();
        if len > MAX_PAYLOAD {
            return Err(invalid(format!("a frame of {len} bytes, over the {MAX_PAYLOAD} limit")));
        }
        dst.reserve(HEADER_LEN + len);
        dst.put_u32(frame.stream);
        dst.put_u8(frame.kind as u8);
        dst.put_u8(frame.flags);
        // The length fits in 24 bits because it is at most MAX_PAYLOAD.
        dst.put_uint(len as u64, 3);
        dst.extend_from_slice(&frame.payload);
        Ok(())
    }
}

fn invalid(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(frame: Frame) -> Frame {
        let mut buf = BytesMut::new();
        FrameCodec.encode(frame, &mut buf).unwrap();
        FrameCodec.decode(&mut buf).unwrap().unwrap()
    }

    #[test]
    fn frames_survive_the_round_trip() {
        let f =
            Frame { stream: 0xdead_beef, kind: Kind::Data, flags: FLAG_END, payload: "hi".into() };
        assert_eq!(round_trip(f.clone()), f);
        let big = Frame::new(7, Kind::Data, vec![7u8; MAX_PAYLOAD]);
        assert_eq!(round_trip(big.clone()), big);
    }

    #[test]
    fn partial_input_waits_for_more() {
        let mut whole = BytesMut::new();
        FrameCodec.encode(Frame::new(1, Kind::Open, "abcdef"), &mut whole).unwrap();
        FrameCodec.encode(Frame::new(1, Kind::Credit, vec![0, 0, 1, 0]), &mut whole).unwrap();
        let mut buf = BytesMut::new();
        let mut got = Vec::new();
        for b in whole {
            buf.put_u8(b);
            while let Some(f) = FrameCodec.decode(&mut buf).unwrap() {
                got.push(f);
            }
        }
        assert_eq!(got.len(), 2);
        assert_eq!(&got[0].payload[..], b"abcdef");
        assert_eq!(got[1].kind, Kind::Credit);
    }

    #[test]
    fn oversized_and_unknown_frames_are_refused() {
        let mut buf = BytesMut::from(&[0, 0, 0, 1, 4, 0, 0x01, 0x00, 0x01][..]);
        assert!(FrameCodec.decode(&mut buf).is_err());
        let mut buf = BytesMut::from(&[0, 0, 0, 1, 200, 0, 0, 0, 0][..]);
        assert!(FrameCodec.decode(&mut buf).is_err());
        let too_big = Frame::new(1, Kind::Data, vec![0u8; MAX_PAYLOAD + 1]);
        assert!(FrameCodec.encode(too_big, &mut BytesMut::new()).is_err());
    }
}
