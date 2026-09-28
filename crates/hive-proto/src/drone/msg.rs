//! Control messages on the drone channel.
//!
//! These are written by hand with `prost` derives rather than generated from a `.proto` file. The
//! channel is internal, both ends ship from this repository, and keeping the definitions in Rust
//! means the guest agent builds without a protobuf compiler.

use prost::Message;

/// The guest agent's opening message.
#[derive(Clone, PartialEq, Message)]
pub struct Hello {
    /// Protocol versions it speaks, newest first.
    #[prost(uint32, repeated, tag = "1")]
    pub versions: Vec<u32>,
    /// Its build, for logs.
    #[prost(string, tag = "2")]
    pub build: String,
    /// Optional features it has, like `pty` or `watch`.
    #[prost(string, repeated, tag = "3")]
    pub caps: Vec<String>,
    /// 32 random bytes, so that no two handshakes have the same transcript.
    #[prost(bytes = "vec", tag = "4")]
    pub nonce: Vec<u8>,
}

/// The node agent's answer.
#[derive(Clone, PartialEq, Message)]
pub struct Welcome {
    /// The version both sides will speak.
    #[prost(uint32, tag = "1")]
    pub chosen: u32,
    /// The capabilities both sides have.
    #[prost(string, repeated, tag = "2")]
    pub caps: Vec<String>,
    /// The node's wall clock in nanoseconds since the Unix epoch, so a restored guest can step
    /// its own.
    #[prost(uint64, tag = "3")]
    pub clock_unix_nanos: u64,
    /// 32 random bytes from the node.
    #[prost(bytes = "vec", tag = "4")]
    pub nonce: Vec<u8>,
    /// The node's proof that it knows the secret. See `handshake`.
    #[prost(bytes = "vec", tag = "5")]
    pub mac: Vec<u8>,
}

/// The guest agent's proof, the last message of the handshake.
#[derive(Clone, PartialEq, Message)]
pub struct Proof {
    /// See `handshake`.
    #[prost(bytes = "vec", tag = "1")]
    pub mac: Vec<u8>,
}

/// Opens a stream to call `method` with `request`.
#[derive(Clone, PartialEq, Message)]
pub struct Open {
    /// Like `process.run` or `fs.read`.
    #[prost(string, tag = "1")]
    pub method: String,
    /// The method's request, protobuf encoded.
    #[prost(bytes = "bytes", tag = "2")]
    pub request: bytes::Bytes,
}

/// How a stream ended. An empty reason means success.
#[derive(Clone, PartialEq, Message)]
pub struct Status {
    /// One of the `hive_types::Reason` wire names, or empty.
    #[prost(string, tag = "1")]
    pub reason: String,
    /// For a human.
    #[prost(string, tag = "2")]
    pub message: String,
    /// The errno name for a `FILE_ERROR`, or empty.
    #[prost(string, tag = "3")]
    pub errno: String,
}

impl Status {
    /// A failure with `reason`.
    pub fn error(reason: hive_types::Reason, message: impl Into<String>) -> Self {
        Self { reason: reason.as_str().to_string(), message: message.into(), errno: String::new() }
    }

    /// The failure this status describes, or `None` for success. An unknown reason from a newer
    /// peer reads as `Internal` rather than being dropped.
    #[must_use]
    pub fn to_error(&self) -> Option<hive_types::Error> {
        if self.reason.is_empty() {
            return None;
        }
        let reason =
            hive_types::Reason::from_name(&self.reason).unwrap_or(hive_types::Reason::Internal);
        let mut e = hive_types::Error::new(reason, self.message.clone());
        if !self.errno.is_empty() {
            e.errno = Some(self.errno.clone());
        }
        Some(e)
    }
}

impl From<hive_types::Error> for Status {
    fn from(e: hive_types::Error) -> Self {
        Self {
            reason: e.reason.as_str().to_string(),
            message: e.message,
            errno: e.errno.unwrap_or_default(),
        }
    }
}
