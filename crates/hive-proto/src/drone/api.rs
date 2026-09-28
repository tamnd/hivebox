//! The methods the guest agent serves over the channel and their messages.
//!
//! Unary methods take one request in the `Open` frame and answer with one message and the end of
//! the stream, or reset it with a `Status`. Streaming methods send tagged data frames: the first
//! byte of every frame says what the rest is (see [`tag`]), so a frame never mixes two kinds and a
//! reader never has to reassemble anything.

use super::frame::MAX_PAYLOAD;
use bytes::{BufMut, Bytes, BytesMut};
use prost::Message;
use std::collections::BTreeMap;

/// Runs a command to completion. Request [`RunRequest`], then optionally more stdin as data, then
/// the end. Answer [`RunResult`].
pub const PROCESS_RUN: &str = "process.run";
/// Runs a command with streamed input and output. Request [`Command`], then tagged frames.
pub const PROCESS_START: &str = "process.start";
/// The drone's view of the cell. Empty request, answer [`Health`].
pub const HEALTH: &str = "health";
/// Starts a persistent shell. Request [`SessionCreate`], answer [`SessionInfo`].
pub const SESSION_CREATE: &str = "session.create";
/// Runs one command in a session's shell. Request [`SessionRun`], answer [`SessionRunResult`].
pub const SESSION_RUN: &str = "session.run";
/// Writes raw input to a session's shell and collects what comes back. Request [`SessionSend`],
/// answer [`SessionSendResult`].
pub const SESSION_SEND: &str = "session.send";
/// Ends a session and everything running in it. Request [`SessionRef`], empty answer.
pub const SESSION_CLOSE: &str = "session.close";

/// The most data one tagged frame carries, so that the tag and the data fit in one frame.
pub const MAX_CHUNK: usize = MAX_PAYLOAD - 1;

/// A data frame payload: `tag` then `data`, which must be at most [`MAX_CHUNK`] bytes.
#[must_use]
pub fn tagged(tag: u8, data: &[u8]) -> Bytes {
    debug_assert!(data.len() <= MAX_CHUNK, "a tagged frame over the limit");
    let mut out = BytesMut::with_capacity(1 + data.len());
    out.put_u8(tag);
    out.put_slice(data);
    out.freeze()
}

/// The first byte of each data frame on a streaming method.
pub mod tag {
    /// Node to drone: bytes for the process's stdin.
    pub const STDIN: u8 = 0;
    /// Drone to node: bytes the process wrote to stdout.
    pub const STDOUT: u8 = 1;
    /// Drone to node: bytes the process wrote to stderr.
    pub const STDERR: u8 = 2;
    /// Drone to node: the process is gone. The rest is a `RunResult` without output.
    pub const EXIT: u8 = 3;
    /// Node to drone: close stdin.
    pub const EOF: u8 = 4;
    /// Node to drone: send the signal in the next four bytes, big endian, to the process group.
    pub const SIGNAL: u8 = 5;
    /// Drone to node: the process started. The rest is its pid as four bytes, big endian.
    pub const PID: u8 = 6;
}

/// What to run and how.
#[derive(Clone, PartialEq, Message)]
pub struct Command {
    /// Run this directly when not empty.
    #[prost(string, repeated, tag = "1")]
    pub argv: Vec<String>,
    /// Otherwise run this with the drone's shell and `-c`.
    #[prost(string, tag = "2")]
    pub shell: String,
    /// The working directory. The drone's default when empty.
    #[prost(string, tag = "3")]
    pub cwd: String,
    /// Added on top of the drone's base environment.
    #[prost(btree_map = "string, string", tag = "4")]
    pub env: BTreeMap<String, String>,
    /// Killed after this many milliseconds. The drone's default when zero.
    #[prost(uint64, tag = "5")]
    pub timeout_ms: u64,
    /// Output kept per stream. The drone's default when zero, and never more than its cap.
    #[prost(uint64, tag = "6")]
    pub max_output_bytes: u64,
    /// The user to run as. The drone's default when unset.
    #[prost(uint32, optional, tag = "7")]
    pub uid: Option<u32>,
    /// The group to run as. The drone's default when unset.
    #[prost(uint32, optional, tag = "8")]
    pub gid: Option<u32>,
}

/// A command to run to completion.
#[derive(Clone, PartialEq, Message)]
pub struct RunRequest {
    /// The command.
    #[prost(message, optional, tag = "1")]
    pub command: Option<Command>,
    /// Written to stdin, which is then closed. Stdin is `/dev/null` when this is empty and no
    /// data follows. Input too big for the open frame is sent as data on the stream instead, and
    /// the drone appends it here before it starts the command.
    #[prost(bytes = "bytes", tag = "2")]
    pub stdin: Bytes,
}

/// How a process ended and what it wrote.
#[derive(Clone, PartialEq, Message)]
pub struct RunResult {
    /// The exit code, or -1 when a signal ended it.
    #[prost(sint32, tag = "1")]
    pub exit_code: i32,
    /// The signal that ended it, or 0.
    #[prost(int32, tag = "2")]
    pub signal: i32,
    /// Stdout, possibly cut in the middle. See `truncated`.
    #[prost(bytes = "bytes", tag = "3")]
    pub stdout: Bytes,
    /// Stderr, likewise.
    #[prost(bytes = "bytes", tag = "4")]
    pub stderr: Bytes,
    /// True when either stream wrote more than was kept. The first 64 KiB and the last part are
    /// kept, and the middle is dropped.
    #[prost(bool, tag = "5")]
    pub truncated: bool,
    /// True when the drone killed it for running past its timeout.
    #[prost(bool, tag = "6")]
    pub timed_out: bool,
    /// From spawn to exit, in nanoseconds.
    #[prost(uint64, tag = "7")]
    pub wall_nanos: u64,
    /// Bytes written to stdout, including any that were dropped.
    #[prost(uint64, tag = "8")]
    pub stdout_bytes: u64,
    /// Bytes written to stderr, including any that were dropped.
    #[prost(uint64, tag = "9")]
    pub stderr_bytes: u64,
}

/// What the drone sees of the cell.
#[derive(Clone, PartialEq, Message)]
pub struct Health {
    /// The drone's build.
    #[prost(string, tag = "1")]
    pub build: String,
    /// Since the drone started, in nanoseconds.
    #[prost(uint64, tag = "2")]
    pub uptime_nanos: u64,
    /// The one minute load average, times 1000.
    #[prost(uint64, tag = "3")]
    pub load1_milli: u64,
    /// From `/proc/meminfo`, or 0 where there is none.
    #[prost(uint64, tag = "4")]
    pub mem_total_bytes: u64,
    /// Likewise.
    #[prost(uint64, tag = "5")]
    pub mem_available_bytes: u64,
    /// Processes the drone is running for callers right now.
    #[prost(uint32, tag = "6")]
    pub processes: u32,
    /// Sessions open right now.
    #[prost(uint32, tag = "7")]
    pub sessions: u32,
}

/// A persistent shell to start.
#[derive(Clone, PartialEq, Message)]
pub struct SessionCreate {
    /// The shell to run. The drone's session shell when empty.
    #[prost(string, tag = "1")]
    pub shell: String,
    /// Where it starts. The drone's default when empty.
    #[prost(string, tag = "2")]
    pub cwd: String,
    /// Added on top of the drone's base environment.
    #[prost(btree_map = "string, string", tag = "3")]
    pub env: BTreeMap<String, String>,
    /// The user it runs as. The drone's default when unset.
    #[prost(uint32, optional, tag = "4")]
    pub uid: Option<u32>,
    /// The group it runs as. The drone's default when unset.
    #[prost(uint32, optional, tag = "5")]
    pub gid: Option<u32>,
}

/// A session that was started.
#[derive(Clone, PartialEq, Message)]
pub struct SessionInfo {
    /// Names the session in later calls.
    #[prost(string, tag = "1")]
    pub id: String,
}

/// Names a session.
#[derive(Clone, PartialEq, Message)]
pub struct SessionRef {
    /// From [`SessionInfo`].
    #[prost(string, tag = "1")]
    pub id: String,
}

/// One command to run in a session.
#[derive(Clone, PartialEq, Message)]
pub struct SessionRun {
    /// The session.
    #[prost(string, tag = "1")]
    pub id: String,
    /// Shell source, run in the session's shell so that `cd`, variables and functions carry
    /// over to the next command. Its stdin is `/dev/null`.
    #[prost(string, tag = "2")]
    pub command: String,
    /// The shell is killed and started again after this many milliseconds. The drone's default
    /// when zero.
    #[prost(uint64, tag = "3")]
    pub timeout_ms: u64,
    /// Output kept. The drone's default when zero, and never more than its cap.
    #[prost(uint64, tag = "4")]
    pub max_output_bytes: u64,
}

/// How a session command ended.
#[derive(Clone, PartialEq, Message)]
pub struct SessionRunResult {
    /// The command's exit code. When the shell itself exited, its exit code, or 128 plus the
    /// signal that ended it.
    #[prost(sint32, tag = "1")]
    pub exit_code: i32,
    /// Stdout and stderr, interleaved as the command wrote them.
    #[prost(bytes = "bytes", tag = "2")]
    pub output: Bytes,
    /// True when output was dropped from the middle. See [`RunResult::truncated`].
    #[prost(bool, tag = "3")]
    pub truncated: bool,
    /// True when the command ran past its timeout and the shell was killed.
    #[prost(bool, tag = "4")]
    pub timed_out: bool,
    /// From writing the command to seeing it finish, in nanoseconds.
    #[prost(uint64, tag = "5")]
    pub wall_nanos: u64,
    /// Bytes of output, including any that were dropped.
    #[prost(uint64, tag = "6")]
    pub output_bytes: u64,
    /// True when the shell is gone, killed at the timeout or exited on its own. The next call
    /// starts a new one with the session's first directory and environment.
    #[prost(bool, tag = "7")]
    pub restarted: bool,
}

/// Raw input for a session's shell, for driving interactive programs like `python` or `gdb`.
#[derive(Clone, PartialEq, Message)]
pub struct SessionSend {
    /// The session.
    #[prost(string, tag = "1")]
    pub id: String,
    /// Written to the shell's stdin as is.
    #[prost(bytes = "bytes", tag = "2")]
    pub input: Bytes,
    /// Stop reading once the output since the input was written contains this.
    #[prost(string, tag = "3")]
    pub expect: String,
    /// Stop reading once no output has come for this many milliseconds. With `expect` empty, 200
    /// when zero. With `expect` set, zero means wait for the match.
    #[prost(uint64, tag = "4")]
    pub quiet_ms: u64,
    /// Stop reading after this many milliseconds, whatever happens. The drone's default when
    /// zero. The shell is left running.
    #[prost(uint64, tag = "5")]
    pub timeout_ms: u64,
    /// Output kept. The drone's default when zero, and never more than its cap.
    #[prost(uint64, tag = "6")]
    pub max_output_bytes: u64,
}

/// What came back after a [`SessionSend`].
#[derive(Clone, PartialEq, Message)]
pub struct SessionSendResult {
    /// Stdout and stderr, interleaved.
    #[prost(bytes = "bytes", tag = "1")]
    pub output: Bytes,
    /// True when output was dropped from the middle.
    #[prost(bool, tag = "2")]
    pub truncated: bool,
    /// True when `expect` was seen.
    #[prost(bool, tag = "3")]
    pub matched: bool,
    /// True when reading stopped at the timeout.
    #[prost(bool, tag = "4")]
    pub timed_out: bool,
    /// Bytes of output, including any that were dropped.
    #[prost(uint64, tag = "5")]
    pub output_bytes: u64,
    /// True when the shell exited. The next call starts a new one.
    #[prost(bool, tag = "6")]
    pub restarted: bool,
}

impl RunResult {
    /// Encodes this result as an [`tag::EXIT`] frame payload.
    #[must_use]
    pub fn exit_frame(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(1 + self.encoded_len());
        out.put_u8(tag::EXIT);
        // Encoding only fails when the buffer cannot grow, and a BytesMut always can.
        let _ = self.encode(&mut out);
        out.freeze()
    }
}
