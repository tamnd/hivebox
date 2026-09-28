use bytes::Bytes;
use hive_proto::drone::api::{
    self, Command, Health, MAX_CHUNK, RunRequest, RunResult, tag, tagged,
};
use hive_proto::drone::handshake::{self, Established, Secret};
use hive_proto::drone::{Channel, FrameCodec, Side, Stream};
use hive_types::{Error, Reason};
use prost::Message;
use std::io;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

// Stdin up to this size goes in the open frame, and anything bigger follows it as data.
const INLINE_STDIN: usize = 32 * 1024;
// Two streams at the drone's largest output limit, with room for the rest of the message.
const RESULT_LIMIT: usize = 2 * (64 << 20) + (1 << 20);

/// The node agent's end of a drone channel.
#[derive(Clone, Debug)]
pub struct Client {
    channel: Channel,
    established: Arc<Established>,
}

impl Client {
    /// Runs the node side of the handshake on `io` with `secret` and starts the channel.
    /// `clock_unix_nanos` is sent to the guest so it can step its clock, and `nonce` must be
    /// fresh random bytes.
    pub async fn connect<T>(
        io: T,
        secret: &Secret,
        clock_unix_nanos: u64,
        nonce: [u8; 32],
    ) -> io::Result<Self>
    where
        T: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let mut framed = Framed::new(io, FrameCodec);
        let established =
            handshake::node(&mut framed, secret, &[], clock_unix_nanos, nonce).await?;
        let parts = framed.into_parts();
        if !parts.read_buf.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "the guest sent frames before the handshake finished",
            ));
        }
        // The guest opens no streams of its own yet, so its queue is dropped and anything it
        // opens is reset.
        let (channel, _incoming) = Channel::start(parts.io, Side::Node);
        Ok(Self { channel, established: Arc::new(established) })
    }

    /// What the handshake agreed on, including the secret for the next connection.
    #[must_use]
    pub fn established(&self) -> &Established {
        &self.established
    }

    /// Whether the connection has gone.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.channel.is_closed()
    }

    /// Runs a command to completion.
    pub async fn run(&self, req: &RunRequest) -> Result<RunResult, Error> {
        if req.stdin.len() <= INLINE_STDIN {
            let answer = self
                .channel
                .call(api::PROCESS_RUN, req.encode_to_vec().into(), RESULT_LIMIT)
                .await?;
            return RunResult::decode(answer).map_err(bad_answer);
        }
        let head = RunRequest { command: req.command.clone(), stdin: Bytes::new() };
        let mut stream = self.channel.open(api::PROCESS_RUN, head.encode_to_vec().into()).await?;
        stream.send(req.stdin.clone()).await?;
        stream.finish().await?;
        let mut answer = Vec::new();
        while let Some(chunk) = stream.recv().await? {
            if answer.len() + chunk.len() > RESULT_LIMIT {
                return Err(Error::new(Reason::OutputLimit, "the answer is over the limit"));
            }
            answer.extend_from_slice(&chunk);
        }
        RunResult::decode(&answer[..]).map_err(bad_answer)
    }

    /// Asks the drone how the cell is doing.
    pub async fn health(&self) -> Result<Health, Error> {
        let answer = self.channel.call(api::HEALTH, Bytes::new(), 4096).await?;
        Health::decode(answer).map_err(bad_answer)
    }

    /// Starts a command with its input and output streamed.
    pub async fn start(&self, cmd: &Command) -> Result<Process, Error> {
        let stream = self.channel.open(api::PROCESS_START, cmd.encode_to_vec().into()).await?;
        Ok(Process { stream, done: false })
    }
}

/// A command started with [`Client::start`]. Dropping it kills the command.
#[derive(Debug)]
pub struct Process {
    stream: Stream,
    done: bool,
}

/// Something a streamed command did.
#[derive(Clone, Debug, PartialEq)]
pub enum Output {
    /// It started with this pid. Not sent when it could not start.
    Started(u32),
    /// It wrote to stdout.
    Stdout(Bytes),
    /// It wrote to stderr.
    Stderr(Bytes),
    /// It is gone. The result carries no output, which came before as [`Output::Stdout`] and
    /// [`Output::Stderr`].
    Exit(RunResult),
}

impl Process {
    /// Writes to the command's stdin.
    pub async fn write(&mut self, data: &[u8]) -> Result<(), Error> {
        for chunk in data.chunks(MAX_CHUNK) {
            self.stream.send(tagged(tag::STDIN, chunk)).await?;
        }
        Ok(())
    }

    /// Closes the command's stdin.
    pub async fn close_stdin(&mut self) -> Result<(), Error> {
        self.stream.send(tagged(tag::EOF, &[])).await
    }

    /// Sends `signal` to the command's process group.
    pub async fn signal(&mut self, signal: i32) -> Result<(), Error> {
        self.stream.send(tagged(tag::SIGNAL, &signal.to_be_bytes())).await
    }

    /// The next thing the command did, or `None` after [`Output::Exit`].
    pub async fn next(&mut self) -> Result<Option<Output>, Error> {
        loop {
            if self.done {
                return Ok(None);
            }
            let Some(msg) = self.stream.recv().await? else {
                return Err(Error::new(
                    Reason::DroneUnreachable,
                    "the drone ended the stream early",
                ));
            };
            let Some((&t, rest)) = msg.split_first() else { continue };
            return Ok(Some(match t {
                tag::PID => {
                    let pid = <[u8; 4]>::try_from(rest)
                        .map_err(|_| bad_answer("a pid frame that is not four bytes"))?;
                    Output::Started(u32::from_be_bytes(pid))
                }
                tag::STDOUT => Output::Stdout(msg.slice(1..)),
                tag::STDERR => Output::Stderr(msg.slice(1..)),
                tag::EXIT => {
                    self.done = true;
                    let result = RunResult::decode(msg.slice(1..)).map_err(bad_answer)?;
                    self.stream.finish().await?;
                    // Read the end so the stream closes cleanly.
                    while self.stream.recv().await?.is_some() {}
                    Output::Exit(result)
                }
                other => return Err(bad_answer(format!("unknown frame tag {other}"))),
            }));
        }
    }

    /// Reads everything to the end and returns the result with the output filled in.
    pub async fn wait(mut self) -> Result<RunResult, Error> {
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        while let Some(out) = self.next().await? {
            match out {
                Output::Started(_) => {}
                Output::Stdout(b) => stdout.extend_from_slice(&b),
                Output::Stderr(b) => stderr.extend_from_slice(&b),
                Output::Exit(mut r) => {
                    r.stdout = stdout.into();
                    r.stderr = stderr.into();
                    return Ok(r);
                }
            }
        }
        Err(Error::new(Reason::Internal, "the process already exited"))
    }
}

fn bad_answer(e: impl ToString) -> Error {
    Error::new(
        Reason::Internal,
        format!("the drone sent something that does not decode: {}", e.to_string()),
    )
}
