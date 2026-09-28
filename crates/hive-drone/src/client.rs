use bytes::Bytes;
use hive_proto::drone::api::{
    self, Command, FileInfo, FsChmod, FsEvents, FsList, FsListResult, FsMkdir, FsPath, FsRead,
    FsRename, FsUpload, FsUploadResult, FsWrite, Health, MAX_CHUNK, RunRequest, RunResult,
    SessionCreate, SessionInfo, SessionRef, SessionRun, SessionRunResult, SessionSend,
    SessionSendResult, tag, tagged,
};
use hive_proto::drone::handshake::{self, Established, Secret};
use hive_proto::drone::{Channel, FrameCodec, RecvHalf, SendHalf, Side, Stream};
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
// File content up to this size goes in the open frame of fs.write.
const INLINE_WRITE: usize = 32 * 1024;
// One FileInfo, with room for a long path and symlink target.
const INFO_LIMIT: usize = 64 * 1024;
// A full listing is 100,000 entries, and each is well under a kilobyte.
const LIST_LIMIT: usize = 128 << 20;

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

    /// Waits until the connection has gone.
    pub async fn closed(&self) {
        self.channel.closed().await;
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

    /// Starts a persistent shell.
    pub async fn session_create(&self, spec: &SessionCreate) -> Result<SessionInfo, Error> {
        self.unary(api::SESSION_CREATE, spec, 4096).await
    }

    /// Runs one command in a session's shell and waits for it to finish.
    pub async fn session_run(&self, req: &SessionRun) -> Result<SessionRunResult, Error> {
        self.unary(api::SESSION_RUN, req, RESULT_LIMIT).await
    }

    /// Writes raw input to a session's shell and collects what comes back.
    pub async fn session_send(&self, req: &SessionSend) -> Result<SessionSendResult, Error> {
        self.unary(api::SESSION_SEND, req, RESULT_LIMIT).await
    }

    /// Ends a session and kills everything running in it.
    pub async fn session_close(&self, id: &str) -> Result<(), Error> {
        let req = SessionRef { id: id.to_string() };
        self.channel.call(api::SESSION_CLOSE, req.encode_to_vec().into(), 0).await.map(|_| ())
    }

    async fn unary<A: Message + Default>(
        &self,
        method: &str,
        req: &impl Message,
        limit: usize,
    ) -> Result<A, Error> {
        let answer = self.channel.call(method, req.encode_to_vec().into(), limit).await?;
        A::decode(answer).map_err(bad_answer)
    }

    /// Reads a file, or the part of it `req` asks for, into memory. Fails with `OUTPUT_LIMIT`
    /// past `limit` bytes. Use [`Client::fs_open`] for files too big to hold.
    pub async fn fs_read(&self, req: &FsRead, limit: usize) -> Result<Bytes, Error> {
        self.channel.call(api::FS_READ, req.encode_to_vec().into(), limit).await
    }

    /// Opens a file for streamed reading.
    pub async fn fs_open(&self, req: &FsRead) -> Result<FileReader, Error> {
        let mut stream = self.channel.open(api::FS_READ, req.encode_to_vec().into()).await?;
        // The node sends nothing on a read, so its side ends right away.
        stream.finish().await?;
        Ok(FileReader { stream })
    }

    /// Writes a file whose content is all in `req.data`.
    pub async fn fs_write(&self, req: &FsWrite) -> Result<FileInfo, Error> {
        if req.data.len() <= INLINE_WRITE {
            return self.unary(api::FS_WRITE, req, INFO_LIMIT).await;
        }
        let head = FsWrite { data: Bytes::new(), ..req.clone() };
        let mut w = self.fs_create(&head).await?;
        w.write(req.data.clone()).await?;
        w.finish().await
    }

    /// Starts writing a file, with `req.data` as its first bytes and the rest to come through
    /// the [`FileWriter`]. The file only changes once [`FileWriter::finish`] returns, and
    /// dropping the writer before that leaves it as it was.
    pub async fn fs_create(&self, req: &FsWrite) -> Result<FileWriter, Error> {
        let stream = self.channel.open(api::FS_WRITE, req.encode_to_vec().into()).await?;
        Ok(FileWriter { stream })
    }

    /// Describes a path.
    pub async fn fs_stat(&self, req: &FsPath) -> Result<FileInfo, Error> {
        self.unary(api::FS_STAT, req, INFO_LIMIT).await
    }

    /// Lists a directory.
    pub async fn fs_list(&self, req: &FsList) -> Result<FsListResult, Error> {
        self.unary(api::FS_LIST, req, LIST_LIMIT).await
    }

    /// Makes a directory.
    pub async fn fs_mkdir(&self, req: &FsMkdir) -> Result<FileInfo, Error> {
        self.unary(api::FS_MKDIR, req, INFO_LIMIT).await
    }

    /// Removes a path.
    pub async fn fs_remove(&self, req: &FsPath) -> Result<(), Error> {
        self.channel.call(api::FS_REMOVE, req.encode_to_vec().into(), 0).await.map(|_| ())
    }

    /// Moves a path.
    pub async fn fs_rename(&self, req: &FsRename) -> Result<FileInfo, Error> {
        self.unary(api::FS_RENAME, req, INFO_LIMIT).await
    }

    /// Changes permission bits.
    pub async fn fs_chmod(&self, req: &FsChmod) -> Result<FileInfo, Error> {
        self.unary(api::FS_CHMOD, req, INFO_LIMIT).await
    }

    /// Unpacks the tar archive `tar` into the directory `req.path`.
    pub async fn fs_upload(&self, req: &FsUpload, tar: Bytes) -> Result<FsUploadResult, Error> {
        let mut w = self.fs_upload_start(req).await?;
        w.write(tar).await?;
        w.finish().await
    }

    /// Starts unpacking a tar archive that is sent through the [`ArchiveWriter`] a piece at a
    /// time.
    pub async fn fs_upload_start(&self, req: &FsUpload) -> Result<ArchiveWriter, Error> {
        let stream = self.channel.open(api::FS_UPLOAD, req.encode_to_vec().into()).await?;
        Ok(ArchiveWriter { stream })
    }

    /// Packs a file or directory into a tar archive in memory. Fails with `OUTPUT_LIMIT` past
    /// `limit` bytes. Use [`Client::fs_download_start`] for trees too big to hold.
    pub async fn fs_download(&self, req: &FsPath, limit: usize) -> Result<Bytes, Error> {
        self.channel.call(api::FS_DOWNLOAD, req.encode_to_vec().into(), limit).await
    }

    /// Starts packing a file or directory into a tar archive that arrives through the
    /// [`ArchiveReader`].
    pub async fn fs_download_start(&self, req: &FsPath) -> Result<ArchiveReader, Error> {
        let mut stream = self.channel.open(api::FS_DOWNLOAD, req.encode_to_vec().into()).await?;
        stream.finish().await?;
        Ok(ArchiveReader { stream })
    }

    /// Watches a directory, and with `req.recursive` everything under it. Returns once the
    /// watch is in place, so any change made after this returns is reported.
    pub async fn fs_watch(&self, req: &FsPath) -> Result<Watcher, Error> {
        let mut stream = self.channel.open(api::FS_WATCH, req.encode_to_vec().into()).await?;
        let first = stream
            .recv()
            .await?
            .ok_or_else(|| Error::new(Reason::Internal, "the watch ended before it started"))?;
        let info = FileInfo::decode(first).map_err(bad_answer)?;
        Ok(Watcher { stream, info })
    }

    /// Starts a command with its input and output streamed.
    pub async fn start(&self, cmd: &Command) -> Result<Process, Error> {
        let stream = self.channel.open(api::PROCESS_START, cmd.encode_to_vec().into()).await?;
        let (tx, rx) = stream.split();
        Ok(Process { input: ProcessInput { tx }, output: ProcessOutput { rx, done: false } })
    }
}

/// A file being read, from [`Client::fs_open`].
#[derive(Debug)]
pub struct FileReader {
    stream: Stream,
}

impl FileReader {
    /// The next part of the file, or `None` at the end.
    pub async fn next(&mut self) -> Result<Option<Bytes>, Error> {
        self.stream.recv().await
    }
}

/// A file being written, from [`Client::fs_create`].
#[derive(Debug)]
pub struct FileWriter {
    stream: Stream,
}

impl FileWriter {
    /// Adds `data` to the file.
    pub async fn write(&mut self, data: Bytes) -> Result<(), Error> {
        self.stream.send(data).await
    }

    /// Ends the file and waits for the drone to put it in place.
    pub async fn finish(mut self) -> Result<FileInfo, Error> {
        self.stream.finish().await?;
        let answer = collect(&mut self.stream, INFO_LIMIT).await?;
        FileInfo::decode(&answer[..]).map_err(bad_answer)
    }
}

// Reads the rest of a stream, up to `limit` bytes.
async fn collect(stream: &mut Stream, limit: usize) -> Result<Vec<u8>, Error> {
    let mut answer = Vec::new();
    while let Some(chunk) = stream.recv().await? {
        if answer.len() + chunk.len() > limit {
            return Err(Error::new(Reason::OutputLimit, "the answer is over the limit"));
        }
        answer.extend_from_slice(&chunk);
    }
    Ok(answer)
}

/// A tar archive being unpacked, from [`Client::fs_upload_start`].
#[derive(Debug)]
pub struct ArchiveWriter {
    stream: Stream,
}

impl ArchiveWriter {
    /// Sends the next part of the archive.
    pub async fn write(&mut self, data: Bytes) -> Result<(), Error> {
        self.stream.send(data).await
    }

    /// Ends the archive and waits for the drone to finish unpacking it.
    pub async fn finish(mut self) -> Result<FsUploadResult, Error> {
        self.stream.finish().await?;
        let answer = collect(&mut self.stream, INFO_LIMIT).await?;
        FsUploadResult::decode(&answer[..]).map_err(bad_answer)
    }
}

/// A tar archive being packed, from [`Client::fs_download_start`].
#[derive(Debug)]
pub struct ArchiveReader {
    stream: Stream,
}

impl ArchiveReader {
    /// The next part of the archive, or `None` at the end.
    pub async fn next(&mut self) -> Result<Option<Bytes>, Error> {
        self.stream.recv().await
    }
}

/// A directory being watched, from [`Client::fs_watch`]. Dropping it ends the watch.
#[derive(Debug)]
pub struct Watcher {
    stream: Stream,
    info: FileInfo,
}

impl Watcher {
    /// The watched directory, as it was when the watch started.
    #[must_use]
    pub fn info(&self) -> &FileInfo {
        &self.info
    }

    /// The next batch of changes, or `None` once the directory has gone and the watch is over.
    pub async fn next(&mut self) -> Result<Option<FsEvents>, Error> {
        match self.stream.recv().await? {
            Some(frame) => FsEvents::decode(frame).map(Some).map_err(bad_answer),
            None => Ok(None),
        }
    }

    /// Ends the watch and waits for the drone to stop it.
    pub async fn close(mut self) -> Result<(), Error> {
        self.stream.finish().await?;
        while self.stream.recv().await?.is_some() {}
        Ok(())
    }
}

/// A command started with [`Client::start`]. Dropping it kills the command.
///
/// Feeding a command a lot of input while not reading its output stalls the way a pipe does, once
/// the command's output fills up. [`Process::split`] gives an input half and an output half that
/// can run on two tasks.
#[derive(Debug)]
pub struct Process {
    input: ProcessInput,
    output: ProcessOutput,
}

/// The input half of a [`Process`].
#[derive(Debug)]
pub struct ProcessInput {
    tx: SendHalf,
}

/// The output half of a [`Process`].
#[derive(Debug)]
pub struct ProcessOutput {
    rx: RecvHalf,
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
    /// Splits into an input half and an output half. The command is killed once both are
    /// dropped, unless it has exited and the input half was closed with
    /// [`ProcessInput::finish`].
    #[must_use]
    pub fn split(self) -> (ProcessInput, ProcessOutput) {
        (self.input, self.output)
    }

    /// Writes to the command's stdin.
    pub async fn write(&mut self, data: &[u8]) -> Result<(), Error> {
        self.input.write(data).await
    }

    /// Closes the command's stdin.
    pub async fn close_stdin(&mut self) -> Result<(), Error> {
        self.input.close_stdin().await
    }

    /// Sends `signal` to the command's process group.
    pub async fn signal(&mut self, signal: i32) -> Result<(), Error> {
        self.input.signal(signal).await
    }

    /// The next thing the command did, or `None` after [`Output::Exit`].
    pub async fn next(&mut self) -> Result<Option<Output>, Error> {
        let out = self.output.next().await?;
        if let Some(Output::Exit(_)) = out {
            self.input.finish().await?;
        }
        Ok(out)
    }

    /// Reads everything to the end and returns the result with the output filled in.
    pub async fn wait(self) -> Result<RunResult, Error> {
        let (mut input, output) = self.split();
        let result = output.wait().await?;
        input.finish().await?;
        Ok(result)
    }
}

impl ProcessInput {
    /// Writes to the command's stdin.
    pub async fn write(&mut self, data: &[u8]) -> Result<(), Error> {
        for chunk in data.chunks(MAX_CHUNK) {
            self.tx.send(tagged(tag::STDIN, chunk)).await?;
        }
        Ok(())
    }

    /// Closes the command's stdin.
    pub async fn close_stdin(&mut self) -> Result<(), Error> {
        self.tx.send(tagged(tag::EOF, &[])).await
    }

    /// Sends `signal` to the command's process group.
    pub async fn signal(&mut self, signal: i32) -> Result<(), Error> {
        self.tx.send(tagged(tag::SIGNAL, &signal.to_be_bytes())).await
    }

    /// Ends the node's side of the call. Nothing more can be sent, and stdin is closed.
    pub async fn finish(&mut self) -> Result<(), Error> {
        self.tx.finish().await
    }
}

impl ProcessOutput {
    /// The next thing the command did, or `None` after [`Output::Exit`].
    pub async fn next(&mut self) -> Result<Option<Output>, Error> {
        loop {
            if self.done {
                return Ok(None);
            }
            let Some(msg) = self.rx.recv().await? else {
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
                    // The exit is the drone's last frame. Reading its end lets the stream close
                    // cleanly.
                    while self.rx.recv().await?.is_some() {}
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
