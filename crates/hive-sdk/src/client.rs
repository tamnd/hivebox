//! The client, the cells it hands out and what can be done in them.

use crate::Error;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt, TryStreamExt};
use hive_proto::convert;
use hive_proto::v1;
use hive_proto::v1::cells_client::CellsClient;
use hive_proto::v1::exec_client::ExecClient;
use hive_proto::v1::files_client::FilesClient;
use hive_proto::v1::snapshots_client::SnapshotsClient;
use hive_proto::v1::verify_client::VerifyClient;
use hive_types::{CellSpec, Reason};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::mpsc;
use tonic::metadata::{AsciiMetadataValue, MetadataValue};
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Streaming};

/// The comb's socket when nothing else is named.
pub const DEFAULT_SOCKET: &str = "/run/hivebox/comb.sock";
/// How much of a file goes in one message of a write.
const WRITE_CHUNK: usize = 1 << 20;
/// The biggest message the client takes, which is mostly a command's output.
const MAX_ANSWER: usize = 256 << 20;

/// A connection to a comb's local API, or to a gate. It is cheap to clone, and the clones share
/// one connection.
#[derive(Clone, Debug)]
pub struct Client {
    channel: Channel,
    project: Option<AsciiMetadataValue>,
    token: Option<AsciiMetadataValue>,
}

impl Client {
    /// Connects to the comb listening on the Unix socket at `path`.
    ///
    /// # Errors
    ///
    /// `INTERNAL`, with the path and the cause, when the socket cannot be opened.
    pub async fn unix(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref().to_path_buf();
        let shown = path.display().to_string();
        // The URI is required and never used, since the connector ignores it.
        let channel = Endpoint::from_static("http://comb")
            .connect_with_connector(tower::service_fn(move |_| {
                let path: PathBuf = path.clone();
                async move {
                    let s = tokio::net::UnixStream::connect(path).await?;
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(s))
                }
            }))
            .await
            .map_err(|e| unreachable(&shown, &e))?;
        Ok(Self::with_channel(channel))
    }

    /// Connects to `endpoint`, which is `unix:/path/to/comb.sock` or an `http://` address.
    ///
    /// # Errors
    ///
    /// The endpoint is not one of those, or nothing answers there.
    pub async fn connect(endpoint: &str) -> Result<Self, Error> {
        if let Some(path) = endpoint.strip_prefix("unix:") {
            return Self::unix(path).await;
        }
        if !endpoint.starts_with("http://") {
            return Err(Error::new(
                Reason::InvalidArgument,
                format!("{endpoint} is not unix:<path> or http://<host>:<port>"),
            ));
        }
        let channel = Endpoint::from_shared(endpoint.to_string())
            .map_err(|e| Error::new(Reason::InvalidArgument, format!("{endpoint}: {e}")))?
            .connect()
            .await
            .map_err(|e| unreachable(endpoint, &e))?;
        Ok(Self::with_channel(channel))
    }

    /// A client on a channel made elsewhere.
    #[must_use]
    pub fn with_channel(channel: Channel) -> Self {
        Self { channel, project: None, token: None }
    }

    /// Makes every call for `project`. A comb puts calls that name none in `local`.
    ///
    /// # Errors
    ///
    /// The name cannot go in a header.
    pub fn project(mut self, project: &str) -> Result<Self, Error> {
        self.project = Some(header(project)?);
        Ok(self)
    }

    /// Sends `token` as the bearer token on every call. A comb's local API ignores it.
    ///
    /// # Errors
    ///
    /// The token cannot go in a header.
    pub fn token(mut self, token: &str) -> Result<Self, Error> {
        self.token = Some(header(&format!("Bearer {token}"))?);
        Ok(self)
    }

    fn req<T>(&self, msg: T) -> Request<T> {
        let mut r = Request::new(msg);
        if let Some(p) = &self.project {
            r.metadata_mut().insert("x-hive-project", p.clone());
        }
        if let Some(t) = &self.token {
            r.metadata_mut().insert("authorization", t.clone());
        }
        r
    }

    fn cells(&self) -> CellsClient<Channel> {
        CellsClient::new(self.channel.clone()).max_decoding_message_size(MAX_ANSWER)
    }

    fn exec(&self) -> ExecClient<Channel> {
        ExecClient::new(self.channel.clone())
            .max_decoding_message_size(MAX_ANSWER)
            .max_encoding_message_size(MAX_ANSWER)
    }

    fn files(&self) -> FilesClient<Channel> {
        FilesClient::new(self.channel.clone()).max_decoding_message_size(MAX_ANSWER)
    }

    /// Makes one cell and waits until it is running.
    ///
    /// # Errors
    ///
    /// The cell could not be made, with the reason the node gave.
    pub async fn create(&self, spec: &CellSpec) -> Result<Cell, Error> {
        self.create_many(spec, 1, None)
            .await?
            .pop()
            .unwrap_or_else(|| Err(Error::new(Reason::Internal, "the create ended without a cell")))
    }

    /// Makes `count` cells from one spec at once, and returns each one's outcome in order. With
    /// `key`, a retry of the same call gets the same cells back instead of new ones.
    ///
    /// # Errors
    ///
    /// The call as a whole failed. A cell that failed on its own is an error in the list.
    pub async fn create_many(
        &self,
        spec: &CellSpec,
        count: u32,
        key: Option<&str>,
    ) -> Result<Vec<Result<Cell, Error>>, Error> {
        let r = v1::CreateRequest {
            spec: Some(convert::spec_to_v1(spec)),
            count,
            idempotency_key: key.unwrap_or_default().to_string(),
            placement: None,
        };
        let mut events = self.cells().create(self.req(r)).await.map_err(from_status)?.into_inner();
        let mut out: Vec<Result<Cell, Error>> = (0..count.max(1))
            .map(|_| Err(Error::new(Reason::Internal, "the create ended without this cell")))
            .collect();
        while let Some(e) = events.message().await.map_err(from_status)? {
            let Some(slot) = out.get_mut(e.index as usize) else { continue };
            *slot = match e.result {
                Some(v1::create_event::Result::Cell(c)) => Ok(Cell::new(self.clone(), c)),
                Some(v1::create_event::Result::Error(e)) => Err(error_from_v1(&e)),
                None => continue,
            };
        }
        Ok(out)
    }

    /// The cell `id`.
    ///
    /// # Errors
    ///
    /// `CELL_NOT_FOUND` when the project has no such cell.
    pub async fn get(&self, id: &str) -> Result<Cell, Error> {
        let r = v1::GetCellRequest { id: id.to_string() };
        let c = self.cells().get(self.req(r)).await.map_err(from_status)?.into_inner();
        Ok(Cell::new(self.clone(), c))
    }

    /// Every cell of the project with all of `labels`, in any of `states` or in any state when
    /// `states` is empty. It pages through the whole list.
    ///
    /// # Errors
    ///
    /// A page could not be fetched.
    pub async fn list(
        &self,
        labels: &BTreeMap<String, String>,
        states: &[v1::CellState],
    ) -> Result<Vec<Cell>, Error> {
        let mut out = Vec::new();
        let mut page_token = String::new();
        loop {
            let r = v1::ListCellsRequest {
                selector: (!labels.is_empty()).then(|| label_selector(labels)),
                states: states.iter().map(|&s| s.into()).collect(),
                page_size: 0,
                page_token,
            };
            let page = self.cells().list(self.req(r)).await.map_err(from_status)?.into_inner();
            out.extend(page.cells.into_iter().map(|c| Cell::new(self.clone(), c)));
            if page.next_page_token.is_empty() {
                return Ok(out);
            }
            page_token = page.next_page_token;
        }
    }

    /// Pauses what `sel` picks.
    ///
    /// # Errors
    ///
    /// The call failed as a whole. Cells that failed on their own are in the result.
    pub async fn pause(&self, sel: &Selector) -> Result<v1::BulkResult, Error> {
        let r = self.cells().pause(self.req(sel.to_v1())).await;
        Ok(r.map_err(from_status)?.into_inner())
    }

    /// Resumes what `sel` picks.
    ///
    /// # Errors
    ///
    /// The call failed as a whole. Cells that failed on their own are in the result.
    pub async fn resume(&self, sel: &Selector) -> Result<v1::BulkResult, Error> {
        let r = self.cells().resume(self.req(sel.to_v1())).await;
        Ok(r.map_err(from_status)?.into_inner())
    }

    /// Stops what `sel` picks.
    ///
    /// # Errors
    ///
    /// The call failed as a whole. Cells that failed on their own are in the result.
    pub async fn stop(&self, sel: &Selector) -> Result<v1::BulkResult, Error> {
        let r = v1::StopRequest { selector: Some(sel.to_v1()), snapshot: false };
        let r = self.cells().stop(self.req(r)).await;
        Ok(r.map_err(from_status)?.into_inner())
    }

    /// Quarantines what `sel` picks: each cell is frozen for good, cut off the network and kept
    /// as an unscrubbed disk snapshot, and stays paused until it is stopped. `reason` goes in the
    /// audit log.
    ///
    /// # Errors
    ///
    /// The call failed as a whole. Cells that failed on their own are in the result.
    pub async fn quarantine(
        &self,
        sel: &Selector,
        reason: &str,
    ) -> Result<v1::QuarantineResponse, Error> {
        let r = v1::QuarantineRequest { selector: Some(sel.to_v1()), reason: reason.into() };
        let r = self.cells().quarantine(self.req(r)).await;
        Ok(r.map_err(from_status)?.into_inner())
    }

    /// Changes a cell's timers: `hard` is how long from now until it is stopped, and `idle`
    /// replaces its idle TTL. Either is left as it was when `None`.
    ///
    /// # Errors
    ///
    /// The cell is not found or has ended.
    pub async fn extend_ttl(
        &self,
        id: &str,
        hard: Option<Duration>,
        idle: Option<Duration>,
    ) -> Result<Cell, Error> {
        let r = v1::ExtendTtlRequest {
            id: id.to_string(),
            hard_ttl: hard.map(convert::duration_to_v1),
            idle_ttl: idle.map(convert::duration_to_v1),
        };
        let c = self.cells().extend_ttl(self.req(r)).await.map_err(from_status)?.into_inner();
        Ok(Cell::new(self.clone(), c))
    }

    /// Tells the node the cell's setup is done, so its setup boost ends and it drops to its steady
    /// CPU quota. A cell without a boost is left as it is.
    ///
    /// # Errors
    ///
    /// The cell is not found or has ended.
    pub async fn ready(&self, id: &str) -> Result<Cell, Error> {
        let r = v1::UpdatePolicyRequest { id: id.to_string(), ready: true, ..Default::default() };
        let c = self.cells().update_policy(self.req(r)).await.map_err(from_status)?.into_inner();
        Ok(Cell::new(self.clone(), c))
    }

    /// Checks a subject cell's changes to a git checkout in a fresh cell with no network, as
    /// `spec/11_rl_integration.md` section 5 tells. A verdict comes back even when hivebox
    /// failed partway, with the failure in its `error`.
    ///
    /// # Errors
    ///
    /// The request is malformed, or the subject is not found.
    pub async fn verify(&self, req: v1::VerifyRequest) -> Result<v1::VerifyResult, Error> {
        let mut c = VerifyClient::new(self.channel.clone())
            .max_decoding_message_size(MAX_ANSWER)
            .max_encoding_message_size(MAX_ANSWER);
        Ok(c.run(self.req(req)).await.map_err(from_status)?.into_inner())
    }

    /// Takes a disk snapshot of the container cell `id` and returns its id. A running cell is
    /// frozen while its changes are sealed. With `scrub`, secrets are taken out first, and a
    /// file that still holds one fails the snapshot unless it is under a path in `allow`. Each
    /// git work tree in `squash_git` is first rebuilt with one commit holding what `HEAD` holds,
    /// and no other history. To restore it, make cells with
    /// [`Source::Snapshot`](hive_types::Source::Snapshot).
    ///
    /// # Errors
    ///
    /// The cell is not running or paused, is not a container, or the node cannot take
    /// snapshots. With `scrub`, a secret was found outside `allow`. A repository in
    /// `squash_git` could not be squashed.
    pub async fn snapshot(
        &self,
        id: &str,
        scrub: bool,
        allow: &[String],
        squash_git: &[String],
    ) -> Result<String, Error> {
        let r = v1::SnapshotRequest {
            cell_id: id.to_string(),
            kind: v1::SnapshotKind::Disk.into(),
            scrub,
            allow: allow.to_vec(),
            squash_git: squash_git.to_vec(),
            ..Default::default()
        };
        let mut c = SnapshotsClient::new(self.channel.clone());
        Ok(c.snapshot(self.req(r)).await.map_err(from_status)?.into_inner().id)
    }

    /// Names the scrubbed snapshot `snapshot` as the image `name` in the project, so cells can
    /// be made from it by name. Committing again under the same name moves it.
    ///
    /// # Errors
    ///
    /// The snapshot is not in the store, was not scrubbed, or the name is not a plain name.
    pub async fn commit(&self, snapshot: &str, name: &str) -> Result<(), Error> {
        let r = v1::CommitRequest {
            snapshot: Some(v1::SnapshotRef { id: snapshot.to_string() }),
            name: name.to_string(),
        };
        let mut c = SnapshotsClient::new(self.channel.clone());
        c.commit(self.req(r)).await.map_err(from_status)?;
        Ok(())
    }

    /// The changes of state of what `sel` picks, starting with each cell's current state. A
    /// watch by id ends once the cell has ended.
    ///
    /// # Errors
    ///
    /// The watch could not start.
    pub async fn watch(
        &self,
        sel: &Selector,
    ) -> Result<BoxStream<'static, Result<v1::CellEvent, Error>>, Error> {
        let r = v1::WatchCellsRequest { selector: Some(sel.to_v1()) };
        let events = self.cells().watch(self.req(r)).await.map_err(from_status)?.into_inner();
        Ok(events.map_err(from_status).boxed())
    }
}

/// Which cells a bulk call acts on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Selector {
    /// One cell.
    Id(String),
    /// Every cell with all of these labels. No labels picks every cell of the project.
    Labels(BTreeMap<String, String>),
}

impl Selector {
    fn to_v1(&self) -> v1::CellSelector {
        let by = match self {
            Self::Id(id) => v1::cell_selector::By::Id(id.clone()),
            Self::Labels(l) => v1::cell_selector::By::Labels(label_selector(l)),
        };
        v1::CellSelector { by: Some(by) }
    }
}

/// A command to run in a cell, as argv or as a shell script.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Command {
    argv: Vec<String>,
    shell: String,
    cwd: String,
    env: BTreeMap<String, String>,
    stdin: Bytes,
    timeout: Option<Duration>,
    max_output: u64,
    user: String,
}

impl Command {
    /// Runs `argv` as it is, with no shell.
    #[must_use]
    pub fn new<S: Into<String>>(argv: impl IntoIterator<Item = S>) -> Self {
        Self { argv: argv.into_iter().map(Into::into).collect(), ..Self::default() }
    }

    /// Runs `script` with `sh -c`.
    #[must_use]
    pub fn shell(script: impl Into<String>) -> Self {
        Self { shell: script.into(), ..Self::default() }
    }

    /// Runs in `dir` instead of the cell's working directory.
    #[must_use]
    pub fn cwd(mut self, dir: impl Into<String>) -> Self {
        self.cwd = dir.into();
        self
    }

    /// Sets an environment variable on top of the cell's.
    #[must_use]
    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert(key.into(), value.into());
        self
    }

    /// What the command reads on stdin, for [`Cell::run`].
    #[must_use]
    pub fn stdin(mut self, data: impl Into<Bytes>) -> Self {
        self.stdin = data.into();
        self
    }

    /// Kills the command after `t`. The cell's limit applies when unset.
    #[must_use]
    pub fn timeout(mut self, t: Duration) -> Self {
        self.timeout = Some(t);
        self
    }

    /// Keeps at most `bytes` of output, for [`Cell::run`]. The cell's limit applies when unset.
    #[must_use]
    pub fn max_output(mut self, bytes: u64) -> Self {
        self.max_output = bytes;
        self
    }

    /// Runs as `uid` or `uid:gid`.
    #[must_use]
    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.user = user.into();
        self
    }
}

impl From<&str> for Command {
    fn from(script: &str) -> Self {
        Self::shell(script)
    }
}

impl From<String> for Command {
    fn from(script: String) -> Self {
        Self::shell(script)
    }
}

impl<S: Into<String>, const N: usize> From<[S; N]> for Command {
    fn from(argv: [S; N]) -> Self {
        Self::new(argv)
    }
}

impl From<Vec<String>> for Command {
    fn from(argv: Vec<String>) -> Self {
        Self::new(argv)
    }
}

/// One cell, as it was when it was fetched.
#[derive(Clone, Debug)]
pub struct Cell {
    client: Client,
    info: v1::Cell,
}

impl Cell {
    fn new(client: Client, info: v1::Cell) -> Self {
        Self { client, info }
    }

    /// Its id.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.info.id
    }

    /// Everything the node said about it when it was fetched.
    #[must_use]
    pub fn info(&self) -> &v1::Cell {
        &self.info
    }

    /// Its state when it was fetched. [`Cell::refresh`] fetches it again.
    #[must_use]
    pub fn state(&self) -> v1::CellState {
        self.info.state()
    }

    /// Fetches the cell again.
    ///
    /// # Errors
    ///
    /// `CELL_NOT_FOUND` once it is gone.
    pub async fn refresh(&mut self) -> Result<(), Error> {
        self.info = self.client.get(&self.info.id).await?.info;
        Ok(())
    }

    fn sel(&self) -> Selector {
        Selector::Id(self.info.id.clone())
    }

    /// Gives the cell `hard` more time from now before it is stopped.
    ///
    /// # Errors
    ///
    /// The cell has ended.
    pub async fn extend(&mut self, hard: Duration) -> Result<(), Error> {
        self.info = self.client.extend_ttl(&self.info.id, Some(hard), None).await?.info;
        Ok(())
    }

    /// Ends the cell's setup boost, once whatever it installs or builds first is done.
    ///
    /// # Errors
    ///
    /// The cell has ended.
    pub async fn ready(&mut self) -> Result<(), Error> {
        self.info = self.client.ready(&self.info.id).await?.info;
        Ok(())
    }

    /// Pauses the cell.
    ///
    /// # Errors
    ///
    /// The cell could not be paused.
    pub async fn pause(&self) -> Result<(), Error> {
        one(self.client.pause(&self.sel()).await?)
    }

    /// Resumes the cell. Running a command or touching a file in a paused cell resumes it too.
    ///
    /// # Errors
    ///
    /// The cell could not be resumed.
    pub async fn resume(&self) -> Result<(), Error> {
        one(self.client.resume(&self.sel()).await?)
    }

    /// Stops the cell.
    ///
    /// # Errors
    ///
    /// The cell could not be stopped.
    pub async fn stop(&self) -> Result<(), Error> {
        one(self.client.stop(&self.sel()).await?)
    }

    /// Quarantines the cell, as [`Client::quarantine`] does, and returns what became of its
    /// network and its snapshot.
    ///
    /// # Errors
    ///
    /// The cell could not be frozen or cut off.
    pub async fn quarantine(&self, reason: &str) -> Result<v1::QuarantinedCell, Error> {
        let mut r = self.client.quarantine(&self.sel(), reason).await?;
        one(r.result.take().unwrap_or_default())?;
        r.cells
            .pop()
            .ok_or_else(|| Error::new(Reason::Internal, "the node did not say what it did"))
    }

    /// Takes a disk snapshot of the cell, as [`Client::snapshot`] does.
    ///
    /// # Errors
    ///
    /// The snapshot could not be taken.
    pub async fn snapshot(
        &self,
        scrub: bool,
        allow: &[String],
        squash_git: &[String],
    ) -> Result<String, Error> {
        self.client.snapshot(self.id(), scrub, allow, squash_git).await
    }

    /// Runs `cmd` and waits for it to end.
    ///
    /// # Errors
    ///
    /// The command could not be run. A command that ran and failed is an `Ok` with its exit
    /// code.
    pub async fn run(&self, cmd: impl Into<Command>) -> Result<v1::RunResult, Error> {
        let c = cmd.into();
        let r = v1::RunRequest {
            cell_id: self.info.id.clone(),
            argv: c.argv,
            shell: c.shell,
            cwd: c.cwd,
            env: c.env.into_iter().collect(),
            stdin: c.stdin,
            timeout: c.timeout.map(convert::duration_to_v1),
            max_output_bytes: c.max_output,
            user: c.user,
            idempotency_key: String::new(),
        };
        let r = self.client.exec().run(self.client.req(r)).await;
        Ok(r.map_err(from_status)?.into_inner())
    }

    /// Starts `cmd` with its input and output streamed. What [`Command::stdin`] set is sent
    /// first.
    ///
    /// # Errors
    ///
    /// The command could not be started.
    pub async fn start(&self, cmd: impl Into<Command>) -> Result<Process, Error> {
        use v1::process_input::Input;
        let c = cmd.into();
        let start = v1::ProcessStart {
            cell_id: self.info.id.clone(),
            argv: c.argv,
            shell: c.shell,
            cwd: c.cwd,
            env: c.env.into_iter().collect(),
            timeout: c.timeout.map(convert::duration_to_v1),
            user: c.user,
            pty: None,
        };
        let (tx, mut rx) = mpsc::channel(16);
        let input = |i| v1::ProcessInput { input: Some(i) };
        // The channel has room for these, and the receiver is still here.
        let _ = tx.try_send(input(Input::Start(start)));
        if !c.stdin.is_empty() {
            let _ = tx.try_send(input(Input::Stdin(c.stdin)));
        }
        let requests = stream::poll_fn(move |cx| rx.poll_recv(cx));
        let out = self.client.exec().start(self.client.req(requests)).await;
        let out = out.map_err(from_status)?.into_inner();
        Ok(Process { tx: Some(tx), out, pid: None })
    }

    /// Opens a shell that keeps its directory and environment from one command to the next.
    ///
    /// # Errors
    ///
    /// The shell could not be started.
    pub async fn session(&self) -> Result<Session, Error> {
        let r = v1::SessionCreateRequest { cell_id: self.info.id.clone(), ..Default::default() };
        let s = self.client.exec().session_create(self.client.req(r)).await;
        let s = s.map_err(from_status)?.into_inner();
        Ok(Session {
            client: self.client.clone(),
            r: v1::SessionRef { cell_id: s.cell_id, id: s.id },
        })
    }

    /// Reads a whole file.
    ///
    /// # Errors
    ///
    /// `FILE_ERROR` with the errno when the file cannot be read.
    pub async fn read(&self, path: &str) -> Result<Vec<u8>, Error> {
        self.read_range(path, 0, 0).await
    }

    /// Reads `length` bytes from `offset`, or to the end when `length` is zero.
    ///
    /// # Errors
    ///
    /// `FILE_ERROR` with the errno when the file cannot be read.
    pub async fn read_range(&self, path: &str, offset: u64, length: u64) -> Result<Vec<u8>, Error> {
        let mut chunks = self.open(path, offset, length).await?;
        let mut out = Vec::new();
        while let Some(c) = chunks.next().await {
            out.extend_from_slice(&c?);
        }
        Ok(out)
    }

    /// Reads a file as it arrives, for files too big to hold.
    ///
    /// # Errors
    ///
    /// `FILE_ERROR` with the errno when the file cannot be opened.
    pub async fn open(
        &self,
        path: &str,
        offset: u64,
        length: u64,
    ) -> Result<BoxStream<'static, Result<Bytes, Error>>, Error> {
        let r = v1::ReadFileRequest {
            cell_id: self.info.id.clone(),
            path: path.into(),
            offset,
            length,
        };
        let chunks = self.client.files().read(self.client.req(r)).await;
        let chunks = chunks.map_err(from_status)?.into_inner();
        Ok(chunks.map_ok(|c| c.data).map_err(from_status).boxed())
    }

    /// Writes a file, making its parent directories. Readers see the old file or the new one,
    /// never half of it.
    ///
    /// # Errors
    ///
    /// `FILE_ERROR` with the errno when the file cannot be written.
    pub async fn write(&self, path: &str, data: impl Into<Bytes>) -> Result<v1::FileInfo, Error> {
        let data: Bytes = data.into();
        let chunks = (0..data.len())
            .step_by(WRITE_CHUNK)
            .map(move |at| data.slice(at..(at + WRITE_CHUNK).min(data.len())));
        self.write_stream(path, 0, stream::iter(chunks)).await
    }

    /// Writes a file from a stream of chunks, with permission bits `mode`, 0644 when zero.
    ///
    /// # Errors
    ///
    /// `FILE_ERROR` with the errno when the file cannot be written.
    pub async fn write_stream(
        &self,
        path: &str,
        mode: u32,
        chunks: impl futures::Stream<Item = Bytes> + Send + 'static,
    ) -> Result<v1::FileInfo, Error> {
        use v1::write_file_chunk::Part;
        let header = v1::WriteFileHeader {
            cell_id: self.info.id.clone(),
            path: path.into(),
            mode,
            make_parents: true,
            ..Default::default()
        };
        let head =
            stream::once(async move { v1::WriteFileChunk { part: Some(Part::Header(header)) } });
        let body = chunks.map(|b| v1::WriteFileChunk { part: Some(Part::Data(b)) });
        let r = self.client.files().write(self.client.req(head.chain(body))).await;
        Ok(r.map_err(from_status)?.into_inner())
    }

    /// Describes a path. A symlink is described as itself, with where it points.
    ///
    /// # Errors
    ///
    /// `FILE_ERROR` with the errno, `ENOENT` when nothing is there.
    pub async fn stat(&self, path: &str) -> Result<v1::FileInfo, Error> {
        let r =
            v1::PathRequest { cell_id: self.info.id.clone(), path: path.into(), recursive: false };
        let r = self.client.files().stat(self.client.req(r)).await;
        Ok(r.map_err(from_status)?.into_inner())
    }

    /// Lists a directory `depth` levels down, one when zero, sorted by path.
    ///
    /// # Errors
    ///
    /// `FILE_ERROR` with the errno when it cannot be listed.
    pub async fn list(&self, path: &str, depth: u32) -> Result<v1::ListDirResponse, Error> {
        let r = v1::ListDirRequest { cell_id: self.info.id.clone(), path: path.into(), depth };
        let r = self.client.files().list(self.client.req(r)).await;
        Ok(r.map_err(from_status)?.into_inner())
    }

    /// Removes a path, and with `recursive` a directory with everything in it.
    ///
    /// # Errors
    ///
    /// `FILE_ERROR` with the errno when it cannot be removed.
    pub async fn remove(&self, path: &str, recursive: bool) -> Result<(), Error> {
        let r = v1::PathRequest { cell_id: self.info.id.clone(), path: path.into(), recursive };
        self.client.files().remove(self.client.req(r)).await.map_err(from_status)?;
        Ok(())
    }

    /// Unpacks the tar archive `tar` into the directory `path`, making it if it is missing.
    ///
    /// # Errors
    ///
    /// `FILE_ERROR` with the errno, or `INVALID_ARGUMENT` for an archive that is not a tar.
    pub async fn upload(&self, path: &str, tar: impl Into<Bytes>) -> Result<(), Error> {
        let r = v1::ApplyRequest {
            cell_id: self.info.id.clone(),
            path: path.into(),
            content: Some(v1::apply_request::Content::Tar(tar.into())),
        };
        self.client.files().apply(self.client.req(r)).await.map_err(from_status)?;
        Ok(())
    }

    /// The changes under the directory `path`, and with `recursive` everything below it, from
    /// the time this returns.
    ///
    /// # Errors
    ///
    /// `FILE_ERROR` with the errno when it cannot be watched.
    pub async fn watch(
        &self,
        path: &str,
        recursive: bool,
    ) -> Result<BoxStream<'static, Result<v1::FsEvent, Error>>, Error> {
        let r = v1::WatchDirRequest { cell_id: self.info.id.clone(), path: path.into(), recursive };
        let events = self.client.files().watch(self.client.req(r)).await;
        let events = events.map_err(from_status)?.into_inner();
        Ok(events.map_err(from_status).boxed())
    }
}

/// A command started with [`Cell::start`]. Dropping it kills the command.
#[derive(Debug)]
pub struct Process {
    tx: Option<mpsc::Sender<v1::ProcessInput>>,
    out: Streaming<v1::ProcessOutput>,
    pid: Option<u32>,
}

/// Something a started command did.
#[derive(Clone, Debug, PartialEq)]
pub enum Output {
    /// Bytes it wrote to stdout.
    Stdout(Bytes),
    /// Bytes it wrote to stderr.
    Stderr(Bytes),
    /// It ended. Nothing comes after this.
    Exit(v1::RunResult),
}

impl Process {
    /// Its pid in the cell, once the first output has been read.
    #[must_use]
    pub fn pid(&self) -> Option<u32> {
        self.pid
    }

    async fn send(&mut self, input: v1::process_input::Input) -> Result<(), Error> {
        let Some(tx) = &self.tx else {
            return Err(Error::new(Reason::InvalidArgument, "stdin is already closed"));
        };
        tx.send(v1::ProcessInput { input: Some(input) })
            .await
            .map_err(|_| Error::new(Reason::Internal, "the process has gone"))
    }

    /// Sends `data` to its stdin.
    ///
    /// # Errors
    ///
    /// Stdin is closed, or the process has gone.
    pub async fn write(&mut self, data: impl Into<Bytes>) -> Result<(), Error> {
        self.send(v1::process_input::Input::Stdin(data.into())).await
    }

    /// Sends it signal `n`.
    ///
    /// # Errors
    ///
    /// Stdin is closed, which also ends the way signals are sent, or the process has gone.
    pub async fn signal(&mut self, n: i32) -> Result<(), Error> {
        self.send(v1::process_input::Input::Signal(n)).await
    }

    /// Closes its stdin, so it reads end of file.
    pub fn close_stdin(&mut self) {
        self.tx = None;
    }

    /// The next thing it did, or `None` after it exited.
    ///
    /// # Errors
    ///
    /// The stream broke, with the reason the node gave.
    pub async fn next(&mut self) -> Result<Option<Output>, Error> {
        use v1::process_output::Output as Out;
        loop {
            let Some(o) = self.out.message().await.map_err(from_status)? else { return Ok(None) };
            match o.output {
                Some(Out::Pid(p)) => self.pid = Some(p),
                Some(Out::Stdout(b)) => return Ok(Some(Output::Stdout(b))),
                Some(Out::Stderr(b)) => return Ok(Some(Output::Stderr(b))),
                Some(Out::Exit(r)) => return Ok(Some(Output::Exit(r))),
                None => {}
            }
        }
    }
}

/// A shell in a cell that keeps its directory and environment between commands.
#[derive(Clone, Debug)]
pub struct Session {
    client: Client,
    r: v1::SessionRef,
}

impl Session {
    /// Its id.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.r.id
    }

    /// Runs `command` in the shell, with stdout and stderr together, and waits for it.
    ///
    /// # Errors
    ///
    /// The shell has gone, or the command could not be sent.
    pub async fn run(&self, command: &str) -> Result<v1::SessionRunResult, Error> {
        self.run_with(command, None, 0).await
    }

    /// [`Session::run`] with a timeout and an output cap, the cell's when unset.
    ///
    /// # Errors
    ///
    /// The shell has gone, or the command could not be sent.
    pub async fn run_with(
        &self,
        command: &str,
        timeout: Option<Duration>,
        max_output: u64,
    ) -> Result<v1::SessionRunResult, Error> {
        let r = v1::SessionRunRequest {
            session: Some(self.r.clone()),
            command: command.into(),
            timeout: timeout.map(convert::duration_to_v1),
            max_output_bytes: max_output,
        };
        let r = self.client.exec().session_run(self.client.req(r)).await;
        Ok(r.map_err(from_status)?.into_inner())
    }

    /// Ends the shell.
    ///
    /// # Errors
    ///
    /// It could not be ended.
    pub async fn close(self) -> Result<(), Error> {
        let r = self.client.exec().session_close(self.client.req(self.r.clone())).await;
        r.map_err(from_status)?;
        Ok(())
    }
}

fn label_selector(l: &BTreeMap<String, String>) -> v1::LabelSelector {
    v1::LabelSelector { r#match: l.clone().into_iter().collect() }
}

/// The outcome of a bulk call on one cell by id.
fn one(r: v1::BulkResult) -> Result<(), Error> {
    match r.failures.first() {
        Some(f) => Err(f.error.as_ref().map_or_else(
            || Error::new(Reason::Internal, "the call failed with no reason"),
            error_from_v1,
        )),
        None => Ok(()),
    }
}

fn header(v: &str) -> Result<AsciiMetadataValue, Error> {
    MetadataValue::try_from(v)
        .map_err(|_| Error::new(Reason::InvalidArgument, format!("{v:?} cannot go in a header")))
}

pub(crate) fn from_status(s: tonic::Status) -> Error {
    convert::error_from_status(&s)
}

fn error_from_v1(e: &v1::Error) -> Error {
    let reason = Reason::from_name(&e.reason).unwrap_or(Reason::Internal);
    let mut out = Error::new(reason, e.message.clone());
    out.errno = (!e.errno.is_empty()).then(|| e.errno.clone());
    out
}

fn unreachable(what: &str, e: &tonic::transport::Error) -> Error {
    use std::error::Error as _;
    let cause = e.source().map_or_else(|| e.to_string(), ToString::to_string);
    Error::new(Reason::Internal, format!("cannot reach {what}: {cause}"))
}
