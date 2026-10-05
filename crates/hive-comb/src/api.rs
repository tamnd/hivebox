//! The local API: the `hivebox.v1` Cells, Exec and Files services on a Unix socket, for
//! standalone mode, where no gate sits in front of the comb, and on TCP for gates when
//! `node.listen` is set.
//!
//! Whoever can open the socket is trusted, the way whoever can open the Docker socket is. The
//! socket is made with mode 0600, so that is root unless the operator hands it on. The TCP port
//! trusts its callers the same way, so it belongs on the private network that only gates reach.
//! A caller names its project in the `x-hive-project` header and gets `local` without one, and
//! every call sees and touches only that project's cells.

use crate::cell::{CellInfo, Status as CellStatus};
use crate::comb::{Comb, CreateRequest};
use futures::stream::{self, BoxStream, FuturesUnordered, StreamExt, TryStreamExt};
use hive_drone::Output;
use hive_proto::convert;
use hive_proto::drone::api as drone;
use hive_proto::v1;
use hive_proto::v1::cells_server::{Cells, CellsServer};
use hive_proto::v1::exec_server::{Exec, ExecServer};
use hive_proto::v1::files_server::{Files, FilesServer};
use hive_types::{CellId, CellState, Error, Reason, is_name};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::convert::Infallible;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime};
use tokio::net::{TcpListener, UnixListener};
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use tonic::codegen::{BoxFuture, Service, http};
use tonic::server::NamedService;
use tonic::{Request, Response, Status, Streaming};

/// The header that names the caller's project.
pub const PROJECT_HEADER: &str = "x-hive-project";
/// The header a gate sets on a keyed create that every node in the key's order turned away,
/// so a node makes it if it has room even when it turned the key away lately.
pub const ANYWAY_HEADER: &str = "x-hive-anyway";
/// The project of a caller that names none.
pub const DEFAULT_PROJECT: &str = "local";
/// The most cells one create call may ask for.
const MAX_COUNT: u32 = 1024;
/// Cells in a list page when the caller asks for no size, and the most it may ask for.
const PAGE: usize = 1000;
const MAX_PAGE: usize = 10_000;
/// The biggest request message, which is mostly stdin for a run.
const MAX_REQUEST: usize = 64 << 20;
/// A write this small or smaller goes to the drone in one message instead of a stream.
const SMALL_WRITE: usize = 32 << 10;

/// Serves the local API on `listener`, from [`bind`], and on `tcp` for gates when there is one,
/// until `stop` is cancelled. Both share one set of services, so a process started over one can
/// be signalled over the other.
///
/// # Errors
///
/// Either server fails. The other is stopped then too.
pub async fn serve(
    comb: Comb,
    listener: UnixListener,
    tcp: Option<TcpListener>,
    stop: CancellationToken,
) -> io::Result<()> {
    let router = Router::new(Api::new(comb, stop.clone()));
    let unix = stream::unfold(listener, |l| async move {
        let conn = l.accept().await.map(|(s, _)| s);
        Some((conn, l))
    });
    let local = tonic::transport::Server::builder().serve_with_incoming_shutdown(
        router.clone(),
        unix,
        stop.clone().cancelled_owned(),
    );
    let Some(tcp) = tcp else { return local.await.map_err(io::Error::other) };
    let incoming = stream::unfold(tcp, |l| async move {
        let conn = l.accept().await.map(|(s, _)| {
            // Small messages go out at once rather than waiting for more to fill a packet.
            let _ = s.set_nodelay(true);
            s
        });
        Some((conn, l))
    });
    let remote = tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(Duration::from_secs(20)))
        .serve_with_incoming_shutdown(router, incoming, stop.clone().cancelled_owned());
    let (a, b) = tokio::join!(
        async {
            let r = local.await;
            stop.cancel();
            r
        },
        async {
            let r = remote.await;
            stop.cancel();
            r
        }
    );
    a.and(b).map_err(io::Error::other)
}

/// Makes the API socket at `path`, replacing one an earlier run left there. It binds under a
/// temporary name and moves the socket into place once only its owner can open it, so there is
/// no moment when anyone else could connect.
///
/// # Errors
///
/// The directory cannot be made, or the socket cannot be bound or moved into place.
pub fn bind(path: &Path) -> io::Result<UnixListener> {
    let dir = path.parent().filter(|d| !d.as_os_str().is_empty()).unwrap_or(Path::new("."));
    std::fs::create_dir_all(dir)?;
    let name = path.file_name().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, format!("{} is not a file", path.display()))
    })?;
    let mut tmp = name.to_os_string();
    tmp.push(format!(".{}.tmp", std::process::id()));
    let tmp = dir.join(tmp);
    let _ = std::fs::remove_file(&tmp);
    let listener = UnixListener::bind(&tmp)?;
    std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    std::fs::rename(&tmp, path)?;
    Ok(listener)
}

/// Sends each call to the service its path names. The generated servers each answer only their
/// own paths, and this saves pulling in a web framework to join two of them.
#[derive(Clone, Debug)]
struct Router {
    cells: CellsServer<Api>,
    exec: ExecServer<Api>,
    files: FilesServer<Api>,
}

impl Router {
    fn new(api: Api) -> Self {
        Self {
            cells: CellsServer::new(api.clone()).max_decoding_message_size(MAX_REQUEST),
            exec: ExecServer::new(api.clone()).max_decoding_message_size(MAX_REQUEST),
            files: FilesServer::new(api).max_decoding_message_size(MAX_REQUEST),
        }
    }
}

impl<B> Service<http::Request<B>> for Router
where
    B: tonic::codegen::Body<Data = bytes::Bytes> + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Infallible>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Infallible>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        let service = req.uri().path().strip_prefix('/').and_then(|p| p.split_once('/'));
        match service.map(|(s, _)| s) {
            Some(<CellsServer<Api> as NamedService>::NAME) => Box::pin(self.cells.call(req)),
            Some(<ExecServer<Api> as NamedService>::NAME) => Box::pin(self.exec.call(req)),
            Some(<FilesServer<Api> as NamedService>::NAME) => Box::pin(self.files.call(req)),
            _ => {
                let status = Status::unimplemented(format!("no service at {}", req.uri().path()));
                Box::pin(async move { Ok(status.into_http()) })
            }
        }
    }
}

/// Where `Exec.Signal` sends a signal for each process `Exec.Start` started, by cell and pid.
type Signals = Arc<Mutex<HashMap<(CellId, u32), mpsc::Sender<i32>>>>;

/// The services, on top of one comb.
#[derive(Clone, Debug)]
pub struct Api {
    comb: Comb,
    stop: CancellationToken,
    /// Where to send a signal for each process started with `Exec.Start` that is still running.
    signals: Signals,
}

impl Api {
    /// The services for `comb`. Streams that would otherwise run forever, such as watches, end
    /// when `stop` is cancelled.
    #[must_use]
    pub fn new(comb: Comb, stop: CancellationToken) -> Self {
        Self { comb, stop, signals: Arc::default() }
    }

    /// The cell `id` if it belongs to `project`. Someone else's cell is not found, the same as
    /// one that does not exist.
    fn owned(&self, project: &str, id: CellId) -> Result<CellInfo, Error> {
        self.comb
            .get(id)
            .and_then(|c| if c.project == project { Ok(c) } else { Err(not_found(id)) })
    }

    async fn drone(&self, project: &str, id: &str) -> Result<(CellId, hive_drone::Client), Status> {
        let id = parse_id(id)?;
        self.owned(project, id).map_err(status)?;
        Ok((id, self.comb.drone(id).await.map_err(status)?))
    }

    /// The cells a selector picks in `project`, and whether it picked them by id. A selector by
    /// id that names no cell of the project fails as a whole.
    fn select(
        &self,
        project: &str,
        sel: Option<v1::CellSelector>,
    ) -> Result<(Vec<CellInfo>, bool), Status> {
        match sel.and_then(|s| s.by) {
            Some(v1::cell_selector::By::Id(id)) => {
                let info = self.owned(project, parse_id(&id)?).map_err(status)?;
                Ok((vec![info], true))
            }
            Some(v1::cell_selector::By::Labels(l)) => {
                let cells = self
                    .comb
                    .list()
                    .into_iter()
                    .filter(|c| c.project == project && labels_match(&l, c))
                    .collect();
                Ok((cells, false))
            }
            None => Err(invalid("the selector needs an id or labels")),
        }
    }

    /// Runs `f` on every cell `sel` picks, all at once. Cells picked by label are only the ones
    /// `wanted` accepts, so stopping by label does not count cells that already ended.
    async fn bulk<F, Fut>(
        &self,
        project: &str,
        sel: Option<v1::CellSelector>,
        wanted: fn(CellState) -> bool,
        f: F,
    ) -> Result<v1::BulkResult, Status>
    where
        F: Fn(Comb, CellId) -> Fut,
        Fut: Future<Output = Result<CellInfo, Error>> + Send + 'static,
    {
        let (cells, by_id) = self.select(project, sel)?;
        let ids: Vec<CellId> =
            cells.into_iter().filter(|c| by_id || wanted(c.status.state)).map(|c| c.id).collect();
        // Each one runs on its own task, so a caller that goes away does not leave the batch
        // half done.
        let tasks: Vec<_> = ids.iter().map(|&id| tokio::spawn(f(self.comb.clone(), id))).collect();
        let mut result = v1::BulkResult { matched: count(ids.len()), ..v1::BulkResult::default() };
        for (id, task) in ids.into_iter().zip(tasks) {
            let outcome = task
                .await
                .unwrap_or_else(|_| Err(Error::new(Reason::Internal, "the call panicked")));
            match outcome {
                Ok(_) => result.succeeded += 1,
                Err(e) => result.failures.push(v1::BulkFailure {
                    cell_id: id.to_string(),
                    error: Some(convert::error_to_v1(&e)),
                }),
            }
        }
        Ok(result)
    }
}

#[tonic::async_trait]
impl Cells for Api {
    type CreateStream = BoxStream<'static, Result<v1::CreateEvent, Status>>;
    type WatchStream = BoxStream<'static, Result<v1::CellEvent, Status>>;

    async fn create(
        &self,
        req: Request<v1::CreateRequest>,
    ) -> Result<Response<Self::CreateStream>, Status> {
        let project = project(&req)?;
        let anyway = req.metadata().contains_key(ANYWAY_HEADER);
        let req = req.into_inner();
        let count = req.count.max(1);
        if count > MAX_COUNT {
            return Err(invalid(format!(
                "count is {count}, and one call makes at most {MAX_COUNT}"
            )));
        }
        let spec = convert::spec_from_v1(req.spec.unwrap_or_default()).map_err(status)?;
        let creates: FuturesUnordered<_> = (0..count)
            .map(|index| {
                let idem_key = match (req.idempotency_key.as_str(), count) {
                    ("", _) => None,
                    (key, 1) => Some(key.to_string()),
                    (key, _) => Some(format!("{key}/{index}")),
                };
                let req = CreateRequest {
                    spec: spec.clone(),
                    project: project.clone(),
                    idem_key,
                    anyway,
                };
                // On its own task, so the cell is still made if the caller goes away, and a
                // retry with the same key finds it.
                let task = tokio::spawn({
                    let comb = self.comb.clone();
                    async move { comb.create(req).await }
                });
                async move {
                    let result = match task.await {
                        Ok(Ok(info)) => v1::create_event::Result::Cell(cell_to_v1(&info)),
                        Ok(Err(e)) => v1::create_event::Result::Error(convert::error_to_v1(&e)),
                        Err(_) => v1::create_event::Result::Error(convert::error_to_v1(
                            &Error::new(Reason::Internal, "the create panicked"),
                        )),
                    };
                    Ok(v1::CreateEvent { index, result: Some(result) })
                }
            })
            .collect();
        Ok(Response::new(creates.boxed()))
    }

    async fn get(&self, req: Request<v1::GetCellRequest>) -> Result<Response<v1::Cell>, Status> {
        let project = project(&req)?;
        let info = self.owned(&project, parse_id(&req.get_ref().id)?).map_err(status)?;
        Ok(Response::new(cell_to_v1(&info)))
    }

    async fn list(
        &self,
        req: Request<v1::ListCellsRequest>,
    ) -> Result<Response<v1::ListCellsResponse>, Status> {
        let project = project(&req)?;
        let req = req.into_inner();
        let states: Vec<CellState> = req.states().filter_map(convert::state_from_v1).collect();
        let after = match req.page_token.as_str() {
            "" => None,
            t => Some(t.parse::<CellId>().map_err(|_| invalid("the page token is not valid"))?),
        };
        let size = match req.page_size as usize {
            0 => PAGE,
            n => n.min(MAX_PAGE),
        };
        // The list is sorted by id, so the page after a token starts past it.
        let mut picked = self.comb.list().into_iter().filter(|c| {
            c.project == project
                && after.is_none_or(|a| c.id > a)
                && (states.is_empty() || states.contains(&c.status.state))
                && req.selector.as_ref().is_none_or(|l| labels_match(l, c))
        });
        let cells: Vec<v1::Cell> = picked.by_ref().take(size).map(|c| cell_to_v1(&c)).collect();
        let next_page_token = match (picked.next(), cells.last()) {
            (Some(_), Some(last)) => last.id.clone(),
            _ => String::new(),
        };
        Ok(Response::new(v1::ListCellsResponse { cells, next_page_token }))
    }

    async fn watch(
        &self,
        req: Request<v1::WatchCellsRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let project = project(&req)?;
        let selector = req.into_inner().selector;
        let labels = match selector.as_ref().and_then(|s| s.by.as_ref()) {
            Some(v1::cell_selector::By::Labels(l)) => Some(l.clone()),
            _ => None,
        };
        // Subscribed before the current states are read, so no change falls between the two.
        let rx = self.comb.subscribe();
        let (cells, by_id) = self.select(&project, selector)?;
        let mut watch = Watch {
            comb: self.comb.clone(),
            project,
            only: by_id.then(|| cells[0].id),
            labels,
            rx,
            seen: HashMap::new(),
            pending: VecDeque::new(),
            done: false,
        };
        for c in &cells {
            watch.push(c, c.status.clone());
        }
        let stop = self.stop.clone();
        let events = stream::unfold(watch, Watch::next).take_until(stop.cancelled_owned());
        Ok(Response::new(events.boxed()))
    }

    async fn pause(
        &self,
        req: Request<v1::CellSelector>,
    ) -> Result<Response<v1::BulkResult>, Status> {
        let project = project(&req)?;
        let running = |s| s == CellState::Running;
        let r = self
            .bulk(&project, Some(req.into_inner()), running, |comb, id| async move {
                comb.pause(id).await
            })
            .await?;
        Ok(Response::new(r))
    }

    async fn resume(
        &self,
        req: Request<v1::CellSelector>,
    ) -> Result<Response<v1::BulkResult>, Status> {
        let project = project(&req)?;
        let paused = |s| s == CellState::Paused;
        let r = self
            .bulk(&project, Some(req.into_inner()), paused, |comb, id| async move {
                comb.resume(id).await
            })
            .await?;
        Ok(Response::new(r))
    }

    async fn stop(
        &self,
        req: Request<v1::StopRequest>,
    ) -> Result<Response<v1::BulkResult>, Status> {
        let project = project(&req)?;
        let req = req.into_inner();
        if req.snapshot {
            return Err(invalid("snapshots are not supported yet"));
        }
        let live = |s: CellState| !s.is_terminal();
        let r = self
            .bulk(&project, req.selector, live, |comb, id| async move { comb.stop(id, None).await })
            .await?;
        Ok(Response::new(r))
    }

    async fn extend_ttl(
        &self,
        req: Request<v1::ExtendTtlRequest>,
    ) -> Result<Response<v1::Cell>, Status> {
        let project = project(&req)?;
        let req = req.into_inner();
        let id = parse_id(&req.id)?;
        let hard = convert::duration_from_v1(req.hard_ttl, "hard_ttl").map_err(status)?;
        let idle = convert::duration_from_v1(req.idle_ttl, "idle_ttl").map_err(status)?;
        self.owned(&project, id).map_err(status)?;
        let info = self.comb.extend_ttl(id, hard, idle).await.map_err(status)?;
        Ok(Response::new(cell_to_v1(&info)))
    }

    async fn update_policy(
        &self,
        req: Request<v1::UpdatePolicyRequest>,
    ) -> Result<Response<v1::Cell>, Status> {
        let project = project(&req)?;
        let req = req.into_inner();
        if !req.network_profile.is_empty() || req.limits.is_some() {
            return Err(Status::unimplemented(
                "UpdatePolicy can only mark a cell ready so far, not change its network or limits",
            ));
        }
        let id = parse_id(&req.id)?;
        let mut info = self.owned(&project, id).map_err(status)?;
        if req.ready {
            info = self.comb.ready(id).await.map_err(status)?;
        }
        Ok(Response::new(cell_to_v1(&info)))
    }

    async fn expose_port(
        &self,
        _: Request<v1::ExposePortRequest>,
    ) -> Result<Response<v1::PortEndpoint>, Status> {
        Err(Status::unimplemented("ExposePort needs a gate, which standalone mode has none of"))
    }
}

/// One watcher's view: the cells it follows and the state it last told the caller for each.
struct Watch {
    comb: Comb,
    project: String,
    /// The one cell a watch by id follows.
    only: Option<CellId>,
    /// The labels a watch by labels follows. A cell's labels never change, so a cell made after
    /// the watch started is followed from its first event if they match.
    labels: Option<v1::LabelSelector>,
    rx: broadcast::Receiver<crate::comb::CellEvent>,
    seen: HashMap<CellId, CellState>,
    pending: VecDeque<v1::CellEvent>,
    done: bool,
}

impl Watch {
    async fn next(mut self) -> Option<(Result<v1::CellEvent, Status>, Self)> {
        loop {
            if let Some(e) = self.pending.pop_front() {
                return Some((Ok(e), self));
            }
            if self.done {
                return None;
            }
            match self.rx.recv().await {
                Ok(e) => self.event(e.id, e.status),
                // Too far behind to know what was missed, so the caller gets every cell whose
                // state moved since it last heard.
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    for c in self.comb.list() {
                        self.event(c.id, c.status);
                    }
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }

    fn event(&mut self, id: CellId, status: CellStatus) {
        if self.only.is_some_and(|o| o != id) {
            return;
        }
        let Ok(info) = self.comb.get(id) else { return };
        if info.project != self.project
            || self.labels.as_ref().is_some_and(|l| !labels_match(l, &info))
        {
            return;
        }
        self.push(&info, status);
    }

    fn push(&mut self, info: &CellInfo, status: CellStatus) {
        let from = self.seen.insert(info.id, status.state);
        if from == Some(status.state) {
            return;
        }
        if self.only.is_some() && status.state.is_terminal() {
            self.done = true;
        }
        let mut cell = cell_to_v1(info);
        set_status(&mut cell, &status);
        let from = from.map_or(v1::CellState::Unspecified, convert::state_to_v1);
        self.pending.push_back(v1::CellEvent { cell: Some(cell), from: from.into() });
    }
}

#[tonic::async_trait]
impl Exec for Api {
    type StartStream = BoxStream<'static, Result<v1::ProcessOutput, Status>>;
    type SessionInteractStream = BoxStream<'static, Result<v1::SessionOutput, Status>>;

    async fn run(&self, req: Request<v1::RunRequest>) -> Result<Response<v1::RunResult>, Status> {
        let started = Instant::now();
        let project = project(&req)?;
        let r = req.into_inner();
        if !r.idempotency_key.is_empty() {
            return Err(invalid("idempotency keys on exec are not supported yet"));
        }
        let (_, drone) = self.drone(&project, &r.cell_id).await?;
        let who = user(&drone, &r.user).await?;
        let command = drone::Command {
            argv: r.argv,
            shell: r.shell,
            cwd: r.cwd,
            env: who.env(r.env),
            timeout_ms: millis(r.timeout)?,
            max_output_bytes: r.max_output_bytes,
            uid: who.uid,
            gid: who.gid,
        };
        let out = drone
            .run(&drone::RunRequest { command: Some(command), stdin: r.stdin })
            .await
            .map_err(status)?;
        self.comb.metrics().exec("run", started.elapsed());
        Ok(Response::new(result_to_v1(out)))
    }

    async fn start(
        &self,
        req: Request<Streaming<v1::ProcessInput>>,
    ) -> Result<Response<Self::StartStream>, Status> {
        let project = project(&req)?;
        let mut input = req.into_inner();
        let first = input.message().await?.and_then(|m| m.input);
        let Some(v1::process_input::Input::Start(s)) = first else {
            return Err(invalid("the first message must be a start"));
        };
        if s.pty.is_some() {
            return Err(invalid("terminals are not supported yet"));
        }
        let (id, drone) = self.drone(&project, &s.cell_id).await?;
        let who = user(&drone, &s.user).await?;
        let command = drone::Command {
            argv: s.argv,
            shell: s.shell,
            cwd: s.cwd,
            env: who.env(s.env),
            timeout_ms: millis(s.timeout)?,
            max_output_bytes: 0,
            uid: who.uid,
            gid: who.gid,
        };
        let (mut tx, rx) = drone.start(&command).await.map_err(status)?.split();
        let (signal_tx, mut signals) = mpsc::channel::<i32>(8);
        let exited = CancellationToken::new();
        // The caller's input goes to the process until the caller closes its side, and signals
        // from Exec.Signal until the process is gone. Dropping both halves kills the process, so
        // a caller that goes away takes it with them.
        tokio::spawn({
            let exited = exited.clone();
            async move {
                let mut open = true;
                loop {
                    tokio::select! {
                        msg = input.message(), if open => {
                            let sent = match msg.ok().flatten().and_then(|m| m.input) {
                                Some(v1::process_input::Input::Stdin(b)) => tx.write(&b).await,
                                Some(v1::process_input::Input::Signal(n)) => tx.signal(n).await,
                                Some(v1::process_input::Input::Eof(_)) => tx.close_stdin().await,
                                Some(_) => Ok(()),
                                None => {
                                    open = false;
                                    tx.close_stdin().await
                                }
                            };
                            if sent.is_err() {
                                break;
                            }
                        }
                        Some(n) = signals.recv() => {
                            if tx.signal(n).await.is_err() {
                                break;
                            }
                        }
                        () = exited.cancelled() => {
                            let _ = tx.finish().await;
                            break;
                        }
                    }
                }
            }
        });
        let out = Started {
            rx,
            id,
            pid: None,
            signal: Some(signal_tx),
            table: self.signals.clone(),
            exited,
            done: false,
        };
        Ok(Response::new(stream::unfold(out, Started::next).boxed()))
    }

    async fn signal(&self, req: Request<v1::SignalRequest>) -> Result<Response<v1::Empty>, Status> {
        let project = project(&req)?;
        let r = req.into_inner();
        let id = parse_id(&r.cell_id)?;
        self.owned(&project, id).map_err(status)?;
        let tx = lock(&self.signals).get(&(id, r.pid)).cloned();
        let Some(tx) = tx else {
            return Err(invalid(format!(
                "no process {} started with Exec.Start is running",
                r.pid
            )));
        };
        tx.send(r.signal).await.map_err(|_| invalid("the process has exited"))?;
        Ok(Response::new(v1::Empty {}))
    }

    async fn session_create(
        &self,
        req: Request<v1::SessionCreateRequest>,
    ) -> Result<Response<v1::Session>, Status> {
        let project = project(&req)?;
        let r = req.into_inner();
        let (_, drone) = self.drone(&project, &r.cell_id).await?;
        let who = user(&drone, &r.user).await?;
        let spec = drone::SessionCreate {
            shell: r.shell,
            cwd: r.cwd,
            env: who.env(r.env),
            uid: who.uid,
            gid: who.gid,
        };
        let info = drone.session_create(&spec).await.map_err(status)?;
        Ok(Response::new(v1::Session { cell_id: r.cell_id, id: info.id }))
    }

    async fn session_run(
        &self,
        req: Request<v1::SessionRunRequest>,
    ) -> Result<Response<v1::SessionRunResult>, Status> {
        let project = project(&req)?;
        let r = req.into_inner();
        let session = r.session.ok_or_else(|| invalid("the request names no session"))?;
        let (_, drone) = self.drone(&project, &session.cell_id).await?;
        let run = drone::SessionRun {
            id: session.id,
            command: r.command,
            timeout_ms: millis(r.timeout)?,
            max_output_bytes: r.max_output_bytes,
        };
        let out = drone.session_run(&run).await.map_err(status)?;
        Ok(Response::new(v1::SessionRunResult {
            exit_code: out.exit_code,
            output: out.output,
            truncated: out.truncated,
            timed_out: out.timed_out,
            wall: Some(convert::duration_to_v1(Duration::from_nanos(out.wall_nanos))),
        }))
    }

    async fn session_interact(
        &self,
        _: Request<Streaming<v1::SessionInput>>,
    ) -> Result<Response<Self::SessionInteractStream>, Status> {
        Err(Status::unimplemented("SessionInteract is not built yet"))
    }

    async fn session_close(
        &self,
        req: Request<v1::SessionRef>,
    ) -> Result<Response<v1::Empty>, Status> {
        let project = project(&req)?;
        let r = req.into_inner();
        let (_, drone) = self.drone(&project, &r.cell_id).await?;
        drone.session_close(&r.id).await.map_err(status)?;
        Ok(Response::new(v1::Empty {}))
    }
}

#[tonic::async_trait]
impl Files for Api {
    type ReadStream = BoxStream<'static, Result<v1::Chunk, Status>>;
    type WatchStream = BoxStream<'static, Result<v1::FsEvent, Status>>;

    async fn read(
        &self,
        req: Request<v1::ReadFileRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let project = project(&req)?;
        let r = req.into_inner();
        let (_, drone) = self.drone(&project, &r.cell_id).await?;
        let read = drone::FsRead { path: r.path, offset: r.offset, length: r.length };
        let reader = drone.fs_open(&read).await.map_err(status)?;
        let chunks = stream::try_unfold(reader, |mut reader| async move {
            let chunk = reader.next().await.map_err(status)?;
            Ok(chunk.map(|data| (v1::Chunk { data }, reader)))
        });
        Ok(Response::new(chunks.boxed()))
    }

    async fn write(
        &self,
        req: Request<Streaming<v1::WriteFileChunk>>,
    ) -> Result<Response<v1::FileInfo>, Status> {
        use v1::write_file_chunk::Part;
        let project = project(&req)?;
        let mut input = req.into_inner();
        let Some(Part::Header(h)) = input.message().await?.and_then(|m| m.part) else {
            return Err(invalid("the first message must be a header"));
        };
        let (_, drone) = self.drone(&project, &h.cell_id).await?;
        let Who { uid, gid, .. } = user(&drone, &h.user).await?;
        let mut write = drone::FsWrite {
            path: h.path,
            data: bytes::Bytes::new(),
            mode: h.mode,
            make_parents: h.make_parents,
            append: h.append,
            uid,
            gid,
        };
        // Most writes are small files, which go in one message. Past that the file is streamed,
        // with what came so far as its start.
        let mut head = bytes::BytesMut::new();
        let mut next = None;
        while let Some(m) = input.message().await? {
            let Some(Part::Data(b)) = m.part else {
                return Err(invalid("only the first message may be a header"));
            };
            if head.len() + b.len() > SMALL_WRITE {
                next = Some(b);
                break;
            }
            head.extend_from_slice(&b);
        }
        write.data = head.freeze();
        let Some(first) = next else {
            let info = drone.fs_write(&write).await.map_err(status)?;
            return Ok(Response::new(info_to_v1(info)));
        };
        let mut w = drone.fs_create(&write).await.map_err(status)?;
        w.write(first).await.map_err(status)?;
        while let Some(m) = input.message().await? {
            let Some(Part::Data(b)) = m.part else {
                return Err(invalid("only the first message may be a header"));
            };
            w.write(b).await.map_err(status)?;
        }
        let info = w.finish().await.map_err(status)?;
        Ok(Response::new(info_to_v1(info)))
    }

    async fn stat(&self, req: Request<v1::PathRequest>) -> Result<Response<v1::FileInfo>, Status> {
        let project = project(&req)?;
        let r = req.into_inner();
        let (_, drone) = self.drone(&project, &r.cell_id).await?;
        let path = drone::FsPath { path: r.path, follow: false, recursive: false };
        let info = drone.fs_stat(&path).await.map_err(status)?;
        Ok(Response::new(info_to_v1(info)))
    }

    async fn list(
        &self,
        req: Request<v1::ListDirRequest>,
    ) -> Result<Response<v1::ListDirResponse>, Status> {
        let project = project(&req)?;
        let r = req.into_inner();
        let (_, drone) = self.drone(&project, &r.cell_id).await?;
        let list = drone.fs_list(&drone::FsList { path: r.path, depth: r.depth }).await;
        let list = list.map_err(status)?;
        Ok(Response::new(v1::ListDirResponse {
            entries: list.entries.into_iter().map(info_to_v1).collect(),
            truncated: list.truncated,
        }))
    }

    async fn remove(&self, req: Request<v1::PathRequest>) -> Result<Response<v1::Empty>, Status> {
        let project = project(&req)?;
        let r = req.into_inner();
        let (_, drone) = self.drone(&project, &r.cell_id).await?;
        let path = drone::FsPath { path: r.path, follow: false, recursive: r.recursive };
        drone.fs_remove(&path).await.map_err(status)?;
        Ok(Response::new(v1::Empty {}))
    }

    async fn watch(
        &self,
        req: Request<v1::WatchDirRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let project = project(&req)?;
        let r = req.into_inner();
        let (_, drone) = self.drone(&project, &r.cell_id).await?;
        let path = drone::FsPath { path: r.path, follow: false, recursive: r.recursive };
        let watcher = drone.fs_watch(&path).await.map_err(status)?;
        // The drone sends changes in batches, and the caller gets them one at a time.
        let events = stream::try_unfold(watcher, |mut w| async move {
            let batch = w.next().await.map_err(status)?;
            Ok::<_, Status>(batch.map(|b| (stream::iter(b.events.into_iter().map(Ok)), w)))
        })
        .try_flatten()
        .map_ok(|e| v1::FsEvent { kind: e.kind, path: e.path })
        .take_until(self.stop.clone().cancelled_owned());
        Ok(Response::new(events.boxed()))
    }

    async fn diff(&self, _: Request<v1::DiffRequest>) -> Result<Response<v1::DiffResult>, Status> {
        Err(Status::unimplemented("Diff is not built yet"))
    }

    async fn apply(&self, req: Request<v1::ApplyRequest>) -> Result<Response<v1::Empty>, Status> {
        let project = project(&req)?;
        let r = req.into_inner();
        let tar = match r.content {
            Some(v1::apply_request::Content::Tar(t)) => t,
            Some(v1::apply_request::Content::Patch(_)) => {
                return Err(Status::unimplemented("applying a patch is not built yet"));
            }
            None => return Err(invalid("the request has no patch or tar")),
        };
        let (_, drone) = self.drone(&project, &r.cell_id).await?;
        let upload = drone::FsUpload { path: r.path, make_parents: true, uid: None, gid: None };
        drone.fs_upload(&upload, tar).await.map_err(status)?;
        Ok(Response::new(v1::Empty {}))
    }
}

/// The output side of an `Exec.Start` call. Dropping it tells the input side to finish.
struct Started {
    rx: hive_drone::ProcessOutput,
    id: CellId,
    pid: Option<u32>,
    signal: Option<mpsc::Sender<i32>>,
    table: Signals,
    exited: CancellationToken,
    done: bool,
}

impl Started {
    async fn next(mut self) -> Option<(Result<v1::ProcessOutput, Status>, Self)> {
        use v1::process_output::Output as Out;
        if self.done {
            return None;
        }
        let out = match self.rx.next().await {
            Ok(Some(Output::Started(pid))) => {
                if let Some(tx) = self.signal.take() {
                    lock(&self.table).insert((self.id, pid), tx);
                }
                self.pid = Some(pid);
                Ok(Out::Pid(pid))
            }
            Ok(Some(Output::Stdout(b))) => Ok(Out::Stdout(b)),
            Ok(Some(Output::Stderr(b))) => Ok(Out::Stderr(b)),
            Ok(Some(Output::Exit(r))) => {
                self.done = true;
                Ok(Out::Exit(result_to_v1(r)))
            }
            Ok(None) => return None,
            Err(e) => {
                self.done = true;
                Err(status(e))
            }
        };
        Some((out.map(|o| v1::ProcessOutput { output: Some(o) }), self))
    }
}

impl Drop for Started {
    fn drop(&mut self) {
        if let Some(pid) = self.pid {
            lock(&self.table).remove(&(self.id, pid));
        }
        self.exited.cancel();
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The project a call is for.
fn project<T>(req: &Request<T>) -> Result<String, Status> {
    match req.metadata().get(PROJECT_HEADER) {
        None => Ok(DEFAULT_PROJECT.to_string()),
        Some(v) => match v.to_str() {
            Ok(p) if is_name(p) => Ok(p.to_string()),
            _ => Err(invalid(format!("{PROJECT_HEADER} is not a project name"))),
        },
    }
}

fn labels_match(sel: &v1::LabelSelector, cell: &CellInfo) -> bool {
    sel.r#match.iter().all(|(k, v)| cell.spec.labels.get(k) == Some(v))
}

/// A cell for the wire.
fn cell_to_v1(c: &CellInfo) -> v1::Cell {
    let mut cell = v1::Cell {
        id: c.id.to_string(),
        project: c.project.clone(),
        backend: convert::backend_to_v1(c.spec.backend).into(),
        spec: Some(convert::spec_to_v1(&c.spec)),
        node: c.id.node().to_string(),
        created_at: Some(c.created.into()),
        expires_at: c.spec.hard_ttl.and_then(|t| c.created.checked_add(t)).map(Into::into),
        labels: c.spec.labels.clone().into_iter().collect(),
        ..v1::Cell::default()
    };
    set_status(&mut cell, &c.status);
    cell
}

fn set_status(cell: &mut v1::Cell, s: &CellStatus) {
    cell.state = convert::state_to_v1(s.state).into();
    cell.cause = s.cause.map_or(v1::Cause::Unspecified, convert::cause_to_v1).into();
    cell.state_since = Some(prost_types::Timestamp::from(s.changed.max(SystemTime::UNIX_EPOCH)));
}

fn result_to_v1(r: drone::RunResult) -> v1::RunResult {
    v1::RunResult {
        exit_code: r.exit_code,
        stdout: r.stdout,
        stderr: r.stderr,
        truncated: r.truncated,
        timed_out: r.timed_out,
        wall: Some(convert::duration_to_v1(Duration::from_nanos(r.wall_nanos))),
        usage: None,
        signal: r.signal,
    }
}

/// A file's description for the wire. The kinds are numbered the same on both sides.
fn info_to_v1(i: drone::FileInfo) -> v1::FileInfo {
    let nanos = i.modified_unix_nanos;
    v1::FileInfo {
        path: i.path,
        r#type: i.kind,
        size: i.size,
        mode: i.mode,
        modified_at: Some(prost_types::Timestamp {
            seconds: nanos.div_euclid(1_000_000_000),
            // Always in 0..1e9, so it fits.
            nanos: i32::try_from(nanos.rem_euclid(1_000_000_000)).unwrap_or(0),
        }),
        symlink_target: i.symlink_target,
    }
}

/// Who a command runs as. A user given by name also gets the `HOME`, `USER` and `LOGNAME` a login
/// would give it, so its shell does not go looking in root's home.
#[derive(Debug, PartialEq)]
struct Who {
    uid: Option<u32>,
    gid: Option<u32>,
    home: Option<(String, String)>,
}

impl Who {
    /// `env` with the user's variables added where the caller did not set them.
    fn env(&self, env: HashMap<String, String>) -> BTreeMap<String, String> {
        let mut env: BTreeMap<_, _> = env.into_iter().collect();
        if let Some((name, home)) = &self.home {
            env.entry("HOME".into()).or_insert_with(|| home.clone());
            env.entry("USER".into()).or_insert_with(|| name.clone());
            env.entry("LOGNAME".into()).or_insert_with(|| name.clone());
        }
        env
    }
}

/// `uid`, `uid:gid`, or a name from the cell's own `/etc/passwd`, which is read through its drone
/// each time, since a command may have just added the user.
async fn user(drone: &hive_drone::Client, u: &str) -> Result<Who, Status> {
    if u == "root" {
        let home = Some(("root".to_owned(), "/root".to_owned()));
        return Ok(Who { uid: Some(0), gid: Some(0), home });
    }
    if u.is_empty() || u.starts_with(|c: char| c.is_ascii_digit()) {
        let (uid, gid) = ids(u)?;
        return Ok(Who { uid, gid, home: None });
    }
    let read = drone::FsRead { path: "/etc/passwd".into(), offset: 0, length: PASSWD_MAX };
    let mut reader = drone.fs_open(&read).await.map_err(status)?;
    let mut text = Vec::new();
    while let Some(chunk) = reader.next().await.map_err(status)? {
        text.extend_from_slice(&chunk);
    }
    let (uid, gid, home) =
        passwd(&text, u).ok_or_else(|| invalid(format!("the cell has no user {u:?}")))?;
    Ok(Who { uid: Some(uid), gid: Some(gid), home: Some((u.to_owned(), home)) })
}

/// The most of `/etc/passwd` read to find a user.
const PASSWD_MAX: u64 = 1 << 20;

/// `uid` or `uid:gid`, or nothing when empty.
fn ids(u: &str) -> Result<(Option<u32>, Option<u32>), Status> {
    if u.is_empty() {
        return Ok((None, None));
    }
    let bad = || invalid(format!("user {u:?} is not a name, a uid or uid:gid"));
    let (uid, gid) = match u.split_once(':') {
        Some((a, b)) => (a, Some(b)),
        None => (u, None),
    };
    let uid = uid.parse().map_err(|_| bad())?;
    let gid = gid.map(str::parse).transpose().map_err(|_| bad())?;
    Ok((Some(uid), gid))
}

/// The uid, gid and home of `name` in a passwd file.
fn passwd(text: &[u8], name: &str) -> Option<(u32, u32, String)> {
    String::from_utf8_lossy(text).lines().find_map(|line| {
        let mut f = line.split(':');
        if f.next()? != name {
            return None;
        }
        let uid = f.nth(1)?.parse().ok()?;
        let gid = f.next()?.parse().ok()?;
        let home = f.nth(1)?;
        Some((uid, gid, home.to_owned()))
    })
}

/// A timeout in milliseconds, 0 when unset. A part of a millisecond counts as a whole one.
fn millis(d: Option<prost_types::Duration>) -> Result<u64, Status> {
    let Some(d) = d else { return Ok(0) };
    let d = Duration::try_from(d).map_err(|_| invalid("a timeout must not be negative"))?;
    Ok(u64::try_from(d.as_nanos().div_ceil(1_000_000)).unwrap_or(u64::MAX))
}

fn parse_id(s: &str) -> Result<CellId, Status> {
    s.parse().map_err(|_| invalid(format!("{s:?} is not a cell id")))
}

fn count(n: usize) -> u32 {
    u32::try_from(n).unwrap_or(u32::MAX)
}

fn not_found(id: CellId) -> Error {
    Error::new(Reason::CellNotFound, format!("no cell {id} on this node"))
}

fn invalid(msg: impl Into<String>) -> Status {
    status(Error::new(Reason::InvalidArgument, msg.into()))
}

fn status(e: Error) -> Status {
    convert::error_to_status(&e)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn users_are_numbers_or_names() {
        assert_eq!(ids("").unwrap(), (None, None));
        assert_eq!(ids("1000").unwrap(), (Some(1000), None));
        assert_eq!(ids("1000:100").unwrap(), (Some(1000), Some(100)));
        assert!(ids("1000:").is_err());
        let text = b"root:x:0:0:root:/root:/bin/bash\nagent:x:1001:1002::/home/agent:/bin/bash\n";
        assert_eq!(passwd(text, "agent"), Some((1001, 1002, "/home/agent".into())));
        assert_eq!(passwd(text, "root"), Some((0, 0, "/root".into())));
        assert_eq!(passwd(text, "age"), None);
    }

    #[test]
    fn a_named_user_gets_its_home() {
        let who = Who {
            uid: Some(1001),
            gid: Some(1002),
            home: Some(("agent".into(), "/home/agent".into())),
        };
        let env = who.env(HashMap::from([("USER".into(), "me".into())]));
        assert_eq!(env.get("HOME").map(String::as_str), Some("/home/agent"));
        assert_eq!(env.get("USER").map(String::as_str), Some("me"));
        assert_eq!(env.get("LOGNAME").map(String::as_str), Some("agent"));
        let who = Who { uid: Some(1000), gid: None, home: None };
        assert!(who.env(HashMap::new()).is_empty());
    }

    #[test]
    fn a_part_of_a_millisecond_is_a_millisecond() {
        assert_eq!(millis(None).unwrap(), 0);
        let d = |seconds, nanos| Some(prost_types::Duration { seconds, nanos });
        assert_eq!(millis(d(0, 1)).unwrap(), 1);
        assert_eq!(millis(d(2, 500_000_000)).unwrap(), 2500);
        assert!(millis(d(-1, 0)).is_err());
    }
}
