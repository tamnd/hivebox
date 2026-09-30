//! The Cells service across the cluster. A call about one cell goes to its comb. Create places
//! the batch with waggle and splits it over the nodes, and calls by label go to every node and
//! have their answers merged.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use futures::StreamExt;
use futures::stream::BoxStream;
use hive_proto::convert;
use hive_proto::v1;
use hive_proto::v1::cells_client::CellsClient;
use hive_proto::v1::cells_server::Cells;
use hive_scout::project_id;
use hive_telemetry::{CounterVec, HistogramVec};
use hive_types::{CellId, CellSpec, Error, Reason};
use hive_waggle::{PlaceReq, Placer};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use crate::PROJECT_HEADER;
use crate::nodes::Nodes;

/// The most cells one create call may ask for.
pub const MAX_COUNT: u32 = 32_768;
/// The most one comb takes in a call, which is its own limit.
const COMB_COUNT: u32 = 1024;
/// How many times cells a node turned away are placed again elsewhere.
const RETRIES: usize = 2;
/// Cells in a list page when the caller asks for no size, and the most it may ask for, the same
/// as a comb's.
const PAGE: u32 = 1000;
const MAX_PAGE: u32 = 10_000;
/// Nodes asked at once for one list page.
const LIST_WINDOW: usize = 16;
/// The biggest answer a comb sends, which is a full list page.
const MAX_ANSWER: usize = 64 << 20;
/// How long a watch waits before asking a node again after its stream broke.
const REWATCH: Duration = Duration::from_secs(1);

/// The Cells service. Cloning it is cheap.
#[derive(Clone, Debug)]
pub struct Api {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    nodes: Nodes,
    placer: Mutex<Placer>,
    start: Instant,
    creates: CounterVec,
    create_seconds: HistogramVec,
}

impl Api {
    /// Cells over `nodes`, with its metrics in `registry`.
    #[must_use]
    pub fn new(nodes: Nodes, registry: &hive_telemetry::Registry) -> Self {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64)
            ^ u64::from(std::process::id());
        Self {
            inner: Arc::new(Inner {
                nodes,
                placer: Mutex::new(Placer::new(seed)),
                start: Instant::now(),
                creates: registry.counter(
                    "hive_gate_cells_created_total",
                    "Cells asked for through the gate, by what became of them.",
                    &["result"],
                ),
                create_seconds: registry.histogram(
                    "hive_gate_create_seconds",
                    "Time from a create call to each cell's answer.",
                    &[],
                ),
            }),
        }
    }

    fn client(&self, channel: Channel) -> CellsClient<Channel> {
        CellsClient::new(channel).max_decoding_message_size(MAX_ANSWER)
    }

    fn owner(&self, id: &str) -> Result<CellsClient<Channel>, Status> {
        let id = parse_id(id)?;
        Ok(self.client(self.inner.nodes.owner(id).map_err(|e| convert::error_to_status(&e))?))
    }

    fn node(&self, node: u16) -> Result<CellsClient<Channel>, Status> {
        Ok(self.client(self.inner.nodes.channel(node).map_err(|e| convert::error_to_status(&e))?))
    }

    /// Runs a bulk call on every node and adds up the answers. A node that fails shows up as
    /// one failure with no cell id, since which of its cells matched is not known.
    async fn everywhere<T, F, Fut>(&self, project: &str, msg: T, call: F) -> v1::BulkResult
    where
        T: Clone + Send + 'static,
        F: Fn(CellsClient<Channel>, Request<T>) -> Fut,
        Fut: Future<Output = Result<Response<v1::BulkResult>, Status>> + Send + 'static,
    {
        let mut calls = JoinSet::new();
        for node in self.inner.nodes.all() {
            let fut = self.node(node).map(|c| call(c, out(project, msg.clone())));
            calls.spawn(async move {
                let r = match fut {
                    Ok(f) => f.await.map(Response::into_inner),
                    Err(s) => Err(s),
                };
                (node, r)
            });
        }
        let mut total = v1::BulkResult::default();
        while let Some(done) = calls.join_next().await {
            let Ok((node, r)) = done else { continue };
            match r {
                Ok(r) => {
                    total.matched += r.matched;
                    total.succeeded += r.succeeded;
                    total.failures.extend(r.failures);
                }
                Err(s) => total.failures.push(v1::BulkFailure {
                    cell_id: String::new(),
                    error: Some(on_node(node, &s)),
                }),
            }
        }
        total
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
        let req = req.into_inner();
        let count = req.count.max(1);
        if count > MAX_COUNT {
            return Err(invalid(format!(
                "count is {count}, and one call makes at most {MAX_COUNT}"
            )));
        }
        let wire = req.spec.unwrap_or_default();
        let spec = convert::spec_from_v1(wire.clone()).map_err(|e| convert::error_to_status(&e))?;
        let affinity = match req.placement.as_ref().map(|p| p.affinity_cell_id.as_str()) {
            None | Some("") => None,
            Some(id) => Some(parse_id(id)?.node()),
        };
        let (tx, rx) = mpsc::channel(256);
        let batch = Batch {
            api: self.clone(),
            project,
            wire,
            spec,
            key: req.idempotency_key,
            count,
            affinity,
            started: Instant::now(),
            tx,
        };
        // On its own task, so the cells are still made if the caller goes away.
        tokio::spawn(batch.run());
        let events =
            futures::stream::unfold(
                rx,
                |mut rx| async move { rx.recv().await.map(|e| (Ok(e), rx)) },
            );
        Ok(Response::new(events.boxed()))
    }

    async fn get(&self, req: Request<v1::GetCellRequest>) -> Result<Response<v1::Cell>, Status> {
        let project = project(&req)?;
        let req = req.into_inner();
        self.owner(&req.id)?.get(out(&project, req)).await
    }

    async fn list(
        &self,
        req: Request<v1::ListCellsRequest>,
    ) -> Result<Response<v1::ListCellsResponse>, Status> {
        let project = project(&req)?;
        let req = req.into_inner();
        let size = match req.page_size {
            0 => PAGE,
            n => n.min(MAX_PAGE),
        };
        let (from, after) = parse_token(&req.page_token)?;
        let nodes: Vec<u16> = self.inner.nodes.all().into_iter().filter(|&n| n >= from).collect();
        let mut page = v1::ListCellsResponse::default();
        let mut left = size;
        // Cell ids sort by node, so the pages walk the nodes in order and a token says which
        // node to go on from and after which cell.
        for window in nodes.chunks(LIST_WINDOW) {
            let mut calls = JoinSet::new();
            for (i, &node) in window.iter().enumerate() {
                let one = v1::ListCellsRequest {
                    page_size: left,
                    page_token: after
                        .filter(|a| a.node() == node)
                        .map(|a| a.to_string())
                        .unwrap_or_default(),
                    ..req.clone()
                };
                let client = self.node(node);
                let project = project.clone();
                calls.spawn(async move {
                    let r = match client {
                        Ok(mut c) => c.list(out(&project, one)).await.map(Response::into_inner),
                        Err(s) => Err(s),
                    };
                    (i, r)
                });
            }
            let mut answers: Vec<Option<v1::ListCellsResponse>> = vec![None; window.len()];
            while let Some(done) = calls.join_next().await {
                let (i, r) = done.map_err(|_| Status::internal("a list call panicked"))?;
                let r = r.map_err(|s| convert::error_to_status(&node_error(window[i], &s)))?;
                answers[i] = Some(r);
            }
            for (i, answer) in answers.into_iter().enumerate() {
                let answer = answer.unwrap_or_default();
                let more = !answer.next_page_token.is_empty();
                let n = answer.cells.len();
                let take = n.min(left as usize);
                page.cells.extend(answer.cells.into_iter().take(take));
                left -= take as u32;
                if left == 0 {
                    // Full: go on after the last cell, or from the next node when this one had
                    // no more.
                    page.next_page_token = if take < n || more {
                        page.cells.last().map(|c| c.id.clone()).unwrap_or_default()
                    } else {
                        nodes
                            .iter()
                            .find(|&&m| m > window[i])
                            .map(|m| format!("n{m}"))
                            .unwrap_or_default()
                    };
                    return Ok(Response::new(page));
                }
            }
        }
        Ok(Response::new(page))
    }

    async fn watch(
        &self,
        req: Request<v1::WatchCellsRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let project = project(&req)?;
        let req = req.into_inner();
        let labels = match req.selector.as_ref().and_then(|s| s.by.as_ref()) {
            Some(v1::cell_selector::By::Id(id)) => {
                let id = id.clone();
                let stream = self.owner(&id)?.watch(out(&project, req)).await?.into_inner();
                return Ok(Response::new(stream.boxed()));
            }
            Some(v1::cell_selector::By::Labels(l)) => l.clone(),
            None => return Err(invalid("the selector needs an id or labels")),
        };
        let (tx, rx) = mpsc::channel(1024);
        tokio::spawn(watch_all(self.clone(), project, labels, tx));
        let events =
            futures::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|e| (e, rx)) });
        Ok(Response::new(events.boxed()))
    }

    async fn pause(
        &self,
        req: Request<v1::CellSelector>,
    ) -> Result<Response<v1::BulkResult>, Status> {
        let project = project(&req)?;
        let sel = req.into_inner();
        if let Some(v1::cell_selector::By::Id(id)) = &sel.by {
            return self.owner(id)?.pause(out(&project, sel)).await;
        }
        let r = self.everywhere(&project, sel, |mut c, r| async move { c.pause(r).await }).await;
        Ok(Response::new(r))
    }

    async fn resume(
        &self,
        req: Request<v1::CellSelector>,
    ) -> Result<Response<v1::BulkResult>, Status> {
        let project = project(&req)?;
        let sel = req.into_inner();
        if let Some(v1::cell_selector::By::Id(id)) = &sel.by {
            return self.owner(id)?.resume(out(&project, sel)).await;
        }
        let r = self.everywhere(&project, sel, |mut c, r| async move { c.resume(r).await }).await;
        Ok(Response::new(r))
    }

    async fn stop(
        &self,
        req: Request<v1::StopRequest>,
    ) -> Result<Response<v1::BulkResult>, Status> {
        let project = project(&req)?;
        let req = req.into_inner();
        if let Some(v1::cell_selector::By::Id(id)) =
            req.selector.as_ref().and_then(|s| s.by.as_ref())
        {
            return self.owner(id)?.stop(out(&project, req)).await;
        }
        let r = self.everywhere(&project, req, |mut c, r| async move { c.stop(r).await }).await;
        Ok(Response::new(r))
    }

    async fn extend_ttl(
        &self,
        req: Request<v1::ExtendTtlRequest>,
    ) -> Result<Response<v1::Cell>, Status> {
        let project = project(&req)?;
        let req = req.into_inner();
        self.owner(&req.id)?.extend_ttl(out(&project, req)).await
    }

    async fn update_policy(
        &self,
        req: Request<v1::UpdatePolicyRequest>,
    ) -> Result<Response<v1::Cell>, Status> {
        let project = project(&req)?;
        let req = req.into_inner();
        self.owner(&req.id)?.update_policy(out(&project, req)).await
    }

    async fn expose_port(
        &self,
        req: Request<v1::ExposePortRequest>,
    ) -> Result<Response<v1::PortEndpoint>, Status> {
        let project = project(&req)?;
        let req = req.into_inner();
        self.owner(&req.id)?.expose_port(out(&project, req)).await
    }
}

/// One create call on its way through the cluster.
struct Batch {
    api: Api,
    project: String,
    wire: v1::CellSpec,
    spec: CellSpec,
    key: String,
    count: u32,
    affinity: Option<u16>,
    started: Instant,
    tx: mpsc::Sender<v1::CreateEvent>,
}

/// What became of the part of a batch sent to one node.
struct Sent {
    node: u16,
    /// Batch indexes the node turned away for lack of room, or never got because it could not
    /// be reached, to place again.
    again: Vec<u32>,
}

impl Batch {
    async fn run(self) {
        let mut left: Vec<u32> = (0..self.count).collect();
        let this = Arc::new(self);
        let mut exclude: Vec<u16> = Vec::new();
        for attempt in 0..=RETRIES {
            let last = attempt == RETRIES;
            let placement = {
                let inner = &this.api.inner;
                let snap = inner.nodes.snapshot();
                let req = PlaceReq {
                    backend: this.spec.backend,
                    resources: this.spec.resources,
                    n: u32::try_from(left.len()).unwrap_or(u32::MAX),
                    layers: &[],
                    project: project_id(&this.project),
                    affinity: this.affinity,
                    exclude: &exclude,
                };
                let now = inner.start.elapsed();
                inner
                    .placer
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .place(&snap.view, &req, now)
            };
            let mut sends = JoinSet::new();
            let mut at = 0;
            for (node, k) in placement.nodes {
                let part = left[at..at + k as usize].to_vec();
                at += k as usize;
                sends.spawn(this.clone().send(node, part, last));
            }
            let mut again = left.split_off(at);
            let unplaced = again.len();
            while let Some(done) = sends.join_next().await {
                let Ok(sent) = done else { continue };
                if !sent.again.is_empty() {
                    let n = u32::try_from(sent.again.len()).unwrap_or(u32::MAX);
                    this.api.inner.placer.lock().unwrap_or_else(PoisonError::into_inner).refused(
                        sent.node,
                        n,
                        &this.spec.resources,
                    );
                    exclude.push(sent.node);
                    again.extend(sent.again);
                }
            }
            if again.is_empty() {
                return;
            }
            if last || (unplaced == again.len() && exclude.is_empty()) {
                // Nowhere had room, and asking again right away would find the same.
                let e = Error::new(Reason::CapacityUnavailable, "no node has room for the cell");
                for index in again {
                    this.answer(index, Err(convert::error_to_v1(&e))).await;
                }
                return;
            }
            again.sort_unstable();
            left = again;
        }
    }

    /// Creates the cells at batch indexes `part` on `node`, in calls a comb takes.
    async fn send(self: Arc<Self>, node: u16, part: Vec<u32>, last: bool) -> Sent {
        let mut again = Vec::new();
        for chunk in part.chunks(COMB_COUNT as usize) {
            again.extend(self.send_chunk(node, chunk, last).await);
        }
        Sent { node, again }
    }

    async fn send_chunk(&self, node: u16, chunk: &[u32], last: bool) -> Vec<u32> {
        let count = u32::try_from(chunk.len()).unwrap_or(u32::MAX);
        let key = match (self.key.as_str(), count) {
            ("", _) => String::new(),
            // As a comb does: the key itself for a single cell, and one per cell otherwise.
            (k, _) if self.count == 1 => k.to_owned(),
            (k, _) => format!("{k}/{}", chunk[0]),
        };
        let req = v1::CreateRequest {
            spec: Some(self.wire.clone()),
            count,
            idempotency_key: key,
            placement: None,
        };
        let call = async {
            let mut client = self.api.node(node)?;
            client.create(out(&self.project, req)).await
        };
        let mut stream = match call.await {
            Ok(r) => r.into_inner(),
            // The comb never saw the call, so the cells can go elsewhere.
            Err(s) if s.code() == tonic::Code::Unavailable && !last => return chunk.to_vec(),
            Err(s) => {
                let e = on_node(node, &s);
                for &index in chunk {
                    self.answer(index, Err(e.clone())).await;
                }
                return Vec::new();
            }
        };
        let mut answered = vec![false; chunk.len()];
        let mut again = Vec::new();
        loop {
            match stream.message().await {
                Ok(Some(ev)) => {
                    let Some(&index) = chunk.get(ev.index as usize) else { continue };
                    answered[ev.index as usize] = true;
                    match ev.result {
                        Some(v1::create_event::Result::Error(e))
                            if !last && e.reason == Reason::CapacityUnavailable.as_str() =>
                        {
                            again.push(index);
                        }
                        Some(v1::create_event::Result::Cell(c)) => self.answer(index, Ok(c)).await,
                        Some(v1::create_event::Result::Error(e)) => {
                            self.answer(index, Err(e)).await
                        }
                        None => {}
                    }
                }
                Ok(None) => break,
                Err(s) => {
                    // The comb had the call, so the cells may be there. Saying so is better
                    // than making them twice.
                    let e = on_node(node, &s);
                    for (i, &index) in chunk.iter().enumerate() {
                        if !answered[i] {
                            self.answer(index, Err(e.clone())).await;
                        }
                    }
                    break;
                }
            }
        }
        again
    }

    async fn answer(&self, index: u32, result: Result<v1::Cell, v1::Error>) {
        let inner = &self.api.inner;
        let (label, result) = match result {
            Ok(c) => ("created", v1::create_event::Result::Cell(c)),
            Err(e) => ("failed", v1::create_event::Result::Error(e)),
        };
        inner.creates.with(&[label]).inc();
        inner.create_seconds.with(&[]).observe_duration(self.started.elapsed());
        // A caller that went away no longer needs the answer, and the cell is made regardless.
        let _ = self.tx.send(v1::CreateEvent { index, result: Some(result) }).await;
    }
}

/// Follows every node for cells with `labels`, and nodes as they join, until the caller goes
/// away.
async fn watch_all(
    api: Api,
    project: String,
    labels: v1::LabelSelector,
    tx: mpsc::Sender<Result<v1::CellEvent, Status>>,
) {
    let mut snaps = api.inner.nodes.subscribe();
    let mut watching: HashSet<u16> = HashSet::new();
    let mut tasks = JoinSet::new();
    loop {
        for node in api.inner.nodes.all() {
            if watching.insert(node) {
                let (api, project, labels, tx) =
                    (api.clone(), project.clone(), labels.clone(), tx.clone());
                tasks.spawn(watch_node(api, node, project, labels, tx));
            }
        }
        tokio::select! {
            () = tx.closed() => return,
            r = snaps.changed() => if r.is_err() { return },
        }
    }
}

/// Follows one node, asking again when its stream breaks, while the node is known.
async fn watch_node(
    api: Api,
    node: u16,
    project: String,
    labels: v1::LabelSelector,
    tx: mpsc::Sender<Result<v1::CellEvent, Status>>,
) {
    let req = v1::WatchCellsRequest {
        selector: Some(v1::CellSelector { by: Some(v1::cell_selector::By::Labels(labels)) }),
    };
    while !tx.is_closed() && api.inner.nodes.all().contains(&node) {
        if let Ok(mut c) = api.node(node)
            && let Ok(r) = c.watch(out(&project, req.clone())).await
        {
            let mut events = r.into_inner();
            loop {
                tokio::select! {
                    () = tx.closed() => return,
                    ev = events.message() => match ev {
                        Ok(Some(ev)) => if tx.send(Ok(ev)).await.is_err() { return },
                        _ => break,
                    },
                }
            }
        }
        tokio::select! {
            () = tx.closed() => return,
            () = tokio::time::sleep(REWATCH) => {}
        }
    }
}

/// Where a list page goes on from: `nNODE` for a node from its start, or a cell id for after
/// that cell on its node.
fn parse_token(t: &str) -> Result<(u16, Option<CellId>), Status> {
    let bad = || invalid("the page token is not valid");
    if t.is_empty() {
        return Ok((0, None));
    }
    if let Some(n) = t.strip_prefix('n') {
        return Ok((n.parse().map_err(|_| bad())?, None));
    }
    let id: CellId = t.parse().map_err(|_| bad())?;
    Ok((id.node(), Some(id)))
}

/// The project the gate stamped on the call when it checked the key.
fn project<T>(req: &Request<T>) -> Result<String, Status> {
    req.metadata()
        .get(PROJECT_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .ok_or_else(|| Status::unauthenticated("no project"))
}

/// `msg` as a call for `project`.
fn out<T>(project: &str, msg: T) -> Request<T> {
    let mut r = Request::new(msg);
    if let Ok(v) = project.parse() {
        r.metadata_mut().insert(PROJECT_HEADER, v);
    }
    r
}

fn parse_id(id: &str) -> Result<CellId, Status> {
    id.parse().map_err(|_| invalid(format!("{id:?} is not a cell id")))
}

fn invalid(msg: impl Into<String>) -> Status {
    convert::error_to_status(&Error::new(Reason::InvalidArgument, msg))
}

/// `s` from `node` as an error for the caller, saying which node.
fn node_error(node: u16, s: &Status) -> Error {
    let mut e = convert::error_from_status(s);
    // A status with no hivebox reason and this code came from the connection, not the comb.
    if e.reason == Reason::Internal && s.code() == tonic::Code::Unavailable {
        e.reason = Reason::DroneUnreachable;
    }
    e.message = format!("node {node}: {}", e.message);
    e
}

fn on_node(node: u16, s: &Status) -> v1::Error {
    convert::error_to_v1(&node_error(node, s))
}
