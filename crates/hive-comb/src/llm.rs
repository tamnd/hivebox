//! The LLM gateway from `spec/11_rl_integration.md`, section 6. Cells with the `llm` network
//! profile reach it at `http://llm.hive.internal`, which the DNS proxy answers with the gateway's
//! address, and an agent that speaks the OpenAI or Anthropic API runs there unchanged.
//!
//! Each call is forwarded to the project's inference engine with the engine's key in place of
//! whatever the cell sent, so the cell never sees it. A cell is known by the address it connects
//! from, and its rollout by its `rollout_id` label. For chat and completions calls the gateway
//! asks the engine for the token ids with vLLM's `return_token_ids`, and for log probabilities
//! too when the node is set to, and keeps them per rollout for the trainer, so it trains on the
//! exact tokens the engine saw and sampled with no tokenizing again. Whatever the gateway asked
//! for and the cell did not is taken out of the answer, streamed or not, so the cell sees the
//! answer it asked for.
//!
//! While the trainer loads new weights it holds the project: new calls get 503 with Retry-After
//! and the agents' clients try again, and the hold waits for the calls in flight to end.

use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};
use std::task::{Context, Poll, ready};
use std::time::{Duration, Instant, SystemTime};

use bytes::{Bytes, BytesMut};
use hive_guard::{LLM_VIP, Profile};
use hive_types::CellId;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::{Body, Frame, Incoming, SizeHint};
use hyper::header::{self, HeaderMap, HeaderName, HeaderValue};
use hyper::{Method, Request, Response, StatusCode, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde_json::{Map, Value};
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::cell::Cell;
use crate::comb::Inner;
use crate::config::Llm;
use crate::net::Net;

/// The biggest request a cell may send.
const MAX_REQUEST: usize = 32 << 20;
/// The biggest answer that is not streamed.
const MAX_ANSWER: usize = 64 << 20;
/// Of an error answer, what is kept in the turn.
const ERROR_TAIL: usize = 1024;
/// The longest hold, so a trainer that dies holding does not hold for ever.
const MAX_HOLD: Duration = Duration::from_secs(3600);

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type Out = BoxBody<Bytes, BoxError>;

/// Where a project's calls go.
#[derive(Clone, Debug)]
pub(crate) struct Route {
    authority: hyper::http::uri::Authority,
    prefix: String,
    key: Option<HeaderValue>,
}

impl Route {
    /// A route to the engine at `upstream`, a plain HTTP base URL, with `key` as its bearer token.
    pub(crate) fn new(upstream: &str, key: Option<&str>) -> Result<Self, String> {
        let uri: Uri = upstream.parse().map_err(|e| format!("not a URL: {e}"))?;
        if uri.scheme_str() != Some("http") {
            return Err(
                "the gateway talks to the engine over plain HTTP, as http://host:port".into()
            );
        }
        let authority = uri.authority().cloned().ok_or("the URL has no host")?;
        if uri.query().is_some() {
            return Err("the URL may not have a query".into());
        }
        let key = match key.filter(|k| !k.is_empty()) {
            Some(k) => {
                let mut v = HeaderValue::from_str(&format!("Bearer {k}"))
                    .map_err(|_| "the key has characters a header cannot carry")?;
                v.set_sensitive(true);
                Some(v)
            }
            None => None,
        };
        Ok(Self { authority, prefix: uri.path().trim_end_matches('/').to_string(), key })
    }

    fn uri(&self, path_and_query: &str) -> Result<Uri, hyper::http::Error> {
        Uri::builder()
            .scheme("http")
            .authority(self.authority.clone())
            .path_and_query(format!("{}{path_and_query}", self.prefix))
            .build()
    }
}

/// A hold on a project's calls.
#[derive(Clone, Copy, Debug)]
struct Hold {
    until: Instant,
    retry_after: Duration,
}

/// One call a cell made, as the trainer gets it.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Turn {
    pub(crate) cell: Option<CellId>,
    pub(crate) rollout: String,
    pub(crate) seq: u64,
    pub(crate) path: String,
    pub(crate) model: String,
    pub(crate) status: u16,
    pub(crate) stream: bool,
    pub(crate) started: Option<SystemTime>,
    pub(crate) took: Duration,
    pub(crate) prompt_ids: Vec<u32>,
    pub(crate) choices: Vec<Choice>,
    pub(crate) prompt_tokens: u64,
    pub(crate) completion_tokens: u64,
    pub(crate) error: String,
}

/// One choice in a turn.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Choice {
    pub(crate) index: u32,
    pub(crate) output_ids: Vec<u32>,
    pub(crate) logprobs: Vec<f32>,
    pub(crate) finish_reason: String,
    pub(crate) prompt_ids: Vec<u32>,
}

impl Turn {
    /// About what the turn takes in memory.
    fn size(&self) -> usize {
        let ids = self.prompt_ids.len()
            + self
                .choices
                .iter()
                .map(|c| c.output_ids.len() + c.logprobs.len() + c.prompt_ids.len() + 16)
                .sum::<usize>();
        160 + 4 * ids + self.rollout.len() + self.path.len() + self.model.len() + self.error.len()
    }
}

/// What a rollout costs in memory on top of its turns. A rollout whose turns were all taken is kept
/// so its numbering goes on, and goes when memory runs short like any other.
const GROUP: usize = 128;

/// The turns the trainer has not taken, by project and rollout. A cell with no rollout label is a
/// rollout of its own, named by its id.
#[derive(Default)]
struct Turns {
    groups: HashMap<(String, String), Group>,
    /// Each group by the last time a turn went in, to drop the stalest one first.
    order: BTreeMap<u64, (String, String)>,
    stamp: u64,
    bytes: usize,
    dropped: HashMap<String, u64>,
}

#[derive(Default)]
struct Group {
    turns: Vec<Turn>,
    bytes: usize,
    stamp: u64,
    next: u64,
}

impl Turns {
    fn add(&mut self, project: &str, key: String, mut turn: Turn, keep: usize) {
        self.stamp += 1;
        let stamp = self.stamp;
        let size = turn.size();
        let id = (project.to_string(), key);
        let g = self
            .groups
            .entry(id.clone())
            .or_insert_with(|| Group { bytes: GROUP, ..Group::default() });
        if g.stamp == 0 {
            self.bytes += GROUP;
        } else {
            self.order.remove(&g.stamp);
        }
        g.stamp = stamp;
        turn.seq = g.next;
        g.next += 1;
        g.bytes += size;
        g.turns.push(turn);
        self.order.insert(stamp, id);
        self.bytes += size;
        while self.bytes > keep {
            let Some((_, id)) = self.order.pop_first() else { break };
            if let Some(g) = self.groups.remove(&id) {
                self.bytes -= g.bytes;
                *self.dropped.entry(id.0).or_default() += g.turns.len() as u64;
            }
        }
    }

    /// The project's turns for `rollout`, or for `cell`, or both, taken out when `take` is set.
    fn get(&mut self, project: &str, rollout: &str, cell: Option<CellId>, take: bool) -> Vec<Turn> {
        let keys: Vec<(String, String)> = if rollout.is_empty() {
            self.groups
                .iter()
                .filter(|((p, _), g)| p == project && g.turns.iter().any(|t| t.cell == cell))
                .map(|(k, _)| k.clone())
                .collect()
        } else {
            vec![(project.to_string(), rollout.to_string())]
        };
        let mut out = Vec::new();
        for key in keys {
            let Some(g) = self.groups.get_mut(&key) else { continue };
            let wanted = |t: &Turn| cell.is_none() || t.cell == cell;
            if !take {
                out.extend(g.turns.iter().filter(|t| wanted(t)).cloned());
                continue;
            }
            let (got, kept): (Vec<Turn>, Vec<Turn>) = g.turns.drain(..).partition(wanted);
            let freed: usize = got.iter().map(Turn::size).sum();
            g.bytes -= freed;
            self.bytes -= freed;
            g.turns = kept;
            out.extend(got);
        }
        out.sort_by_key(|t| (t.started, t.seq));
        out
    }
}

/// The gateway's state, which the API reads and changes.
pub(crate) struct Gateway {
    cfg: Llm,
    fallback: Option<Route>,
    routes: RwLock<HashMap<String, Route>>,
    holds: Mutex<HashMap<String, Hold>>,
    flight: Mutex<HashMap<String, u32>>,
    landed: Notify,
    turns: Mutex<Turns>,
    client: Client<HttpConnector, Full<Bytes>>,
}

impl std::fmt::Debug for Gateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gateway").field("fallback", &self.fallback).finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Gateway {
    /// The gateway `cfg` sets up, with the key read from its file.
    pub(crate) fn new(cfg: &Llm) -> io::Result<Self> {
        let key = match &cfg.api_key_file {
            Some(path) => Some(std::fs::read_to_string(path).map_err(|e| {
                io::Error::new(e.kind(), format!("reading {}: {e}", path.display()))
            })?),
            None => None,
        };
        let fallback = match &cfg.upstream {
            Some(up) => Some(
                Route::new(up, key.as_deref().map(str::trim))
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
            ),
            None => None,
        };
        let mut http = HttpConnector::new();
        http.set_nodelay(true);
        http.set_connect_timeout(Some(Duration::from_secs(10)));
        let client = Client::builder(TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(60))
            .build(http);
        Ok(Self {
            cfg: cfg.clone(),
            fallback,
            routes: RwLock::default(),
            holds: Mutex::default(),
            flight: Mutex::default(),
            landed: Notify::new(),
            turns: Mutex::default(),
            client,
        })
    }

    /// Sends the project's calls to `route`, or back to the node's own with `None`.
    pub(crate) fn set_route(&self, project: &str, route: Option<Route>) {
        let mut routes = self.routes.write().unwrap_or_else(PoisonError::into_inner);
        match route {
            Some(r) => routes.insert(project.to_string(), r),
            None => routes.remove(project),
        };
    }

    fn route(&self, project: &str) -> Option<Route> {
        let routes = self.routes.read().unwrap_or_else(PoisonError::into_inner);
        routes.get(project).cloned().or_else(|| self.fallback.clone())
    }

    /// Holds the project's new calls for `ttl`, then waits up to `drain` for the ones in flight,
    /// and returns how many are left.
    pub(crate) async fn hold(
        &self,
        project: &str,
        retry_after: Duration,
        ttl: Duration,
        drain: Duration,
    ) -> u32 {
        let until = Instant::now() + ttl.min(MAX_HOLD);
        lock(&self.holds).insert(project.to_string(), Hold { until, retry_after });
        let deadline = tokio::time::Instant::now() + drain;
        loop {
            let landed = self.landed.notified();
            tokio::pin!(landed);
            landed.as_mut().enable();
            let left = self.in_flight(project);
            if left == 0 || tokio::time::Instant::now() >= deadline {
                return left;
            }
            let _ = tokio::time::timeout_at(deadline, landed).await;
        }
    }

    /// Ends the project's hold.
    pub(crate) fn release(&self, project: &str) -> u32 {
        lock(&self.holds).remove(project);
        self.in_flight(project)
    }

    fn held(&self, project: &str) -> Option<Duration> {
        let mut holds = lock(&self.holds);
        let h = *holds.get(project)?;
        if Instant::now() >= h.until {
            holds.remove(project);
            return None;
        }
        Some(h.retry_after)
    }

    fn in_flight(&self, project: &str) -> u32 {
        lock(&self.flight).get(project).copied().unwrap_or(0)
    }

    /// The project's turns for a rollout or a cell, and how many it has had dropped.
    pub(crate) fn turns(
        &self,
        project: &str,
        rollout: &str,
        cell: Option<CellId>,
        take: bool,
    ) -> (Vec<Turn>, u64) {
        let mut t = lock(&self.turns);
        let got = t.get(project, rollout, cell, take);
        (got, t.dropped.get(project).copied().unwrap_or(0))
    }

    fn keep(&self, project: &str, turn: Turn) {
        let key = if turn.rollout.is_empty() {
            turn.cell.map(|c| c.to_string()).unwrap_or_default()
        } else {
            turn.rollout.clone()
        };
        let keep = usize::try_from(self.cfg.keep_bytes).unwrap_or(usize::MAX);
        lock(&self.turns).add(project, key, turn, keep);
    }

    /// Serves cells on the gateway's address until `stop` is cancelled. The comb before a restart
    /// can hold the port for a moment, so binding is tried for a few seconds.
    pub(crate) async fn serve(
        self: Arc<Self>,
        inner: Arc<Inner>,
        net: Arc<Net>,
        stop: CancellationToken,
    ) {
        let mut tries = 0;
        let listener = loop {
            match TcpListener::bind((LLM_VIP, 80)).await {
                Ok(l) => break l,
                Err(e) if e.kind() == io::ErrorKind::AddrInUse && tries < 50 => {
                    tries += 1;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => {
                    eprintln!("hive-comb: the LLM gateway did not start: {e}");
                    return;
                }
            }
        };
        loop {
            let accepted = tokio::select! {
                a = listener.accept() => a,
                () = stop.cancelled() => return,
            };
            let (stream, SocketAddr::V4(peer)) = (match accepted {
                Ok(a) => a,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
            }) else {
                continue;
            };
            let _ = stream.set_nodelay(true);
            let (gw, inner, net, stop) = (self.clone(), inner.clone(), net.clone(), stop.clone());
            tokio::spawn(async move {
                let svc = hyper::service::service_fn(move |req| {
                    let (gw, inner, net) = (gw.clone(), inner.clone(), net.clone());
                    async move { Ok::<_, Infallible>(gw.call(&inner, &net, *peer.ip(), req).await) }
                });
                let conn = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), svc);
                tokio::select! {
                    _ = conn => {}
                    () = stop.cancelled() => {}
                }
            });
        }
    }

    async fn call(
        self: Arc<Self>,
        inner: &Inner,
        net: &Net,
        peer: Ipv4Addr,
        req: Request<Incoming>,
    ) -> Response<Out> {
        let cell = match net.cell_at(peer) {
            Some((id, Profile::LLM)) => inner.find(id).ok(),
            _ => None,
        };
        let Some(cell) = cell else {
            return error(StatusCode::FORBIDDEN, "only cells with the llm profile may call", None);
        };
        cell.touch();
        let project = cell.project.clone();
        // Counted before the hold is looked at, so a hold that finds nothing in flight never
        // misses a call that is about to start.
        let flight = Flight::start(self.clone(), &project);
        if let Some(wait) = self.held(&project) {
            drop(flight);
            let msg = "the engine is loading new weights";
            return error(StatusCode::SERVICE_UNAVAILABLE, msg, Some(wait));
        }
        let Some(route) = self.route(&project) else {
            drop(flight);
            let msg = format!("project {project} has no LLM route yet");
            return error(StatusCode::SERVICE_UNAVAILABLE, &msg, Some(Duration::from_secs(5)));
        };
        let (head, body) = req.into_parts();
        let body = match Limited::new(body, MAX_REQUEST).collect().await {
            Ok(b) => b.to_bytes(),
            Err(_) => return error(StatusCode::PAYLOAD_TOO_LARGE, "the request is too big", None),
        };
        let path = head.uri.path().to_string();
        let kind = if head.method == Method::POST { Kind::of(&path) } else { Kind::Other };
        let (body, ask) = Ask::prepare(kind, body, self.cfg.logprobs);
        let pq = head.uri.path_and_query().map_or("/", |p| p.as_str());
        let Ok(uri) = route.uri(pq) else {
            return error(StatusCode::BAD_REQUEST, "the path cannot be forwarded", None);
        };
        let mut up = Request::builder().method(head.method.clone()).uri(uri);
        if let Some(h) = up.headers_mut() {
            copy_headers(&head.headers, h);
            for name in [header::AUTHORIZATION, HeaderName::from_static("x-api-key")] {
                h.remove(name);
            }
            if let Some(key) = &route.key {
                h.insert(header::AUTHORIZATION, key.clone());
            }
        }
        let Ok(up) = up.body(Full::new(body)) else {
            return error(StatusCode::BAD_REQUEST, "the request cannot be forwarded", None);
        };
        let rollout = cell.spec.labels.get(&self.cfg.label).cloned().unwrap_or_default();
        let mut call = ask.map(|ask| Call {
            gw: self.clone(),
            cell: cell.clone(),
            project: project.clone(),
            started: SystemTime::now(),
            t0: Instant::now(),
            ask,
            capture: Capture::default(),
            turn: Turn { cell: Some(cell.id), rollout, path, ..Turn::default() },
            _flight: None,
        });
        let deadline = tokio::time::Instant::now() + self.cfg.timeout;
        let resp = match tokio::time::timeout_at(deadline, self.client.request(up)).await {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                let msg = format!("the engine could not be reached: {e}");
                if let Some(c) = call.take() {
                    c.finish(StatusCode::BAD_GATEWAY, msg.clone());
                }
                return error(StatusCode::BAD_GATEWAY, &msg, None);
            }
            Err(_) => {
                let msg = "the engine did not answer in time";
                if let Some(c) = call.take() {
                    c.finish(StatusCode::GATEWAY_TIMEOUT, msg.into());
                }
                return error(StatusCode::GATEWAY_TIMEOUT, msg, None);
            }
        };
        let (mut parts, body) = resp.into_parts();
        let mut headers = HeaderMap::new();
        copy_headers(&parts.headers, &mut headers);
        headers.remove(header::CONTENT_LENGTH);
        parts.headers = headers;
        let sse = parts
            .headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("text/event-stream"));
        let Some(mut c) = call else {
            // Not a call to keep: it goes through as it is, still counted as in flight.
            let body = Tap::new(body, deadline, None, None, Some(flight));
            return Response::from_parts(parts, body.boxed());
        };
        c.turn.status = parts.status.as_u16();
        if sse && parts.status.is_success() {
            c.turn.stream = true;
            c._flight = Some(flight);
            let sse = Sse::new(c.ask.strips());
            let body = Tap::new(body, deadline, Some(sse), Some(c), None);
            return Response::from_parts(parts, body.boxed());
        }
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        let read = tokio::time::timeout(left, Limited::new(body, MAX_ANSWER).collect()).await;
        let bytes = match read {
            Ok(Ok(b)) => b.to_bytes(),
            Ok(Err(e)) => {
                let msg = format!("reading the engine's answer: {e}");
                c.finish(StatusCode::BAD_GATEWAY, msg.clone());
                return error(StatusCode::BAD_GATEWAY, &msg, None);
            }
            Err(_) => {
                let msg = "the engine did not finish its answer in time";
                c.finish(StatusCode::GATEWAY_TIMEOUT, msg.into());
                return error(StatusCode::GATEWAY_TIMEOUT, msg, None);
            }
        };
        let status = parts.status;
        let bytes = if status.is_success() {
            match serde_json::from_slice::<Value>(&bytes) {
                Ok(mut v) => {
                    c.capture.take(&v);
                    if c.ask.strips() {
                        c.ask.strip(&mut v);
                        serde_json::to_vec(&v).map_or(bytes, Bytes::from)
                    } else {
                        bytes
                    }
                }
                Err(_) => bytes,
            }
        } else {
            bytes
        };
        let msg = if status.is_success() {
            String::new()
        } else {
            String::from_utf8_lossy(&bytes[..bytes.len().min(ERROR_TAIL)]).into_owned()
        };
        c.finish(status, msg);
        drop(flight);
        Response::from_parts(parts, Full::new(bytes).map_err(|n| match n {}).boxed())
    }
}

/// Counts a call in flight for its project, until it is dropped.
struct Flight {
    gw: Arc<Gateway>,
    project: String,
}

impl Flight {
    fn start(gw: Arc<Gateway>, project: &str) -> Self {
        *lock(&gw.flight).entry(project.to_string()).or_default() += 1;
        Self { gw, project: project.to_string() }
    }
}

impl Drop for Flight {
    fn drop(&mut self) {
        let mut flight = lock(&self.gw.flight);
        if let Some(n) = flight.get_mut(&self.project) {
            *n -= 1;
            if *n == 0 {
                flight.remove(&self.project);
            }
        }
        drop(flight);
        self.gw.landed.notify_waiters();
    }
}

/// The calls whose tokens are kept.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Chat,
    Completions,
    Messages,
    Other,
}

impl Kind {
    fn of(path: &str) -> Self {
        if path.ends_with("/chat/completions") {
            Self::Chat
        } else if path.ends_with("/completions") {
            Self::Completions
        } else if path.ends_with("/messages") {
            Self::Messages
        } else {
            Self::Other
        }
    }
}

/// What the gateway added to a request, to take back out of the answer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Ask {
    ids: bool,
    logprobs: bool,
}

impl Ask {
    /// The body to send for a call of `kind`, and what was added to it, or `None` for a call
    /// whose tokens are not kept.
    fn prepare(kind: Kind, body: Bytes, logprobs: bool) -> (Bytes, Option<Self>) {
        if kind == Kind::Other {
            return (body, None);
        }
        let Ok(Value::Object(mut obj)) = serde_json::from_slice::<Value>(&body) else {
            return (body, None);
        };
        if kind == Kind::Messages {
            // vLLM's Anthropic API gives no token ids, so only the counts are kept.
            return (body, Some(Self::default()));
        }
        let mut ask = Self::default();
        if !obj.get("return_token_ids").and_then(Value::as_bool).unwrap_or(false) {
            obj.insert("return_token_ids".into(), Value::Bool(true));
            ask.ids = true;
        }
        if logprobs {
            let asked = match kind {
                Kind::Chat => obj.get("logprobs").and_then(Value::as_bool).unwrap_or(false),
                _ => obj.get("logprobs").is_some_and(|v| !v.is_null()),
            };
            if !asked {
                let v = if kind == Kind::Chat { Value::Bool(true) } else { Value::from(0) };
                obj.insert("logprobs".into(), v);
                ask.logprobs = true;
            }
        }
        if ask == Self::default() {
            return (body, Some(ask));
        }
        match serde_json::to_vec(&obj) {
            Ok(b) => (Bytes::from(b), Some(ask)),
            Err(_) => (body, None),
        }
    }

    fn strips(self) -> bool {
        self.ids || self.logprobs
    }

    /// Takes what the gateway added out of an answer or a streamed chunk.
    fn strip(self, v: &mut Value) {
        let Some(obj) = v.as_object_mut() else { return };
        if self.ids {
            obj.remove("prompt_token_ids");
        }
        let Some(Value::Array(choices)) = obj.get_mut("choices") else { return };
        for c in choices.iter_mut().filter_map(Value::as_object_mut) {
            if self.ids {
                c.remove("token_ids");
                c.remove("prompt_token_ids");
            }
            if self.logprobs {
                c.insert("logprobs".into(), Value::Null);
            }
        }
    }
}

/// The tokens of an answer, gathered from it whole or chunk by chunk.
#[derive(Debug, Default)]
struct Capture {
    model: String,
    prompt_ids: Vec<u32>,
    choices: BTreeMap<u32, Choice>,
    prompt_tokens: u64,
    completion_tokens: u64,
}

fn ids(v: &Value) -> impl Iterator<Item = u32> + '_ {
    v.as_array()
        .into_iter()
        .flatten()
        .filter_map(|x| x.as_u64().and_then(|x| u32::try_from(x).ok()))
}

#[allow(clippy::cast_possible_truncation)]
fn logprob(v: &Value) -> f32 {
    v.as_f64().map_or(f32::NAN, |x| x as f32)
}

impl Capture {
    fn take(&mut self, v: &Value) {
        let Some(obj) = v.as_object() else { return };
        if self.model.is_empty()
            && let Some(m) = obj.get("model").and_then(Value::as_str)
        {
            self.model = m.to_string();
        }
        if self.prompt_ids.is_empty()
            && let Some(p) = obj.get("prompt_token_ids")
        {
            self.prompt_ids.extend(ids(p));
        }
        // OpenAI's usage, Anthropic's whole answer, and Anthropic's first and last stream events.
        for usage in [obj.get("usage"), obj.get("message").and_then(|m| m.get("usage"))] {
            let Some(u) = usage else { continue };
            let n = |keys: [&str; 2]| keys.iter().find_map(|k| u.get(*k).and_then(Value::as_u64));
            if let Some(n) = n(["prompt_tokens", "input_tokens"]) {
                self.prompt_tokens = n;
            }
            if let Some(n) = n(["completion_tokens", "output_tokens"]) {
                self.completion_tokens = n;
            }
        }
        let Some(Value::Array(choices)) = obj.get("choices") else { return };
        for (i, c) in choices.iter().enumerate() {
            let index = c.get("index").and_then(Value::as_u64).unwrap_or(i as u64);
            let ch = self.choices.entry(u32::try_from(index).unwrap_or(u32::MAX)).or_default();
            ch.index = u32::try_from(index).unwrap_or(u32::MAX);
            if ch.prompt_ids.is_empty()
                && let Some(p) = c.get("prompt_token_ids")
            {
                ch.prompt_ids.extend(ids(p));
            }
            if let Some(t) = c.get("token_ids") {
                ch.output_ids.extend(ids(t));
            }
            if let Some(lp) = c.get("logprobs").filter(|l| !l.is_null()) {
                if let Some(Value::Array(content)) = lp.get("content") {
                    ch.logprobs.extend(content.iter().map(|t| logprob(&t["logprob"])));
                } else if let Some(Value::Array(lps)) = lp.get("token_logprobs") {
                    ch.logprobs.extend(lps.iter().map(logprob));
                }
            }
            if let Some(r) = c.get("finish_reason").and_then(Value::as_str) {
                ch.finish_reason = r.to_string();
            }
        }
    }

    fn into_turn(mut self, turn: &mut Turn) {
        if self.prompt_ids.is_empty()
            && let Some(first) = self.choices.values().next()
        {
            self.prompt_ids.clone_from(&first.prompt_ids);
        }
        for ch in self.choices.values_mut() {
            if ch.prompt_ids == self.prompt_ids {
                ch.prompt_ids.clear();
            }
        }
        if !self.model.is_empty() {
            turn.model = self.model;
        }
        turn.prompt_ids = self.prompt_ids;
        turn.choices = self.choices.into_values().collect();
        turn.prompt_tokens = self.prompt_tokens;
        turn.completion_tokens = self.completion_tokens;
    }
}

/// A call being kept, which goes into the turns when it ends, however it ends.
struct Call {
    gw: Arc<Gateway>,
    cell: Arc<Cell>,
    project: String,
    started: SystemTime,
    t0: Instant,
    ask: Ask,
    capture: Capture,
    turn: Turn,
    _flight: Option<Flight>,
}

impl Call {
    fn finish(mut self, status: StatusCode, error: String) {
        self.cell.touch();
        let mut turn = std::mem::take(&mut self.turn);
        turn.status = status.as_u16();
        turn.error = error;
        turn.started = Some(self.started);
        turn.took = self.t0.elapsed();
        std::mem::take(&mut self.capture).into_turn(&mut turn);
        self.gw.keep(&self.project, turn);
    }
}

/// Server-sent events from the engine, read for their tokens and, when the gateway added to the
/// request, written out again without what it added.
struct Sse {
    buf: BytesMut,
    strip: bool,
}

impl Sse {
    fn new(strip: bool) -> Self {
        Self { buf: BytesMut::new(), strip }
    }

    /// Reads `data`, and returns what goes on to the cell: the same bytes when nothing is taken
    /// out, and otherwise the events that are whole so far.
    fn feed(&mut self, data: Bytes, ask: Ask, capture: &mut Capture) -> Bytes {
        self.buf.extend_from_slice(&data);
        let mut out = BytesMut::new();
        while let Some((end, sep)) = event_end(&self.buf) {
            let event = self.buf.split_to(end + sep);
            let text = &event[..end];
            let rewritten = rewrite(text, ask, capture, self.strip);
            if self.strip {
                out.extend_from_slice(&rewritten);
                out.extend_from_slice(&event[end..]);
            }
        }
        if self.strip { out.freeze() } else { data }
    }

    /// The rest of the stream, when it ends with no blank line after the last event.
    fn flush(&mut self, ask: Ask, capture: &mut Capture) -> Bytes {
        if self.buf.is_empty() {
            return Bytes::new();
        }
        let rest = self.buf.split();
        let rewritten = rewrite(&rest, ask, capture, self.strip);
        if self.strip { Bytes::from(rewritten) } else { Bytes::new() }
    }
}

/// Where the first whole event in `buf` ends, and the length of the blank line after it.
fn event_end(buf: &[u8]) -> Option<(usize, usize)> {
    let lf = buf.windows(2).position(|w| w == b"\n\n").map(|i| (i, 2));
    let crlf = buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| (i, 4));
    match (lf, crlf) {
        (Some(a), Some(b)) => Some(if a.0 <= b.0 { a } else { b }),
        (a, b) => a.or(b),
    }
}

/// Reads the data lines of one event and, when `strip` is set, returns the event with each data
/// line written without what the gateway added.
fn rewrite(event: &[u8], ask: Ask, capture: &mut Capture, strip: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(if strip { event.len() } else { 0 });
    for (i, line) in event.split(|&b| b == b'\n').enumerate() {
        if strip && i > 0 {
            out.push(b'\n');
        }
        let data = line.strip_prefix(b"data:").map(|d| d.strip_prefix(b" ").unwrap_or(d));
        let parsed = data
            .filter(|d| d.trim_ascii() != b"[DONE]")
            .and_then(|d| serde_json::from_slice::<Value>(d.trim_ascii()).ok());
        match parsed {
            Some(mut v) => {
                capture.take(&v);
                if strip {
                    ask.strip(&mut v);
                    out.extend_from_slice(b"data: ");
                    out.extend_from_slice(&serde_json::to_vec(&v).unwrap_or_default());
                    if line.ends_with(b"\r") {
                        out.push(b'\r');
                    }
                }
            }
            None if strip => out.extend_from_slice(line),
            None => {}
        }
    }
    out
}

/// The engine's answer on its way to the cell, read as it passes, and the call kept when it
/// ends, runs out of time or the cell hangs up.
struct Tap {
    inner: Incoming,
    deadline: Pin<Box<tokio::time::Sleep>>,
    sse: Option<Sse>,
    call: Option<Call>,
    flight: Option<Flight>,
    done: bool,
}

impl Tap {
    fn new(
        inner: Incoming,
        deadline: tokio::time::Instant,
        sse: Option<Sse>,
        call: Option<Call>,
        flight: Option<Flight>,
    ) -> Self {
        let deadline = Box::pin(tokio::time::sleep_until(deadline));
        Self { inner, deadline, sse, call, flight, done: false }
    }

    fn end(&mut self, status: Option<StatusCode>, error: Option<String>) {
        self.done = true;
        self.flight = None;
        if let Some(c) = self.call.take() {
            let status = status
                .or_else(|| StatusCode::from_u16(c.turn.status).ok())
                .unwrap_or(StatusCode::OK);
            c.finish(status, error.unwrap_or_default());
        }
    }
}

impl Body for Tap {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let this = &mut *self;
        if this.done {
            return Poll::Ready(None);
        }
        if this.deadline.as_mut().poll(cx).is_ready() {
            let msg = "the engine did not finish its answer in time";
            this.end(Some(StatusCode::GATEWAY_TIMEOUT), Some(msg.into()));
            return Poll::Ready(Some(Err(msg.into())));
        }
        loop {
            match ready!(Pin::new(&mut this.inner).poll_frame(cx)) {
                Some(Ok(frame)) => {
                    let data = match frame.into_data() {
                        Ok(d) => d,
                        Err(frame) => return Poll::Ready(Some(Ok(frame))),
                    };
                    let (Some(sse), Some(c)) = (this.sse.as_mut(), this.call.as_mut()) else {
                        return Poll::Ready(Some(Ok(Frame::data(data))));
                    };
                    c.cell.touch();
                    let out = sse.feed(data, c.ask, &mut c.capture);
                    if !out.is_empty() {
                        return Poll::Ready(Some(Ok(Frame::data(out))));
                    }
                }
                Some(Err(e)) => {
                    this.end(None, Some(format!("the engine's stream broke: {e}")));
                    return Poll::Ready(Some(Err(e.into())));
                }
                None => {
                    let rest = match (this.sse.as_mut(), this.call.as_mut()) {
                        (Some(sse), Some(c)) => sse.flush(c.ask, &mut c.capture),
                        _ => Bytes::new(),
                    };
                    this.end(None, None);
                    if !rest.is_empty() {
                        return Poll::Ready(Some(Ok(Frame::data(rest))));
                    }
                    return Poll::Ready(None);
                }
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done
    }

    fn size_hint(&self) -> SizeHint {
        if self.sse.as_ref().is_some_and(|s| s.strip) {
            SizeHint::default()
        } else {
            self.inner.size_hint()
        }
    }
}

impl Drop for Tap {
    fn drop(&mut self) {
        if self.call.is_some() {
            self.end(None, Some("the cell hung up before the answer ended".into()));
        }
    }
}

/// Hop-by-hop headers, which are the connection's own and do not go on.
fn hop(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "proxy-connection"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "host"
            | "content-length"
    )
}

fn copy_headers(from: &HeaderMap, to: &mut HeaderMap) {
    for (name, value) in from {
        if !hop(name) {
            to.append(name.clone(), value.clone());
        }
    }
}

/// An answer from the gateway itself, as an OpenAI style error.
fn error(status: StatusCode, message: &str, retry_after: Option<Duration>) -> Response<Out> {
    let mut err = Map::new();
    err.insert("message".into(), Value::from(message));
    err.insert("type".into(), Value::from("hivebox_gateway"));
    err.insert("code".into(), Value::from(status.as_u16()));
    let body =
        serde_json::to_vec(&Value::Object(Map::from_iter([("error".into(), Value::Object(err))])))
            .unwrap_or_default();
    let mut r = Response::new(Full::new(Bytes::from(body)).map_err(|n| match n {}).boxed());
    *r.status_mut() = status;
    r.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if let Some(wait) = retry_after {
        let secs = wait.as_secs() + u64::from(wait.subsec_nanos() > 0);
        r.headers_mut().insert(header::RETRY_AFTER, HeaderValue::from(secs.max(1)));
    }
    r
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn turn(cell: u64, rollout: &str, ids: usize) -> Turn {
        Turn {
            cell: Some(CellId::from_bits(u128::from(cell))),
            rollout: rollout.into(),
            started: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(cell)),
            prompt_ids: vec![1; ids],
            ..Turn::default()
        }
    }

    #[test]
    fn a_route_is_a_plain_http_base() {
        let r = Route::new("http://10.0.0.5:30000/v1/", Some("sk-1")).unwrap();
        assert_eq!(
            r.uri("/chat/completions?x=1").unwrap().to_string(),
            "http://10.0.0.5:30000/v1/chat/completions?x=1"
        );
        assert!(r.key.unwrap().is_sensitive());
        let r = Route::new("http://e:8000", Some("")).unwrap();
        assert_eq!(r.uri("/v1/models").unwrap().to_string(), "http://e:8000/v1/models");
        assert!(r.key.is_none());
        assert!(Route::new("https://e:8000", None).unwrap_err().contains("plain HTTP"));
        assert!(Route::new("e:8000", None).is_err());
        assert!(Route::new("http://e:8000/?a=b", None).is_err());
        assert!(Route::new("http://e:8000", Some("a\nb")).is_err());
    }

    #[test]
    fn the_gateway_asks_for_token_ids_and_logprobs_only_when_the_cell_did_not() {
        let body = |v: Value| Bytes::from(v.to_string());
        let (b, ask) = Ask::prepare(Kind::Chat, body(json!({"model": "m", "messages": []})), true);
        let v: Value = serde_json::from_slice(&b).unwrap();
        assert_eq!(
            (v["return_token_ids"].clone(), v["logprobs"].clone()),
            (json!(true), json!(true))
        );
        assert_eq!(ask, Some(Ask { ids: true, logprobs: true }));
        let asked = json!({"prompt": "p", "return_token_ids": true, "logprobs": 2});
        let (b, ask) = Ask::prepare(Kind::Completions, body(asked.clone()), true);
        assert_eq!(serde_json::from_slice::<Value>(&b).unwrap(), asked);
        assert_eq!(ask, Some(Ask::default()));
        let (b, ask) = Ask::prepare(Kind::Completions, body(json!({"prompt": "p"})), true);
        assert_eq!(serde_json::from_slice::<Value>(&b).unwrap()["logprobs"], json!(0));
        assert_eq!(ask, Some(Ask { ids: true, logprobs: true }));
        let (_, ask) = Ask::prepare(Kind::Chat, body(json!({"messages": []})), false);
        assert_eq!(ask, Some(Ask { ids: true, logprobs: false }));
        let raw = Bytes::from_static(b"not json");
        assert_eq!(Ask::prepare(Kind::Chat, raw.clone(), true), (raw.clone(), None));
        assert_eq!(Ask::prepare(Kind::Other, raw.clone(), true), (raw, None));
        let m = body(json!({"model": "m", "messages": [], "max_tokens": 9}));
        assert_eq!(Ask::prepare(Kind::Messages, m.clone(), true), (m, Some(Ask::default())));
        assert_eq!(Kind::of("/v1/chat/completions"), Kind::Chat);
        assert_eq!(Kind::of("/v1/completions"), Kind::Completions);
        assert_eq!(Kind::of("/v1/messages"), Kind::Messages);
        assert_eq!(Kind::of("/v1/models"), Kind::Other);
    }

    #[test]
    fn a_whole_answer_gives_its_tokens_and_loses_what_was_added() {
        let mut v = json!({
            "model": "qwen", "prompt_token_ids": [1, 2, 3],
            "choices": [
                {"index": 0, "token_ids": [7, 8], "finish_reason": "stop",
                 "logprobs": {"content": [{"logprob": -0.5}, {"logprob": -0.25}]},
                 "message": {"content": "hi"}},
                {"index": 1, "token_ids": [9], "finish_reason": "length", "logprobs": null}
            ],
            "usage": {"prompt_tokens": 3, "completion_tokens": 3}
        });
        let mut c = Capture::default();
        c.take(&v);
        let mut t = Turn::default();
        c.into_turn(&mut t);
        assert_eq!((t.model.as_str(), t.prompt_ids.clone()), ("qwen", vec![1, 2, 3]));
        assert_eq!((t.prompt_tokens, t.completion_tokens), (3, 3));
        assert_eq!(t.choices[0].output_ids, [7, 8]);
        assert_eq!(t.choices[0].logprobs, [-0.5, -0.25]);
        assert_eq!(t.choices[1].output_ids, [9]);
        assert_eq!(t.choices[1].finish_reason, "length");
        Ask { ids: true, logprobs: true }.strip(&mut v);
        assert_eq!(v.get("prompt_token_ids"), None);
        assert_eq!(v["choices"][0].get("token_ids"), None);
        assert_eq!(v["choices"][0]["logprobs"], Value::Null);
        assert_eq!(v["choices"][0]["message"]["content"], "hi");
    }

    #[test]
    fn completions_with_several_prompts_keep_each_prompt() {
        let v = json!({"choices": [
            {"index": 0, "prompt_token_ids": [1, 2], "token_ids": [5],
             "logprobs": {"token_logprobs": [-1.0]}},
            {"index": 1, "prompt_token_ids": [3], "token_ids": [6],
             "logprobs": {"token_logprobs": [null]}}
        ]});
        let mut c = Capture::default();
        c.take(&v);
        let mut t = Turn::default();
        c.into_turn(&mut t);
        assert_eq!(t.prompt_ids, [1, 2]);
        assert!(t.choices[0].prompt_ids.is_empty());
        assert_eq!(t.choices[1].prompt_ids, [3]);
        assert_eq!(t.choices[0].logprobs, [-1.0]);
        assert!(t.choices[1].logprobs[0].is_nan());
    }

    #[test]
    fn a_stream_split_anywhere_reads_the_same_and_loses_what_was_added() {
        let events = [
            json!({"model": "m", "prompt_token_ids": [1, 2], "choices": [{"index": 0, "delta": {"role": "assistant"}, "token_ids": []}]}),
            json!({"choices": [{"index": 0, "delta": {"content": "a"}, "token_ids": [5], "logprobs": {"content": [{"logprob": -0.5}]}}]}),
            json!({"choices": [{"index": 0, "delta": {"content": "b"}, "token_ids": [6, 7], "finish_reason": "stop"}]}),
            json!({"choices": [], "usage": {"prompt_tokens": 2, "completion_tokens": 3}}),
        ];
        let mut stream = String::new();
        for e in &events {
            stream += &format!("data: {e}\n\n");
        }
        stream += "data: [DONE]\n\n";
        for (ask, crlf) in [(Ask { ids: true, logprobs: true }, false), (Ask::default(), true)] {
            let stream = if crlf { stream.replace('\n', "\r\n") } else { stream.clone() };
            for size in [1, 7, 64, stream.len()] {
                let mut sse = Sse::new(ask.strips());
                let mut cap = Capture::default();
                let mut out = Vec::new();
                for piece in stream.as_bytes().chunks(size) {
                    out.extend_from_slice(&sse.feed(Bytes::copy_from_slice(piece), ask, &mut cap));
                }
                out.extend_from_slice(&sse.flush(ask, &mut cap));
                let mut t = Turn::default();
                cap.into_turn(&mut t);
                assert_eq!(t.prompt_ids, [1, 2], "{size}");
                assert_eq!(t.choices.len(), 1);
                assert_eq!(t.choices[0].output_ids, [5, 6, 7]);
                assert_eq!(t.choices[0].logprobs, [-0.5]);
                assert_eq!(t.choices[0].finish_reason, "stop");
                assert_eq!((t.prompt_tokens, t.completion_tokens), (2, 3));
                let out = String::from_utf8(out).unwrap();
                if !ask.strips() {
                    assert_eq!(out, stream);
                    continue;
                }
                assert!(!out.contains("token_ids"), "{out}");
                assert!(out.contains(r#""content":"b""#), "{out}");
                assert!(out.ends_with("data: [DONE]\n\n"), "{out}");
                assert_eq!(out.matches("\n\n").count(), 5);
            }
        }
    }

    #[test]
    fn anthropic_answers_give_their_counts() {
        let mut c = Capture::default();
        c.take(&json!({"type": "message_start", "message": {"model": "m", "usage": {"input_tokens": 11}}}));
        c.take(&json!({"type": "message_delta", "usage": {"output_tokens": 4}}));
        let mut t = Turn::default();
        c.into_turn(&mut t);
        assert_eq!((t.prompt_tokens, t.completion_tokens), (11, 4));
    }

    #[test]
    fn turns_are_kept_by_rollout_and_the_stalest_rollout_goes_first() {
        let mut t = Turns::default();
        let one = turn(1, "r1", 10).size();
        let keep = one * 5 + GROUP * 4;
        t.add("p", "r1".into(), turn(1, "r1", 10), keep);
        t.add("p", "r1".into(), turn(2, "r1", 10), keep);
        t.add("p", "r2".into(), turn(3, "r2", 10), keep);
        t.add("q", "r1".into(), turn(4, "r1", 10), keep);
        let got = t.get("p", "r1", None, false);
        assert_eq!(got.iter().map(|x| x.seq).collect::<Vec<_>>(), [0, 1]);
        let cell2 = Some(CellId::from_bits(2));
        assert_eq!(t.get("p", "r1", cell2, false).len(), 1);
        assert_eq!(t.get("p", "", cell2, false).len(), 1);
        assert_eq!(t.get("q", "r2", None, false).len(), 0);
        // r1 in p is the stalest once r2 and q's r1 have newer turns, so it goes first.
        t.add("p", "r2".into(), turn(5, "r2", 10), keep);
        t.add("p", "r3".into(), turn(6, "r3", 10), keep);
        assert!(t.get("p", "r1", None, false).is_empty());
        assert_eq!(t.dropped.get("p"), Some(&2));
        assert_eq!(t.bytes, one * 4 + GROUP * 3);
        let taken = t.get("p", "r2", None, true);
        assert_eq!(taken.iter().map(|x| x.seq).collect::<Vec<_>>(), [0, 1]);
        assert!(t.get("p", "r2", None, false).is_empty());
        assert_eq!(t.bytes, one * 2 + GROUP * 3);
        // The rollout's numbering goes on after the trainer took its turns.
        t.add("p", "r2".into(), turn(8, "r2", 10), keep);
        assert_eq!(t.get("p", "r2", None, true)[0].seq, 2);
        // Taking one cell's turns leaves the rest of the rollout.
        t.add("p", "r3".into(), turn(7, "r3", 10), keep);
        let seven = Some(CellId::from_bits(7));
        assert_eq!(t.get("p", "r3", seven, true).len(), 1);
        assert_eq!(t.get("p", "r3", None, false).len(), 1);
        assert_eq!(t.bytes, one * 2 + GROUP * 3);
    }
}
