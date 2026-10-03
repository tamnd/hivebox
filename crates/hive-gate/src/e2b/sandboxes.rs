//! The E2B REST API: sandboxes made, found, listed, paused, resumed and ended, each through the
//! gate's own Cells service, so placement and quotas are the same as for any create.

use std::collections::HashMap;
use std::time::Duration;

use futures::StreamExt;
use hive_proto::v1::cells_server::Cells;
use hive_proto::{convert, v1};
use serde::Deserialize;
use serde_json::{Value, json};
use tonic::Status;
use tonic::body::Body;
use tonic::codegen::http::{self, Method, StatusCode};

use super::{CLIENT_ID, Caller, ENVD_VERSION, empty, from_status, json_answer, query, status_of};
use crate::{MAX_REQUEST, connect};

/// The label that keeps the template a sandbox was made from.
const TEMPLATE_LABEL: &str = "e2b/template";

/// How long a sandbox lives when create does not say, the same as E2B's.
const DEFAULT_TIMEOUT: u64 = 15;

/// Sandboxes in a list page when the caller asks for no size.
const PAGE: u32 = 100;

pub(super) async fn serve(c: &Caller, req: http::Request<Body>) -> http::Response<Body> {
    let path = req.uri().path().to_owned();
    let method = req.method().clone();
    let parts: Vec<&str> = path.trim_start_matches("/v2").trim_matches('/').split('/').collect();
    let done = match (&method, parts.as_slice()) {
        (&Method::POST, ["sandboxes"]) => create(c, req).await,
        (&Method::GET, ["sandboxes"]) => list(c, req.uri(), path.starts_with("/v2")).await,
        (&Method::GET, ["sandboxes", id]) => {
            get(c, id).await.map(|cell| json_answer(StatusCode::OK, &detail(c, &cell)))
        }
        (&Method::DELETE, ["sandboxes", id]) => kill(c, id).await,
        (&Method::POST, ["sandboxes", id, "timeout"]) => timeout(c, id, req).await,
        (&Method::POST, ["sandboxes", id, "refreshes"]) => refresh(c, id, req).await,
        (&Method::POST, ["sandboxes", id, "pause"]) => pause(c, id).await,
        (&Method::POST, ["sandboxes", id, "resume"]) => resume(c, id, req, false).await,
        (&Method::POST, ["sandboxes", id, "connect"]) => resume(c, id, req, true).await,
        _ => Err(Status::not_found(format!("{method} {path} is not served"))),
    };
    done.unwrap_or_else(|s| from_status(&s))
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NewSandbox {
    #[serde(rename = "templateID", default)]
    template_id: String,
    timeout: Option<u64>,
    metadata: Option<HashMap<String, String>>,
    env_vars: Option<HashMap<String, String>>,
}

#[derive(Default, Deserialize)]
struct Lifetime {
    timeout: Option<u64>,
    duration: Option<u64>,
}

/// The body of `req` as JSON, or the default when it has none.
async fn body<T: Default + for<'a> Deserialize<'a>>(req: http::Request<Body>) -> Result<T, Status> {
    let (bytes, _) = connect::collect(req.into_body(), MAX_REQUEST).await?;
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(T::default());
    }
    serde_json::from_slice(&bytes)
        .map_err(|e| Status::invalid_argument(format!("the body is not what E2B sends: {e}")))
}

async fn create(c: &Caller, req: http::Request<Body>) -> Result<http::Response<Body>, Status> {
    let new: NewSandbox = body(req).await?;
    let cfg = &c.e2b.cfg;
    let metadata = new.metadata.unwrap_or_default();
    let image = cfg
        .image_key
        .as_ref()
        .and_then(|k| metadata.get(k))
        .or_else(|| cfg.templates.get(&new.template_id))
        .ok_or_else(|| {
            let key = cfg
                .image_key
                .as_deref()
                .map(|k| format!(" and no {k} in the metadata"))
                .unwrap_or_default();
            Status::invalid_argument(format!(
                "there is no image for template {:?}{key}",
                new.template_id
            ))
        })?;
    let resources = match metadata.get(&cfg.size_key()) {
        Some(name) if !cfg.sizes.is_empty() => {
            let s = cfg.sizes.get(name).ok_or_else(|| {
                let mut names: Vec<&str> = cfg.sizes.keys().map(String::as_str).collect();
                names.sort_unstable();
                Status::invalid_argument(format!(
                    "there is no size {name:?}, only {}",
                    names.join(", ")
                ))
            })?;
            Some(v1::Resources {
                vcpu_milli: s.vcpu_milli,
                mem_mib: s.mem_mib,
                disk_gib: s.disk_gib,
                ..Default::default()
            })
        }
        _ => None,
    };
    let mut labels = metadata.clone();
    if !new.template_id.is_empty() {
        labels.insert(TEMPLATE_LABEL.into(), new.template_id.clone());
    }
    let spec = v1::CellSpec {
        source: Some(v1::cell_spec::Source::Image(v1::ImageRef { r#ref: image.clone() })),
        backend: convert::backend_to_v1(cfg.backend).into(),
        resources,
        hard_ttl: Some(seconds(new.timeout.unwrap_or(DEFAULT_TIMEOUT))),
        labels,
        env: new.env_vars.unwrap_or_default(),
        ..Default::default()
    };
    let msg = v1::CreateRequest { spec: Some(spec), count: 1, ..Default::default() };
    let mut events = c.gate.api.create(c.req(msg)).await?.into_inner();
    let event =
        events.next().await.ok_or_else(|| Status::internal("create answered nothing"))??;
    match event.result {
        Some(v1::create_event::Result::Cell(cell)) => {
            Ok(json_answer(StatusCode::CREATED, &sandbox(c, &cell)))
        }
        Some(v1::create_event::Result::Error(e)) => Err(status_of(&e)),
        None => Err(Status::internal("create answered with neither a cell nor an error")),
    }
}

/// A live sandbox, as get finds it. A cell that has ended is no sandbox at all to E2B.
async fn get(c: &Caller, id: &str) -> Result<v1::Cell, Status> {
    let msg = v1::GetCellRequest { id: id.into() };
    let cell = c.gate.api.get(c.req(msg)).await.map_err(gone)?.into_inner();
    if state(&cell).is_none() {
        return Err(Status::not_found(format!("sandbox {id} has ended")));
    }
    Ok(cell)
}

async fn list(c: &Caller, uri: &http::Uri, v2: bool) -> Result<http::Response<Body>, Status> {
    let q = query(uri);
    let mut labels = HashMap::new();
    if let Some(m) = q.get("metadata") {
        for pair in m.split('&').filter(|p| !p.is_empty()) {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            labels.insert(super::unescape(k), super::unescape(v));
        }
    }
    let mut states = vec![v1::CellState::Running as i32];
    if v2 {
        states = match q.get("state") {
            None => vec![v1::CellState::Running as i32, v1::CellState::Paused as i32],
            Some(s) => s
                .split(',')
                .map(|s| match s {
                    "running" => Ok(v1::CellState::Running as i32),
                    "paused" => Ok(v1::CellState::Paused as i32),
                    _ => Err(Status::invalid_argument(format!("there is no state {s:?}"))),
                })
                .collect::<Result<_, _>>()?,
        };
    }
    let page_size = q.get("limit").and_then(|l| l.parse().ok()).unwrap_or(PAGE);
    let msg = v1::ListCellsRequest {
        selector: (!labels.is_empty()).then_some(v1::LabelSelector { r#match: labels }),
        states,
        page_size,
        page_token: q.get("nextToken").cloned().unwrap_or_default(),
    };
    let page = c.gate.api.list(c.req(msg)).await?.into_inner();
    let all: Vec<Value> = page
        .cells
        .iter()
        .filter(|cell| state(cell).is_some())
        .map(|cell| detail(c, cell))
        .collect();
    let mut resp = json_answer(StatusCode::OK, &Value::Array(all));
    if let Ok(v) = http::HeaderValue::from_str(&page.next_page_token)
        && !page.next_page_token.is_empty()
    {
        resp.headers_mut().insert("x-next-token", v);
    }
    Ok(resp)
}

async fn kill(c: &Caller, id: &str) -> Result<http::Response<Body>, Status> {
    let msg = v1::StopRequest { selector: Some(by_id(id)), snapshot: false };
    let r = c.gate.api.stop(c.req(msg)).await.map_err(gone)?.into_inner();
    bulk(id, &r)?;
    Ok(empty(StatusCode::NO_CONTENT))
}

async fn pause(c: &Caller, id: &str) -> Result<http::Response<Body>, Status> {
    let cell = get(c, id).await?;
    if state(&cell) == Some("paused") {
        return Err(Status::already_exists(format!("sandbox {id} is paused already")));
    }
    let r = c.gate.api.pause(c.req(by_id(id))).await.map_err(gone)?.into_inner();
    bulk(id, &r)?;
    Ok(empty(StatusCode::NO_CONTENT))
}

/// Resume, and connect, which is resume that is fine with a running sandbox. Both may set a
/// new timeout, and answer with the sandbox.
async fn resume(
    c: &Caller,
    id: &str,
    req: http::Request<Body>,
    connect: bool,
) -> Result<http::Response<Body>, Status> {
    let life: Lifetime = body(req).await?;
    let mut cell = get(c, id).await?;
    let paused = state(&cell) == Some("paused");
    if !paused && !connect {
        return Err(Status::already_exists(format!("sandbox {id} is not paused")));
    }
    if paused {
        let r = c.gate.api.resume(c.req(by_id(id))).await.map_err(gone)?.into_inner();
        bulk(id, &r)?;
    }
    if let Some(t) = life.timeout {
        cell = extend(c, id, t).await?;
    }
    let status = if paused { StatusCode::CREATED } else { StatusCode::OK };
    Ok(json_answer(status, &sandbox(c, &cell)))
}

async fn timeout(
    c: &Caller,
    id: &str,
    req: http::Request<Body>,
) -> Result<http::Response<Body>, Status> {
    let life: Lifetime = body(req).await?;
    let t = life.timeout.ok_or_else(|| Status::invalid_argument("timeout is needed"))?;
    extend(c, id, t).await?;
    Ok(empty(StatusCode::NO_CONTENT))
}

/// Keeps the sandbox for at least `duration` more seconds, never cutting its time short.
async fn refresh(
    c: &Caller,
    id: &str,
    req: http::Request<Body>,
) -> Result<http::Response<Body>, Status> {
    let life: Lifetime = body(req).await?;
    let want = life.duration.unwrap_or(60);
    let cell = get(c, id).await?;
    let now = std::time::SystemTime::now();
    let left = cell
        .expires_at
        .and_then(|t| std::time::SystemTime::try_from(t).ok())
        .and_then(|end| end.duration_since(now).ok());
    if left.is_some_and(|left| left >= Duration::from_secs(want)) {
        return Ok(empty(StatusCode::NO_CONTENT));
    }
    extend(c, id, want).await?;
    Ok(empty(StatusCode::NO_CONTENT))
}

async fn extend(c: &Caller, id: &str, secs: u64) -> Result<v1::Cell, Status> {
    let msg = v1::ExtendTtlRequest { id: id.into(), hard_ttl: Some(seconds(secs)), idle_ttl: None };
    Ok(c.gate.api.extend_ttl(c.req(msg)).await.map_err(gone)?.into_inner())
}

/// What create, resume and connect answer with.
fn sandbox(c: &Caller, cell: &v1::Cell) -> Value {
    json!({
        "templateID": template(cell),
        "sandboxID": cell.id,
        "clientID": CLIENT_ID,
        "envdVersion": ENVD_VERSION,
        "envdAccessToken": c.secret,
        "domain": Value::Null,
    })
}

/// What get and list answer with.
fn detail(c: &Caller, cell: &v1::Cell) -> Value {
    let spec = cell.spec.clone().unwrap_or_default();
    let r = spec.resources.unwrap_or_default();
    let started = time(cell.created_at.as_ref());
    let mut metadata = spec.labels;
    metadata.remove(TEMPLATE_LABEL);
    json!({
        "templateID": template(cell),
        "sandboxID": cell.id,
        "clientID": CLIENT_ID,
        "envdVersion": ENVD_VERSION,
        "envdAccessToken": c.secret,
        "startedAt": started,
        "endAt": cell.expires_at.as_ref().map_or_else(|| started.clone(), |t| time(Some(t))),
        "cpuCount": r.vcpu_milli.div_ceil(1000).max(1),
        "memoryMB": r.mem_mib,
        "diskSizeMB": r.disk_gib * 1024,
        "metadata": metadata,
        "state": state(cell).unwrap_or("running"),
    })
}

fn template(cell: &v1::Cell) -> String {
    let labels = cell.spec.as_ref().map(|s| &s.labels);
    labels.and_then(|l| l.get(TEMPLATE_LABEL)).cloned().unwrap_or_else(|| "base".into())
}

/// What E2B calls the cell's state, or nothing once it has ended.
fn state(cell: &v1::Cell) -> Option<&'static str> {
    use v1::CellState as S;
    match cell.state() {
        S::Pausing | S::Paused => Some("paused"),
        S::Unspecified | S::Pending | S::Preparing | S::Starting | S::Running => Some("running"),
        S::Stopping | S::Stopped | S::Failed | S::Expired => None,
    }
}

/// RFC 3339, the way E2B writes times.
fn time(t: Option<&prost_types::Timestamp>) -> String {
    t.cloned().unwrap_or_default().to_string()
}

fn seconds(secs: u64) -> prost_types::Duration {
    convert::duration_to_v1(Duration::from_secs(secs))
}

fn by_id(id: &str) -> v1::CellSelector {
    v1::CellSelector { by: Some(v1::cell_selector::By::Id(id.into())) }
}

/// A sandbox id that is not a cell id is a sandbox that is not there.
fn gone(s: Status) -> Status {
    if s.code() == tonic::Code::InvalidArgument && s.message().contains("is not a cell id") {
        return Status::not_found(s.message().to_owned());
    }
    s
}

/// The outcome of a bulk call about one sandbox.
fn bulk(id: &str, r: &v1::BulkResult) -> Result<(), Status> {
    if let Some(e) = r.failures.iter().find_map(|f| f.error.as_ref()) {
        return Err(status_of(e));
    }
    if r.matched == 0 {
        return Err(Status::not_found(format!("there is no sandbox {id}")));
    }
    Ok(())
}
