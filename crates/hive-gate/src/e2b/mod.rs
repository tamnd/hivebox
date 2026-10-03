//! The E2B API and the parts of envd the E2B SDK needs, served over the gate's own services.
//!
//! The SDK talks to two places: the E2B API, which makes and ends sandboxes, and envd, a daemon
//! in each sandbox that runs commands and moves files. With `E2B_API_URL` and `E2B_SANDBOX_URL`
//! both set to the gate, the SDK sends both here and names the sandbox of each envd call in the
//! `E2b-Sandbox-Id` header. A sandbox is a cell, and its id is the cell id.
//!
//! API calls carry the key in `X-API-Key`. envd calls carry only what create gave back as the
//! sandbox's `envdAccessToken`, so that is the key or token the sandbox was made with, and the
//! gate checks either one the way it checks a bearer key.
//!
//! Served: create, get, list, kill, pause, resume, connect, timeout and refreshes, and of envd
//! the health check, processes with their input and signals, reading and writing files, and
//! stat, list, make dir, move and remove. Not served: templates, snapshots, metrics, logs,
//! terminals, watching directories, and gzip uploads. Input to a process goes through the gate
//! that started it, since that gate holds the process's stream to its comb.

mod envd;
mod sandboxes;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use hive_proto::{convert, v1};
use hive_types::CellId;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tonic::Status;
use tonic::body::Body;
use tonic::codegen::http;
use tonic::transport::Channel;

use crate::{Credential, Gate, Grant, PROJECT_HEADER, config, connect};

/// The header that names the sandbox of an envd call.
const SANDBOX_HEADER: &str = "e2b-sandbox-id";

/// The envd version the gate answers as. The SDK turns features on by version, and this one has
/// octet stream uploads and closing a process's input, and not file metadata.
const ENVD_VERSION: &str = "0.5.7";

/// What E2B calls the client that made a sandbox. The SDK wants one and does nothing with it.
const CLIENT_ID: &str = "hivebox";

/// The E2B side of a gate.
#[derive(Debug)]
pub(crate) struct E2b {
    cfg: config::E2b,
    /// The input of each process started with its input open, by cell and pid.
    stdin: Mutex<Inputs>,
}

type Inputs = HashMap<(CellId, u32), mpsc::Sender<v1::ProcessInput>>;

impl E2b {
    pub(crate) fn new(cfg: config::E2b) -> Self {
        Self { cfg, stdin: Mutex::default() }
    }

    fn stdin(&self) -> std::sync::MutexGuard<'_, Inputs> {
        self.stdin.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Which side of E2B a call is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    /// The REST API that makes and ends sandboxes.
    Api,
    /// envd in one sandbox.
    Envd,
}

impl Kind {
    /// The side `req` is for, if it is an E2B call at all.
    pub(crate) fn of<B>(req: &http::Request<B>) -> Option<Self> {
        if req.headers().contains_key(SANDBOX_HEADER) {
            return Some(Self::Envd);
        }
        let path = req.uri().path();
        let api = ["/sandboxes", "/v2/sandboxes"].iter().any(|r| {
            path.strip_prefix(r).is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
        });
        api.then_some(Self::Api)
    }
}

/// Serves one E2B call.
pub(crate) async fn call(
    gate: Gate,
    e2b: Arc<E2b>,
    kind: Kind,
    req: http::Request<Body>,
) -> http::Response<Body> {
    let header = match kind {
        Kind::Api => "x-api-key",
        Kind::Envd => "x-access-token",
    };
    let secret = req.headers().get(header).and_then(|v| v.to_str().ok()).map(str::to_owned);
    let who = match &secret {
        Some(s) => gate.who(s),
        None => Err(format!("the {header} header is missing")),
    };
    let (project, credential) = match who {
        Ok(w) => w,
        Err(why) => {
            gate.calls.with(&["e2b", "denied"]).inc();
            return error(http::StatusCode::UNAUTHORIZED, &why);
        }
    };
    gate.calls.with(&["e2b", "ok"]).inc();
    let grant = match credential {
        Credential::Token(g) => Some(g),
        Credential::Key(_) => None,
    };
    let caller = Caller { gate, e2b, project, grant, secret: secret.unwrap_or_default() };
    match kind {
        Kind::Api => sandboxes::serve(&caller, req).await,
        Kind::Envd => envd::serve(&caller, req).await,
    }
}

/// Who made a call, and what it needs to make calls of its own.
struct Caller {
    gate: Gate,
    e2b: Arc<E2b>,
    project: Arc<str>,
    grant: Option<Grant>,
    /// The key or token the call came with, which create gives back as the envd token.
    secret: String,
}

impl Caller {
    /// `msg` as a call from this caller to the gate's services or to a comb.
    fn req<T>(&self, msg: T) -> tonic::Request<T> {
        let mut r = tonic::Request::new(msg);
        if let Ok(v) = self.project.parse() {
            r.metadata_mut().insert(PROJECT_HEADER, v);
        }
        if let Some(g) = &self.grant {
            r.extensions_mut().insert(g.clone());
        }
        r
    }

    /// The comb that owns sandbox `id`, once the caller may do `op` on it.
    fn comb(&self, id: &str, op: &str) -> Result<(CellId, Channel), Status> {
        let cell: CellId = id.parse().map_err(|_| Status::not_found(format!("no sandbox {id}")))?;
        Grant::check(self.grant.as_ref(), op, Some(&cell.to_string()))?;
        let channel = self.gate.nodes.owner(cell).map_err(|e| convert::error_to_status(&e))?;
        Ok((cell, channel))
    }
}

/// An E2B error: the HTTP status, and the code and message in JSON.
fn error(status: http::StatusCode, message: &str) -> http::Response<Body> {
    json_answer(status, &json!({ "code": status.as_u16(), "message": message }))
}

/// The E2B error for a failed call.
fn from_status(s: &Status) -> http::Response<Body> {
    error(connect::http_status(s.code()), s.message())
}

fn json_answer(status: http::StatusCode, v: &Value) -> http::Response<Body> {
    let body = serde_json::to_vec(v).unwrap_or_default();
    connect::answer(status, "application/json", body.into())
}

fn empty(status: http::StatusCode) -> http::Response<Body> {
    http::Response::builder().status(status).body(Body::empty()).unwrap_or_default()
}

/// The status for an error a comb sent as a message rather than as a status.
fn status_of(e: &v1::Error) -> Status {
    let reason = hive_types::Reason::from_name(&e.reason).unwrap_or(hive_types::Reason::Internal);
    convert::error_to_status(&hive_types::Error::new(reason, e.message.clone()))
}

/// The query string of `uri`, decoded. A name given twice keeps its last value.
fn query(uri: &http::Uri) -> HashMap<String, String> {
    uri.query()
        .unwrap_or_default()
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (unescape(k), unescape(v))
        })
        .collect()
}

/// `text` with `+` as a space and `%XX` as the byte it stands for.
fn unescape(text: &str) -> String {
    let b = text.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        let hex = |c: u8| char::from(c).to_digit(16);
        match b[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < b.len() => match (hex(b[i + 1]), hex(b[i + 2])) {
                (Some(h), Some(l)) => {
                    out.push((h * 16 + l) as u8);
                    i += 2;
                }
                _ => out.push(b'%'),
            },
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calls_are_told_apart() {
        let req = |path: &str, sandbox: bool| {
            let mut b = http::Request::builder().uri(path);
            if sandbox {
                b = b.header("E2b-Sandbox-Id", "x");
            }
            b.body(()).unwrap()
        };
        assert_eq!(Kind::of(&req("/sandboxes", false)), Some(Kind::Api));
        assert_eq!(Kind::of(&req("/v2/sandboxes?limit=3", false)), Some(Kind::Api));
        assert_eq!(Kind::of(&req("/sandboxes/abc/timeout", false)), Some(Kind::Api));
        assert_eq!(Kind::of(&req("/files?path=/a", true)), Some(Kind::Envd));
        assert_eq!(Kind::of(&req("/sandboxesx", false)), None);
        assert_eq!(Kind::of(&req("/hivebox.v1.Cells/Get", false)), None);
    }

    #[test]
    fn queries_are_decoded() {
        let uri: http::Uri = "/files?path=%2Ftmp%2Fa+b.txt&username=agent&x".parse().unwrap();
        let q = query(&uri);
        assert_eq!(q["path"], "/tmp/a b.txt");
        assert_eq!(q["username"], "agent");
        assert_eq!(q["x"], "");
        assert_eq!(unescape("100%"), "100%");
        assert_eq!(unescape("%zz%41"), "%zzA");
    }
}
