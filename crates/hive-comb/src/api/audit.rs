//! What the API tells the audit log: an event for each call, with who made it, the project, the
//! cell, a digest of what was asked and how it went, from `spec/10_security.md`, section 7.

use super::{Api, invalid, project};
use crate::comb::Comb;
use hive_telemetry::AuditEvent;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::{Request, Status};

/// The header that says who made a call, which a gate sets from the caller's key or token.
pub const PRINCIPAL_HEADER: &str = "x-hive-principal";
/// Who made a call that came with no principal, which is a caller on the comb's own socket.
pub const DEFAULT_PRINCIPAL: &str = "local";
/// The longest principal the log takes.
const MAX_PRINCIPAL: usize = 128;

/// One call on its way to the audit log.
#[derive(Clone, Debug)]
pub(super) struct Call {
    comb: Comb,
    pub(super) project: String,
    principal: String,
    trace: String,
    op: &'static str,
    /// When the call came in, in nanoseconds since the Unix epoch.
    ts: u64,
}

impl Call {
    /// `req` as call `op`, once its project and principal check out.
    pub(super) fn new<T>(api: &Api, req: &Request<T>, op: &'static str) -> Result<Self, Status> {
        let project = project(req)?;
        let principal = match req.metadata().get(PRINCIPAL_HEADER) {
            None => DEFAULT_PRINCIPAL.to_string(),
            Some(v) => match v.to_str() {
                Ok(p) if !p.is_empty() && p.len() <= MAX_PRINCIPAL => p.to_string(),
                _ => {
                    return Err(invalid(format!(
                        "{PRINCIPAL_HEADER} is empty or longer than {MAX_PRINCIPAL} characters"
                    )));
                }
            },
        };
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_nanos() as u64);
        Ok(Self { comb: api.comb.clone(), project, principal, trace: trace(req), op, ts })
    }

    /// Records the call as having touched `cell` with arguments `args` and come to `result`.
    pub(super) fn record(&self, cell: &str, args: &Args, result: String) {
        let Some(log) = self.comb.audit() else { return };
        log.record(AuditEvent {
            ts: self.ts,
            principal: self.principal.clone(),
            project: self.project.clone(),
            cell: cell.to_string(),
            op: self.op.to_string(),
            args: args.digest(),
            result,
            trace: self.trace.clone(),
        });
    }

    /// Records `r` if it is an error, and passes it on.
    pub(super) fn check<R>(
        &self,
        cell: &str,
        args: &Args,
        r: Result<R, Status>,
    ) -> Result<R, Status> {
        if let Err(e) = &r {
            self.record(cell, args, failed(e));
        }
        r
    }

    /// Records the call with `ok` or the code of the error it got.
    pub(super) fn done<R>(&self, cell: &str, args: &Args, r: &Result<R, Status>) {
        self.record(cell, args, r.as_ref().map_or_else(failed, |_| "ok".to_string()));
    }
}

/// How a call that failed is recorded: the gRPC code, such as `NotFound`.
pub(super) fn failed(s: &Status) -> String {
    format!("{:?}", s.code())
}

/// The trace id of a call from its W3C `traceparent` header, or empty.
fn trace<T>(req: &Request<T>) -> String {
    let parent = req.metadata().get("traceparent").and_then(|v| v.to_str().ok());
    match parent.and_then(|p| p.split('-').nth(1)) {
        Some(id) if id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit()) => id.to_string(),
        _ => String::new(),
    }
}

/// The arguments of a call, hashed as they come. Each one goes in with its length in front, so
/// two different lists of arguments never hash alike by running into each other.
#[derive(Clone, Debug, Default)]
pub(super) struct Args(blake3::Hasher);

impl Args {
    pub(super) fn bytes(mut self, b: &[u8]) -> Self {
        self.0.update(&(b.len() as u64).to_le_bytes());
        self.0.update(b);
        self
    }

    pub(super) fn str(self, s: &str) -> Self {
        self.bytes(s.as_bytes())
    }

    pub(super) fn num(self, n: u64) -> Self {
        self.bytes(&n.to_le_bytes())
    }

    pub(super) fn strs(self, list: &[String]) -> Self {
        list.iter().fold(self.num(list.len() as u64), |a, s| a.str(s))
    }

    /// A map, in key order, since the wire's maps come in any order.
    pub(super) fn map<'a>(self, m: impl IntoIterator<Item = (&'a String, &'a String)>) -> Self {
        let mut pairs: Vec<_> = m.into_iter().collect();
        pairs.sort_unstable();
        let a = self.num(pairs.len() as u64);
        pairs.into_iter().fold(a, |a, (k, v)| a.str(k).str(v))
    }

    fn digest(&self) -> String {
        self.0.finalize().to_hex().to_string()
    }
}
