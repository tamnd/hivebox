//! The only component with a foot on both the trusted and the untrusted network. It checks the
//! caller's key, places batches of cells with waggle, and sends every call about one cell to the
//! comb that owns it, which it knows from the node in the cell id. A verify goes to the comb that
//! owns its subject, or where waggle would put the verifier cell when it has none. It takes gRPC
//! and Connect, the second over HTTP/1.1 as well, so `curl` can call it.
//!
//! The gate keeps no state of its own. It follows scout for the nodes and their addresses, so
//! any number of gates can run side by side, and one that restarts is serving again as soon as
//! scout has sent it the cluster. The design is in `spec/04_control_plane.md`, section 3.
//!
//! With an `[e2b]` table in its config, the gate also speaks enough of the E2B API and of envd,
//! the daemon in an E2B sandbox, for the E2B SDK to make cells and run commands in them.

#![forbid(unsafe_code)]

pub mod cells;
pub mod config;
mod connect;
mod e2b;
pub mod keys;
pub mod nodes;
pub mod proxy;
pub mod quota;
pub mod tokens;
mod verify;

use std::convert::Infallible;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use futures::{FutureExt, stream};
use hive_proto::v1::cells_server::CellsServer;
use hive_proto::v1::tokens_server::TokensServer;
use hive_proto::v1::verify_server::VerifyServer;
use hive_telemetry::{CounterVec, Registry};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tonic::body::Body;
use tonic::codegen::{BoxFuture, Service, http};

pub use config::Config;
pub use keys::Keys;
pub use nodes::Nodes;
pub use quota::Quotas;

/// The header that tells a comb which project a call is for. The gate sets it from the key and
/// drops whatever the caller put there.
pub const PROJECT_HEADER: &str = "x-hive-project";

/// The header that tells a comb to make a keyed cell if it has room even when it turned the
/// key away lately, which the gate sets once every node in the key's order turned it away.
pub const ANYWAY_HEADER: &str = "x-hive-anyway";

/// A token the call came with, which the services ask what it allows once they know the call's
/// cell. A call with an API key has none.
#[derive(Clone, Debug)]
pub struct Grant(pub Arc<hive_auth::Token>);

impl Grant {
    /// Whether the call's token, if it came with one, allows `op` on `cell`.
    ///
    /// # Errors
    ///
    /// `permission_denied`, saying why.
    pub fn check(grant: Option<&Self>, op: &str, cell: Option<&str>) -> Result<(), Status> {
        let Some(Self(t)) = grant else { return Ok(()) };
        t.allows(op, cell, std::time::SystemTime::now())
            .map_err(|e| Status::permission_denied(e.to_string()))
    }
}

/// The biggest request message, which is mostly stdin for a run. The same as a comb takes.
const MAX_REQUEST: usize = 64 << 20;

/// The gate's services, cheap to clone, one per connection.
#[derive(Clone, Debug)]
pub struct Gate {
    keys: Keys,
    nodes: Nodes,
    api: cells::Api,
    cells: CellsServer<cells::Api>,
    verify: VerifyServer<cells::Api>,
    tokens: Option<TokensServer<tokens::Api>>,
    e2b: Option<Arc<e2b::E2b>>,
    calls: CounterVec,
}

impl Gate {
    /// A gate that lets in `keys`, reaches the combs in `nodes` and holds creates to `quotas`
    /// if there are any, with its metrics in `registry`.
    #[must_use]
    pub fn new(
        keys: impl Into<Keys>,
        nodes: Nodes,
        quotas: Option<Quotas>,
        registry: &Registry,
    ) -> Self {
        let api = cells::Api::new(nodes.clone(), quotas, registry);
        Self {
            keys: keys.into(),
            nodes,
            cells: CellsServer::new(api.clone()).max_decoding_message_size(MAX_REQUEST),
            verify: VerifyServer::new(api.clone()).max_decoding_message_size(MAX_REQUEST),
            api,
            tokens: None,
            e2b: None,
            calls: registry.counter(
                "hive_gate_calls_total",
                "Calls the gate took, by method and whether the key was good.",
                &["op", "result"],
            ),
        }
    }

    /// The gate with the Tokens service, which asks the keeper to sign tokens.
    #[must_use]
    pub fn with_tokens(mut self, api: tokens::Api) -> Self {
        self.tokens = Some(TokensServer::new(api));
        self
    }

    /// The gate with the E2B API, making sandboxes into cells as `cfg` says.
    #[must_use]
    pub fn with_e2b(mut self, cfg: config::E2b) -> Self {
        self.e2b = Some(Arc::new(e2b::E2b::new(cfg)));
        self
    }

    /// Who the `authorization: Bearer KEY` header says the caller is: the project, and the
    /// token or the hash of the key it was.
    fn caller(&self, req: &http::Request<Body>) -> Result<(Arc<str>, Credential), String> {
        let missing = || "the key or token is missing".to_string();
        let value = req.headers().get(http::header::AUTHORIZATION).ok_or_else(missing)?;
        let bearer =
            value.to_str().ok().and_then(|v| v.strip_prefix("Bearer ")).ok_or_else(missing)?;
        self.who(bearer)
    }

    /// Who a key or a token says the caller is.
    fn who(&self, bearer: &str) -> Result<(Arc<str>, Credential), String> {
        if hive_auth::is_token(bearer) {
            let t = self.keys.token(bearer)?;
            return Ok((Arc::from(t.project.as_str()), Credential::Token(Grant(t))));
        }
        let hash = *blake3::hash(bearer.as_bytes()).as_bytes();
        let project = self.keys.project(&hash).ok_or("the key is not one the gate knows")?;
        Ok((project, Credential::Key(tokens::KeyHash(hash))))
    }
}

/// What a caller showed to get in.
enum Credential {
    Key(tokens::KeyHash),
    Token(Grant),
}

impl Service<http::Request<Body>> for Gate {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Infallible>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Infallible>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut req: http::Request<Body>) -> Self::Future {
        // Before Connect, which would take the E2B calls for its own, being JSON too.
        if let Some(e2b) = &self.e2b
            && let Some(kind) = e2b::Kind::of(&req)
        {
            return Box::pin(e2b::call(self.clone(), e2b.clone(), kind, req).map(Ok));
        }
        if let Some(codec) = connect::Codec::of(req.headers()) {
            return Box::pin(connect::call(self.clone(), codec, req).map(Ok));
        }
        let (to, op) = match req.uri().path().strip_prefix('/').and_then(|p| p.split_once('/')) {
            Some(("hivebox.v1.Cells", m)) => (To::Cells, m),
            Some(("hivebox.v1.Exec" | "hivebox.v1.Files", m)) => (To::Comb, m),
            Some(("hivebox.v1.Verify", m)) => (To::Verify, m),
            Some(("hivebox.v1.Tokens", m)) => (To::Tokens, m),
            _ => (To::Nowhere, "unknown"),
        };
        let (project, credential) = match self.caller(&req) {
            Ok(c) => c,
            Err(why) => {
                self.calls.with(&[op, "denied"]).inc();
                let status = Status::unauthenticated(why);
                return Box::pin(async move { Ok(status.into_http()) });
            }
        };
        self.calls.with(&[op, "ok"]).inc();
        match credential {
            Credential::Key(hash) => req.extensions_mut().insert(hash).map(drop),
            Credential::Token(grant) => req.extensions_mut().insert(grant).map(drop),
        };
        let headers = req.headers_mut();
        headers.remove(http::header::AUTHORIZATION);
        if let Ok(v) = http::HeaderValue::from_str(&project) {
            headers.insert(PROJECT_HEADER, v);
        }
        match to {
            To::Cells => Box::pin(self.cells.call(req)),
            To::Verify => Box::pin(self.verify.call(req)),
            To::Comb => {
                let nodes = self.nodes.clone();
                Box::pin(async move { Ok(proxy::forward(&nodes, req).await) })
            }
            To::Tokens => match &mut self.tokens {
                Some(t) => Box::pin(t.call(req)),
                None => {
                    let status = Status::unimplemented("this gate has no keeper to sign tokens");
                    Box::pin(async move { Ok(status.into_http()) })
                }
            },
            To::Nowhere => {
                let status = Status::unimplemented(format!("no service at {}", req.uri().path()));
                Box::pin(async move { Ok(status.into_http()) })
            }
        }
    }
}

/// Where a call goes.
enum To {
    /// The gate's own Cells service.
    Cells,
    /// Straight to the comb that owns the cell.
    Comb,
    /// The gate's Verify service.
    Verify,
    /// The gate's Tokens service.
    Tokens,
    Nowhere,
}

/// Serves `gate` on `listener` until `stop` is cancelled.
///
/// # Errors
///
/// The server fails.
pub async fn serve(gate: Gate, listener: TcpListener, stop: CancellationToken) -> io::Result<()> {
    let incoming = stream::unfold(listener, |l| async move {
        let conn = l.accept().await.map(|(s, _)| {
            // Small messages go out at once rather than waiting for more to fill a packet.
            let _ = s.set_nodelay(true);
            s
        });
        Some((conn, l))
    });
    tonic::transport::Server::builder()
        // Connect callers may come over HTTP/1.1.
        .accept_http1(true)
        .http2_keepalive_interval(Some(Duration::from_secs(20)))
        .serve_with_incoming_shutdown(gate, incoming, stop.cancelled_owned())
        .await
        .map_err(io::Error::other)
}
