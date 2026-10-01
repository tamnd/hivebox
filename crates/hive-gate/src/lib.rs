//! The only component with a foot on both the trusted and the untrusted network. It checks the
//! caller's key, places batches of cells with waggle, and sends every call about one cell to the
//! comb that owns it, which it knows from the node in the cell id. It takes gRPC and Connect, the
//! second over HTTP/1.1 as well, so `curl` can call it.
//!
//! The gate keeps no state of its own. It follows scout for the nodes and their addresses, so
//! any number of gates can run side by side, and one that restarts is serving again as soon as
//! scout has sent it the cluster. The design is in `spec/04_control_plane.md`, section 3.

#![forbid(unsafe_code)]

pub mod cells;
pub mod config;
mod connect;
pub mod keys;
pub mod nodes;
pub mod proxy;
pub mod quota;

use std::convert::Infallible;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use futures::{FutureExt, stream};
use hive_proto::v1::cells_server::CellsServer;
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

/// The biggest request message, which is mostly stdin for a run. The same as a comb takes.
const MAX_REQUEST: usize = 64 << 20;

/// The gate's services, cheap to clone, one per connection.
#[derive(Clone, Debug)]
pub struct Gate {
    keys: Keys,
    nodes: Nodes,
    cells: CellsServer<cells::Api>,
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
            cells: CellsServer::new(api).max_decoding_message_size(MAX_REQUEST),
            calls: registry.counter(
                "hive_gate_calls_total",
                "Calls the gate took, by method and whether the key was good.",
                &["op", "result"],
            ),
        }
    }

    /// The project the `authorization: Bearer KEY` header opens.
    fn project(&self, req: &http::Request<Body>) -> Option<Arc<str>> {
        let value = req.headers().get(http::header::AUTHORIZATION)?.to_str().ok()?;
        let key = value.strip_prefix("Bearer ")?;
        self.keys.project(blake3::hash(key.as_bytes()).as_bytes())
    }
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
        if let Some(codec) = connect::Codec::of(req.headers()) {
            return Box::pin(connect::call(self.clone(), codec, req).map(Ok));
        }
        let (to, op) = match req.uri().path().strip_prefix('/').and_then(|p| p.split_once('/')) {
            Some(("hivebox.v1.Cells", m)) => (To::Cells, m),
            Some(("hivebox.v1.Exec" | "hivebox.v1.Files", m)) => (To::Comb, m),
            _ => (To::Nowhere, "unknown"),
        };
        let Some(project) = self.project(&req) else {
            self.calls.with(&[op, "denied"]).inc();
            let status = Status::unauthenticated("the key is missing or not one the gate knows");
            return Box::pin(async move { Ok(status.into_http()) });
        };
        self.calls.with(&[op, "ok"]).inc();
        let headers = req.headers_mut();
        headers.remove(http::header::AUTHORIZATION);
        if let Ok(v) = http::HeaderValue::from_str(&project) {
            headers.insert(PROJECT_HEADER, v);
        }
        match to {
            To::Cells => Box::pin(self.cells.call(req)),
            To::Comb => {
                let nodes = self.nodes.clone();
                Box::pin(async move { Ok(proxy::forward(&nodes, req).await) })
            }
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
