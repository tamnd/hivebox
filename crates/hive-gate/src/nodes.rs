//! The combs the gate can reach, from scout's snapshot, and one connection to each.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use hive_scout::Snapshot;
use hive_types::{CellId, Error, Reason};
use tokio::sync::watch;
use tonic::transport::{Channel, Endpoint};

/// How long a connection to a comb may take before the call fails.
const CONNECT: Duration = Duration::from_secs(2);

/// A channel to each node, with the address it was made for.
type Channels = HashMap<u16, (Arc<str>, Channel)>;

/// The nodes scout knows and a channel to each. Cloning it is cheap.
#[derive(Clone, Debug)]
pub struct Nodes {
    snap: watch::Receiver<Arc<Snapshot>>,
    channels: Arc<Mutex<Channels>>,
}

impl Nodes {
    /// Nodes as `snap` holds them, always the latest.
    #[must_use]
    pub fn new(snap: watch::Receiver<Arc<Snapshot>>) -> Self {
        Self { snap, channels: Arc::default() }
    }

    /// The latest snapshot.
    #[must_use]
    pub fn snapshot(&self) -> Arc<Snapshot> {
        self.snap.borrow().clone()
    }

    /// A receiver that sees each new snapshot.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<Arc<Snapshot>> {
        self.snap.clone()
    }

    /// The channel to `node`. It connects on first use and reconnects by itself, and is made
    /// again when the node comes back at another address.
    ///
    /// # Errors
    ///
    /// Scout does not know the node, or its address is not one the gate can dial.
    pub fn channel(&self, node: u16) -> Result<Channel, Error> {
        self.find(node).unwrap_or_else(|| {
            Err(Error::new(Reason::DroneUnreachable, format!("node {node} is not known")))
        })
    }

    /// The channel to the node that owns `id`. A cell whose node scout does not know is not
    /// found, since as far as anyone can tell it is gone.
    ///
    /// # Errors
    ///
    /// As [`Nodes::channel`], with the node unknown reported as the cell not found.
    pub fn owner(&self, id: CellId) -> Result<Channel, Error> {
        self.find(id.node()).unwrap_or_else(|| {
            Err(Error::new(Reason::CellNotFound, format!("cell {id} not found")))
        })
    }

    fn find(&self, node: u16) -> Option<Result<Channel, Error>> {
        let snap = self.snapshot();
        let at = snap.addrs.binary_search_by_key(&node, |(n, _)| *n).ok()?;
        let addr = &snap.addrs[at].1;
        let mut channels = self.channels.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((a, c)) = channels.get(&node)
            && a == addr
        {
            return Some(Ok(c.clone()));
        }
        Some(
            dial(addr)
                .inspect(|c| {
                    channels.insert(node, (addr.clone(), c.clone()));
                })
                .map_err(|e| {
                    Error::new(Reason::DroneUnreachable, format!("node {node} at {addr}: {e}"))
                }),
        )
    }

    /// Every node scout has an address for, in node order.
    #[must_use]
    pub fn all(&self) -> Vec<u16> {
        self.snapshot().addrs.iter().map(|(n, _)| *n).collect()
    }
}

/// A lazy channel to a comb advertised as `http://HOST:PORT` or `unix:PATH`.
fn dial(addr: &str) -> Result<Channel, String> {
    if let Some(path) = addr.strip_prefix("unix:") {
        let path = Arc::<std::path::Path>::from(std::path::Path::new(path));
        let connector = tower::service_fn(move |_: tonic::transport::Uri| {
            let path = path.clone();
            async move {
                let s = tokio::net::UnixStream::connect(&*path).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(s))
            }
        });
        // The URI is not used to connect, but a channel needs one.
        let e = Endpoint::from_static("http://comb").connect_timeout(CONNECT);
        return Ok(e.connect_with_connector_lazy(connector));
    }
    let e = Endpoint::from_shared(addr.to_owned()).map_err(|e| e.to_string())?;
    Ok(e.connect_timeout(CONNECT)
        .tcp_nodelay(true)
        .http2_keep_alive_interval(Duration::from_secs(20))
        .keep_alive_while_idle(true)
        .connect_lazy())
}
