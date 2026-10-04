//! The combs the gate can reach, from scout's snapshot, and one connection to each.
//!
//! With a keeper, the gate also reads every node's lease from it once a second. Scout shows a
//! node that stopped reporting as down within seconds, but only the keeper knows when its lease
//! ran out, which is when its comb has stopped its cells and they are lost.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::Duration;

use hive_proto::internal as pb;
use hive_scout::Snapshot;
use hive_types::{CellId, Error, Reason};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint};

/// How long a connection to a comb may take before the call fails.
const CONNECT: Duration = Duration::from_secs(2);

/// A channel to each node, with the address it was made for.
type Channels = HashMap<u16, (Arc<str>, Channel)>;

/// A node's lease as the keeper last told it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Lease {
    /// The epoch the node runs in.
    pub epoch: u16,
    /// Whether the lease had run out.
    pub lost: bool,
}

/// The nodes scout knows and a channel to each. Cloning it is cheap.
#[derive(Clone, Debug)]
pub struct Nodes {
    snap: watch::Receiver<Arc<Snapshot>>,
    channels: Arc<Mutex<Channels>>,
    leases: Arc<RwLock<Arc<HashMap<u16, Lease>>>>,
}

impl Nodes {
    /// Nodes as `snap` holds them, always the latest.
    #[must_use]
    pub fn new(snap: watch::Receiver<Arc<Snapshot>>) -> Self {
        Self { snap, channels: Arc::default(), leases: Arc::default() }
    }

    /// Swaps in the leases the keeper holds, by node.
    pub fn set_leases(&self, leases: HashMap<u16, Lease>) {
        *self.leases.write().unwrap_or_else(PoisonError::into_inner) = Arc::new(leases);
    }

    fn lease(&self, node: u16) -> Option<Lease> {
        self.leases.read().unwrap_or_else(PoisonError::into_inner).get(&node).copied()
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

    /// The channel to the node that owns `id`. A cell made in an older epoch of its node is
    /// lost, as the comb registered again since and no longer has it, and so is one on a node
    /// whose lease ran out, as its comb stopped its cells when it saw that. A cell on a node
    /// neither scout nor the keeper knows is not found.
    ///
    /// # Errors
    ///
    /// As [`Nodes::channel`], with `CELL_LOST` for a stale epoch or a lost lease, and the node
    /// unknown reported as the cell not found.
    pub fn owner(&self, id: CellId) -> Result<Channel, Error> {
        let (node, epoch) = (id.node(), id.epoch());
        let lease = self.lease(node);
        let newest = self.snapshot().epoch(node).max(lease.map(|l| l.epoch));
        if newest.is_some_and(|e| epoch < e) {
            return Err(Error::new(
                Reason::CellLost,
                format!("cell {id} is from epoch {epoch} of node {node}, which is gone"),
            ));
        }
        if lease.is_some_and(|l| l.lost && l.epoch == epoch) {
            return Err(Error::new(
                Reason::CellLost,
                format!("cell {id} was on node {node}, whose lease ran out"),
            ));
        }
        self.find(node).unwrap_or_else(|| match lease {
            Some(_) => Err(Error::new(
                Reason::DroneUnreachable,
                format!("node {node} of cell {id} holds its lease but does not report to scout"),
            )),
            None => Err(Error::new(Reason::CellNotFound, format!("cell {id} not found"))),
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

/// Reads every node's lease from the keeper at `members` every [`crate::keys::EVERY`], one
/// member at a time and the next when one fails, and puts them in `nodes`, until `stop`. A
/// keeper that cannot be reached leaves the last leases in place.
///
/// # Errors
///
/// A member address does not parse.
pub fn follow(nodes: Nodes, members: &[String], stop: CancellationToken) -> Result<(), String> {
    let mut clients = crate::keys::clients(members)?;
    tokio::spawn(async move {
        let mut at = 0;
        let mut ok = None;
        let mut tick = tokio::time::interval(crate::keys::EVERY);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = stop.cancelled() => return,
                _ = tick.tick() => {}
            }
            let n = clients.len();
            let (member, client) = &mut clients[at];
            match client.list_nodes(pb::ListNodesRequest {}).await {
                Ok(r) => {
                    nodes.set_leases(leases(&r.into_inner().nodes));
                    ok = Some(true);
                }
                Err(e) => {
                    // Only a change from answers to failures is logged, not every second.
                    if ok != Some(false) {
                        eprintln!(
                            "hive-gate: reading leases from the keeper at {member}: {}, keeping the last",
                            e.message()
                        );
                        ok = Some(false);
                    }
                    at = (at + 1) % n;
                }
            }
        }
    });
    Ok(())
}

/// The leases in the keeper's answer, by node.
#[must_use]
pub fn leases(records: &[pb::NodeRecord]) -> HashMap<u16, Lease> {
    records
        .iter()
        .filter_map(|r| {
            let node = u16::try_from(r.node).ok()?;
            Some((node, Lease { epoch: u16::try_from(r.epoch).ok()?, lost: r.lost }))
        })
        .collect()
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

#[cfg(test)]
mod tests {
    use super::*;

    fn nodes() -> Nodes {
        let (_tx, rx) = watch::channel(Arc::new(Snapshot::default()));
        Nodes::new(rx)
    }

    fn reason(nodes: &Nodes, node: u16, epoch: u16) -> Reason {
        let id = CellId::new(1, node, epoch, 7, 9).unwrap();
        nodes.owner(id).unwrap_err().reason
    }

    #[test]
    fn a_cell_on_a_node_whose_lease_ran_out_is_lost() {
        let nodes = nodes();
        // Without a keeper, a node scout does not know is all the gate has to go on.
        assert_eq!(reason(&nodes, 2, 1), Reason::CellNotFound);

        let records = [
            pb::NodeRecord { node: 2, epoch: 1, lost: true, ..Default::default() },
            pb::NodeRecord { node: 3, epoch: 4, lost: false, ..Default::default() },
        ];
        nodes.set_leases(leases(&records));
        assert_eq!(reason(&nodes, 2, 1), Reason::CellLost);
        assert_eq!(reason(&nodes, 3, 3), Reason::CellLost);
        // The lease is live, so the cell may well be running, just out of the gate's sight.
        assert_eq!(reason(&nodes, 3, 4), Reason::DroneUnreachable);
        assert_eq!(reason(&nodes, 5, 1), Reason::CellNotFound);
    }
}
