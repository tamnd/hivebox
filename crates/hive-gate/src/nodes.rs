//! The combs the gate can reach, from scout's snapshot, and one connection to each.
//!
//! With a keeper, the gate also reads every node's lease from it once a second. Scout shows a
//! node that stopped reporting as down within seconds, but only the keeper knows when its lease
//! ran out, which is when its comb has stopped its cells and they are lost.
//!
//! A gate that knows which unit it serves sends a call about another unit's cell to that unit's
//! gate, which it knows from its config, and the unit is in the cell id like the node is.

use std::collections::{BTreeMap, HashMap};
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
    units: Arc<Units>,
}

/// The unit the gate serves and the gates of the others.
#[derive(Debug, Default)]
struct Units {
    /// `None` serves cells of every unit as this unit's, which is all a gate in front of a
    /// single unit needs.
    home: Option<u8>,
    /// A channel to the gate of each other unit, with its address.
    peers: HashMap<u8, (Arc<str>, Channel)>,
}

/// Where a call came from, which decides whether a cell of another unit may be sent on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    /// A caller the gate checked the key of. Its calls about other units' cells go to their gates.
    Caller,
    /// The gate of another unit, which only sends calls about this unit's cells. Nothing it sends
    /// is passed on, so a config that points two gates at each other cannot make a loop.
    Peer,
}

impl Nodes {
    /// Nodes as `snap` holds them, always the latest.
    #[must_use]
    pub fn new(snap: watch::Receiver<Arc<Snapshot>>) -> Self {
        Self { snap, channels: Arc::default(), leases: Arc::default(), units: Arc::default() }
    }

    /// The same nodes, as unit `home`, with the gate of each other unit at its address in
    /// `peers`, like `http://10.0.1.5:7402`.
    ///
    /// # Errors
    ///
    /// An address is not one the gate can dial.
    pub fn with_units(mut self, home: u8, peers: &BTreeMap<u8, String>) -> Result<Self, String> {
        let mut dialed = HashMap::new();
        for (&unit, addr) in peers {
            let channel =
                dial(addr).map_err(|e| format!("the gate of unit {unit} at {addr}: {e}"))?;
            dialed.insert(unit, (Arc::from(addr.as_str()), channel));
        }
        self.units = Arc::new(Units { home: Some(home), peers: dialed });
        Ok(self)
    }

    /// The gates of the other units, in the order of their units, for the calls that go to every
    /// unit.
    #[must_use]
    pub fn peers(&self) -> Vec<(u8, Channel)> {
        let mut peers: Vec<_> =
            self.units.peers.iter().map(|(&u, (_, c))| (u, c.clone())).collect();
        peers.sort_unstable_by_key(|&(u, _)| u);
        peers
    }

    /// The unit the gate serves, if it was told.
    #[must_use]
    pub fn unit(&self) -> Option<u8> {
        self.units.home
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
    /// A cell of another unit goes to that unit's gate when the call came from a caller, and is
    /// not found when the gate knows no gate for the unit or the call came from one.
    ///
    /// # Errors
    ///
    /// As [`Nodes::channel`], with `CELL_LOST` for a stale epoch or a lost lease, and the node
    /// or the unit unknown reported as the cell not found.
    pub fn owner(&self, id: CellId, origin: Origin) -> Result<Channel, Error> {
        if let Some(home) = self.units.home
            && id.unit() != home
        {
            return match self.units.peers.get(&id.unit()) {
                Some((_, c)) if origin == Origin::Caller => Ok(c.clone()),
                _ => Err(Error::new(Reason::CellNotFound, format!("cell {id} not found"))),
            };
        }
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
        nodes.owner(id, Origin::Caller).unwrap_err().reason
    }

    #[tokio::test]
    async fn a_cell_of_another_unit_goes_to_its_gate_only_from_a_caller() {
        let peers = BTreeMap::from([(2, "http://127.0.0.1:1".to_owned())]);
        let unit = nodes().with_units(1, &peers).unwrap();
        assert_eq!(unit.unit(), Some(1));
        assert_eq!(unit.peers().iter().map(|p| p.0).collect::<Vec<_>>(), [2]);
        let there = CellId::new(2, 1, 1, 7, 9).unwrap();
        assert!(unit.owner(there, Origin::Caller).is_ok());
        let e = unit.owner(there, Origin::Peer).unwrap_err();
        assert_eq!(e.reason, Reason::CellNotFound);
        let nowhere = CellId::new(3, 1, 1, 7, 9).unwrap();
        assert_eq!(unit.owner(nowhere, Origin::Caller).unwrap_err().reason, Reason::CellNotFound);
        // A cell of this unit is looked for among its nodes, which scout has not sent yet.
        let here = CellId::new(1, 1, 1, 7, 9).unwrap();
        assert_eq!(unit.owner(here, Origin::Peer).unwrap_err().reason, Reason::CellNotFound);
        // Without a unit, the unit in the id is not looked at.
        assert!(nodes().unit().is_none());
        let id = CellId::new(2, 1, 1, 7, 9).unwrap();
        assert_eq!(nodes().owner(id, Origin::Peer).unwrap_err().reason, Reason::CellNotFound);
        assert!(nodes().with_units(1, &BTreeMap::from([(2, "not a url".to_owned())])).is_err());
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
