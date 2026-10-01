//! The small amount of state that has to be consistent: projects, API keys, quotas and the
//! combs with their leases and epochs. It is a Raft group of three or five and it is not on the
//! path of an exec or a file read.
//!
//! The group runs on openraft, and each member keeps its log and its state in one redb file. Any
//! member takes any call: a write on a follower is passed to the leader, and a read is answered
//! from the member's own copy. The design is in `spec/04_control_plane.md`, section 2.

#![forbid(unsafe_code)]

pub mod config;
pub mod net;
pub mod service;
pub mod state;
pub mod store;

use std::collections::BTreeMap;
use std::sync::Arc;

use hive_proto::internal::keeper_server::KeeperServer;
use hive_proto::internal::raft_server::RaftServer;
use openraft::storage::Adaptor;
use openraft::{BasicNode, Raft, SnapshotPolicy};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

pub use crate::config::Config;
pub use crate::service::Keeper;
pub use crate::store::{Store, TypeConfig};

/// The file in the data directory that holds the log and the state.
pub const DB_FILE: &str = "keeper.redb";

/// Runs one member on `listener` until `stop` is cancelled, then shuts Raft down and returns.
///
/// The first time a member starts on an empty directory it proposes the group from the config.
/// Every member may do that, since they all propose the same one.
///
/// # Errors
///
/// The database cannot be opened, Raft does not start, or the listener fails.
pub async fn run(
    cfg: Config,
    listener: TcpListener,
    stop: CancellationToken,
) -> Result<(), String> {
    std::fs::create_dir_all(&cfg.data)
        .map_err(|e| format!("making {}: {e}", cfg.data.display()))?;
    let path = cfg.data.join(DB_FILE);
    let store = Store::open(&path).map_err(|e| format!("opening {}: {e}", path.display()))?;
    let raft_cfg = openraft::Config {
        cluster_name: "hive-keeper".into(),
        // A busy machine can hold up a heartbeat for a few hundred milliseconds, and an election
        // costs more than waiting that out, so a follower gives the leader a few seconds.
        heartbeat_interval: 300,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        snapshot_policy: SnapshotPolicy::LogsSinceLast(10_000),
        max_in_snapshot_log_to_keep: 1000,
        ..Default::default()
    }
    .validate()
    .map_err(|e| e.to_string())?;
    let peers = net::Peers::default();
    let (log, sm) = Adaptor::new(store.clone());
    let raft = Raft::new(cfg.id, Arc::new(raft_cfg), net::Network::new(peers.clone()), log, sm)
        .await
        .map_err(|e| format!("starting raft: {e}"))?;
    if !raft.is_initialized().await.map_err(|e| e.to_string())? {
        let members: BTreeMap<u64, BasicNode> =
            cfg.members.iter().map(|(id, addr)| (*id, BasicNode::new(addr))).collect();
        if let Err(e) = raft.initialize(members).await {
            eprintln!(
                "hive-keeper: member {} did not start the group, another one did: {e}",
                cfg.id
            );
        }
    }
    let keeper = Keeper::new(cfg.id, raft.clone(), store, peers, cfg.lease);
    let served = tonic::transport::Server::builder()
        .add_service(KeeperServer::new(keeper))
        .add_service(RaftServer::new(net::RaftService::new(raft.clone())))
        .serve_with_incoming_shutdown(accept_all(listener), stop.cancelled_owned())
        .await;
    let down = raft.shutdown().await;
    served.map_err(|e| format!("serving: {e}"))?;
    down.map_err(|e| format!("stopping raft: {e}"))
}

/// The listener's connections as a stream, with Nagle off since Raft messages are small.
fn accept_all(
    listener: TcpListener,
) -> impl futures::Stream<Item = std::io::Result<tokio::net::TcpStream>> {
    futures::stream::unfold(listener, |l| async move {
        let next = l.accept().await.map(|(s, _)| {
            let _ = s.set_nodelay(true);
            s
        });
        Some((next, l))
    })
}
