//! The Raft messages between members, as JSON inside a gRPC call.

// openraft's network trait returns its RPCError, which is over 200 bytes.
#![allow(clippy::result_large_err)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use hive_proto::internal::RaftMessage;
use hive_proto::internal::raft_client::RaftClient;
use hive_proto::internal::raft_server;
use openraft::error::{
    ClientWriteError, InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError,
    Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, ClientWriteResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Raft};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

use crate::state::Command;
use crate::store::TypeConfig;

/// The answer to a write passed to the leader.
pub type Proposed =
    Result<ClientWriteResponse<TypeConfig>, RaftError<u64, ClientWriteError<u64, BasicNode>>>;

/// gRPC channels to the other members, by address, made once and shared.
#[derive(Clone, Debug, Default)]
pub struct Peers {
    channels: Arc<Mutex<HashMap<String, Channel>>>,
}

impl Peers {
    /// A client for the member at `addr`, which is `host:port`.
    ///
    /// # Errors
    ///
    /// The address does not parse.
    pub fn client(&self, addr: &str) -> Result<RaftClient<Channel>, Status> {
        let mut channels = self.channels.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(c) = channels.get(addr) {
            return Ok(RaftClient::new(c.clone()));
        }
        let channel = Endpoint::from_shared(format!("http://{addr}"))
            .map_err(|e| Status::invalid_argument(format!("member address {addr}: {e}")))?
            .connect_timeout(Duration::from_secs(1))
            .tcp_nodelay(true)
            .connect_lazy();
        channels.insert(addr.to_owned(), channel.clone());
        Ok(RaftClient::new(channel))
    }

    /// Passes a write to the leader at `addr` and returns its answer.
    ///
    /// # Errors
    ///
    /// The leader could not be reached or the answer did not decode.
    pub async fn propose(&self, addr: &str, cmd: &Command) -> Result<Proposed, Status> {
        let mut client = self.client(addr)?;
        let resp = client.propose(Request::new(encode(cmd)?)).await?;
        decode(&resp.into_inner())
    }
}

/// Makes a [`Peer`] for each member openraft talks to.
#[derive(Clone, Debug)]
pub struct Network {
    peers: Peers,
}

impl Network {
    /// A network that dials through `peers`.
    #[must_use]
    pub fn new(peers: Peers) -> Self {
        Self { peers }
    }
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = Peer;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Peer {
        Peer { target, client: self.peers.client(&node.addr).map_err(|e| e.message().to_owned()) }
    }
}

/// One member, as openraft sends to it.
#[derive(Debug)]
pub struct Peer {
    target: u64,
    client: Result<RaftClient<Channel>, String>,
}

type RpcResult<T, E> = Result<T, RPCError<u64, BasicNode, RaftError<u64, E>>>;

impl Peer {
    async fn call<Q, A, E>(&mut self, which: Rpc, req: &Q, option: &RPCOption) -> RpcResult<A, E>
    where
        Q: Serialize,
        A: DeserializeOwned,
        E: std::error::Error + DeserializeOwned,
    {
        let mut client = match &self.client {
            Ok(c) => c.clone(),
            Err(e) => return Err(RPCError::Unreachable(Unreachable::new(&Text(e.clone())))),
        };
        let msg = encode(req).map_err(|s| RPCError::Network(NetworkError::new(&s)))?;
        let call = async {
            match which {
                Rpc::Append => client.append_entries(msg).await,
                Rpc::Vote => client.vote(msg).await,
                Rpc::Snapshot => client.install_snapshot(msg).await,
            }
        };
        let resp = match tokio::time::timeout(option.hard_ttl(), call).await {
            Ok(Ok(r)) => r.into_inner(),
            Ok(Err(s)) if s.code() == tonic::Code::Unavailable => {
                return Err(RPCError::Unreachable(Unreachable::new(&s)));
            }
            Ok(Err(s)) => return Err(RPCError::Network(NetworkError::new(&s))),
            Err(_) => {
                let e = Text(format!(
                    "no answer from member {} in {:?}",
                    self.target,
                    option.hard_ttl()
                ));
                return Err(RPCError::Unreachable(Unreachable::new(&e)));
            }
        };
        let answer: Result<A, RaftError<u64, E>> =
            decode(&resp).map_err(|s| RPCError::Network(NetworkError::new(&s)))?;
        answer.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}

#[derive(Clone, Copy)]
enum Rpc {
    Append,
    Vote,
    Snapshot,
}

impl RaftNetwork<TypeConfig> for Peer {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> RpcResult<AppendEntriesResponse<u64>, openraft::error::Infallible> {
        self.call(Rpc::Append, &rpc, &option).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> RpcResult<InstallSnapshotResponse<u64>, InstallSnapshotError> {
        self.call(Rpc::Snapshot, &rpc, &option).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        option: RPCOption,
    ) -> RpcResult<VoteResponse<u64>, openraft::error::Infallible> {
        self.call(Rpc::Vote, &rpc, &option).await
    }
}

/// Takes the Raft messages from the other members.
#[derive(Clone)]
pub struct RaftService {
    raft: Raft<TypeConfig>,
}

impl std::fmt::Debug for RaftService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftService").finish_non_exhaustive()
    }
}

impl RaftService {
    /// A service in front of `raft`.
    #[must_use]
    pub fn new(raft: Raft<TypeConfig>) -> Self {
        Self { raft }
    }
}

#[tonic::async_trait]
impl raft_server::Raft for RaftService {
    async fn append_entries(
        &self,
        req: Request<RaftMessage>,
    ) -> Result<Response<RaftMessage>, Status> {
        let rpc = decode(req.get_ref())?;
        Ok(Response::new(encode(&self.raft.append_entries(rpc).await)?))
    }

    async fn vote(&self, req: Request<RaftMessage>) -> Result<Response<RaftMessage>, Status> {
        let rpc = decode(req.get_ref())?;
        Ok(Response::new(encode(&self.raft.vote(rpc).await)?))
    }

    async fn install_snapshot(
        &self,
        req: Request<RaftMessage>,
    ) -> Result<Response<RaftMessage>, Status> {
        let rpc = decode(req.get_ref())?;
        Ok(Response::new(encode(&self.raft.install_snapshot(rpc).await)?))
    }

    async fn propose(&self, req: Request<RaftMessage>) -> Result<Response<RaftMessage>, Status> {
        let cmd: Command = decode(req.get_ref())?;
        let answer: Proposed = self.raft.client_write(cmd).await;
        Ok(Response::new(encode(&answer)?))
    }
}

fn encode<T: Serialize>(v: &T) -> Result<RaftMessage, Status> {
    serde_json::to_vec(v)
        .map(|json| RaftMessage { json })
        .map_err(|e| Status::internal(e.to_string()))
}

fn decode<T: DeserializeOwned>(m: &RaftMessage) -> Result<T, Status> {
    serde_json::from_slice(&m.json)
        .map_err(|e| Status::invalid_argument(format!("a raft message: {e}")))
}

/// A message as an error, for the openraft errors that want one.
#[derive(Debug)]
struct Text(String);

impl std::fmt::Display for Text {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Text {}
