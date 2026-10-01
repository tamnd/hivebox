//! The keeper's gRPC service: writes go through the Raft log, reads come from this member's copy.

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hive_proto::internal as pb;
use hive_proto::internal::keeper_server;
use openraft::Raft;
use openraft::error::{ClientWriteError, RaftError};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tonic::{Request, Response, Status};

use crate::net::Peers;
use crate::state::{Command, Key, Node, Project, Quota, Refusal, Reply};
use crate::store::{Store, TypeConfig};

/// How long a write waits for the group to have a leader before it gives up.
const NO_LEADER: Duration = Duration::from_secs(5);

/// The most commands put in one log entry.
const MAX_BATCH: usize = 1024;

/// How many batches can be on their way through the log at once.
const IN_FLIGHT: usize = 4;

/// The `Keeper` gRPC service on one member.
#[derive(Clone)]
pub struct Keeper {
    id: u64,
    raft: Raft<TypeConfig>,
    store: Store,
    writes: mpsc::Sender<Waiting>,
    lease_ms: u64,
}

/// A write waiting for its batch, and where its answer goes.
type Waiting = (Command, oneshot::Sender<Result<Reply, Status>>);

impl std::fmt::Debug for Keeper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Keeper").field("id", &self.id).finish_non_exhaustive()
    }
}

impl Keeper {
    /// The service for member `id`. Leases it gives out last `lease`.
    ///
    /// It starts a task that gathers the writes arriving together into one log entry. The task
    /// ends when the last clone of the service is dropped.
    #[must_use]
    pub fn new(
        id: u64,
        raft: Raft<TypeConfig>,
        store: Store,
        peers: Peers,
        lease: Duration,
    ) -> Self {
        let lease_ms = u64::try_from(lease.as_millis()).unwrap_or(u64::MAX);
        let (writes, rx) = mpsc::channel(MAX_BATCH * IN_FLIGHT);
        tokio::spawn(batch(Writer { raft: raft.clone(), peers }, rx));
        Self { id, raft, store, writes, lease_ms }
    }

    /// Puts `cmd` in the log, with the other writes that came in at the same time, and returns
    /// what applying it gave.
    async fn write(&self, cmd: Command) -> Result<Reply, Status> {
        let (tx, rx) = oneshot::channel();
        let gone = || Status::unavailable("the keeper is stopping");
        self.writes.send((cmd, tx)).await.map_err(|_| gone())?;
        rx.await.map_err(|_| gone())?
    }
}

/// Takes the writes as they come and sends each lot that is waiting as one batch, with up to
/// [`IN_FLIGHT`] batches going at once. When the disk is slow more writes wait, so the batches
/// grow and the rate holds.
async fn batch(writer: Writer, mut rx: mpsc::Receiver<Waiting>) {
    let slots = Arc::new(Semaphore::new(IN_FLIGHT));
    while let Some(first) = rx.recv().await {
        let Ok(slot) = Arc::clone(&slots).acquire_owned().await else { return };
        let mut lot = vec![first];
        while lot.len() < MAX_BATCH {
            match rx.try_recv() {
                Ok(w) => lot.push(w),
                Err(_) => break,
            }
        }
        let writer = writer.clone();
        tokio::spawn(async move {
            let (mut cmds, answers): (Vec<_>, Vec<_>) = lot.into_iter().unzip();
            let replies = if cmds.len() == 1 {
                let cmd = cmds.pop().unwrap_or(Command::Batch(Vec::new()));
                vec![writer.write(cmd).await.and_then(refusal)]
            } else {
                match writer.write(Command::Batch(cmds)).await {
                    Ok(Reply::Batch(rs)) => rs.into_iter().map(refusal).collect(),
                    Ok(r) => vec![Err(unexpected(&r))],
                    Err(s) => {
                        answers.iter().map(|_| Err(Status::new(s.code(), s.message()))).collect()
                    }
                }
            };
            let mut replies = replies.into_iter();
            for a in answers {
                let _ = a.send(
                    replies
                        .next()
                        .unwrap_or_else(|| Err(Status::internal("a batch came back short"))),
                );
            }
            drop(slot);
        });
    }
}

/// What a batch needs to go through the log.
#[derive(Clone)]
struct Writer {
    raft: Raft<TypeConfig>,
    peers: Peers,
}

impl Writer {
    /// Puts `cmd` in the log, through the leader when this member is not it, and returns what
    /// applying it gave.
    async fn write(&self, cmd: Command) -> Result<Reply, Status> {
        let deadline = tokio::time::Instant::now() + NO_LEADER;
        loop {
            let err = match self.raft.client_write(cmd.clone()).await {
                Ok(r) => return Ok(r.data),
                Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
                    // A leader that went away or stepped down is tried again until a new one
                    // takes the write. If it had taken it and only the answer was lost, the
                    // second try is refused the way a repeat is, like a project that exists.
                    match f.leader_node {
                        Some(node) => match self.peers.propose(&node.addr, &cmd).await {
                            Ok(Ok(r)) => return Ok(r.data),
                            Ok(Err(e)) => e.to_string(),
                            Err(s) => format!("leader {}: {}", node.addr, s.message()),
                        },
                        None => "the keeper group has no leader".to_owned(),
                    }
                }
                Err(e) => e.to_string(),
            };
            if tokio::time::Instant::now() >= deadline {
                return Err(Status::unavailable(err));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

#[tonic::async_trait]
impl keeper_server::Keeper for Keeper {
    async fn create_project(
        &self,
        req: Request<pb::CreateProjectRequest>,
    ) -> Result<Response<pb::Project>, Status> {
        let req = req.into_inner();
        let q = req.quota.unwrap_or_default();
        let quota = Quota { cells: q.cells, creates_per_s: q.creates_per_s };
        match self.write(Command::CreateProject { name: req.name, quota, now_ms: now_ms() }).await?
        {
            Reply::Project(p) => Ok(Response::new(project(&p))),
            r => Err(unexpected(&r)),
        }
    }

    async fn list_projects(
        &self,
        _: Request<pb::ListProjectsRequest>,
    ) -> Result<Response<pb::ListProjectsResponse>, Status> {
        let projects = self.store.read(|s| s.projects.values().map(project).collect());
        Ok(Response::new(pb::ListProjectsResponse { projects }))
    }

    async fn create_key(
        &self,
        req: Request<pb::CreateKeyRequest>,
    ) -> Result<Response<pb::CreateKeyResponse>, Status> {
        let project = req.into_inner().project;
        let mut raw = [0u8; 24];
        getrandom::fill(&mut raw).map_err(|e| Status::internal(e.to_string()))?;
        let key = format!("hb_{}", hex(&raw));
        let hash = *blake3::hash(key.as_bytes()).as_bytes();
        let cmd = Command::AddKey { hash, prefix: key[..8].to_owned(), project, now_ms: now_ms() };
        match self.write(cmd).await? {
            Reply::Key(h, k) => {
                Ok(Response::new(pb::CreateKeyResponse { key, info: Some(key_info(&h, &k)) }))
            }
            r => Err(unexpected(&r)),
        }
    }

    async fn revoke_key(
        &self,
        req: Request<pb::RevokeKeyRequest>,
    ) -> Result<Response<pb::KeyInfo>, Status> {
        let req = req.into_inner();
        let hash = match req.hash.len() {
            0 => None,
            32 => req.hash.as_slice().try_into().ok(),
            n => return Err(Status::invalid_argument(format!("a key hash is 32 bytes, not {n}"))),
        };
        match self.write(Command::RevokeKey { hash, prefix: req.prefix, now_ms: now_ms() }).await? {
            Reply::Key(h, k) => Ok(Response::new(key_info(&h, &k))),
            r => Err(unexpected(&r)),
        }
    }

    async fn list_keys(
        &self,
        req: Request<pb::ListKeysRequest>,
    ) -> Result<Response<pb::ListKeysResponse>, Status> {
        let want = req.into_inner().project;
        let keys = self.store.read(|s| {
            s.keys
                .iter()
                .filter(|(_, k)| want.is_empty() || k.project == want)
                .map(|(h, k)| key_info(h, k))
                .collect()
        });
        Ok(Response::new(pb::ListKeysResponse { keys }))
    }

    async fn register(
        &self,
        req: Request<pb::RegisterRequest>,
    ) -> Result<Response<pb::Lease>, Status> {
        let req = req.into_inner();
        let epoch = u16::try_from(req.epoch)
            .map_err(|_| Status::invalid_argument("epoch is past 65535"))?;
        let cmd = Command::Register {
            name: req.name,
            addr: req.addr,
            epoch,
            now_ms: now_ms(),
            ttl_ms: self.lease_ms,
        };
        match self.write(cmd).await? {
            Reply::Node(n) => Ok(Response::new(self.lease(&n))),
            r => Err(unexpected(&r)),
        }
    }

    async fn renew(&self, req: Request<pb::RenewRequest>) -> Result<Response<pb::Lease>, Status> {
        let req = req.into_inner();
        let node =
            u16::try_from(req.node).map_err(|_| Status::invalid_argument("node is past 65535"))?;
        let epoch = u16::try_from(req.epoch)
            .map_err(|_| Status::invalid_argument("epoch is past 65535"))?;
        match self
            .write(Command::Renew { node, epoch, now_ms: now_ms(), ttl_ms: self.lease_ms })
            .await?
        {
            Reply::Node(n) => Ok(Response::new(self.lease(&n))),
            r => Err(unexpected(&r)),
        }
    }

    async fn list_nodes(
        &self,
        _: Request<pb::ListNodesRequest>,
    ) -> Result<Response<pb::ListNodesResponse>, Status> {
        let now = now_ms();
        let nodes = self.store.read(|s| {
            s.nodes
                .values()
                .map(|n| pb::NodeRecord {
                    node: u32::from(n.node),
                    name: n.name.clone(),
                    addr: n.addr.clone(),
                    epoch: u32::from(n.epoch),
                    expires_ms: n.expires_ms,
                    lost: n.expires_ms <= now,
                })
                .collect()
        });
        Ok(Response::new(pb::ListNodesResponse { nodes }))
    }

    async fn status(
        &self,
        _: Request<pb::StatusRequest>,
    ) -> Result<Response<pb::StatusResponse>, Status> {
        let m = self.raft.metrics().borrow().clone();
        Ok(Response::new(pb::StatusResponse {
            id: self.id,
            leader: m.current_leader.unwrap_or(0),
            term: m.current_term,
            applied: self.store.applied(),
            voters: m.membership_config.membership().voter_ids().collect(),
        }))
    }
}

impl Keeper {
    fn lease(&self, n: &Node) -> pb::Lease {
        pb::Lease {
            node: u32::from(n.node),
            epoch: u32::from(n.epoch),
            expires_ms: n.expires_ms,
            ttl_ms: self.lease_ms,
        }
    }
}

/// The time by this member's clock, in milliseconds since the Unix epoch.
fn now_ms() -> u64 {
    let d = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

fn refusal(r: Reply) -> Result<Reply, Status> {
    match r {
        Reply::Refused(Refusal::Invalid(m)) => Err(Status::invalid_argument(m)),
        Reply::Refused(Refusal::NotFound(m)) => Err(Status::not_found(m)),
        Reply::Refused(Refusal::Exists(m)) => Err(Status::already_exists(m)),
        Reply::Refused(Refusal::LeaseLost(m)) => Err(Status::failed_precondition(m)),
        Reply::Refused(Refusal::Exhausted(m)) => Err(Status::resource_exhausted(m)),
        r => Ok(r),
    }
}

fn unexpected(r: &Reply) -> Status {
    Status::internal(format!("the log gave {r:?}"))
}

fn project(p: &Project) -> pb::Project {
    pb::Project {
        name: p.name.clone(),
        quota: Some(pb::Quota { cells: p.quota.cells, creates_per_s: p.quota.creates_per_s }),
        created_ms: p.created_ms,
    }
}

fn key_info(hash: &[u8; 32], k: &Key) -> pb::KeyInfo {
    pb::KeyInfo {
        prefix: k.prefix.clone(),
        project: k.project.clone(),
        hash: hash.to_vec(),
        created_ms: k.created_ms,
        revoked_ms: k.revoked_ms,
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}
