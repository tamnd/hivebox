//! Keeper groups on loopback, with real Raft between the members.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use hive_keeper::Config;
use hive_proto::internal as pb;
use hive_proto::internal::keeper_client::KeeperClient;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

/// A directory removed when dropped.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let p = std::env::temp_dir().join(format!("hive-keeper-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Member {
    addr: SocketAddr,
    stop: CancellationToken,
    task: JoinHandle<Result<(), String>>,
}

struct Group {
    dir: Scratch,
    addrs: BTreeMap<u64, SocketAddr>,
    lease_ms: u64,
    members: BTreeMap<u64, Member>,
}

fn listen(addr: SocketAddr) -> tokio::net::TcpListener {
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.set_reuseaddr(true).unwrap();
    socket.bind(addr).unwrap();
    socket.listen(64).unwrap()
}

impl Group {
    async fn new(name: &str, n: u64, lease_ms: u64) -> Self {
        let mut listeners = BTreeMap::new();
        for id in 1..=n {
            listeners.insert(id, listen("127.0.0.1:0".parse().unwrap()));
        }
        let addrs = listeners.iter().map(|(id, l)| (*id, l.local_addr().unwrap())).collect();
        let mut g = Self { dir: Scratch::new(name), addrs, lease_ms, members: BTreeMap::new() };
        for (id, l) in listeners {
            g.start_on(id, l);
        }
        g
    }

    fn start_on(&mut self, id: u64, listener: tokio::net::TcpListener) {
        let cfg = Config {
            id,
            listen: self.addrs[&id],
            data: self.dir.0.join(id.to_string()),
            lease: Duration::from_millis(self.lease_ms),
            members: self.addrs.iter().map(|(i, a)| (*i, a.to_string())).collect(),
        };
        let stop = CancellationToken::new();
        let task = tokio::spawn(hive_keeper::run(cfg, listener, stop.clone()));
        self.members.insert(id, Member { addr: self.addrs[&id], stop, task });
    }

    fn start(&mut self, id: u64) {
        let l = listen(self.addrs[&id]);
        self.start_on(id, l);
    }

    async fn stop(&mut self, id: u64) {
        let m = self.members.remove(&id).unwrap();
        m.stop.cancel();
        m.task.await.unwrap().unwrap();
    }

    async fn client(&self, id: u64) -> KeeperClient<Channel> {
        let url = format!("http://{}", self.members[&id].addr);
        KeeperClient::new(Channel::from_shared(url).unwrap().connect_lazy())
    }

    /// The leader, once the members still running agree on one.
    async fn leader(&mut self) -> u64 {
        for _ in 0..200 {
            let mut seen = Vec::new();
            for id in self.members.keys() {
                if let Ok(s) = self.client(*id).await.status(pb::StatusRequest {}).await {
                    seen.push(s.into_inner().leader);
                }
            }
            let l = seen.first().copied().unwrap_or(0);
            let agreed = seen.len() == self.members.len() && seen.iter().all(|s| *s == l);
            if agreed && self.members.contains_key(&l) {
                return l;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let mut last = Vec::new();
        for id in self.members.keys() {
            let s = self.client(*id).await.status(pb::StatusRequest {}).await;
            last.push((*id, s.map(|s| s.into_inner().leader)));
        }
        let mut exited = Vec::new();
        for (id, m) in &mut self.members {
            if m.task.is_finished() {
                exited.push((*id, (&mut m.task).await));
            }
        }
        panic!("no leader the members agree on, (id, leader): {last:?}, exited: {exited:?}");
    }

    /// Waits until every running member has applied at least `index`.
    async fn caught_up(&self, index: u64) {
        for _ in 0..200 {
            let mut all = true;
            for id in self.members.keys() {
                let s = self.client(*id).await.status(pb::StatusRequest {}).await;
                all &= s.is_ok_and(|s| s.into_inner().applied >= index);
            }
            if all {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("the members did not reach index {index}");
    }

    async fn applied(&self, id: u64) -> u64 {
        self.client(id).await.status(pb::StatusRequest {}).await.unwrap().into_inner().applied
    }
}

fn register(name: &str, epoch: u32) -> pb::RegisterRequest {
    pb::RegisterRequest { name: name.into(), addr: format!("http://{name}:7420"), epoch }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_members_agree_and_outlive_their_leader() {
    let mut g = Group::new("three", 3, 10_000).await;
    let leader = g.leader().await;
    let follower = *g.members.keys().find(|id| **id != leader).unwrap();

    // Writes through a follower land on the leader and show up on every member.
    let mut c = g.client(follower).await;
    let p = c
        .create_project(pb::CreateProjectRequest {
            name: "swe".into(),
            quota: Some(pb::Quota { cells: 500, creates_per_s: 50 }),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!((p.name.as_str(), p.quota.unwrap().cells), ("swe", 500));
    let again =
        c.create_project(pb::CreateProjectRequest { name: "swe".into(), quota: None }).await;
    assert_eq!(again.unwrap_err().code(), tonic::Code::AlreadyExists);
    let nope = c.create_key(pb::CreateKeyRequest { project: "nope".into() }).await;
    assert_eq!(nope.unwrap_err().code(), tonic::Code::NotFound);
    let key =
        c.create_key(pb::CreateKeyRequest { project: "swe".into() }).await.unwrap().into_inner();
    assert!(key.key.starts_with("hb_") && key.key.len() == 51, "{}", key.key);
    let info = key.info.unwrap();
    assert_eq!(info.hash, blake3::hash(key.key.as_bytes()).as_bytes().to_vec());
    assert_eq!(info.prefix, key.key[..8]);
    let lease = c.register(register("box-a", 0)).await.unwrap().into_inner();
    assert_eq!((lease.node, lease.epoch, lease.ttl_ms), (1, 1, 10_000));
    let renewed = c.renew(pb::RenewRequest { node: 1, epoch: 1 }).await.unwrap().into_inner();
    assert!(renewed.expires_ms >= lease.expires_ms);
    let stale = c.renew(pb::RenewRequest { node: 1, epoch: 7 }).await.unwrap_err();
    assert_eq!(stale.code(), tonic::Code::FailedPrecondition);

    let index = g.applied(leader).await;
    g.caught_up(index).await;
    for id in [1, 2, 3] {
        let mut c = g.client(id).await;
        let projects =
            c.list_projects(pb::ListProjectsRequest {}).await.unwrap().into_inner().projects;
        assert_eq!(projects.len(), 1, "member {id}");
        let keys = c.list_keys(pb::ListKeysRequest { project: "swe".into() }).await.unwrap();
        assert_eq!(keys.into_inner().keys, vec![info.clone()], "member {id}");
        let nodes = c.list_nodes(pb::ListNodesRequest {}).await.unwrap().into_inner().nodes;
        assert_eq!(nodes.len(), 1, "member {id}");
        assert!(!nodes[0].lost);
    }

    // The leader goes away. The other two pick a new one and keep taking writes.
    g.stop(leader).await;
    let next = g.leader().await;
    assert_ne!(next, leader);
    let other = *g.members.keys().find(|id| **id != next).unwrap();
    let lease = g.client(other).await.register(register("box-b", 0)).await.unwrap().into_inner();
    assert_eq!((lease.node, lease.epoch), (2, 1));
    let revoked = g
        .client(other)
        .await
        .revoke_key(pb::RevokeKeyRequest { hash: vec![], prefix: info.prefix.clone() })
        .await
        .unwrap()
        .into_inner();
    assert_ne!(revoked.revoked_ms, 0);

    // The old leader comes back from its own disk and catches up.
    g.start(leader);
    let index = g.applied(next).await;
    g.caught_up(index).await;
    let mut c = g.client(leader).await;
    let nodes = c.list_nodes(pb::ListNodesRequest {}).await.unwrap().into_inner().nodes;
    let names: Vec<&str> = nodes.iter().map(|n| n.name.as_str()).collect();
    assert_eq!(names, ["box-a", "box-b"]);
    let keys = c.list_keys(pb::ListKeysRequest { project: String::new() }).await.unwrap();
    assert_ne!(keys.into_inner().keys[0].revoked_ms, 0);

    for id in [1, 2, 3] {
        g.stop(id).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lease_that_ran_out_means_a_new_epoch() {
    let mut g = Group::new("lease", 1, 1000).await;
    g.leader().await;
    let mut c = g.client(1).await;
    let first = c.register(register("box-a", 0)).await.unwrap().into_inner();
    assert_eq!((first.node, first.epoch), (1, 1));

    // Within the lease, a comb that restarts keeps its epoch.
    let back = c.register(register("box-a", 1)).await.unwrap().into_inner();
    assert_eq!((back.node, back.epoch), (1, 1));

    tokio::time::sleep(Duration::from_millis(1300)).await;
    let nodes = c.list_nodes(pb::ListNodesRequest {}).await.unwrap().into_inner().nodes;
    assert!(nodes[0].lost);
    let e = c.renew(pb::RenewRequest { node: 1, epoch: 1 }).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::FailedPrecondition, "{e:?}");
    let again = c.register(register("box-a", 1)).await.unwrap().into_inner();
    assert_eq!((again.node, again.epoch), (1, 2));

    // A member that restarts reads its state back from disk.
    g.stop(1).await;
    g.start(1);
    g.leader().await;
    let mut c = g.client(1).await;
    let nodes = c.list_nodes(pb::ListNodesRequest {}).await.unwrap().into_inner().nodes;
    assert_eq!((nodes[0].node, nodes[0].epoch), (1, 2));
    g.stop(1).await;
}
