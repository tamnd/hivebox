//! The gate taking keys and tokens from a real keeper of one member.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hive_gate::{Gate, Keys, Nodes};
use hive_proto::internal as pb;
use hive_proto::internal::keeper_client::KeeperClient;
use hive_proto::v1;
use hive_proto::v1::cells_client::CellsClient;
use hive_proto::v1::tokens_client::TokensClient;
use hive_types::CellId;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tonic::{Code, Request};

struct Dir(PathBuf);

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Waits up to 5 s for `want` to hold, and returns how long it took.
async fn until(want: impl Fn() -> bool) -> Duration {
    let started = Instant::now();
    while !want() {
        assert!(started.elapsed() < Duration::from_secs(5), "waited 5 s");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    started.elapsed()
}

/// A keeper of one member at a new address, with a project `swe`, and a client to it.
async fn keeper(dir: &Dir, stop: &CancellationToken) -> (String, KeeperClient<Channel>) {
    let _ = std::fs::remove_dir_all(&dir.0);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let cfg = hive_keeper::Config {
        id: 1,
        listen: addr,
        data: dir.0.clone(),
        lease: Duration::from_secs(10),
        members: [(1, addr.to_string())].into(),
    };
    tokio::spawn(hive_keeper::run(cfg, listener, stop.clone()));
    let channel = Channel::from_shared(format!("http://{addr}")).unwrap().connect_lazy();
    let mut k = KeeperClient::new(channel);
    let mut tries = 0;
    while let Err(e) =
        k.create_project(pb::CreateProjectRequest { name: "swe".into(), quota: None }).await
    {
        tries += 1;
        assert!(tries < 50, "{e}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    (addr.to_string(), k)
}

fn denied<T: std::fmt::Debug>(r: Result<T, tonic::Status>) -> Code {
    r.unwrap_err().code()
}

fn call<T>(bearer: &str, msg: T) -> Request<T> {
    let mut r = Request::new(msg);
    r.metadata_mut().insert("authorization", format!("Bearer {bearer}").parse().unwrap());
    r
}

#[tokio::test(flavor = "multi_thread")]
async fn tokens_open_what_they_say_and_end_with_their_key() {
    let dir = Dir(std::env::temp_dir().join(format!("hive-gate-tokens-{}", std::process::id())));
    let stop = CancellationToken::new();
    let (addr, mut k) = keeper(&dir, &stop).await;
    let keys = Keys::default();
    hive_gate::keys::follow(keys.clone(), std::slice::from_ref(&addr), stop.clone()).unwrap();
    // No combs: a call that gets past the token check fails for want of a node instead.
    let (_tx, snap) = tokio::sync::watch::channel(Arc::default());
    let gate = Gate::new(keys.clone(), Nodes::new(snap), None, &hive_telemetry::Registry::new())
        .with_tokens(hive_gate::tokens::Api::new(std::slice::from_ref(&addr)).unwrap());
    let gl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gaddr = gl.local_addr().unwrap();
    tokio::spawn(hive_gate::serve(gate, gl, stop.clone()));

    let made =
        k.create_key(pb::CreateKeyRequest { project: "swe".into() }).await.unwrap().into_inner();
    let hash = *blake3::hash(made.key.as_bytes()).as_bytes();
    until(|| keys.project(&hash).is_some()).await;
    let ch = Channel::from_shared(format!("http://{gaddr}")).unwrap().connect_lazy();
    let mut tokens = TokensClient::new(ch.clone());
    let mut cells = CellsClient::new(ch);
    let mine = CellId::new(1, 3, 1, 1, 1).unwrap().to_string();
    let other = CellId::new(1, 3, 1, 2, 1).unwrap().to_string();
    let ask = v1::MintTokenRequest {
        ttl: Some(hive_proto::convert::duration_to_v1(Duration::from_secs(600))),
        cell_ids: vec![mine.clone()],
        ops: vec!["get".into(), "exec".into()],
    };
    let t = tokens.mint(call(&made.key, ask.clone())).await.unwrap().into_inner();
    let left = t.expires_at.unwrap().seconds
        - i64::try_from(made.info.as_ref().unwrap().created_ms / 1000).unwrap();
    assert!((595..=605).contains(&left), "{left}");

    // The keeper's public key reaches the gate with the next key list.
    let get = |id: &str| v1::GetCellRequest { id: id.to_owned() };
    let started = Instant::now();
    let code = loop {
        let code = cells.get(call(&t.token, get(&mine))).await.unwrap_err().code();
        if code != Code::Unauthenticated || started.elapsed() > Duration::from_secs(3) {
            break code;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_ne!(code, Code::PermissionDenied);
    assert_ne!(code, Code::Unauthenticated);
    assert_eq!(denied(cells.get(call(&t.token, get(&other))).await), Code::PermissionDenied);
    let list = v1::ListCellsRequest::default();
    assert_eq!(denied(cells.list(call(&t.token, list)).await), Code::PermissionDenied);
    let ext = v1::ExtendTtlRequest { id: mine.clone(), ..Default::default() };
    assert_eq!(denied(cells.extend_ttl(call(&t.token, ext)).await), Code::PermissionDenied);
    assert_eq!(denied(tokens.mint(call(&t.token, ask)).await), Code::PermissionDenied);

    // Narrowed down offline to exec only, the token can no longer get the cell.
    let narrow = hive_auth::Narrow { ops: vec!["exec".into()], ..Default::default() };
    let small = hive_auth::narrow(&t.token, &narrow).unwrap();
    assert_eq!(denied(cells.get(call(&small, get(&mine))).await), Code::PermissionDenied);
    assert_eq!(denied(cells.get(call("garbage", get(&mine))).await), Code::Unauthenticated);

    // Revoking the key ends its tokens.
    let prefix = made.info.unwrap().prefix;
    k.revoke_key(pb::RevokeKeyRequest { hash: Vec::new(), prefix }).await.unwrap();
    until(|| keys.project(&hash).is_none()).await;
    assert_eq!(denied(cells.get(call(&t.token, get(&mine))).await), Code::Unauthenticated);
    stop.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_key_made_in_the_keeper_works_and_stops_working_when_revoked() {
    let dir = Dir(std::env::temp_dir().join(format!("hive-gate-keys-{}", std::process::id())));
    let _ = std::fs::remove_dir_all(&dir.0);
    let stop = CancellationToken::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let cfg = hive_keeper::Config {
        id: 1,
        listen: addr,
        data: dir.0.clone(),
        lease: Duration::from_secs(10),
        members: [(1, addr.to_string())].into(),
    };
    tokio::spawn(hive_keeper::run(cfg, listener, stop.clone()));

    let keys = Keys::default();
    hive_gate::keys::follow(keys.clone(), &[addr.to_string()], stop.clone()).unwrap();
    let channel = Channel::from_shared(format!("http://{addr}")).unwrap().connect_lazy();
    let mut k = KeeperClient::new(channel);
    let mut tries = 0;
    while let Err(e) =
        k.create_project(pb::CreateProjectRequest { name: "swe".into(), quota: None }).await
    {
        tries += 1;
        assert!(tries < 50, "{e}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let made =
        k.create_key(pb::CreateKeyRequest { project: "swe".into() }).await.unwrap().into_inner();
    let hash = *blake3::hash(made.key.as_bytes()).as_bytes();
    let took = until(|| keys.project(&hash).as_deref() == Some("swe")).await;
    assert!(took < Duration::from_secs(3), "{took:?}");

    let prefix = made.info.unwrap().prefix;
    k.revoke_key(pb::RevokeKeyRequest { hash: Vec::new(), prefix }).await.unwrap();
    let took = until(|| keys.project(&hash).is_none()).await;
    assert!(took < Duration::from_secs(3), "{took:?}");
    stop.cancel();
}
