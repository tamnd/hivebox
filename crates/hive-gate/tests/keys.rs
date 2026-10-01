//! The gate taking keys from a real keeper of one member.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use hive_gate::Keys;
use hive_proto::internal as pb;
use hive_proto::internal::keeper_client::KeeperClient;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

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
