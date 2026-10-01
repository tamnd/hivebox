//! Two gates sharing a project's quota through a real keeper of one member.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use hive_gate::{Nodes, Quotas};
use hive_proto::internal as pb;
use hive_proto::internal::keeper_client::KeeperClient;
use hive_types::Reason;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

struct Dir(PathBuf);

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn two_gates_split_a_quota_and_one_gives_its_share_back() {
    let dir = Dir(std::env::temp_dir().join(format!("hive-gate-quota-{}", std::process::id())));
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
    let channel = Channel::from_shared(format!("http://{addr}")).unwrap().connect_lazy();
    let mut k = KeeperClient::new(channel);
    let quota = pb::Quota { cells: 100, creates_per_s: 0 };
    let mut tries = 0;
    while let Err(e) =
        k.create_project(pb::CreateProjectRequest { name: "swe".into(), quota: Some(quota) }).await
    {
        tries += 1;
        assert!(tries < 50, "{e}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // No scout, so the live counts are only what each gate made itself just now.
    let nodes = Nodes::new(tokio::sync::watch::channel(Arc::default()).1);
    let members = [addr.to_string()];
    let a =
        Quotas::new("a".into(), &members, nodes.clone(), &hive_telemetry::Registry::new()).unwrap();
    let b = Quotas::new("b".into(), &members, nodes, &hive_telemetry::Registry::new()).unwrap();

    // Gate a asks for 64 and makes 30 of them. That leaves 36, but gate b gets an even split
    // of 50, which is still short of 60.
    a.charge("swe", 30).await.unwrap();
    let e = b.charge("swe", 60).await.unwrap_err();
    assert_eq!(e.reason, Reason::QuotaExceeded, "{e}");
    b.charge("swe", 30).await.unwrap();

    // Once gate a gives its share back, gate b gets the 100 less the 30 it made just now. With
    // no scout, the cells gate a made are not counted anywhere.
    a.give_back().await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    b.charge("swe", 70).await.unwrap();
    let e = b.charge("swe", 1).await.unwrap_err();
    assert_eq!(e.reason, Reason::QuotaExceeded, "{e}");

    // A project the keeper does not know has no quota to hold it to.
    a.charge("only-in-the-config", 5000).await.unwrap();
    stop.cancel();
}
