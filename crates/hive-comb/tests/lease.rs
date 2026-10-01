//! A comb's lease, against a real keeper of one member.

#![cfg(target_os = "linux")]

use std::path::PathBuf;
use std::time::{Duration, Instant};

use hive_comb::KeeperLink;
use hive_comb::lease::{self, Keeper, Lease};
use tokio_util::sync::CancellationToken;

struct Dir(PathBuf);

impl Dir {
    fn new(name: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("hive-comb-lease-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Starts a keeper of one member with leases of `lease_ms`, and returns its address.
async fn keeper(dir: &Dir, lease_ms: u64, stop: &CancellationToken) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let cfg = hive_keeper::Config {
        id: 1,
        listen: addr,
        data: dir.0.join("keeper"),
        lease: Duration::from_millis(lease_ms),
        members: [(1, addr.to_string())].into(),
    };
    tokio::spawn(hive_keeper::run(cfg, listener, stop.clone()));
    addr.to_string()
}

fn link(addr: &str, name: &str) -> KeeperLink {
    KeeperLink {
        members: vec![addr.to_owned()],
        name: name.to_owned(),
        advertise: "http://10.0.0.7:7400".into(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_comb_keeps_its_epoch_until_it_registers_afresh() {
    let dir = Dir::new("epoch");
    let stop = CancellationToken::new();
    let addr = keeper(&dir, 3000, &stop).await;
    let link = link(&addr, "node-a");
    let data = dir.0.join("comb");
    std::fs::create_dir_all(&data).unwrap();

    let mut k = Keeper::new(&link.members).unwrap();
    let first = lease::register(&mut k, &link, &data).await.unwrap();
    assert_eq!((first.node, first.epoch), (1, 1));
    assert_eq!(first.ttl, Duration::from_secs(3));
    assert_eq!(lease::read(&data.join(lease::FILE)), Some((1, 1)));

    // Back within its lease, with the epoch in its file: the same epoch, so the cells stay.
    let again = lease::register(&mut k, &link, &data).await.unwrap();
    assert_eq!(again, first);
    let held = tokio::spawn(lease::keep(k, again, stop.clone()));

    // The same name with no file, like a machine that lost its disk, gets a new epoch, and the
    // comb still holding the old one finds out at its next renewal.
    let fresh = dir.0.join("fresh");
    std::fs::create_dir_all(&fresh).unwrap();
    let mut k = Keeper::new(&link.members).unwrap();
    let second = lease::register(&mut k, &link, &fresh).await.unwrap();
    assert_eq!((second.node, second.epoch), (1, 2));
    let started = Instant::now();
    let e = tokio::time::timeout(Duration::from_secs(5), held).await.unwrap().unwrap().unwrap_err();
    assert!(e.contains("lost the lease of node 1 in epoch 1"), "{e}");
    assert!(started.elapsed() < Duration::from_secs(2), "{:?}", started.elapsed());
    stop.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_comb_that_cannot_reach_the_keeper_stops_when_its_lease_runs_out() {
    // Nothing listens on port 1.
    let k = Keeper::new(&["127.0.0.1:1".to_owned()]).unwrap();
    let l = Lease { node: 4, epoch: 2, ttl: Duration::from_millis(1200) };
    let started = Instant::now();
    let e = lease::keep(k, l, CancellationToken::new()).await.unwrap_err();
    assert!(e.contains("before it ran out"), "{e}");
    let took = started.elapsed();
    assert!(took >= Duration::from_millis(1200) && took < Duration::from_secs(3), "{took:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bad_name_is_refused_and_not_tried_again() {
    let dir = Dir::new("name");
    let stop = CancellationToken::new();
    let addr = keeper(&dir, 3000, &stop).await;
    let bad = link(&addr, "no spaces");
    let mut k = Keeper::new(&bad.members).unwrap();
    let e = lease::register(&mut k, &bad, &dir.0).await.unwrap_err();
    assert!(e.contains("refused node no spaces"), "{e}");
    stop.cancel();
}
