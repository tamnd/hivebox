//! A comb's lease, against a real keeper of one member.

#![cfg(target_os = "linux")]

use std::path::PathBuf;
use std::time::{Duration, Instant};

use hive_comb::KeeperLink;
use hive_comb::lease::{self, Keeper, Lease, Start};
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

fn leased(s: Start) -> (Lease, tokio::time::Instant) {
    match s {
        Start::Leased(l, sent) => (l, sent),
        Start::Lapsed(..) => panic!("{s:?}"),
    }
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
    let (first, _) = leased(lease::register(&mut k, &link, &data).await.unwrap());
    assert_eq!((first.node, first.epoch), (1, 1));
    assert_eq!(first.ttl, Duration::from_secs(3));
    assert_eq!(lease::read(&data.join(lease::FILE)), Some((1, 1)));

    // Back within its lease, with the epoch in its file: the same epoch, so the cells stay.
    let (again, sent) = leased(lease::register(&mut k, &link, &data).await.unwrap());
    assert_eq!(again, first);
    let renewing = CancellationToken::new();
    let file = data.join(lease::FILE);
    let held = tokio::spawn(lease::keep(k, again, sent, file, renewing.clone()));

    // The same name with no file, like a machine that lost its disk, waits while the comb that
    // holds the node renews its lease, and gets a new epoch once that comb stops and its lease
    // runs out.
    let fresh = dir.0.join("fresh");
    std::fs::create_dir_all(&fresh).unwrap();
    let mut k = Keeper::new(&link.members).unwrap();
    let second = tokio::spawn(async move { lease::register(&mut k, &link, &fresh).await });
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(!second.is_finished());
    renewing.cancel();
    held.await.unwrap().unwrap();
    let stopped = Instant::now();
    let (second, _) = leased(second.await.unwrap().unwrap());
    assert_eq!((second.node, second.epoch), (1, 2));
    let took = stopped.elapsed();
    assert!(took >= Duration::from_secs(1) && took < Duration::from_secs(10), "{took:?}");
    stop.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_comb_that_cannot_register_before_its_last_lease_runs_out_says_so() {
    let dir = Dir::new("lapse");
    let file = dir.0.join(lease::FILE);
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap();
    let until = u64::try_from(now.as_millis()).unwrap() + 1200;
    std::fs::write(&file, format!("4 2 {until}\n")).unwrap();
    // Nothing listens on port 1.
    let link = link("127.0.0.1:1", "node-a");
    let mut k = Keeper::new(&link.members).unwrap();
    let started = Instant::now();
    assert_eq!(lease::register(&mut k, &link, &dir.0).await.unwrap(), Start::Lapsed(4, 2));
    let took = started.elapsed();
    assert!(took >= Duration::from_millis(1200) && took < Duration::from_secs(4), "{took:?}");

    // Once the comb has stopped those cells, the next start waits for as long as it takes.
    lease::lapsed(&file).unwrap();
    assert_eq!(lease::read(&file), Some((4, 2)));
    let wait = lease::register(&mut k, &link, &dir.0);
    assert!(tokio::time::timeout(Duration::from_secs(2), wait).await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_comb_that_cannot_reach_the_keeper_stops_when_its_lease_runs_out() {
    // Nothing listens on port 1.
    let k = Keeper::new(&["127.0.0.1:1".to_owned()]).unwrap();
    let l = Lease { node: 4, epoch: 2, ttl: Duration::from_millis(1200) };
    let started = Instant::now();
    let dir = Dir::new("renew");
    let file = dir.0.join(lease::FILE);
    let since = tokio::time::Instant::now();
    let e = lease::keep(k, l, since, file, CancellationToken::new()).await.unwrap_err();
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
