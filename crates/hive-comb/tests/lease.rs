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
    let held = tokio::spawn(lease::keep(
        k,
        again,
        sent,
        file,
        lease::AuditLink::default(),
        renewing.clone(),
    ));

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
    // Timed from before the lease's end is set, so a slow setup can't make the wait look short.
    let started = Instant::now();
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap();
    let until = u64::try_from(now.as_millis()).unwrap() + 1200;
    std::fs::write(&file, format!("4 2 {until}\n")).unwrap();
    // Nothing listens on port 1.
    let link = link("127.0.0.1:1", "node-a");
    let mut k = Keeper::new(&link.members).unwrap();
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
    let e = lease::keep(k, l, since, file, lease::AuditLink::default(), CancellationToken::new())
        .await
        .unwrap_err();
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

#[tokio::test(flavor = "multi_thread")]
async fn a_comb_publishes_its_audit_roots_as_it_renews() {
    use hive_proto::internal as pb;
    use hive_telemetry::audit::{self, AuditEvent, AuditLog};
    use std::sync::Arc;

    let dir = Dir::new("audit");
    let stop = CancellationToken::new();
    // A long lease, so a slow disk under the keeper does not lose it before the roots go in.
    let addr = keeper(&dir, 6000, &stop).await;
    let link = link(&addr, "node-a");
    let data = dir.0.join("comb");
    std::fs::create_dir_all(&data).unwrap();

    // Three hours of events, 2026-10-07T06 to T08, so two are sealed.
    let logs = dir.0.join("audit");
    let log = Arc::new(AuditLog::open(&logs, "node-a").unwrap());
    let t0 = 1_791_352_800 * 1_000_000_000u64;
    for i in 0..150u64 {
        let ts = t0 + (i / 50) * 3_600_000_000_000 + i;
        log.record(AuditEvent { ts, op: "exec.run".into(), ..AuditEvent::default() });
    }
    log.flush().unwrap();

    let mut k = Keeper::new(&link.members).unwrap();
    let (l, sent) = leased(lease::register(&mut k, &link, &data).await.unwrap());
    let roots = lease::AuditLink::default();
    roots.set((Arc::downgrade(&log), logs.clone())).unwrap();
    let renewing = CancellationToken::new();
    let file = data.join(lease::FILE);
    let held = tokio::spawn(lease::keep(k, l, sent, file, roots, renewing.clone()));

    // The first renewal learns where the keeper is, and the next ones send the sealed hours.
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}")).unwrap();
    let mut c = pb::keeper_client::KeeperClient::new(channel.connect_lazy());
    let began = Instant::now();
    let chain = loop {
        let req = pb::GetAuditChainRequest { node: "node-a".into() };
        let got = c.get_audit_chain(req).await;
        if let Ok(c) = &got
            && c.get_ref().hours.len() == 2
        {
            break got.unwrap().into_inner();
        }
        if held.is_finished() {
            panic!("the lease was given up: {:?}", held.await.unwrap());
        }
        assert!(
            began.elapsed() < Duration::from_secs(30),
            "the roots never got to the keeper: {got:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let hours: Vec<_> =
        chain.hours.iter().map(|h| (h.hour.as_str(), h.first_seq, h.count)).collect();
    assert_eq!(hours, [("2026-10-07T06", 0, 50), ("2026-10-07T07", 50, 50)]);
    let tip = chain.tip.unwrap();
    assert_eq!((tip.hour.as_str(), tip.seq), ("2026-10-07T08", 150));

    // The chain on disk matches what the keeper holds.
    let mut anchors: Vec<(u64, [u8; 32])> = chain
        .hours
        .iter()
        .map(|h| (h.first_seq + h.count, h.root.clone().try_into().unwrap()))
        .collect();
    anchors.push((tip.seq, tip.root.try_into().unwrap()));
    assert_eq!(audit::verify_with(&logs, &anchors).unwrap().unwrap().events, 150);

    drop(log);
    renewing.cancel();
    held.await.unwrap().unwrap();
    stop.cancel();
}
