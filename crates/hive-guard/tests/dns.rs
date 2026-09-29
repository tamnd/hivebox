//! What the DNS proxy costs, against a resolver on loopback that answers at once, so the numbers
//! are the proxy's own. Run with
//! `cargo test --release -p hive-guard --test dns -- --ignored --nocapture`, with `CLIENTS` to
//! change how many cells ask at once and `QUERIES` how many each one asks.

use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use hickory_proto::op::{Message, MessageType, OpCode, Query};
use hickory_proto::rr::rdata::A;
use hickory_proto::rr::{Name, RData, Record, RecordType};
use hive_guard::Profile;
use hive_guard::dns::{Cells, Policy, Proxy, Settings};
use tokio::net::UdpSocket;

/// Every address on loopback is a cell, numbered by its last two bytes.
#[derive(Default)]
struct Loopback {
    allowed: AtomicU64,
}

impl Cells for Loopback {
    fn cell(&self, ip: Ipv4Addr) -> Option<(u32, Profile)> {
        let [a, _, c, d] = ip.octets();
        (a == 127).then_some(((u32::from(c) << 8) | u32::from(d), Profile(16)))
    }

    fn allow(&self, _: u32, ips: &[Ipv4Addr], _: Duration) -> io::Result<()> {
        self.allowed.fetch_add(ips.len() as u64, Ordering::Relaxed);
        Ok(())
    }
}

async fn resolver() -> SocketAddr {
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = sock.local_addr().unwrap();
    tokio::spawn(async move {
        let mut buf = vec![0u8; 4096];
        loop {
            let (n, from) = sock.recv_from(&mut buf).await.unwrap();
            let q = Message::from_vec(&buf[..n]).unwrap();
            let mut r = Message::response(q.metadata.id, OpCode::Query);
            r.queries.clone_from(&q.queries);
            let name = q.queries[0].name.clone();
            r.answers.push(Record::from_rdata(name, 300, RData::A(A::new(151, 101, 0, 223))));
            let _ = sock.send_to(&r.to_vec().unwrap(), from).await;
        }
    });
    addr
}

/// Asks `to` for `queries` names from `from`, one at a time, and returns each round trip.
async fn client(from: Ipv4Addr, to: SocketAddr, queries: usize) -> Vec<Duration> {
    let sock = UdpSocket::bind((from, 0)).await.unwrap();
    sock.connect(to).await.unwrap();
    let mut buf = vec![0u8; 4096];
    let mut took = Vec::with_capacity(queries);
    for i in 0..queries {
        let mut m = Message::new(i as u16, MessageType::Query, OpCode::Query);
        let name = Name::from_ascii(format!("h{i}.files.pythonhosted.org.")).unwrap();
        m.queries.push(Query::query(name, RecordType::A));
        let bytes = m.to_vec().unwrap();
        let t = Instant::now();
        sock.send(&bytes).await.unwrap();
        let n = tokio::time::timeout(Duration::from_secs(5), sock.recv(&mut buf))
            .await
            .expect("an answer within 5s")
            .unwrap();
        took.push(t.elapsed());
        let r = Message::from_vec(&buf[..n]).unwrap();
        assert_eq!(r.answers.len(), 1, "{r:?}");
    }
    took
}

async fn run(to: SocketAddr, clients: usize, queries: usize) -> (Duration, Vec<Duration>) {
    let t = Instant::now();
    let tasks: Vec<_> = (0..clients)
        .map(|i| {
            let from = Ipv4Addr::from(0x7f00_0001 + i as u32);
            tokio::spawn(client(from, to, queries))
        })
        .collect();
    let mut all = Vec::new();
    for task in tasks {
        all.extend(task.await.unwrap());
    }
    let wall = t.elapsed();
    all.sort();
    (wall, all)
}

fn report(what: &str, wall: Duration, took: &[Duration]) {
    let at = |p: f64| took[((took.len() as f64 * p) as usize).min(took.len() - 1)];
    println!(
        "{what}: {} queries in {wall:.2?}, {:.0}/s, p50 {:.0?} p99 {:.0?} max {:.0?}",
        took.len(),
        took.len() as f64 / wall.as_secs_f64(),
        at(0.5),
        at(0.99),
        took[took.len() - 1],
    );
}

fn env(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, not a test"]
async fn proxy_throughput() {
    let (clients, queries) = (env("CLIENTS", 64), env("QUERIES", 2000));
    let upstream = resolver().await;
    let cells = Arc::new(Loopback::default());
    let settings = Settings {
        upstream: vec![upstream],
        policies: [(Profile(16), Policy::new(&["pypi.org", "*.pythonhosted.org"]).unwrap())].into(),
        // Each client here asks as fast as it can, far past what a cell may.
        rate: u32::MAX,
        burst: u32::MAX,
        ..Settings::default()
    };
    let proxy = Arc::new(Proxy::new(settings, cells.clone()));
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = sock.local_addr().unwrap();
    tokio::spawn(proxy.serve(sock));

    let (wall, took) = run(upstream, 1, 1000).await;
    report("resolver alone, one client", wall, &took);
    let (wall, took) = run(addr, 1, 1000).await;
    report("through the proxy, one client", wall, &took);
    let (wall, took) = run(upstream, clients, queries).await;
    report(&format!("resolver alone, {clients} clients"), wall, &took);
    let (wall, took) = run(addr, clients, queries).await;
    report(&format!("through the proxy, {clients} clients"), wall, &took);
    assert_eq!(cells.allowed.load(Ordering::Relaxed), (1000 + clients * queries) as u64);
}

/// Real names through the host's own resolvers, with and without the proxy in front.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, not a test"]
async fn real_names() {
    let upstream =
        hive_guard::dns::upstreams(&std::fs::read_to_string("/etc/resolv.conf").unwrap());
    println!("resolvers {upstream:?}");
    let cells = Arc::new(Loopback::default());
    let names = ["pypi.org", "files.pythonhosted.org", "github.com", "example.com"];
    let settings = Settings {
        upstream: upstream.clone(),
        policies: [(Profile(16), Policy::new(&names).unwrap())].into(),
        ..Settings::default()
    };
    let proxy = Arc::new(Proxy::new(settings, cells.clone()));
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = sock.local_addr().unwrap();
    tokio::spawn(proxy.serve(sock));
    let sock = UdpSocket::bind("127.0.0.2:0").await.unwrap();
    let mut buf = vec![0u8; 4096];
    for (i, name) in names.into_iter().chain(["pypi.org.evil.example", "localhost"]).enumerate() {
        for (via, to) in [("direct", upstream[0]), ("proxy", addr)] {
            let mut m = Message::new(i as u16, MessageType::Query, OpCode::Query);
            m.metadata.recursion_desired = true;
            m.queries.push(Query::query(Name::from_ascii(name).unwrap(), RecordType::A));
            let t = Instant::now();
            sock.send_to(&m.to_vec().unwrap(), to).await.unwrap();
            let (n, _) = sock.recv_from(&mut buf).await.unwrap();
            let took = t.elapsed();
            let r = Message::from_vec(&buf[..n]).unwrap();
            let ips: Vec<String> = r
                .answers
                .iter()
                .filter_map(
                    |a| if let RData::A(a) = &a.data { Some(a.0.to_string()) } else { None },
                )
                .collect();
            let ttl = r.answers.iter().map(|a| a.ttl).min();
            println!(
                "{name} {via}: {:?} in {took:.2?}, ttl {ttl:?}, {}",
                r.metadata.response_code,
                ips.join(" ")
            );
        }
    }
}
