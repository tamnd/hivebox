//! Streams made up node reports at a running scout, one connection per node, to see what the
//! service costs with many nodes over real sockets. Read the scout's CPU time from `/proc`
//! around a run.
//!
//! ```text
//! cargo run --release -p hive-scout --example load -- http://127.0.0.1:7410 1000 60
//! ```
//!
//! The arguments are the scout's address, the number of nodes, and the seconds to run. Nodes
//! are numbered from 1, so a comb reporting as node 0 to the same scout is left alone. It prints
//! how long scout took to answer each report, and its own CPU time, since on a shared host the
//! sender can fall behind before scout does.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::StreamExt;
use hive_proto::internal as pb;
use hive_proto::internal::scout_client::ScoutClient;
use hive_scout::NodeReport;
use hive_types::Backend;
use hive_waggle::{BackendSet, LayerBloom};
use rustix::time::{ClockId, clock_gettime};

/// Reports sent in the first seconds, while connections are still coming up, are left out of
/// the answer times.
const WARMUP: Duration = Duration::from_secs(5);

#[derive(Default)]
struct Counts {
    sent: AtomicU64,
    acked: AtomicU64,
    stale: AtomicU64,
    /// Reports sent more than 1.5 s after the one before, which is the sender falling behind.
    late: AtomicU64,
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let addr = args.get(1).cloned().unwrap_or_else(|| "http://127.0.0.1:7410".into());
    let nodes: u16 = args.get(2).map_or(100, |a| a.parse().expect("nodes"));
    let secs: u64 = args.get(3).map_or(30, |a| a.parse().expect("seconds"));
    let endpoint =
        tonic::transport::Endpoint::from_shared(addr).expect("address").tcp_nodelay(true);
    let counts = Arc::new(Counts::default());
    let answers = Arc::new(Mutex::new(Vec::new()));
    let start = Instant::now();
    let end = start + Duration::from_secs(secs);
    let mut tasks = Vec::new();
    for node in 1..=nodes {
        let (endpoint, counts, answers) = (endpoint.clone(), counts.clone(), answers.clone());
        tasks.push(tokio::spawn(async move {
            // Nodes start spread over the first second, as real combs would be.
            let offset = u64::from(node) * 1_000_000 / u64::from(nodes);
            tokio::time::sleep(Duration::from_micros(offset)).await;
            // One connection per node, as each comb has its own.
            let mut client = ScoutClient::new(endpoint.connect().await.expect("connect"));
            let mut bloom = LayerBloom::default();
            for i in 0..64u16 {
                bloom.insert(blake3::hash(&(node ^ i).to_le_bytes()).as_bytes());
            }
            let pending = Arc::new(Mutex::new(VecDeque::new()));
            let (sending, counted) = (pending.clone(), counts.clone());
            // Each report is due a second after the one before was due, so a slow wakeup does
            // not push the rest back.
            let first = tokio::time::Instant::now();
            let out = futures::stream::unfold((0u64, first, None), move |(seq, due, before)| {
                let (bloom, sending, counts) = (bloom.clone(), sending.clone(), counted.clone());
                async move {
                    tokio::time::sleep_until(due).await;
                    let now = Instant::now();
                    if now >= end {
                        return None;
                    }
                    if before.is_some_and(|b: Instant| now - b > Duration::from_millis(1500)) {
                        counts.late.fetch_add(1, Ordering::Relaxed);
                    }
                    counts.sent.fetch_add(1, Ordering::Relaxed);
                    sending.lock().unwrap().push_back(now);
                    let r = report(node, seq + 1, (seq == 0).then_some(bloom));
                    let next = (seq + 1, due + Duration::from_secs(1), Some(now));
                    Some((pb::NodeReport::from(&r), next))
                }
            });
            let mut acks = client.report(out).await.expect("report").into_inner();
            let mut mine = Vec::new();
            while let Some(a) = acks.next().await {
                let a = a.expect("ack");
                let sent = pending.lock().unwrap().pop_front().expect("an ack for a report sent");
                if sent - start >= WARMUP {
                    mine.push(sent.elapsed());
                }
                counts.acked.fetch_add(1, Ordering::Relaxed);
                if a.stale {
                    counts.stale.fetch_add(1, Ordering::Relaxed);
                }
            }
            answers.lock().unwrap().extend(mine);
        }));
    }
    for t in tasks {
        t.await.expect("stream");
    }
    let wall = start.elapsed();
    let cpu = clock_gettime(ClockId::ProcessCPUTime);
    let mut answers = std::mem::take(&mut *answers.lock().unwrap());
    answers.sort();
    let at = |q: f64| {
        let i = ((answers.len().max(1) - 1) as f64 * q) as usize;
        answers.get(i).map_or(0.0, |d| d.as_secs_f64() * 1e3)
    };
    let c = |n: &AtomicU64| n.load(Ordering::Relaxed);
    println!(
        "nodes {nodes} wall {:.1} s sent {} acked {} stale {} late {} answer ms p50 {:.2} p99 {:.2} max {:.2} sender cpu {:.1} s",
        wall.as_secs_f64(),
        c(&counts.sent),
        c(&counts.acked),
        c(&counts.stale),
        c(&counts.late),
        at(0.5),
        at(0.99),
        at(1.0),
        cpu.tv_sec as f64 + cpu.tv_nsec as f64 / 1e9,
    );
}

fn report(node: u16, seq: u64, layers: Option<LayerBloom>) -> NodeReport {
    NodeReport {
        node,
        epoch: 1,
        seq,
        addr: Arc::from(format!("10.0.{}.{}:7400", node >> 8, node & 0xff)),
        healthy: true,
        cloud: false,
        idle_cells: 0,
        idle_mem_mib: 0,
        backends: BackendSet::of(&[Backend::Container, Backend::Microvm]),
        cpu_milli: 96_000,
        cpu_committed_milli: 40_000,
        mem_admit_mib: 384 * 1024,
        mem_committed_mib: (u64::from(node) * 7919 + seq) % (384 * 1024),
        cells: (u32::from(node) * 31) % 2000,
        max_cells: 4096,
        pool_depth: 64,
        create_rate: 12.0,
        burst_cap: 300,
        layers,
        top_projects: vec![(1, 300), (2, 120), (3, 40), (4, 12)],
    }
}
