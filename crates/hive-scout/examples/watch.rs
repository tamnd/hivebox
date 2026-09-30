//! Follows a running scout for a while and prints what the deltas cost against sending the
//! whole cluster every time. Run it next to `load` to see a busy cluster.
//!
//! ```text
//! cargo run --release -p hive-scout --example watch -- http://127.0.0.1:7410 60
//! ```
//!
//! The arguments are the scout's address and the seconds to follow it.

use std::time::{Duration, Instant};

use hive_proto::internal as pb;
use hive_proto::internal::scout_client::ScoutClient;
use hive_scout::Mirror;
use prost::Message;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let addr = args.get(1).cloned().unwrap_or_else(|| "http://127.0.0.1:7410".into());
    let secs: u64 = args.get(2).map_or(30, |a| a.parse().expect("seconds"));
    let channel = tonic::transport::Endpoint::from_shared(addr)
        .expect("address")
        .tcp_nodelay(true)
        .connect()
        .await
        .expect("connect");
    let mut stream = ScoutClient::new(channel)
        .max_decoding_message_size(usize::MAX)
        .watch(pb::WatchRequest {})
        .await
        .expect("watch")
        .into_inner();
    let mut mirror = Mirror::new();
    let first = stream.message().await.expect("first").expect("first");
    let (full_bytes, full_nodes) = (first.encoded_len(), first.nodes.len());
    mirror.apply(first).expect("apply");
    let end = Instant::now() + Duration::from_secs(secs);
    let (mut deltas, mut bytes, mut nodes, mut layers, mut removed) =
        (0u64, 0u64, 0u64, 0u64, 0u64);
    let mut apply = Vec::new();
    let mut whole = 0u64;
    while let Ok(Ok(Some(d))) = tokio::time::timeout_at(end.into(), stream.message()).await {
        deltas += 1;
        bytes += d.encoded_len() as u64;
        nodes += d.nodes.len() as u64;
        layers += d.nodes.iter().filter(|n| !n.layers.is_empty()).count() as u64;
        removed += d.removed.len() as u64;
        let t = Instant::now();
        let snap = mirror.apply(d).expect("apply");
        apply.push(t.elapsed());
        // What this snapshot would cost sent whole: every node with its filter.
        whole += snap.view.nodes.len() as u64 * (full_bytes / full_nodes.max(1)) as u64;
    }
    apply.sort();
    let at = |q: f64| {
        let i = ((apply.len().max(1) - 1) as f64 * q) as usize;
        apply.get(i).map_or(0.0, |d| d.as_secs_f64() * 1e6)
    };
    let per = |n: u64| n as f64 / secs as f64;
    println!(
        "first message {full_nodes} nodes {full_bytes} bytes; {deltas} deltas in {secs} s, {:.1} a second, {:.0} bytes a second, {:.1} nodes and {:.2} filters a delta, {removed} removed; sent whole it would be {:.0} bytes a second; mirror apply us p50 {:.0} p99 {:.0} max {:.0}",
        per(deltas),
        per(bytes),
        nodes as f64 / deltas.max(1) as f64,
        layers as f64 / deltas.max(1) as f64,
        per(whole),
        at(0.5),
        at(0.99),
        at(1.0),
    );
}
