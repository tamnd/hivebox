//! Measures how long the chunks a trace names take to come in, from the data blob in place and
//! from a relaid copy, with nothing else running between the reads.
//!
//! ```text
//! cargo run --release -p hive-nectar --example relayout -- DIR S3URL IMAGE ROUNDS
//! relayout /tmp/rl http://127.0.0.1:9000/bucket/prefix 1246...f5f5 5
//! ```
//!
//! `IMAGE` is a relaid image, as `hive-nectar relayout` prints. Each round, into a new cache
//! under `DIR` every time, it mounts the data of every traced layer three ways, the way a lazy
//! mount does: filled in order with no trace, filled with the trace leading, and filled from the
//! relaid copy. Each time it then reads 4 KiB from every traced chunk in trace order, one read
//! after another, the way a program that only reads would, and says how long that took and how
//! many reads of the store had been made by then. The keys come from `AWS_ACCESS_KEY_ID` and
//! `AWS_SECRET_ACCESS_KEY`.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hive_nectar::cache::CHUNK;
use hive_nectar::oci::load_manifest;
use hive_nectar::{BlobId, BlobStore, Cache, LayerRef, Relaid, S3Config, S3Store};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [dir, url, image, rounds] = &args[..] else {
        eprintln!("usage: relayout DIR S3URL IMAGE ROUNDS");
        std::process::exit(2);
    };
    let dir = PathBuf::from(dir);
    let s3: Arc<dyn BlobStore> =
        Arc::new(S3Store::new(S3Config::from_url_and_env(url).unwrap()).unwrap());
    let m = load_manifest(&*s3, image.parse().unwrap()).await.unwrap();
    let mut layers = Vec::new();
    for l in m.layers.iter().filter(|l| l.data_relaid.is_some()) {
        let trace = hive_nectar::trace::load(&*s3, l.data_trace.unwrap()).await.unwrap();
        let order = hive_nectar::trace::load(&*s3, l.data_order.unwrap()).await.unwrap();
        layers.push((l.clone(), trace, order));
    }
    let chunks: usize = layers.iter().map(|(_, t, _)| t.len()).sum();
    println!("{} traced layers, {chunks} traced chunks", layers.len());
    for round in 1..=rounds.parse().unwrap() {
        for how in ["in order", "trace first", "relaid"] {
            // A cache of its own each time, so fetches the last one left running land elsewhere.
            let cache_dir = dir.join(format!("cache-{round}-{}", how.replace(' ', "-")));
            let cache = Cache::open(&cache_dir, 1 << 40).unwrap();
            let t = Instant::now();
            let each =
                layers.iter().map(|(l, trace, order)| one(&cache, &s3, l, trace, order, how));
            let got = futures::future::join_all(each).await;
            let took = t.elapsed();
            let reads: u64 = got.iter().map(|(r, _, _)| r).sum();
            let bytes: u64 = got.iter().map(|(_, b, _)| b).sum();
            let slowest = got.iter().map(|(_, _, d)| *d).max().unwrap_or_default();
            println!(
                "{round} {how}: traced chunks in after {took:.2?} (slowest layer {slowest:.2?}), \
                 {reads} reads and {:.1} MiB fetched by then",
                bytes as f64 / f64::from(1 << 20)
            );
            drop(cache);
            let _ = std::fs::remove_dir_all(&cache_dir);
        }
    }
    std::fs::remove_dir_all(&dir).unwrap();
}

/// One layer: reads, bytes and time until its traced chunks were read.
async fn one(
    cache: &Arc<Cache>,
    s3: &Arc<dyn BlobStore>,
    l: &LayerRef,
    trace: &[u64],
    order: &[u64],
    how: &str,
) -> (u64, u64, Duration) {
    let t = Instant::now();
    let relaid =
        (how == "relaid").then(|| Relaid { blob: l.data_relaid.unwrap(), order: order.to_vec() });
    let leaves: BlobId = l.data_leaves.unwrap();
    let lazy = cache.lazy_from(s3.clone(), l.data, l.data_size, leaves, relaid).await.unwrap();
    let first = match how {
        "in order" => Vec::new(),
        "trace first" => trace.to_vec(),
        _ => order.to_vec(),
    };
    let filler = lazy.clone();
    let fill = tokio::spawn(async move { filler.fill_rest(&first).await });
    for &c in trace {
        let len = 4096.min(l.data_size - c * CHUNK) as usize;
        lazy.read_at(c * CHUNK, len).await.unwrap();
    }
    let p = lazy.progress();
    let took = t.elapsed();
    fill.abort();
    (p.requests, p.bytes, took)
}
