//! Measures a lazy fill against a whole fetch of one big blob from a bucket.
//!
//! ```text
//! cargo run --release -p hive-nectar --example lazy -- DIR S3URL MIB
//! lazy /tmp/lz http://127.0.0.1:9000/bucket/bench 1024
//! ```
//!
//! It makes one random blob of `MIB` MiB and its leaves under `DIR` and puts both in the bucket.
//! Then, three times each and into an empty cache every time, it fetches the blob whole, and it
//! opens it lazily, reads 4 KiB from the middle, does 200 random reads of 4 KiB and fills in the
//! rest. The blobs are deleted at the end. The keys come from `AWS_ACCESS_KEY_ID` and
//! `AWS_SECRET_ACCESS_KEY`.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hive_nectar::{BlobId, BlobStore, Cache, Leaves, S3Config, S3Store};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [dir, url, mib] = &args[..] else {
        eprintln!("usage: lazy DIR S3URL MIB");
        std::process::exit(2);
    };
    let dir = PathBuf::from(dir);
    let size = mib.parse::<u64>().expect("MIB") << 20;
    let s3: Arc<dyn BlobStore> =
        Arc::new(S3Store::new(S3Config::from_url_and_env(url).unwrap()).unwrap());
    std::fs::create_dir_all(&dir).unwrap();
    let src = dir.join("blob");
    write_random(&src, size, u64::from(std::process::id()));
    let leaves = Leaves::of_file(&src).unwrap();
    let blob = leaves.blob();
    let leaves_path = dir.join("leaves");
    std::fs::write(&leaves_path, leaves.to_bytes()).unwrap();
    let leaves_id = BlobId::of(&leaves.to_bytes());
    s3.put(blob, &src).await.unwrap();
    s3.put(leaves_id, &leaves_path).await.unwrap();
    std::fs::remove_file(&src).unwrap();
    let total = size as f64 / f64::from(1 << 20);
    let cache_dir = dir.join("cache");
    for round in 1..=3 {
        let _ = std::fs::remove_dir_all(&cache_dir);
        let cache = Cache::open(&cache_dir, 1 << 40).unwrap();
        let t = Instant::now();
        cache.get(&*s3, blob).await.unwrap();
        println!(
            "whole {round}: {total:.0} MiB in {:.2?}, {:.0} MiB/s",
            t.elapsed(),
            rate(total, t)
        );
        drop(cache);

        let _ = std::fs::remove_dir_all(&cache_dir);
        let cache = Cache::open(&cache_dir, 1 << 40).unwrap();
        let t = Instant::now();
        let lazy = cache.lazy(s3.clone(), blob, size, leaves_id).await.unwrap();
        let opened = t.elapsed();
        lazy.read_at(size / 2, 4096).await.unwrap();
        let first = t.elapsed();
        let mut took = Vec::new();
        let mut x = 0x9e37_79b9_7f4a_7c15_u64 ^ round;
        for _ in 0..200 {
            x = next(x);
            let r = Instant::now();
            lazy.read_at(x % (size - 4096), 4096).await.unwrap();
            took.push(r.elapsed());
        }
        let random = lazy.progress();
        let f = Instant::now();
        lazy.fill_rest(&[]).await.unwrap();
        let p = lazy.progress();
        println!(
            "lazy {round}: open {opened:.2?}, first 4 KiB {first:.2?}, 200 random 4 KiB {} \
             fetching {} chunks in {} requests, rest {:.2?} at {:.0} MiB/s, {} requests and {} MiB in all",
            percentiles(&mut took),
            random.have,
            random.requests,
            f.elapsed(),
            rate((p.have - random.have) as f64 * 0.25, f),
            p.requests,
            p.bytes >> 20,
        );
        assert!(lazy.is_complete());
        drop(lazy);
        drop(cache);
    }
    std::fs::remove_dir_all(&dir).unwrap();
    s3.delete(blob).await.unwrap();
    s3.delete(leaves_id).await.unwrap();
}

fn rate(mib: f64, since: Instant) -> f64 {
    mib / since.elapsed().as_secs_f64()
}

fn next(mut x: u64) -> u64 {
    x ^= x << 13;
    x ^= x >> 7;
    x ^ (x << 17)
}

fn write_random(path: &std::path::Path, len: u64, seed: u64) {
    use std::io::Write;
    let mut f = std::io::BufWriter::new(std::fs::File::create(path).unwrap());
    let mut x = seed | 1;
    for _ in 0..len / 8 {
        x = next(x);
        f.write_all(&x.to_le_bytes()).unwrap();
    }
    f.flush().unwrap();
}

fn percentiles(took: &mut [Duration]) -> String {
    took.sort_unstable();
    let at = |p: usize| took[(took.len() * p / 100).min(took.len() - 1)].as_secs_f64() * 1000.0;
    format!("p50 {:.2} ms, p99 {:.2} ms", at(50), at(99))
}
