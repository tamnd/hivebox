//! Measures a directory store and a bucket side by side with the same blobs.
//!
//! ```text
//! cargo run --release -p hive-nectar --example stores -- DIR S3URL BLOBS MIB
//! stores /tmp/st http://127.0.0.1:9000/bucket/bench 8 64
//! ```
//!
//! It makes `BLOBS` random blobs of `MIB` MiB under `DIR`, puts them in a `PosixStore` in `DIR`
//! and in the bucket, then for each store fetches them all into an empty L1 cache three times and
//! does 200 random reads of 4 KiB and of 1 MiB. The bucket's blobs are deleted at the end. The
//! keys come from `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use hive_nectar::{BlobId, BlobStore, Cache, PosixStore, ReadReq, S3Config, S3Store};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [dir, url, blobs, mib] = &args[..] else {
        eprintln!("usage: stores DIR S3URL BLOBS MIB");
        std::process::exit(2);
    };
    let dir = PathBuf::from(dir);
    let count: u64 = blobs.parse().expect("BLOBS");
    let size = mib.parse::<usize>().expect("MIB") << 20;
    let posix = PosixStore::open(dir.join("store")).unwrap();
    let s3 = S3Store::new(S3Config::from_url_and_env(url).unwrap()).unwrap();
    let src = dir.join("src");
    std::fs::create_dir_all(&src).unwrap();
    let mut ids = Vec::new();
    for i in 0..count {
        let bytes = random(size, u64::from(std::process::id()) * 1000 + i);
        let id = BlobId::from(blake3::hash(&bytes));
        std::fs::write(src.join(id.to_string()), bytes).unwrap();
        ids.push(id);
    }
    let total = (count * size as u64) as f64 / f64::from(1 << 20);
    for (name, store) in [("posix", &posix as &dyn BlobStore), ("s3", &s3)] {
        let t = Instant::now();
        for id in &ids {
            store.put(*id, &src.join(id.to_string())).await.unwrap();
        }
        println!("{name} put: {total:.0} MiB in {:.2?}, {:.0} MiB/s", t.elapsed(), rate(total, t));
    }
    for (name, store) in [("posix", &posix as &dyn BlobStore), ("s3", &s3)] {
        for round in 1..=3 {
            let cache_dir = dir.join(format!("cache-{name}"));
            let _ = std::fs::remove_dir_all(&cache_dir);
            let cache = Cache::open(&cache_dir, 1 << 40).unwrap();
            let t = Instant::now();
            futures::future::try_join_all(ids.iter().map(|id| cache.get(store, *id)))
                .await
                .unwrap();
            println!(
                "{name} fetch {round}: {total:.0} MiB in {:.2?}, {:.0} MiB/s",
                t.elapsed(),
                rate(total, t)
            );
            drop(cache);
            std::fs::remove_dir_all(&cache_dir).unwrap();
        }
        for len in [4 << 10, 1 << 20] {
            let mut took = Vec::new();
            let mut x = 0x9e37_79b9_7f4a_7c15_u64;
            for _ in 0..200 {
                x = next(x);
                let id = ids[(x % count) as usize];
                let offset = (x >> 16) % (size - len) as u64;
                let t = Instant::now();
                let got = store.read_vectored(id, vec![ReadReq { offset, buf: vec![0; len] }]);
                got.await.unwrap();
                took.push(t.elapsed());
            }
            println!("{name} read {} KiB: {}", len >> 10, percentiles(&mut took));
        }
    }
    for id in &ids {
        s3.delete(*id).await.unwrap();
    }
    remove(&src);
}

fn rate(mib: f64, since: Instant) -> f64 {
    mib / since.elapsed().as_secs_f64()
}

fn next(mut x: u64) -> u64 {
    x ^= x << 13;
    x ^= x >> 7;
    x ^ (x << 17)
}

fn random(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        x = next(x);
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(len);
    out
}

fn percentiles(took: &mut [Duration]) -> String {
    took.sort_unstable();
    let at = |p: usize| took[(took.len() * p / 100).min(took.len() - 1)].as_secs_f64() * 1000.0;
    format!("p50 {:.2} ms, p99 {:.2} ms", at(50), at(99))
}

fn remove(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}
