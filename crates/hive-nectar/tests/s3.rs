//! Runs [`S3Store`] against a real bucket, when `HB_S3_URL` names one, as in
//! `http://127.0.0.1:9000/bucket/test`, with `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` set.
//! Without it the test passes having done nothing.

use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use hive_nectar::cas::{self, Cuts};
use hive_nectar::erofs::{DEFAULT_CHUNK_SIZE, Writer};
use hive_nectar::gc::{self, Collect};
use hive_nectar::oci::{Imported, Importer, load_manifest};
use hive_nectar::{BlobId, BlobStore, Manifest, ReadReq, S3Config, S3Store, s3};

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("hb-s3-{}-{name}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Bytes that differ at every offset, so a read from the wrong place shows.
fn pattern(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bucket_keeps_blobs_and_reads_ranges() {
    let Ok(url) = std::env::var("HB_S3_URL") else { return };
    let store = S3Store::new(S3Config::from_url_and_env(&url).unwrap()).unwrap();
    let dir = scratch("blobs");
    let run: u64 = std::process::id().into();
    // One small enough for a single PUT and one that goes up in parts, with a short last part.
    for (len, seed) in [(3 << 20, run), (s3::SINGLE_PUT as usize + (5 << 20) + 17, run + 1)] {
        let bytes = pattern(len, seed);
        let path = dir.join(format!("{len}"));
        std::fs::write(&path, &bytes).unwrap();
        let blob = BlobId::from(blake3::hash(&bytes));

        assert_eq!(store.stat(blob).await.unwrap_err().kind(), io::ErrorKind::NotFound);
        let put = store.put(blob, &path).await.unwrap();
        assert_eq!((put.size, put.existed), (len as u64, false));
        assert!(store.put(blob, &path).await.unwrap().existed);
        assert_eq!(store.stat(blob).await.unwrap().size, len as u64);

        // Two that touch, which go as one GET, one apart, one empty and the last byte.
        let at = [(0, 4096), (4096, 100_000), (1 << 20, 7), (2 << 20, 0), (len - 1, 1)];
        let reqs = at.iter().map(|&(o, n)| ReadReq { offset: o as u64, buf: vec![0; n] }).collect();
        let got = store.read_vectored(blob, reqs).await.unwrap();
        for (r, &(o, n)) in got.iter().zip(&at) {
            assert_eq!(r.offset, o as u64);
            assert!(r.buf == bytes[o..o + n], "the read at {o} of {n} bytes differs");
        }

        let past = vec![ReadReq { offset: len as u64 - 10, buf: vec![0; 20] }];
        let err = store.read_vectored(blob, past).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof, "{err}");

        store.delete(blob).await.unwrap();
        store.delete(blob).await.unwrap();
        assert_eq!(store.stat(blob).await.unwrap_err().kind(), io::ErrorKind::NotFound);
    }

    // A file that does not hash to the name it is put under is refused.
    let path = dir.join("wrong");
    std::fs::write(&path, b"one").unwrap();
    let err = store.put(BlobId::from(blake3::hash(b"two")), &path).await.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    std::fs::remove_dir_all(dir).unwrap();
}

/// Imports a one layer image of `files` into `store`, each a name and its bytes, keeping data as
/// chunks.
async fn import(dir: &Path, store: &S3Store, name: &str, files: &[(&str, &[u8])]) -> Imported {
    let path = dir.join(format!("{name}.tar"));
    let mut b = tar::Builder::new(std::fs::File::create(&path).unwrap());
    for (file, data) in files {
        let mut h = tar::Header::new_gnu();
        h.set_entry_type(tar::EntryType::Regular);
        h.set_mode(0o644);
        h.set_mtime(1_700_000_000);
        h.set_size(data.len() as u64);
        b.append_data(&mut h, file, *data).unwrap();
    }
    b.finish().unwrap();
    let importer = Importer::new(Writer::new(DEFAULT_CHUNK_SIZE).unwrap(), dir.join("work"))
        .unwrap()
        .chunked(Cuts::default());
    importer.import_tar(store, &path).await.unwrap()
}

/// Every blob `m` needs that the store has, its recipes' chunks included.
async fn needs(store: &S3Store, m: &Manifest) -> HashSet<BlobId> {
    let mut all = HashSet::new();
    for l in &m.layers {
        for b in l.blobs() {
            if store.stat(b).await.is_ok() {
                all.insert(b);
            }
        }
        for r in [l.data_chunks, l.relaid_chunks].into_iter().flatten() {
            all.extend(cas::load(store, r).await.unwrap().chunks());
        }
    }
    all
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bucket_is_collected() {
    let Ok(url) = std::env::var("HB_S3_URL") else { return };
    let url = format!("{}/gc-{}", url.trim_end_matches('/'), std::process::id());
    let store = S3Store::new(S3Config::from_url_and_env(&url).unwrap()).unwrap();
    let dir = scratch("gc");
    let lib = pattern(6 << 20, 1);
    let (main_a, main_b) = (pattern(50_000, 2), pattern(330_000, 3));
    let a = import(&dir, &store, "a", &[("app/main.py", &main_a), ("usr/lib/big.so", &lib)]).await;
    let b = import(&dir, &store, "b", &[("app/main.py", &main_b), ("usr/lib/big.so", &lib)]).await;
    let (need_a, need_b) = (needs(&store, &a.manifest).await, needs(&store, &b.manifest).await);
    assert!(need_a.intersection(&need_b).count() > 50, "the library's chunks are shared");

    // Every blob is older than a cutoff a minute from now, so only what a needs is kept.
    let soon = SystemTime::now() + Duration::from_secs(60);
    let plan = gc::plan(&store, &[a.id], soon).await.unwrap();
    assert_eq!(plan.young, 0);
    let gone: HashSet<BlobId> = plan.remove.iter().map(|b| b.blob).collect();
    assert_eq!(gone, &(&need_b - &need_a) | &HashSet::from([b.id]));
    assert_eq!(plan.remove[0].blob, b.id);
    assert_eq!(plan.remove[1].blob, b.manifest.layers[0].data_chunks.unwrap());
    let swept = gc::sweep(&store, plan).await.unwrap();
    assert_eq!((swept.removed.0, swept.spared), (gone.len() as u64, 0));
    for blob in &gone {
        assert_eq!(store.stat(*blob).await.unwrap_err().kind(), io::ErrorKind::NotFound);
    }
    for blob in &need_a {
        assert!(store.stat(*blob).await.is_ok(), "{blob} went");
    }
    assert_eq!(load_manifest(&store, a.id).await.unwrap(), a.manifest);
    assert!(gc::plan(&store, &[a.id], soon).await.unwrap().remove.is_empty());

    // With no roots everything goes, which leaves the prefix empty.
    let all = gc::plan(&store, &[], soon).await.unwrap();
    gc::sweep(&store, all).await.unwrap();
    assert!(store.list().await.unwrap().is_empty());
    std::fs::remove_dir_all(dir).unwrap();
}
