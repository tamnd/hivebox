//! Runs [`S3Store`] against a real bucket, when `HB_S3_URL` names one, as in
//! `http://127.0.0.1:9000/bucket/test`, with `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` set.
//! Without it the test passes having done nothing.

use std::io;
use std::path::PathBuf;

use hive_nectar::{BlobId, BlobStore, ReadReq, S3Config, S3Store, s3};

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
