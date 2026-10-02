//! The `hive-nectar` binary, for putting images into a store and fetching them onto a node by
//! hand.
//!
//! ```text
//! hive-nectar import-oci LAYOUT --store DIR     # an OCI image layout, as `docker save` writes
//! hive-nectar import-tar TAR --store DIR        # a flat root filesystem, as `docker export` writes
//! hive-nectar show IMAGE --store DIR
//! hive-nectar fetch IMAGE --store DIR --cache DIR [--capacity BYTES]
//! hive-nectar copy IMAGE --store DIR --to-s3 URL      # an image and its layers, to a bucket
//! ```
//!
//! Imports take `--mkfs PATH` for a `mkfs.erofs` that is not on the path, and `--chunk BYTES` for
//! the layer chunk size. They remember built layers in `--work DIR`, which is `STORE/import` by
//! default.
//!
//! In place of `--store DIR`, `--s3 URL` uses a bucket, as in `http://10.0.0.5:9000/bucket/prefix`,
//! signing as `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in `AWS_REGION`. Imports to a bucket
//! need `--work DIR`.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

use hive_nectar::erofs::{DEFAULT_CHUNK_SIZE, Mkfs};
use hive_nectar::oci::{Importer, Platform, load_manifest};
use hive_nectar::{BlobId, BlobStore, Cache, PosixStore, S3Config, S3Store};

const USAGE: &str = "usage: hive-nectar import-oci LAYOUT | import-tar TAR | show IMAGE | fetch IMAGE \
                     | copy IMAGE --store DIR | --s3 URL [--work DIR] [--mkfs PATH] [--chunk BYTES] \
                     [--cache DIR] [--capacity BYTES] [--to-s3 URL]";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (Some(cmd), Some(what)) = (args.first(), args.get(1)) else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    let mut flags = HashMap::new();
    let mut rest = args[2..].iter();
    while let Some(flag) = rest.next() {
        match (flag.strip_prefix("--"), rest.next()) {
            (Some(name), Some(value)) => {
                flags.insert(name.to_owned(), value.clone());
            }
            _ => {
                eprintln!("{USAGE}");
                return ExitCode::from(2);
            }
        }
    }
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("a runtime");
    match rt.block_on(run(cmd, what, &flags)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("hive-nectar: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cmd: &str, what: &str, flags: &HashMap<String, String>) -> Result<(), String> {
    let (store, store_dir): (Box<dyn BlobStore>, Option<PathBuf>) =
        match (flags.get("store"), flags.get("s3")) {
            (Some(dir), None) => {
                let dir = PathBuf::from(dir);
                let store =
                    PosixStore::open(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
                (Box::new(store), Some(dir))
            }
            (None, Some(url)) => {
                let cfg = S3Config::from_url_and_env(url).map_err(|e| e.to_string())?;
                (Box::new(S3Store::new(cfg).map_err(|e| e.to_string())?), None)
            }
            _ => return Err("one of --store or --s3 is needed".into()),
        };
    let store = store.as_ref();
    let number = |name: &str, default: u64| {
        flags
            .get(name)
            .map_or(Ok(default), |v| v.parse().map_err(|_| format!("--{name} takes a number")))
    };
    match cmd {
        "import-oci" | "import-tar" => {
            let chunk = u32::try_from(number("chunk", DEFAULT_CHUNK_SIZE.into())?)
                .map_err(|_| "--chunk is too big")?;
            let program = flags.get("mkfs").map_or_else(|| "mkfs.erofs".into(), PathBuf::from);
            let mkfs = Mkfs::new(program, chunk).map_err(|e| e.to_string())?;
            let work = match (flags.get("work"), &store_dir) {
                (Some(w), _) => PathBuf::from(w),
                (None, Some(dir)) => dir.join("import"),
                (None, None) => return Err("an import to a bucket needs --work".into()),
            };
            let importer = Importer::new(mkfs, work).map_err(|e| e.to_string())?;
            let started = Instant::now();
            let path = PathBuf::from(what);
            let got = if cmd == "import-oci" {
                importer.import_layout(store, &path, &Platform::default()).await
            } else {
                importer.import_tar(store, &path).await
            }
            .map_err(|e| format!("importing {what}: {e}"))?;
            let m = &got.manifest;
            println!("{}", got.id);
            eprintln!(
                "{} layers, {} built and {} already there, in {:.2?}: {} of metadata and {} of data",
                m.layers.len(),
                got.built,
                got.reused,
                started.elapsed(),
                mib(m.layers.iter().map(|l| l.meta_size).sum()),
                mib(m.layers.iter().map(|l| l.data_size).sum()),
            );
            Ok(())
        }
        "show" => {
            let id: BlobId = what.parse().map_err(|e| format!("{what}: {e}"))?;
            let m = load_manifest(store, id).await.map_err(|e| format!("image {id}: {e}"))?;
            println!("{}", serde_json::to_string_pretty(&m).expect("a manifest serializes"));
            Ok(())
        }
        "fetch" => {
            let id: BlobId = what.parse().map_err(|e| format!("{what}: {e}"))?;
            let cache_dir = PathBuf::from(flags.get("cache").ok_or("--cache is needed")?);
            let cache = Cache::open(&cache_dir, number("capacity", 64 << 30)?)
                .map_err(|e| format!("{}: {e}", cache_dir.display()))?;
            let started = Instant::now();
            let m = load_manifest(store, id).await.map_err(|e| format!("image {id}: {e}"))?;
            let blobs: Vec<BlobId> = m.layers.iter().flat_map(|l| [l.meta, l.data]).collect();
            let held = futures::future::try_join_all(blobs.iter().map(|b| cache.get(store, *b)))
                .await
                .map_err(|e| e.to_string())?;
            let took = started.elapsed();
            let bytes: u64 = held.iter().map(hive_nectar::Held::size).sum();
            eprintln!(
                "{} blobs, {} in {took:.2?}, {:.0} MiB/s",
                held.len(),
                mib(bytes),
                bytes as f64 / (1 << 20) as f64 / took.as_secs_f64()
            );
            for h in &held {
                println!("{}", h.path().display());
            }
            Ok(())
        }
        "copy" => {
            let id: BlobId = what.parse().map_err(|e| format!("{what}: {e}"))?;
            let from = PosixStore::open(store_dir.ok_or("copy reads from --store DIR")?)
                .map_err(|e| e.to_string())?;
            let url = flags.get("to-s3").ok_or("--to-s3 is needed")?;
            let to = S3Config::from_url_and_env(url)
                .and_then(S3Store::new)
                .map_err(|e| e.to_string())?;
            let started = Instant::now();
            let m = load_manifest(&from, id).await.map_err(|e| format!("image {id}: {e}"))?;
            let mut blobs: Vec<BlobId> = m.layers.iter().flat_map(|l| [l.meta, l.data]).collect();
            // The manifest goes last, so an image in the bucket always has all its layers.
            blobs.push(id);
            let mut sent = 0;
            for b in blobs {
                let put = to.put(b, &from.path(b)).await.map_err(|e| format!("{b}: {e}"))?;
                if !put.existed {
                    sent += put.size;
                }
            }
            let took = started.elapsed();
            eprintln!(
                "{} sent in {took:.2?}, {:.0} MiB/s",
                mib(sent),
                sent as f64 / (1 << 20) as f64 / took.as_secs_f64()
            );
            Ok(())
        }
        _ => Err(USAGE.into()),
    }
}

fn mib(bytes: u64) -> String {
    format!("{:.1} MiB", bytes as f64 / (1 << 20) as f64)
}
