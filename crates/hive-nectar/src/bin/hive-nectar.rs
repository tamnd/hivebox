//! The `hive-nectar` binary, for putting images into a store and fetching them onto a node by
//! hand.
//!
//! ```text
//! hive-nectar import-oci LAYOUT --store DIR     # an OCI image layout, as `docker save` writes
//! hive-nectar import-tar TAR --store DIR        # a flat root filesystem, as `docker export` writes
//! hive-nectar show IMAGE --store DIR
//! hive-nectar fetch IMAGE --store DIR --cache DIR [--capacity BYTES]
//! hive-nectar copy IMAGE --store DIR --to-s3 URL      # an image and its layers, to a bucket
//! hive-nectar relayout IMAGE --store DIR [--work DIR]
//! hive-nectar run IMAGE --store DIR --cache DIR --work DIR --cmd CMD [--mode MODE]
//! ```
//!
//! `run` mounts the image as a cell would get it and runs `CMD` with `sh -c` chrooted into it,
//! then says how long the mount and the command took. It needs root. `--mode whole` fetches every
//! blob before mounting, `lazy`, the default, mounts lazily over NBD with the image's prefetch
//! traces leading the fill, and `trace` mounts lazily, records what the command read, stores it
//! as prefetch traces and prints the name of the traced image. `run` also says how many reads
//! the lazy fill made and how much it fetched by the time the command ended.
//!
//! `relayout` stores a copy of each traced layer's data with the traced chunks first and prints
//! the name of the relaid image, whose lazy mounts fetch from the copies.
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
use std::sync::Arc;
use std::time::Instant;

use hive_nectar::erofs::{DEFAULT_CHUNK_SIZE, Mkfs};
use hive_nectar::oci::{Importer, Platform, load_manifest};
use hive_nectar::{BlobId, BlobStore, Cache, PosixStore, S3Config, S3Store};

const USAGE: &str = "usage: hive-nectar import-oci LAYOUT | import-tar TAR | show IMAGE | fetch IMAGE \
                     | copy IMAGE | relayout IMAGE | run IMAGE --store DIR | --s3 URL [--work DIR] [--mkfs PATH] [--chunk BYTES] \
                     [--cache DIR] [--capacity BYTES] [--to-s3 URL] [--cmd CMD] [--mode whole|lazy|trace]";

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
    let (shared, store_dir): (Arc<dyn BlobStore>, Option<PathBuf>) =
        match (flags.get("store"), flags.get("s3")) {
            (Some(dir), None) => {
                let dir = PathBuf::from(dir);
                let store =
                    PosixStore::open(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
                (Arc::new(store), Some(dir))
            }
            (None, Some(url)) => {
                let cfg = S3Config::from_url_and_env(url).map_err(|e| e.to_string())?;
                (Arc::new(S3Store::new(cfg).map_err(|e| e.to_string())?), None)
            }
            _ => return Err("one of --store or --s3 is needed".into()),
        };
    let store = shared.as_ref();
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
            let mut blobs: Vec<BlobId> = m
                .layers
                .iter()
                .flat_map(|l| {
                    [
                        Some(l.meta),
                        Some(l.data),
                        l.data_leaves,
                        l.data_trace,
                        l.data_relaid,
                        l.data_order,
                    ]
                })
                .flatten()
                .collect();
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
        "relayout" => {
            let id: BlobId = what.parse().map_err(|e| format!("{what}: {e}"))?;
            let work = match (flags.get("work"), &store_dir) {
                (Some(w), _) => PathBuf::from(w),
                (None, Some(dir)) => dir.join("import"),
                (None, None) => return Err("relayout in a bucket needs --work".into()),
            };
            std::fs::create_dir_all(&work).map_err(|e| format!("{}: {e}", work.display()))?;
            let m = load_manifest(store, id).await.map_err(|e| format!("image {id}: {e}"))?;
            let started = Instant::now();
            let done = hive_nectar::relayout::put(store, &m, &work)
                .await
                .map_err(|e| format!("relayout: {e}"))?;
            eprintln!(
                "{} layers relaid in {:.2?}, {} traced chunks that were {} runs and are now {}",
                done.layers,
                started.elapsed(),
                done.traced,
                done.runs,
                done.layers
            );
            println!("{}", done.id);
            Ok(())
        }
        #[cfg(target_os = "linux")]
        "run" => {
            let id: BlobId = what.parse().map_err(|e| format!("{what}: {e}"))?;
            let cache_dir = PathBuf::from(flags.get("cache").ok_or("--cache is needed")?);
            let work = PathBuf::from(flags.get("work").ok_or("--work is needed")?);
            let cmd = flags.get("cmd").ok_or("--cmd is needed")?;
            let mode = flags.get("mode").map_or("lazy", String::as_str);
            if !matches!(mode, "whole" | "lazy" | "trace") {
                return Err(format!("--mode {mode}: whole, lazy or trace"));
            }
            let cache = Cache::open(&cache_dir, number("capacity", 64 << 30)?)
                .map_err(|e| format!("{}: {e}", cache_dir.display()))?;
            let m = load_manifest(store, id).await.map_err(|e| format!("image {id}: {e}"))?;
            let started = Instant::now();
            let layers = hive_nectar::mount::Layers::new(work.join("layers"), cache, None)
                .map_err(|e| e.to_string())?;
            let layers = if mode == "whole" { layers } else { layers.lazily() };
            let lowers = layers.mount(&shared, &m).await.map_err(|e| e.to_string())?;
            let root = overlay(&work, &lowers).map_err(|e| format!("overlay: {e}"))?;
            let mounted = started.elapsed();
            let status = tokio::process::Command::new("chroot")
                .arg(&root.merged)
                .args(["/bin/sh", "-c", cmd])
                .status()
                .await
                .map_err(|e| format!("chroot: {e}"))?;
            let ran = started.elapsed() - mounted;
            let (reads, fetched) = layers
                .progress()
                .iter()
                .fold((0, 0), |(r, b), (_, p)| (r + p.requests, b + p.bytes));
            eprintln!(
                "{mode}: mounted in {mounted:.2?}, ran in {ran:.2?}, {status}, {:.2?} in all, \
                 {reads} reads and {} fetched lazily by then",
                started.elapsed(),
                mib(fetched)
            );
            drop(root);
            if mode == "trace" {
                let traces = layers.traces();
                let chunks: usize = traces.values().map(Vec::len).sum();
                let (traced, _) = hive_nectar::trace::put(store, &m, &traces, &work)
                    .await
                    .map_err(|e| format!("storing traces: {e}"))?;
                eprintln!("{chunks} chunks traced across {} layers", traces.len());
                println!("{traced}");
            }
            Ok(())
        }
        _ => Err(USAGE.into()),
    }
}

/// A cell's overlay on the layers, with its writes kept under `work`, until it drops.
#[cfg(target_os = "linux")]
struct Overlay {
    work: PathBuf,
    merged: PathBuf,
}

#[cfg(target_os = "linux")]
fn overlay(work: &std::path::Path, lowers: &[PathBuf]) -> std::io::Result<Overlay> {
    let root = Overlay { work: work.to_owned(), merged: work.join("root") };
    let (upper, scratch) = (work.join("upper"), work.join("ovl"));
    for d in [&upper, &scratch, &root.merged] {
        std::fs::create_dir_all(d)?;
    }
    let lowerdir: Vec<String> = lowers.iter().map(|p| p.display().to_string()).collect();
    let options = format!(
        "lowerdir={},upperdir={},workdir={},volatile",
        lowerdir.join(":"),
        upper.display(),
        scratch.display()
    );
    rustix::mount::mount(
        "overlay",
        &root.merged,
        "overlay",
        rustix::mount::MountFlags::empty(),
        std::ffi::CString::new(options).map_err(std::io::Error::other)?.as_c_str(),
    )?;
    Ok(root)
}

#[cfg(target_os = "linux")]
impl Drop for Overlay {
    fn drop(&mut self) {
        let _ = rustix::mount::unmount(&self.merged, rustix::mount::UnmountFlags::DETACH);
        for d in ["root", "upper", "ovl"] {
            let _ = std::fs::remove_dir_all(self.work.join(d));
        }
    }
}

fn mib(bytes: u64) -> String {
    format!("{:.1} MiB", bytes as f64 / (1 << 20) as f64)
}
