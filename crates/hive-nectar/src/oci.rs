//! Importing images: OCI image layouts, such as `docker save` writes, and flat root filesystem
//! tars, such as `docker export` writes. Each OCI layer becomes one EROFS layer, built in parallel,
//! and the blobs go into a [`BlobStore`].
//!
//! Every OCI blob is checked against its sha256 digest before it is used. Layers already imported
//! are remembered by that digest, so an image that shares a base with one imported before only
//! builds the layers that are new.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::BlobId;
use crate::cas::Cuts;
use crate::erofs::Mkfs;
use crate::image::{ImageConfig, LayerRef, Manifest};
use crate::leaves::Leaves;
use crate::store::{BlobStore, blocking, hash_file, write_synced};

const INDEX: &[&str] = &[
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
];
const MANIFEST: &[&str] = &[
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
];

/// Which image of a multi-platform index to take.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Platform {
    /// Such as `linux`.
    pub os: String,
    /// Such as `amd64`, in OCI's spelling.
    pub arch: String,
}

impl Default for Platform {
    /// This machine.
    fn default() -> Self {
        let arch = match std::env::consts::ARCH {
            "x86_64" => "amd64",
            "aarch64" => "arm64",
            "x86" => "386",
            "powerpc64" => "ppc64le",
            other => other,
        };
        Self { os: std::env::consts::OS.into(), arch: arch.into() }
    }
}

/// What an import made.
#[derive(Clone, Debug)]
pub struct Imported {
    /// The manifest's name, which is also the name the image goes by.
    pub id: BlobId,
    /// The manifest.
    pub manifest: Manifest,
    /// Layers built this time.
    pub built: usize,
    /// Layers found already imported.
    pub reused: usize,
    /// Bytes of layer data the store did not have before: whole data blobs, or the new chunks of
    /// them when the importer stores chunks.
    pub stored: u64,
}

/// Builds layers with one `mkfs.erofs` into one store, using `work` for scratch space and for
/// remembering what it built.
#[derive(Debug)]
pub struct Importer {
    mkfs: Mkfs,
    work: PathBuf,
    seq: AtomicU64,
    chunked: Option<Cuts>,
}

impl Importer {
    /// An importer working in `work`, which it makes if needed.
    ///
    /// # Errors
    ///
    /// `work` cannot be made.
    pub fn new(mkfs: Mkfs, work: impl Into<PathBuf>) -> io::Result<Self> {
        let work = work.into();
        std::fs::create_dir_all(work.join("layers"))?;
        std::fs::create_dir_all(work.join("build"))?;
        Ok(Self { mkfs, work, seq: AtomicU64::new(0), chunked: None })
    }

    /// The same importer, storing each data blob as [`crate::cas`] chunks cut by `cuts` rather
    /// than whole.
    #[must_use]
    pub const fn chunked(mut self, cuts: Cuts) -> Self {
        self.chunked = Some(cuts);
        self
    }

    /// Imports the image for `platform` from the OCI image layout in the directory `layout`.
    ///
    /// # Errors
    ///
    /// The layout is malformed, has no image for `platform`, a blob does not match its digest, a
    /// layer is compressed with something other than gzip, or a build or the store fails.
    pub async fn import_layout(
        &self,
        store: &dyn BlobStore,
        layout: &Path,
        platform: &Platform,
    ) -> io::Result<Imported> {
        let (manifest, config, source) = {
            let layout = layout.to_path_buf();
            let platform = platform.clone();
            blocking(move || read_image(&layout, &platform)).await?
        };
        let diff_ids = config.rootfs.map(|r| r.diff_ids).unwrap_or_default();
        let mut jobs = Vec::new();
        for (i, layer) in manifest.layers.iter().enumerate() {
            if layer.media_type.contains("zstd") {
                return Err(bad(format!(
                    "layer {} is zstd, and only gzip is supported yet",
                    layer.digest
                )));
            }
            if !layer.media_type.contains("tar") {
                return Err(bad(format!(
                    "layer {} is a {}, not a tar",
                    layer.digest, layer.media_type
                )));
            }
            let src = Source {
                tar: blob_path(layout, &layer.digest)?,
                digest: Some(layer.digest.clone()),
                diff_id: diff_ids.get(i).cloned(),
            };
            jobs.push(self.layer(store, src));
        }
        let mut layers = Vec::new();
        let (mut built, mut reused, mut stored) = (0, 0, 0);
        for r in futures::future::join_all(jobs).await {
            let (layer, new, sent) = r?;
            if new {
                built += 1
            } else {
                reused += 1
            }
            stored += sent;
            layers.push(layer);
        }
        let run = config.config.unwrap_or_default();
        let manifest = Manifest {
            layers,
            config: ImageConfig {
                env: run.env.unwrap_or_default(),
                entrypoint: run.entrypoint.unwrap_or_default(),
                cmd: run.cmd.unwrap_or_default(),
                working_dir: run.working_dir.unwrap_or_default(),
                user: run.user.unwrap_or_default(),
            },
            source,
        };
        let id = self.put_manifest(store, &manifest).await?;
        Ok(Imported { id, manifest, built, reused, stored })
    }

    /// Imports a flat root filesystem from the tar file `tar` as an image of one layer with no
    /// config.
    ///
    /// # Errors
    ///
    /// The build or the store fails.
    pub async fn import_tar(&self, store: &dyn BlobStore, tar: &Path) -> io::Result<Imported> {
        let src = Source { tar: tar.to_path_buf(), digest: None, diff_id: None };
        let (layer, _, stored) = self.layer(store, src).await?;
        let manifest =
            Manifest { layers: vec![layer], config: ImageConfig::default(), source: None };
        let id = self.put_manifest(store, &manifest).await?;
        Ok(Imported { id, manifest, built: 1, reused: 0, stored })
    }

    /// Builds one layer, or finds it built before, and puts its blobs in the store. Says whether
    /// it was built and how many bytes of its data the store did not have.
    async fn layer(&self, store: &dyn BlobStore, src: Source) -> io::Result<(LayerRef, bool, u64)> {
        let memo = src.digest.as_ref().map(|d| {
            self.work.join("layers").join(format!(
                "{}-{}{}.json",
                d.replace(':', "-"),
                self.mkfs.chunk_size(),
                self.chunked.map(|c| format!("-cas{}", c.avg())).unwrap_or_default()
            ))
        });
        if let Some(memo) = &memo
            && let Ok(bytes) = tokio::fs::read(memo).await
            && let Ok(layer) = serde_json::from_slice::<LayerRef>(&bytes)
            && store.stat(layer.meta).await.is_ok()
            && store.stat(layer.data_chunks.unwrap_or(layer.data)).await.is_ok()
            && let Some(leaves) = layer.data_leaves
            && store.stat(leaves).await.is_ok()
        {
            return Ok((layer, false, 0));
        }
        let dir = self.work.join("build").join(format!(
            "{}.{}",
            std::process::id(),
            self.seq.fetch_add(1, Ordering::Relaxed)
        ));
        let mkfs = self.mkfs.clone();
        let result = async {
            let (dir2, chunk) = (dir.clone(), mkfs.chunk_size());
            let (built, meta, data, (leaves, leaves_path), meta_size, data_size) =
                blocking(move || {
                    std::fs::create_dir_all(&dir2)?;
                    // One read of the layer both checks it and feeds mkfs.erofs, and a layer that
                    // turns out not to match is thrown away with the build directory.
                    let mut hashed = Hashed { inner: File::open(&src.tar)?, sha: Sha256::new() };
                    let built = mkfs.build(&mut hashed, &dir2)?;
                    if let Some(digest) = &src.digest {
                        let got = format!("sha256:{:x}", hashed.sha.finalize());
                        if got != *digest {
                            return Err(bad(format!("layer {digest} hashes to {got}")));
                        }
                    }
                    let meta = hash_file(&built.meta)?;
                    // One read of the data blob gives both its name and its leaves.
                    let leaves = Leaves::of_file(&built.data)?;
                    let data = leaves.blob();
                    let leaves_path = dir2.join("data.leaves");
                    let bytes = leaves.to_bytes();
                    std::fs::write(&leaves_path, &bytes)?;
                    let leaves = (BlobId::of(&bytes), leaves_path);
                    let sizes = (
                        std::fs::metadata(&built.meta)?.len(),
                        std::fs::metadata(&built.data)?.len(),
                    );
                    Ok((built, meta, data, leaves, sizes.0, sizes.1))
                })
                .await?;
            store.put(meta, &built.meta).await?;
            let (data_chunks, stored) = if let Some(cuts) = self.chunked {
                let put = crate::cas::put(store, data, &built.data, cuts, &dir).await?;
                (Some(put.recipe), put.new_bytes)
            } else {
                let put = store.put(data, &built.data).await?;
                (None, if put.existed { 0 } else { data_size })
            };
            store.put(leaves, &leaves_path).await?;
            let layer = LayerRef {
                digest: LayerRef::digest_of(meta, data),
                meta,
                data,
                meta_size,
                data_size,
                chunk_size: chunk,
                data_leaves: Some(leaves),
                data_trace: None,
                data_relaid: None,
                data_order: None,
                data_chunks,
                relaid_chunks: None,
                diff_id: src.diff_id,
            };
            if let Some(memo) = &memo {
                let bytes = serde_json::to_vec(&layer).map_err(io::Error::other)?;
                let memo = memo.clone();
                blocking(move || write_synced(&memo, &bytes)).await?;
            }
            Ok((layer, true, stored))
        }
        .await;
        let _ = tokio::fs::remove_dir_all(&dir).await;
        result
    }

    async fn put_manifest(&self, store: &dyn BlobStore, manifest: &Manifest) -> io::Result<BlobId> {
        let bytes = manifest.to_bytes();
        let id = BlobId::of(&bytes);
        let path = self.work.join("build").join(format!(
            "{id}.{}.{}",
            std::process::id(),
            self.seq.fetch_add(1, Ordering::Relaxed)
        ));
        tokio::fs::write(&path, &bytes).await?;
        let r = store.put(id, &path).await;
        let _ = tokio::fs::remove_file(&path).await;
        r.map(|_| id)
    }
}

/// Reads a manifest blob back out of a store.
///
/// # Errors
///
/// The store does not have it, or it is not a manifest.
pub async fn load_manifest(store: &dyn BlobStore, id: BlobId) -> io::Result<Manifest> {
    let size = store.stat(id).await?.size;
    let size = usize::try_from(size).map_err(io::Error::other)?;
    let reqs =
        store.read_vectored(id, vec![crate::ReadReq { offset: 0, buf: vec![0; size] }]).await?;
    if BlobId::of(&reqs[0].buf) != id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("manifest {id} does not match its name"),
        ));
    }
    Manifest::from_bytes(&reqs[0].buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

struct Source {
    tar: PathBuf,
    digest: Option<String>,
    diff_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Descriptor {
    #[serde(rename = "mediaType", default)]
    media_type: String,
    digest: String,
    #[serde(default)]
    platform: Option<PlatformJson>,
    #[serde(default)]
    annotations: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct PlatformJson {
    os: String,
    architecture: String,
}

#[derive(Debug, Deserialize)]
struct Index {
    #[serde(default)]
    manifests: Vec<Descriptor>,
}

#[derive(Debug, Deserialize)]
struct OciManifest {
    config: Descriptor,
    layers: Vec<Descriptor>,
}

#[derive(Debug, Deserialize)]
struct ConfigFile {
    #[serde(default)]
    config: Option<RunConfig>,
    #[serde(default)]
    rootfs: Option<RootFs>,
}

// Docker writes null for a list it has nothing in, hence the options.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct RunConfig {
    #[serde(default)]
    env: Option<Vec<String>>,
    #[serde(default)]
    entrypoint: Option<Vec<String>>,
    #[serde(default)]
    cmd: Option<Vec<String>>,
    #[serde(default)]
    working_dir: Option<String>,
    #[serde(default)]
    user: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RootFs {
    #[serde(default)]
    diff_ids: Vec<String>,
}

/// Finds the image for `platform` in the layout and reads its manifest and config.
fn read_image(
    layout: &Path,
    platform: &Platform,
) -> io::Result<(OciManifest, ConfigFile, Option<String>)> {
    let index: Index = json(&std::fs::read(layout.join("index.json"))?)?;
    let mut source = None;
    let found = pick(layout, &index, platform, 0, &mut source)?.ok_or_else(|| {
        bad(format!("{} has no image for {}/{}", layout.display(), platform.os, platform.arch))
    })?;
    let manifest: OciManifest = json(&read_blob(layout, &found)?)?;
    let config: ConfigFile = json(&read_blob(layout, &manifest.config.digest)?)?;
    Ok((manifest, config, source))
}

/// Walks an index, and the indexes inside it, for the first manifest that is for `platform`, or
/// says nothing about platforms, and whose blob is in the layout.
fn pick(
    layout: &Path,
    index: &Index,
    platform: &Platform,
    depth: usize,
    source: &mut Option<String>,
) -> io::Result<Option<String>> {
    if depth > 4 {
        return Err(bad("indexes nest too deep".into()));
    }
    for d in &index.manifests {
        if source.is_none() {
            *source = d
                .annotations
                .get("io.containerd.image.name")
                .or_else(|| d.annotations.get("org.opencontainers.image.ref.name"))
                .cloned();
        }
        if let Some(p) = &d.platform
            && (p.os != platform.os || p.architecture != platform.arch)
        {
            continue;
        }
        let path = blob_path(layout, &d.digest)?;
        if !path.exists() {
            continue;
        }
        if INDEX.contains(&d.media_type.as_str()) {
            let inner: Index = json(&read_blob(layout, &d.digest)?)?;
            if let Some(found) = pick(layout, &inner, platform, depth + 1, source)? {
                return Ok(Some(found));
            }
        } else if MANIFEST.contains(&d.media_type.as_str()) {
            return Ok(Some(d.digest.clone()));
        }
    }
    Ok(None)
}

fn blob_path(layout: &Path, digest: &str) -> io::Result<PathBuf> {
    match digest.split_once(':') {
        Some(("sha256", hex)) if hex.len() == 64 && hex.bytes().all(|c| c.is_ascii_hexdigit()) => {
            Ok(layout.join("blobs").join("sha256").join(hex))
        }
        _ => Err(bad(format!("{digest} is not a sha256 digest"))),
    }
}

/// Reads a small blob and checks it against its digest.
fn read_blob(layout: &Path, digest: &str) -> io::Result<Vec<u8>> {
    let bytes = std::fs::read(blob_path(layout, digest)?)?;
    let got = format!("sha256:{:x}", Sha256::digest(&bytes));
    if got != digest {
        return Err(bad(format!("blob {digest} hashes to {got}")));
    }
    Ok(bytes)
}

/// A reader that hashes what passes through it.
struct Hashed<R> {
    inner: R,
    sha: Sha256,
}

impl<R: Read> Read for Hashed<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.sha.update(&buf[..n]);
        Ok(n)
    }
}

fn json<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> io::Result<T> {
    serde_json::from_slice(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn bad(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}
