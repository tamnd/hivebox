//! Relayout: a copy of a layer's data blob with the chunks its prefetch trace names first, in the
//! order the trace read them, and the rest after them in order. A lazy mount of a relaid layer
//! fetches from the copy, so what a run reads first comes in a few long reads rather than one
//! short read for each place it sits in the original. See [`crate::Relaid`].
//!
//! The copy and its order are blobs of their own, and the layer names them in
//! [`crate::LayerRef::data_relaid`] and [`crate::LayerRef::data_order`]. The data blob stays, so a
//! whole fetch works as before and the cache still keeps the original. Relaying out makes a new
//! manifest, so the image gets a new name and the old one stays as it was.

use std::io;
use std::path::Path;

use tokio::io::AsyncWriteExt;

use crate::cache::CHUNK;
use crate::cas::Recipe;
use crate::trace::{put_bytes, to_bytes};
use crate::{BlobId, BlobStore, Leaves, Manifest, ReadReq};

/// Chunks read from the store at a time while copying, 16 MiB.
const BATCH: usize = 64;

/// What [`put`] did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Relaidout {
    /// The new image's name.
    pub id: BlobId,
    /// The new image.
    pub manifest: Manifest,
    /// Layers that got a relaid copy.
    pub layers: usize,
    /// Chunks the traces of those layers name.
    pub traced: u64,
    /// Runs of chunks that sit one after another in the original that those traces make. The
    /// copy has each trace as one run.
    pub runs: u64,
}

/// The order a blob of `size` bytes is relaid in: the chunks of `trace` first, each once, then
/// the rest in order.
#[must_use]
pub fn order(trace: &[u64], size: u64) -> Vec<u64> {
    lead(trace, size).0
}

/// [`order`], and how many chunks from the trace it starts with.
fn lead(trace: &[u64], size: u64) -> (Vec<u64>, usize) {
    let chunks = size.div_ceil(CHUNK);
    let mut taken = vec![false; usize::try_from(chunks).unwrap_or(0)];
    let mut out = Vec::with_capacity(taken.len());
    for &c in trace {
        if c < chunks && !taken[c as usize] {
            taken[c as usize] = true;
            out.push(c);
        }
    }
    let traced = out.len();
    out.extend((0..chunks).filter(|&c| !taken[c as usize]));
    (out, traced)
}

/// How many runs of chunks that sit one after another `chunks` is made of.
#[must_use]
pub fn runs(chunks: &[u64]) -> u64 {
    let breaks = chunks.windows(2).filter(|w| w[1] != w[0] + 1).count();
    if chunks.is_empty() { 0 } else { breaks as u64 + 1 }
}

/// Stores a relaid copy of the data of every layer of `image` that has a trace and leaves, the
/// order of each, and a copy of `image` whose layers name them. Layers with no trace, already
/// relaid for the trace they have, or whose trace leaves the order as it was, keep what they had. Files are staged in `work` on the way.
///
/// # Errors
///
/// A blob cannot be read or stored, or a chunk of a data blob does not match its leaf.
pub async fn put(store: &dyn BlobStore, image: &Manifest, work: &Path) -> io::Result<Relaidout> {
    let mut relaid = image.clone();
    let (mut layers, mut traced, mut runs_in) = (0, 0, 0);
    for layer in &mut relaid.layers {
        let (Some(trace), Some(leaves)) = (layer.data_trace, layer.data_leaves) else { continue };
        if layer.data_relaid.is_some() {
            continue;
        }
        let (order, moved) = lead(&crate::trace::load(store, trace).await?, layer.data_size);
        if order.iter().enumerate().all(|(p, &c)| p as u64 == c) {
            continue;
        }
        let leaves = load_leaves(store, layer.data, layer.data_size, leaves).await?;
        let recipe = match layer.data_chunks {
            Some(r) => Some(crate::cas::load(store, r).await?),
            None => None,
        };
        let from = Data { store, blob: layer.data, size: layer.data_size, recipe: recipe.as_ref() };
        let (copied, chunks) = copy(&from, &leaves, &order, work).await?;
        (layer.data_relaid, layer.relaid_chunks) = (Some(copied), chunks);
        layer.data_order = Some(put_bytes(store, &to_bytes(&order), work).await?);
        layers += 1;
        traced += moved as u64;
        runs_in += runs(&order[..moved]);
    }
    // The manifest goes last, so an image in the store always has all its copies.
    let id = put_bytes(store, &relaid.to_bytes(), work).await?;
    Ok(Relaidout { id, manifest: relaid, layers, traced, runs: runs_in })
}

async fn load_leaves(
    store: &dyn BlobStore,
    blob: BlobId,
    size: u64,
    leaves: BlobId,
) -> io::Result<Leaves> {
    let len = usize::try_from(size.div_ceil(CHUNK).max(1) * 32).map_err(io::Error::other)?;
    let got = store.read_vectored(leaves, vec![ReadReq { offset: 0, buf: vec![0; len] }]).await?;
    Leaves::from_bytes(blob, size, &got[0].buf)
}

/// A data blob to copy from, and its recipe when it is kept as chunks.
struct Data<'a> {
    store: &'a dyn BlobStore,
    blob: BlobId,
    size: u64,
    recipe: Option<&'a Recipe>,
}

impl Data<'_> {
    async fn read(&self, reqs: Vec<ReadReq>) -> io::Result<Vec<ReadReq>> {
        match self.recipe {
            Some(r) => r.read(self.store, reqs).await,
            None => self.store.read_vectored(self.blob, reqs).await,
        }
    }
}

/// Writes the chunks of the data blob in `order` to a file in `work`, checking each, and stores
/// it, as chunks when the data blob is kept as chunks, with the recipe's name. A short last chunk
/// is padded with zeros, so every chunk of the copy is whole and the short one can go anywhere.
async fn copy(
    from: &Data<'_>,
    leaves: &Leaves,
    order: &[u64],
    work: &Path,
) -> io::Result<(BlobId, Option<BlobId>)> {
    let (store, blob, size) = (from.store, from.blob, from.size);
    let path = work.join(format!("{blob}.relaid.{}", std::process::id()));
    let written = async {
        let mut out = tokio::io::BufWriter::new(tokio::fs::File::create(&path).await?);
        let mut hash = blake3::Hasher::new();
        for batch in order.chunks(BATCH) {
            let reqs = batch
                .iter()
                .map(|&c| {
                    let len = CHUNK.min(size - c * CHUNK) as usize;
                    ReadReq { offset: c * CHUNK, buf: vec![0; len] }
                })
                .collect();
            for (r, &c) in from.read(reqs).await?.iter().zip(batch) {
                if !leaves.check(c, &r.buf) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("chunk {c} of {blob} from the store does not match its leaf"),
                    ));
                }
                let pad = vec![0; CHUNK as usize - r.buf.len()];
                for bytes in [&r.buf, &pad] {
                    hash.update(bytes);
                    out.write_all(bytes).await?;
                }
            }
        }
        out.flush().await?;
        Ok(BlobId::from(hash.finalize()))
    };
    let id = match written.await {
        Ok(id) if let Some(r) = from.recipe => {
            crate::cas::put(store, id, &path, r.cuts(), work).await.map(|p| (id, Some(p.recipe)))
        }
        Ok(id) => store.put(id, &path).await.map(|_| (id, None)),
        Err(e) => Err(e),
    };
    let _ = tokio::fs::remove_file(&path).await;
    id
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::{Cache, ImageConfig, LayerRef, PosixStore, Relaid};

    #[test]
    fn the_trace_goes_first_once_and_the_rest_after_in_order() {
        let c = CHUNK;
        assert_eq!(order(&[3, 1, 3, 9, 4], 5 * c), [3, 1, 4, 0, 2]);
        assert_eq!(order(&[4, 2], 4 * c + 1), [4, 2, 0, 1, 3]);
        assert_eq!(order(&[], 3 * c), [0, 1, 2]);
        assert_eq!(order(&[], 0), Vec::<u64>::new());
        assert_eq!(runs(&[3, 1, 4, 0, 2]), 5);
        assert_eq!(runs(&[7, 8, 9, 2, 3]), 2);
        assert_eq!(runs(&[]), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_relaid_layer_fills_the_trace_in_one_read_and_ends_as_the_original() {
        let dir = crate::tests::scratch();
        let work = dir.join("work");
        std::fs::create_dir_all(&work).unwrap();
        let store = Arc::new(PosixStore::open(dir.join("store")).unwrap());
        let c = CHUNK as usize;
        let len = 40 * c + 777;
        let bytes: Vec<u8> = (0..len).map(|i| (i * 7 + i / 4091) as u8).collect();
        std::fs::write(dir.join("src"), &bytes).unwrap();
        let leaves = Leaves::of_file(&dir.join("src")).unwrap();
        let data = leaves.blob();
        store.put(data, &dir.join("src")).await.unwrap();
        let leaves_id = put_bytes(&*store, &leaves.to_bytes(), &work).await.unwrap();
        let layer = LayerRef {
            digest: LayerRef::digest_of(BlobId::of(b"meta"), data),
            meta: BlobId::of(b"meta"),
            data,
            meta_size: 4,
            data_size: len as u64,
            chunk_size: CHUNK as u32,
            data_leaves: Some(leaves_id),
            data_trace: None,
            data_relaid: None,
            data_order: None,
            data_chunks: None,
            relaid_chunks: None,
            diff_id: None,
        };
        let image = Manifest {
            layers: vec![layer],
            config: ImageConfig::default(),
            source: None,
            provenance: None,
        };
        let trace = vec![30, 2, 17, 40, 5, 6, 33];
        let traces = HashMap::from([(image.layers[0].digest, trace.clone())]);
        let (_, traced) = crate::trace::put(&*store, &image, &traces, &work).await.unwrap();

        let done = put(&*store, &traced, &work).await.unwrap();
        assert_eq!((done.layers, done.traced, done.runs), (1, 7, 6));
        let l = &done.manifest.layers[0];
        let order = crate::trace::load(&*store, l.data_order.unwrap()).await.unwrap();
        assert_eq!(order[..8], trace.iter().copied().chain([0]).collect::<Vec<_>>());
        let copy = std::fs::read(store.path(l.data_relaid.unwrap())).unwrap();
        assert_eq!(copy.len(), 41 * c);
        assert_eq!(copy[..c], bytes[30 * c..31 * c]);
        // The short last chunk moved up with the trace, padded.
        assert_eq!(copy[3 * c..3 * c + 777], bytes[40 * c..]);
        assert!(copy[3 * c + 777..4 * c].iter().all(|&b| b == 0));
        assert_eq!(crate::oci::load_manifest(&*store, done.id).await.unwrap(), done.manifest);
        assert!(std::fs::read_dir(&work).unwrap().next().is_none(), "nothing left staged");

        // A lazy fill from the copy reads ahead in trace order, and the cache ends up with the
        // original under its own name.
        let cache = Cache::open(dir.join("l1"), 1 << 30).unwrap();
        let relaid = Relaid { blob: l.data_relaid.unwrap(), order: order.clone() };
        let lazy = cache
            .lazy_from(store.clone(), data, len as u64, leaves_id, Some(relaid))
            .await
            .unwrap();
        assert_eq!(lazy.read_at(30 * CHUNK + 9, 5).await.unwrap(), bytes[30 * c + 9..30 * c + 14]);
        let p = lazy.progress();
        assert_eq!((p.requests, p.have), (1, crate::lazy::DEMAND));
        assert_eq!(lazy.read_at(len as u64 - 7, 7).await.unwrap(), bytes[len - 7..]);
        for &t in &order[4..7] {
            let at = t as usize * c;
            assert_eq!(lazy.read_at(at as u64, 100).await.unwrap(), bytes[at..at + 100]);
        }
        // Reading ahead in the copy is reading ahead in the trace, so the seven traced chunks,
        // six runs in the original, took two reads.
        assert_eq!(lazy.progress().requests, 2);
        lazy.fill_rest(&order).await.unwrap();
        assert_eq!(lazy.progress().bytes, 41 * CHUNK);
        assert_eq!(std::fs::read(dir.join("l1").join(data.to_string())).unwrap(), bytes);
        assert!(cache.has(data) && !cache.has(l.data_relaid.unwrap()));

        // A relaid image relaid again is left as it is.
        let again = put(&*store, &done.manifest, &work).await.unwrap();
        assert_eq!(again.id, done.id);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_layer_kept_as_chunks_is_relaid_as_chunks_that_mostly_exist_already() {
        let dir = crate::tests::scratch();
        let work = dir.join("work");
        std::fs::create_dir_all(&work).unwrap();
        let store: Arc<dyn BlobStore> = Arc::new(PosixStore::open(dir.join("store")).unwrap());
        let c = CHUNK as usize;
        let len = 40 * c + 777;
        let mut x = 7u64;
        let bytes: Vec<u8> = (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                (x >> 24) as u8
            })
            .collect();
        std::fs::write(dir.join("src"), &bytes).unwrap();
        let leaves = Leaves::of_file(&dir.join("src")).unwrap();
        let data = leaves.blob();
        let kept =
            crate::cas::put(&*store, data, &dir.join("src"), crate::cas::Cuts::default(), &work)
                .await
                .unwrap();
        let leaves_id = put_bytes(&*store, &leaves.to_bytes(), &work).await.unwrap();
        let layer = LayerRef {
            digest: LayerRef::digest_of(BlobId::of(b"meta"), data),
            meta: BlobId::of(b"meta"),
            data,
            meta_size: 4,
            data_size: len as u64,
            chunk_size: CHUNK as u32,
            data_leaves: Some(leaves_id),
            data_trace: None,
            data_relaid: None,
            data_order: None,
            data_chunks: Some(kept.recipe),
            relaid_chunks: None,
            diff_id: None,
        };
        let image = Manifest {
            layers: vec![layer],
            config: ImageConfig::default(),
            source: None,
            provenance: None,
        };
        let traces = HashMap::from([(image.layers[0].digest, vec![30, 2, 17, 5, 6])]);
        let (_, traced) = crate::trace::put(&*store, &image, &traces, &work).await.unwrap();
        let done = put(&*store, &traced, &work).await.unwrap();
        let l = &done.manifest.layers[0];
        let (relaid, recipe) = (l.data_relaid.unwrap(), l.relaid_chunks.unwrap());
        assert!(store.stat(data).await.is_err() && store.stat(relaid).await.is_err());
        let chunked =
            crate::cas::Chunked::open(store.clone(), [kept.recipe, recipe]).await.unwrap();
        let got = chunked
            .read_vectored(relaid, vec![ReadReq { offset: 0, buf: vec![0; 2 * c] }])
            .await
            .unwrap();
        assert_eq!(got[0].buf[..c], bytes[30 * c..31 * c]);
        assert_eq!(got[0].buf[c..], bytes[2 * c..3 * c]);
        // Most of the copy is runs of the original, which cut the same way.
        let r = crate::cas::load(&*store, recipe).await.unwrap();
        let old = crate::cas::load(&*store, kept.recipe).await.unwrap();
        let shared = r.chunks().iter().filter(|c| old.chunks().contains(c)).count();
        assert!(shared * 2 > r.chunks().len(), "{shared} of {}", r.chunks().len());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_order_that_is_not_a_reordering_is_refused() {
        let dir = crate::tests::scratch();
        let work = dir.join("work");
        std::fs::create_dir_all(&work).unwrap();
        let store = Arc::new(PosixStore::open(dir.join("store")).unwrap());
        let len = 3 * CHUNK as usize + 5;
        std::fs::write(dir.join("src"), vec![9; len]).unwrap();
        let leaves = Leaves::of_file(&dir.join("src")).unwrap();
        let data = leaves.blob();
        store.put(data, &dir.join("src")).await.unwrap();
        let leaves_id = put_bytes(&*store, &leaves.to_bytes(), &work).await.unwrap();
        let cache = Cache::open(dir.join("l1"), 1 << 30).unwrap();
        for bad in [vec![0, 1, 2], vec![0, 0, 2, 3], vec![0, 1, 2, 7]] {
            let relaid = Relaid { blob: data, order: bad };
            let e = cache
                .lazy_from(store.clone(), data, len as u64, leaves_id, Some(relaid))
                .await
                .unwrap_err();
            assert_eq!(e.kind(), io::ErrorKind::InvalidData);
        }
        assert_eq!(cache.usage().blobs, 0);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
