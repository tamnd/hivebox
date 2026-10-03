//! Prefetch traces: the chunks of a layer's data that a run of the image read, in the order it
//! first read them. A lazy mount of the layer fills those chunks first, so the next start of the
//! same program finds most of what it reads already there or on the way.
//!
//! A trace is stored as a blob of its own, little endian `u32` chunk numbers, and the layer names
//! it in [`crate::LayerRef::data_trace`]. Adding traces makes a new manifest, so the image gets a
//! new name and the old one stays as it was.

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{BlobId, BlobStore, Manifest, ReadReq};

/// The bytes a trace is stored as.
#[must_use]
pub fn to_bytes(order: &[u64]) -> Vec<u8> {
    order.iter().flat_map(|&c| u32::try_from(c).unwrap_or(u32::MAX).to_le_bytes()).collect()
}

/// A trace read back.
///
/// # Errors
///
/// `InvalidData` if the length is not a whole number of chunk numbers.
pub fn from_bytes(bytes: &[u8]) -> io::Result<Vec<u64>> {
    let (whole, rest) = bytes.as_chunks::<4>();
    if !rest.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "a trace of a broken length"));
    }
    Ok(whole.iter().map(|b| u64::from(u32::from_le_bytes(*b))).collect())
}

/// Reads the trace `id` from `store`.
///
/// # Errors
///
/// The store does not have it, or it does not match its name.
pub async fn load(store: &dyn BlobStore, id: BlobId) -> io::Result<Vec<u64>> {
    let size = usize::try_from(store.stat(id).await?.size).map_err(io::Error::other)?;
    let reqs = store.read_vectored(id, vec![ReadReq { offset: 0, buf: vec![0; size] }]).await?;
    if BlobId::of(&reqs[0].buf) != id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("trace {id} does not match its name"),
        ));
    }
    from_bytes(&reqs[0].buf)
}

/// Stores `traces`, keyed by layer digest, and a copy of `image` whose layers name them, and
/// returns the new image's name and manifest. Layers with no trace, or an empty one, keep what
/// they had. Files are staged in `work` on the way.
///
/// # Errors
///
/// A blob cannot be staged or stored.
pub async fn put(
    store: &dyn BlobStore,
    image: &Manifest,
    traces: &HashMap<BlobId, Vec<u64>>,
    work: &Path,
) -> io::Result<(BlobId, Manifest)> {
    let mut traced = image.clone();
    for layer in &mut traced.layers {
        let Some(order) = traces.get(&layer.digest).filter(|o| !o.is_empty()) else { continue };
        layer.data_trace = Some(put_bytes(store, &to_bytes(order), work).await?);
    }
    // The manifest goes last, so an image in the store always has all its traces.
    let id = put_bytes(store, &traced.to_bytes(), work).await?;
    Ok((id, traced))
}

async fn put_bytes(store: &dyn BlobStore, bytes: &[u8], work: &Path) -> io::Result<BlobId> {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let id = BlobId::of(bytes);
    let path =
        work.join(format!("{id}.{}.{}", std::process::id(), SEQ.fetch_add(1, Ordering::Relaxed)));
    tokio::fs::write(&path, bytes).await?;
    let r = store.put(id, &path).await;
    let _ = tokio::fs::remove_file(&path).await;
    r.map(|_| id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn traces_round_trip() {
        let order = [10, 13, 1, 2, 40, 70_000];
        assert_eq!(from_bytes(&to_bytes(&order)).unwrap(), order);
        assert_eq!(from_bytes(&[]).unwrap(), Vec::<u64>::new());
        let e = from_bytes(&[1, 2, 3]).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::InvalidData);
    }
}
