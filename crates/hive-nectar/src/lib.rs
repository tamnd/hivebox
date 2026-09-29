//! Everything about getting bytes onto a node: OCI to EROFS conversion, the blob store implementations, the L1 cache and the lazy filler. A cell cannot start faster than its root filesystem appears, so this crate decides the create latency more than any other.
//!
//! The design is in `spec/06_storage_images.md`. This is version 0 of it:
//!
//! - [`oci::Importer`] turns an OCI image layout, or a flat root filesystem tar, into EROFS layers
//!   with `mkfs.erofs`, each split into a metadata blob and a data blob, and stores them with a
//!   [`Manifest`] that names them.
//! - [`PosixStore`] keeps blobs in a directory, local or shared.
//! - [`Cache`] is the node's L1: blobs fetched whole from a store onto local disk, resumable after
//!   a crash, checked against their names, pinned while in use and evicted least recently used.
//!
//! - [`mount::Layers`] mounts an image's layers on a node, once each, with EROFS on loop devices
//!   and an idmapped mount for the cells' id range.
//!
//! Lazy filling, chunk level dedup and the other stores come later.

// Only the mount module has any, for loop device ioctls and `mount_setattr`.
#![deny(unsafe_code)]

mod blob;
pub mod cache;
pub mod erofs;
pub mod image;
#[cfg(target_os = "linux")]
pub mod mount;
pub mod oci;
pub mod store;

pub use blob::{BadBlobId, BlobId};
pub use cache::{Cache, Held};
pub use image::{ImageConfig, LayerRef, Manifest};
pub use store::{BlobCaps, BlobStat, BlobStore, PosixStore, PutReceipt, ReadReq};

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A fresh directory for one test.
    pub(crate) fn scratch() -> PathBuf {
        static N: AtomicU32 = AtomicU32::new(0);
        let dir = std::env::temp_dir().join(format!(
            "hive-nectar-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}
