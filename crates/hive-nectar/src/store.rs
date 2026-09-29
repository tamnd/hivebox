//! Where blobs live between nodes: the [`BlobStore`] trait and [`PosixStore`], the one for a local
//! disk or a shared mount such as NFS or Lustre.

use std::fs::File;
use std::io::{self, Read, Write};
use std::os::unix::fs::{FileExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use futures::future::BoxFuture;

use crate::BlobId;

/// How a store likes to be read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlobCaps {
    /// The largest single read it takes.
    pub max_io: usize,
    /// The read size it is fastest at. Readers round up to this.
    pub ideal_io: usize,
    /// Whether a blob can be mapped straight from the store.
    pub supports_mmap: bool,
}

/// One read of a batch: fills `buf` with the bytes at `offset`.
#[derive(Debug)]
pub struct ReadReq {
    /// Where in the blob to start.
    pub offset: u64,
    /// What to fill, all of it.
    pub buf: Vec<u8>,
}

/// What a store knows about a blob.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlobStat {
    /// Its size in bytes.
    pub size: u64,
}

/// What [`BlobStore::put`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PutReceipt {
    /// The blob's size in bytes.
    pub size: u64,
    /// Whether the store had it already, in which case nothing was copied.
    pub existed: bool,
}

/// Content addressed storage for layer blobs. Every blob is named by the BLAKE3 hash of its bytes
/// and never changes, so a store may be cached anywhere without invalidation.
pub trait BlobStore: Send + Sync + 'static {
    /// How it likes to be read.
    fn caps(&self) -> BlobCaps;

    /// Fills every request in one batch and hands them back in the same order. Reading past the
    /// end of the blob is an `UnexpectedEof` error.
    fn read_vectored(
        &self,
        blob: BlobId,
        reqs: Vec<ReadReq>,
    ) -> BoxFuture<'_, io::Result<Vec<ReadReq>>>;

    /// Stores the file at `src` as `blob`, checking that its bytes hash to that name first.
    /// Storing a blob the store has already is cheap and does nothing. A store may keep `src`
    /// itself rather than a copy, so the caller must not change it afterwards.
    fn put<'a>(&'a self, blob: BlobId, src: &'a Path) -> BoxFuture<'a, io::Result<PutReceipt>>;

    /// What it knows about `blob`, or `NotFound`.
    fn stat(&self, blob: BlobId) -> BoxFuture<'_, io::Result<BlobStat>>;

    /// Removes `blob`. Removing one that is not there is fine.
    fn delete(&self, blob: BlobId) -> BoxFuture<'_, io::Result<()>>;
}

/// A store in a directory, one file per blob under `blobs/`, fanned out by the first byte of the
/// name so no directory gets huge.
#[derive(Debug)]
pub struct PosixStore {
    root: PathBuf,
    tmp: AtomicU64,
}

impl PosixStore {
    /// Opens the store at `root`, making it if it is not there.
    ///
    /// # Errors
    ///
    /// `root` cannot be made.
    pub fn open(root: impl Into<PathBuf>) -> io::Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(root.join("blobs"))?;
        std::fs::create_dir_all(root.join("tmp"))?;
        Ok(Self { root, tmp: AtomicU64::new(0) })
    }

    /// Where `blob` is kept, whether or not it is there.
    #[must_use]
    pub fn path(&self, blob: BlobId) -> PathBuf {
        let name = blob.to_string();
        self.root.join("blobs").join(&name[..2]).join(name)
    }
}

impl BlobStore for PosixStore {
    fn caps(&self) -> BlobCaps {
        BlobCaps { max_io: 8 << 20, ideal_io: 1 << 20, supports_mmap: true }
    }

    fn read_vectored(
        &self,
        blob: BlobId,
        mut reqs: Vec<ReadReq>,
    ) -> BoxFuture<'_, io::Result<Vec<ReadReq>>> {
        let path = self.path(blob);
        Box::pin(async move {
            blocking(move || {
                let f = File::open(path)?;
                for r in &mut reqs {
                    f.read_exact_at(&mut r.buf, r.offset)?;
                }
                Ok(reqs)
            })
            .await
        })
    }

    fn put<'a>(&'a self, blob: BlobId, src: &'a Path) -> BoxFuture<'a, io::Result<PutReceipt>> {
        let path = self.path(blob);
        let tmp = self.root.join("tmp").join(format!(
            "{blob}.{}.{}",
            std::process::id(),
            self.tmp.fetch_add(1, Ordering::Relaxed)
        ));
        let src = src.to_path_buf();
        Box::pin(blocking(move || put_file(blob, &src, &tmp, &path)))
    }

    fn stat(&self, blob: BlobId) -> BoxFuture<'_, io::Result<BlobStat>> {
        let path = self.path(blob);
        Box::pin(async move { Ok(BlobStat { size: tokio::fs::metadata(path).await?.len() }) })
    }

    fn delete(&self, blob: BlobId) -> BoxFuture<'_, io::Result<()>> {
        let path = self.path(blob);
        Box::pin(async move {
            match tokio::fs::remove_file(path).await {
                Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
                _ => Ok(()),
            }
        })
    }
}

fn put_file(blob: BlobId, src: &Path, tmp: &Path, path: &Path) -> io::Result<PutReceipt> {
    if let Ok(m) = std::fs::metadata(path) {
        return Ok(PutReceipt { size: m.len(), existed: true });
    }
    let dir = path.parent().expect("blob paths have a parent");
    std::fs::create_dir_all(dir)?;
    let mismatch = |got: BlobId| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{} hashes to {got}, not {blob}", src.display()),
        )
    };
    // On the same filesystem the blob is a second link to `src`, and nothing is copied.
    let got = hash_file(src)?;
    if got != blob {
        return Err(mismatch(got));
    }
    std::fs::set_permissions(src, std::fs::Permissions::from_mode(0o444))?;
    let size = {
        let f = File::open(src)?;
        f.sync_all()?;
        f.metadata()?.len()
    };
    match std::fs::hard_link(src, path) {
        Ok(()) => {
            File::open(dir)?.sync_all()?;
            return Ok(PutReceipt { size, existed: false });
        }
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
            return Ok(PutReceipt { size, existed: true });
        }
        // Another filesystem, or one without hard links.
        Err(_) => {}
    }
    let result = (|| {
        // The copy stays in the kernel, and on XFS or btrfs it shares extents instead of copying.
        // It is hashed again, so what gets renamed in is what was checked.
        let size = io::copy(&mut File::open(src)?, &mut File::create(tmp)?)?;
        let got = hash_file(tmp)?;
        if got != blob {
            return Err(mismatch(got));
        }
        std::fs::set_permissions(tmp, std::fs::Permissions::from_mode(0o444))?;
        File::open(tmp)?.sync_all()?;
        std::fs::rename(tmp, path)?;
        File::open(dir)?.sync_all()?;
        Ok(PutReceipt { size, existed: false })
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(tmp);
    }
    result
}

/// The name of the file at `path`.
///
/// # Errors
///
/// It cannot be read.
pub fn hash_file(path: &Path) -> io::Result<BlobId> {
    let mut h = blake3::Hasher::new();
    let mut f = File::open(path)?;
    let mut buf = vec![0; 1 << 20];
    loop {
        match f.read(&mut buf)? {
            0 => return Ok(h.finalize().into()),
            n => {
                h.update(&buf[..n]);
            }
        }
    }
}

/// Writes `bytes` to `path` and flushes them to disk.
pub(crate) fn write_synced(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut f = File::create(path)?;
    f.write_all(bytes)?;
    f.sync_all()
}

pub(crate) async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> io::Result<T> {
    tokio::task::spawn_blocking(f).await.map_err(io::Error::other)?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn it_keeps_what_it_is_given_and_nothing_else() {
        let dir = crate::tests::scratch();
        let store = PosixStore::open(dir.join("store")).unwrap();
        let src = dir.join("src");
        let bytes: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(&src, &bytes).unwrap();
        let id = BlobId::of(&bytes);

        let wrong = BlobId::of(b"something else");
        let e = store.put(wrong, &src).await.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::InvalidData);
        assert_eq!(store.stat(wrong).await.unwrap_err().kind(), io::ErrorKind::NotFound);
        assert_eq!(std::fs::read_dir(dir.join("store/tmp")).unwrap().count(), 0);

        let r = store.put(id, &src).await.unwrap();
        assert_eq!(r, PutReceipt { size: bytes.len() as u64, existed: false });
        assert!(store.put(id, &src).await.unwrap().existed);
        assert_eq!(store.stat(id).await.unwrap().size, bytes.len() as u64);

        let reqs = vec![
            ReadReq { offset: 0, buf: vec![0; 10] },
            ReadReq { offset: 2_999_990, buf: vec![0; 10] },
        ];
        let reqs = store.read_vectored(id, reqs).await.unwrap();
        assert_eq!(reqs[0].buf, bytes[..10]);
        assert_eq!(reqs[1].buf, bytes[2_999_990..]);
        let past = vec![ReadReq { offset: 2_999_995, buf: vec![0; 10] }];
        let e = store.read_vectored(id, past).await.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof);

        store.delete(id).await.unwrap();
        store.delete(id).await.unwrap();
        assert_eq!(store.stat(id).await.unwrap_err().kind(), io::ErrorKind::NotFound);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
