//! Where blobs live between nodes: the [`BlobStore`] trait and [`PosixStore`], the one for a local
//! disk or a shared mount such as NFS or Lustre.

use std::fs::File;
use std::io::{self, Read, Write};
use std::os::unix::fs::{FileExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use futures::future::BoxFuture;
use futures::{StreamExt, TryStreamExt};

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

    /// Stores each file as [`put`](Self::put) does, and hands the receipts back in the same
    /// order. A store may make them durable all together, which for thousands of small chunks
    /// is much cheaper than one at a time. If it fails, some of them may be stored.
    fn put_many(
        &self,
        blobs: Vec<(BlobId, PathBuf)>,
    ) -> BoxFuture<'_, io::Result<Vec<PutReceipt>>> {
        Box::pin(async move {
            futures::stream::iter(blobs)
                .map(|(blob, src)| async move { self.put(blob, &src).await })
                .buffered(16)
                .try_collect()
                .await
        })
    }

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

    fn put_many(
        &self,
        blobs: Vec<(BlobId, PathBuf)>,
    ) -> BoxFuture<'_, io::Result<Vec<PutReceipt>>> {
        let id = std::process::id();
        let blobs: Vec<Put> = blobs
            .into_iter()
            .map(|(blob, src)| Put {
                path: self.path(blob),
                tmp: self
                    .root
                    .join("tmp")
                    .join(format!("{blob}.{id}.{}", self.tmp.fetch_add(1, Ordering::Relaxed))),
                blob,
                src,
            })
            .collect();
        let root = self.root.clone();
        Box::pin(blocking(move || put_all(&root, &blobs)))
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

struct Put {
    blob: BlobId,
    src: PathBuf,
    tmp: PathBuf,
    path: PathBuf,
}

/// What checking one blob of a [`put_all`] found.
enum Checked {
    /// The store has it, this big.
    Existed(u64),
    /// It is to be linked from its source, on the store's filesystem.
    Link(u64),
    /// It was copied to its temporary file, from another filesystem.
    Copied(u64),
}

/// Puts every blob as [`put_file`] does one, but flushes the filesystem twice for all of them, once
/// for their bytes and once for their names, where [`put_file`] flushes a file and a directory for
/// each. A blob still only gets its name once its bytes are on disk. The blobs are checked on a
/// few threads at once, since thousands of small reads are mostly waiting.
fn put_all(root: &Path, blobs: &[Put]) -> io::Result<Vec<PutReceipt>> {
    let dev = std::fs::metadata(root)?.dev();
    let threads = std::thread::available_parallelism().map_or(1, std::num::NonZero::get).min(8);
    let checked: Vec<io::Result<Checked>> = std::thread::scope(|s| {
        let parts: Vec<_> = blobs
            .chunks(blobs.len().div_ceil(threads).max(1))
            .map(|part| s.spawn(move || part.iter().map(|p| check(p, dev)).collect::<Vec<_>>()))
            .collect();
        parts.into_iter().flat_map(|h| h.join().expect("a check thread panicked")).collect()
    });
    let r = (|| {
        let checked = checked
            .iter()
            .map(|c| c.as_ref().map_err(clone_error))
            .collect::<Result<Vec<_>, _>>()?;
        let fs = File::open(root)?;
        rustix::fs::syncfs(&fs)?;
        let mut receipts = Vec::with_capacity(blobs.len());
        for (p, c) in blobs.iter().zip(checked) {
            let (size, named) = match c {
                Checked::Existed(size) => {
                    receipts.push(PutReceipt { size: *size, existed: true });
                    continue;
                }
                Checked::Link(size) | Checked::Copied(size) => (*size, p.path.parent()),
            };
            std::fs::create_dir_all(named.expect("blob paths have a parent"))?;
            let named = match c {
                Checked::Link(_) => std::fs::hard_link(&p.src, &p.path),
                _ => std::fs::rename(&p.tmp, &p.path),
            };
            receipts.push(match named {
                Ok(()) => PutReceipt { size, existed: false },
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                    PutReceipt { size, existed: true }
                }
                // A filesystem without hard links: the one at a time way copies it.
                Err(_) if matches!(c, Checked::Link(_)) => {
                    put_file(p.blob, &p.src, &p.tmp, &p.path)?
                }
                Err(e) => return Err(e),
            });
        }
        rustix::fs::syncfs(&fs)?;
        Ok(receipts)
    })();
    for (p, c) in blobs.iter().zip(&checked) {
        if matches!(c, Ok(Checked::Copied(_))) {
            let _ = std::fs::remove_file(&p.tmp);
        }
    }
    r
}

fn check(p: &Put, dev: u64) -> io::Result<Checked> {
    if let Ok(m) = std::fs::metadata(&p.path) {
        return Ok(Checked::Existed(m.len()));
    }
    let got = hash_file(&p.src)?;
    if got != p.blob {
        let what = format!("{} hashes to {got}, not {}", p.src.display(), p.blob);
        return Err(io::Error::new(io::ErrorKind::InvalidData, what));
    }
    std::fs::set_permissions(&p.src, std::fs::Permissions::from_mode(0o444))?;
    let m = std::fs::metadata(&p.src)?;
    // On the same filesystem the blob is a second link to `src`, and on another one it is copied
    // next to where it goes, so both can be named after one flush.
    if m.dev() == dev {
        return Ok(Checked::Link(m.len()));
    }
    let r = (|| {
        io::copy(&mut File::open(&p.src)?, &mut File::create(&p.tmp)?)?;
        let got = hash_file(&p.tmp)?;
        if got != p.blob {
            let what = format!("{} copied hashes to {got}", p.src.display());
            return Err(io::Error::new(io::ErrorKind::InvalidData, what));
        }
        Ok(Checked::Copied(m.len()))
    })();
    if r.is_err() {
        let _ = std::fs::remove_file(&p.tmp);
    }
    r
}

fn clone_error(e: &io::Error) -> io::Error {
    io::Error::new(e.kind(), e.to_string())
}

/// The name of the file at `path`.
///
/// # Errors
///
/// It cannot be read.
pub fn hash_file(path: &Path) -> io::Result<BlobId> {
    let mut h = blake3::Hasher::new();
    let mut f = File::open(path)?;
    // No bigger than the file, since zeroing 1 MiB costs more than hashing a 64 KiB chunk.
    let len = f.metadata()?.len().clamp(1, 1 << 20);
    let mut buf = vec![0; usize::try_from(len).map_err(io::Error::other)?];
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

    #[tokio::test(flavor = "multi_thread")]
    async fn many_are_stored_together_and_checked_each() {
        let dir = crate::tests::scratch();
        let store = PosixStore::open(dir.join("store")).unwrap();
        let mut blobs = Vec::new();
        for i in 0..40u32 {
            let bytes: Vec<u8> = (0..1000 + i).map(|b| (b * (i + 1)) as u8).collect();
            let src = dir.join(format!("c{i}"));
            std::fs::write(&src, &bytes).unwrap();
            blobs.push((BlobId::of(&bytes), src));
        }
        store.put(blobs[3].0, &blobs[3].1).await.unwrap();
        let receipts = store.put_many(blobs.clone()).await.unwrap();
        assert_eq!(receipts.len(), 40);
        for (i, (r, (id, _))) in receipts.iter().zip(&blobs).enumerate() {
            assert_eq!(*r, PutReceipt { size: 1000 + i as u64, existed: i == 3 });
            assert_eq!(store.stat(*id).await.unwrap().size, 1000 + i as u64);
        }
        assert!(store.put_many(blobs.clone()).await.unwrap().iter().all(|r| r.existed));

        let src = dir.join("odd");
        std::fs::write(&src, b"not what it says").unwrap();
        let wrong = BlobId::of(b"something else");
        let e = store.put_many(vec![(wrong, src)]).await.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::InvalidData);
        assert_eq!(store.stat(wrong).await.unwrap_err().kind(), io::ErrorKind::NotFound);
        assert_eq!(std::fs::read_dir(dir.join("store/tmp")).unwrap().count(), 0);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
