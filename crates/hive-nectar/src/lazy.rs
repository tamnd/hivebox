//! Lazy filling: a blob in the L1 cache that can be read before it has all arrived.
//!
//! [`Cache::lazy`] gives a [`Lazy`] over the same `<name>.part` and `<name>.map` files a whole
//! fetch uses. A read fetches the chunks it needs that are not in yet, plus a little more after
//! them, and checks each one against the blob's [`Leaves`] before it lets anyone see it. Reads
//! that need a chunk already on its way wait for that fetch rather than starting another, and a
//! fetch runs on its own task, so a reader that gives up does not strand the chunks it claimed.
//! [`Lazy::fill_rest`] fetches the rest in the background in big reads, in a given order first,
//! so a prefetch trace can lead.
//!
//! A chunk is readable as soon as its bytes are written. The map that records it is written
//! after the part file is synced, at most [`FLUSH_AFTER`] later and in one go for every chunk
//! that came in meanwhile, so a read never waits on a disk sync and a crash still never leaves a
//! bit set for bytes that are not on disk. When the last chunk is in, the part file is renamed to
//! the blob's name, as a whole fetch does, and the blob counts as in the cache.

use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::os::unix::fs::{FileExt, PermissionsExt};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use tokio::sync::watch;

use crate::BlobId;
use crate::cache::{Bitmap, CHUNK, Cache, Held, open_part};
use crate::leaves::Leaves;
use crate::store::{BlobStore, ReadReq, blocking};

/// Chunks a read fetches from its first missing one, 1 MiB, so a file read front to back costs
/// one request a MiB rather than one a chunk.
const DEMAND: u64 = 4;

/// Background fetches in flight for one blob. Reads never queue behind them.
const BACKGROUND: usize = 4;

/// Reads up to this long are done on the calling task rather than on a blocking thread.
const INLINE_READ: usize = 256 << 10;

/// The longest a chunk that came in waits before the map records it.
pub const FLUSH_AFTER: Duration = Duration::from_millis(50);

/// How a fetch ended, once it has.
type Outcome = Option<Result<(), (io::ErrorKind, Arc<str>)>>;

/// A blob in the cache that may still be arriving. Clones share one fill.
#[derive(Clone, Debug)]
pub struct Lazy {
    inner: Arc<Inner>,
}

pub(crate) struct Inner {
    blob: BlobId,
    size: u64,
    chunks: u64,
    store: Arc<dyn BlobStore>,
    leaves: Option<Leaves>,
    /// The part file while filling, which stays open across the rename at the end.
    file: File,
    map_file: Option<File>,
    part_path: PathBuf,
    /// Background read size in chunks.
    ideal: u64,
    fill: Mutex<Fill>,
    finished: watch::Sender<Outcome>,
    requests: AtomicU64,
    bytes: AtomicU64,
    trace: Mutex<Trace>,
    /// Keeps the blob pinned while anything still uses it.
    held: Held,
}

/// The chunks reads have asked for, in the order they first asked.
#[derive(Default)]
struct Trace {
    seen: Vec<u64>,
    order: Vec<u64>,
}

impl Trace {
    fn touch(&mut self, first: u64, end: u64) {
        for c in first..end {
            let (word, bit) = ((c / 64) as usize, 1 << (c % 64));
            if self.seen.len() <= word {
                self.seen.resize(word + 1, 0);
            }
            if self.seen[word] & bit == 0 {
                self.seen[word] |= bit;
                self.order.push(c);
            }
        }
    }
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lazy").field("blob", &self.blob).field("size", &self.size).finish()
    }
}

struct Fill {
    /// `None` once every chunk is in.
    map: Option<Bitmap>,
    missing: u64,
    flying: HashMap<u64, watch::Receiver<Outcome>>,
    flush_queued: bool,
}

impl Fill {
    fn has(&self, chunk: u64) -> bool {
        self.map.as_ref().is_none_or(|m| m.has(chunk))
    }

    /// Claims the chunks from `start` that are neither in nor on their way, up to `end`.
    fn claim(&mut self, start: u64, end: u64) -> (Run, watch::Receiver<Outcome>) {
        let mut stop = start + 1;
        while stop < end && !self.has(stop) && !self.flying.contains_key(&stop) {
            stop += 1;
        }
        let (tx, rx) = watch::channel(None);
        for c in start..stop {
            self.flying.insert(c, rx.clone());
        }
        (Run { start, end: stop, tx }, rx)
    }
}

/// Chunks `start..end`, being fetched by one request.
struct Run {
    start: u64,
    end: u64,
    tx: watch::Sender<Outcome>,
}

/// What a fill has done.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Progress {
    /// Chunks in the cache.
    pub have: u64,
    /// Chunks in the blob.
    pub chunks: u64,
    /// Requests made to the store.
    pub requests: u64,
    /// Bytes fetched from the store.
    pub bytes: u64,
}

impl Cache {
    /// `blob`, `size` bytes, readable at once, its chunks fetched from `store` as they are read
    /// and checked against the leaves stored as `leaves`. A blob already in the cache comes back
    /// complete. Callers for the same blob share one fill, and [`Cache::get`] on a blob being
    /// filled finishes the fill.
    ///
    /// # Errors
    ///
    /// There is no room, the leaves cannot be had or do not belong to `blob`, or the part file
    /// cannot be opened.
    ///
    /// # Panics
    ///
    /// Only if another thread panicked while holding the cache's lock.
    pub async fn lazy(
        self: &Arc<Self>,
        store: Arc<dyn BlobStore>,
        blob: BlobId,
        size: u64,
        leaves: BlobId,
    ) -> io::Result<Lazy> {
        let (size, fill) = match self.pin(blob) {
            Some(v) => v,
            None => self.reserve(blob, size)?,
        };
        let held = Held { cache: self.clone(), blob, path: self.dir.join(blob.to_string()), size };
        let _one = fill.lock().await;
        if let Some(inner) =
            self.lazies.lock().expect("lazy lock").get(&blob).and_then(Weak::upgrade)
        {
            return Ok(Lazy { inner });
        }
        let made = if self.has(blob) {
            Inner::complete(store, held)
        } else {
            match Inner::open(store, held, leaves).await {
                Ok(inner) => Ok(inner),
                Err((held, e)) => {
                    drop(held);
                    self.forget_if_unused(blob);
                    return Err(e);
                }
            }
        }?;
        let inner = Arc::new(made);
        self.lazies.lock().expect("lazy lock").insert(blob, Arc::downgrade(&inner));
        let left = {
            let mut f = inner.fill.lock().expect("fill lock");
            // A part file a crash left with every chunk in only needs its rename.
            let all_in = f.map.is_some() && f.missing == 0;
            if all_in {
                f.map = None;
            }
            all_in
        };
        if left {
            inner.clone().finish();
        }
        Ok(Lazy { inner })
    }

    /// The fill of `blob` under way, if there is one.
    pub(crate) fn filling(&self, blob: BlobId) -> Option<Lazy> {
        let inner = self.lazies.lock().expect("lazy lock").get(&blob).and_then(Weak::upgrade)?;
        Some(Lazy { inner })
    }
}

impl Inner {
    fn complete(store: Arc<dyn BlobStore>, held: Held) -> io::Result<Self> {
        let file = File::open(&held.path)?;
        let (finished, _) = watch::channel(Some(Ok(())));
        let fill = Fill { map: None, missing: 0, flying: HashMap::new(), flush_queued: false };
        Ok(Self {
            blob: held.blob,
            size: held.size,
            chunks: held.size.div_ceil(CHUNK),
            ideal: 1,
            store,
            leaves: None,
            file,
            map_file: None,
            part_path: held.path.clone(),
            fill: Mutex::new(fill),
            finished,
            requests: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            trace: Mutex::default(),
            held,
        })
    }

    async fn open(
        store: Arc<dyn BlobStore>,
        held: Held,
        leaves_id: BlobId,
    ) -> Result<Self, (Held, io::Error)> {
        let (blob, size) = (held.blob, held.size);
        let leaves = async {
            let len =
                usize::try_from(size.div_ceil(CHUNK).max(1) * 32).map_err(io::Error::other)?;
            let got =
                store.read_vectored(leaves_id, vec![ReadReq { offset: 0, buf: vec![0; len] }]);
            let bytes = got.await?.pop().expect("one read in, one out").buf;
            Leaves::from_bytes(blob, size, &bytes)
        };
        let leaves = match leaves.await {
            Ok(l) => l,
            Err(e) => return Err((held, e)),
        };
        let name = blob.to_string();
        let part_path = held.cache.dir.join(format!("{name}.part"));
        let map_path = held.cache.dir.join(format!("{name}.map"));
        let opened = {
            let (part_path, map_path) = (part_path.clone(), map_path.clone());
            blocking(move || {
                let (part, map, missing) = open_part(&part_path, &map_path, size)?;
                let map_file = map.file.try_clone()?;
                Ok((part, map, map_file, missing.len() as u64))
            })
            .await
        };
        let (file, map, map_file, missing) = match opened {
            Ok(v) => v,
            Err(e) => return Err((held, e)),
        };
        let ideal = (store.caps().ideal_io as u64 / CHUNK).max(1);
        let fill = Fill { map: Some(map), missing, flying: HashMap::new(), flush_queued: false };
        Ok(Self {
            blob,
            size,
            chunks: size.div_ceil(CHUNK),
            ideal,
            store,
            leaves: Some(leaves),
            file,
            map_file: Some(map_file),
            part_path,
            fill: Mutex::new(fill),
            finished: watch::channel(None).0,
            requests: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            trace: Mutex::default(),
            held,
        })
    }

    /// Makes sure chunks `first..end` are in, fetching what is missing and waiting for what is
    /// on its way.
    async fn ensure(self: &Arc<Self>, first: u64, end: u64) -> io::Result<()> {
        let mut waits = Vec::new();
        {
            let mut f = self.fill.lock().expect("fill lock");
            let mut c = first;
            while c < end {
                if f.has(c) {
                    c += 1;
                } else if let Some(rx) = f.flying.get(&c) {
                    waits.push(rx.clone());
                    c += 1;
                } else {
                    // Read ahead a little past what was asked, but no further than the store
                    // likes in one read.
                    let limit =
                        (c + DEMAND.max(end - c).min(self.ideal.max(DEMAND))).min(self.chunks);
                    let (run, rx) = f.claim(c, limit);
                    c = run.end;
                    waits.push(rx);
                    tokio::spawn(self.clone().fetch(run));
                }
            }
        }
        for rx in waits {
            wait(rx).await?;
        }
        Ok(())
    }

    /// Fetches one run, checks it, writes it, and tells whoever waits on it.
    async fn fetch(self: Arc<Self>, run: Run) {
        let got = self.fetch_run(run.start, run.end).await;
        let last = {
            let mut f = self.fill.lock().expect("fill lock");
            for c in run.start..run.end {
                f.flying.remove(&c);
            }
            if got.is_ok() {
                let map = f.map.as_mut().expect("a fill in progress has a map");
                for c in run.start..run.end {
                    map.set(c);
                }
                f.missing -= run.end - run.start;
                if f.missing == 0 {
                    f.map = None;
                } else if !f.flush_queued {
                    f.flush_queued = true;
                    tokio::spawn(self.clone().flush());
                }
            }
            got.is_ok() && f.missing == 0
        };
        if last {
            self.clone().finish();
        }
        let _ = run.tx.send(Some(got.map_err(|e| (e.kind(), Arc::from(e.to_string())))));
    }

    async fn fetch_run(self: &Arc<Self>, start: u64, end: u64) -> io::Result<()> {
        let leaves = self.leaves.as_ref().expect("a fill in progress has leaves");
        let offset = start * CHUNK;
        let len =
            usize::try_from((end * CHUNK).min(self.size) - offset).map_err(io::Error::other)?;
        let req = vec![ReadReq { offset, buf: vec![0; len] }];
        let buf = self.store.read_vectored(self.blob, req).await?.pop().expect("one read").buf;
        self.requests.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(len as u64, Ordering::Relaxed);
        for (i, chunk) in buf.chunks(CHUNK as usize).enumerate() {
            let c = start + i as u64;
            if !leaves.check(c, chunk) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("chunk {c} of {} from the store does not match its leaf", self.blob),
                ));
            }
        }
        let me = self.clone();
        blocking(move || me.file.write_all_at(&buf, offset)).await
    }

    /// Records every chunk that came in since the last flush, once their bytes are on disk.
    async fn flush(self: Arc<Self>) {
        tokio::time::sleep(FLUSH_AFTER).await;
        let bits = {
            let mut f = self.fill.lock().expect("fill lock");
            f.flush_queued = false;
            match &f.map {
                Some(m) => m.bits.clone(),
                None => return,
            }
        };
        let me = self.clone();
        // Losing a flush only means fetching those chunks again after a crash.
        let _ = blocking(move || {
            me.file.sync_data()?;
            let map = me.map_file.as_ref().expect("a fill in progress has a map");
            map.write_all_at(&bits, 0)?;
            map.sync_data()
        })
        .await;
    }

    /// Puts the finished blob in place, once every chunk is in and the map is gone.
    fn finish(self: Arc<Self>) {
        tokio::spawn(async move {
            let me = self.clone();
            let done = blocking(move || {
                me.file.sync_data()?;
                std::fs::set_permissions(&me.part_path, std::fs::Permissions::from_mode(0o444))?;
                let dir = me.part_path.parent().expect("cache files have a parent");
                std::fs::rename(&me.part_path, &me.held.path)?;
                File::open(dir)?.sync_all()?;
                let _ = std::fs::remove_file(me.part_path.with_extension("map"));
                Ok(())
            })
            .await;
            if done.is_ok() {
                let mut st = self.held.cache.state.lock().expect("cache lock");
                if let Some(e) = st.blobs.get_mut(&self.blob) {
                    e.ready = true;
                }
            }
            let _ =
                self.finished.send(Some(done.map_err(|e| (e.kind(), Arc::from(e.to_string())))));
        });
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        let mut lazies = self.held.cache.lazies.lock().expect("lazy lock");
        if lazies.get(&self.blob).is_some_and(|w| w.strong_count() == 0) {
            lazies.remove(&self.blob);
        }
    }
}

impl Lazy {
    /// Which blob it is.
    #[must_use]
    pub fn blob(&self) -> BlobId {
        self.inner.blob
    }

    /// Its size in bytes.
    #[must_use]
    pub fn size(&self) -> u64 {
        self.inner.size
    }

    /// `len` bytes from `offset`, fetching what is not in yet.
    ///
    /// # Errors
    ///
    /// The range runs past the end, which is `UnexpectedEof`, the store fails, or what it sends
    /// does not match the leaves, which is `InvalidData`.
    pub async fn read_at(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let end =
            offset.checked_add(len as u64).filter(|&e| e <= self.inner.size).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("{} ends before that", self.inner.blob),
                )
            })?;
        if len == 0 {
            return Ok(Vec::new());
        }
        let (first, last) = (offset / CHUNK, end.div_ceil(CHUNK));
        self.inner.trace.lock().unwrap_or_else(PoisonError::into_inner).touch(first, last);
        self.inner.ensure(first, last).await?;
        // The chunks were just written or read, so a small read is almost always in the page
        // cache and costs less than the hop to a blocking thread.
        if len <= INLINE_READ {
            let mut buf = vec![0; len];
            self.inner.file.read_exact_at(&mut buf, offset)?;
            return Ok(buf);
        }
        let inner = self.inner.clone();
        blocking(move || {
            let mut buf = vec![0; len];
            inner.file.read_exact_at(&mut buf, offset)?;
            Ok(buf)
        })
        .await
    }

    /// Fetches every chunk not in yet, those in `first` before the rest, and returns once the
    /// blob is complete and in place.
    ///
    /// # Errors
    ///
    /// The store fails, or what it sends does not match the leaves.
    ///
    /// # Panics
    ///
    /// Only if another thread panicked while holding the fill's lock.
    pub async fn fill_rest(&self, first: &[u64]) -> io::Result<()> {
        let inner = &self.inner;
        // A plain loop rather than stream combinators, whose closures make the future too
        // general to be Send when the cache awaits it.
        let order = first.iter().copied().filter(|&c| c < inner.chunks).chain(0..inner.chunks);
        let mut out = FuturesUnordered::new();
        for c in order {
            let claimed = {
                let mut f = inner.fill.lock().expect("fill lock");
                if f.has(c) || f.flying.contains_key(&c) {
                    None
                } else {
                    let (run, rx) = f.claim(c, (c + inner.ideal).min(inner.chunks));
                    tokio::spawn(inner.clone().fetch(run));
                    Some(rx)
                }
            };
            if let Some(rx) = claimed {
                out.push(wait(rx));
            }
            while out.len() >= BACKGROUND {
                if let Some(r) = out.next().await {
                    r?;
                }
            }
        }
        while let Some(r) = out.next().await {
            r?;
        }
        // Reads may still have fetches out for the last few chunks.
        inner.ensure(0, inner.chunks).await?;
        wait(inner.finished.subscribe()).await
    }

    /// Whether every chunk is in.
    ///
    /// # Panics
    ///
    /// Only if another thread panicked while holding the fill's lock.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.inner.fill.lock().expect("fill lock").missing == 0
    }

    /// What it has done so far.
    ///
    /// # Panics
    ///
    /// Only if another thread panicked while holding the fill's lock.
    #[must_use]
    pub fn progress(&self) -> Progress {
        let missing = self.inner.fill.lock().expect("fill lock").missing;
        Progress {
            have: self.inner.chunks - missing,
            chunks: self.inner.chunks,
            requests: self.inner.requests.load(Ordering::Relaxed),
            bytes: self.inner.bytes.load(Ordering::Relaxed),
        }
    }

    /// The chunks reads have asked for so far, in the order they first asked, as a prefetch
    /// trace for [`Lazy::fill_rest`] the next time. Chunks only the background fill brought in
    /// are not in it.
    #[must_use]
    pub fn trace(&self) -> Vec<u64> {
        self.inner.trace.lock().unwrap_or_else(PoisonError::into_inner).order.clone()
    }
}

async fn wait(mut rx: watch::Receiver<Outcome>) -> io::Result<()> {
    let got =
        rx.wait_for(Option::is_some).await.map_err(|_| io::Error::other("a fetch was dropped"))?;
    match got.as_ref().expect("waited for it") {
        Ok(()) => Ok(()),
        Err((kind, why)) => Err(io::Error::new(*kind, why.to_string())),
    }
}

/// A lazy blob serves a block device, so EROFS can mount a layer before it is all in.
#[cfg(target_os = "linux")]
impl hive_blockd::Source for Lazy {
    fn size(&self) -> u64 {
        self.inner.size
    }

    fn read_at(&self, offset: u64, len: usize) -> futures::future::BoxFuture<'_, io::Result<Vec<u8>>> {
        Box::pin(Self::read_at(self, offset, len))
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::atomic::AtomicBool;

    use futures::future::BoxFuture;

    use super::*;
    use crate::store::{BlobCaps, BlobStat, PosixStore, PutReceipt};

    /// A store that counts reads and can be told to send bad bytes.
    struct Counting {
        inner: PosixStore,
        reads: AtomicU64,
        corrupt: AtomicBool,
    }

    impl BlobStore for Counting {
        fn caps(&self) -> BlobCaps {
            BlobCaps { ideal_io: 1 << 20, ..self.inner.caps() }
        }

        fn read_vectored(
            &self,
            blob: BlobId,
            reqs: Vec<ReadReq>,
        ) -> BoxFuture<'_, io::Result<Vec<ReadReq>>> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            Box::pin(async move {
                // Slow enough that readers at once overlap.
                tokio::time::sleep(Duration::from_millis(20)).await;
                let mut got = self.inner.read_vectored(blob, reqs).await?;
                if self.corrupt.load(Ordering::Relaxed) {
                    for r in &mut got {
                        if let Some(b) = r.buf.last_mut() {
                            *b ^= 1;
                        }
                    }
                }
                Ok(got)
            })
        }

        fn put<'a>(&'a self, blob: BlobId, src: &'a Path) -> BoxFuture<'a, io::Result<PutReceipt>> {
            self.inner.put(blob, src)
        }

        fn stat(&self, blob: BlobId) -> BoxFuture<'_, io::Result<BlobStat>> {
            self.inner.stat(blob)
        }

        fn delete(&self, blob: BlobId) -> BoxFuture<'_, io::Result<()>> {
            self.inner.delete(blob)
        }
    }

    impl Counting {
        fn reads(&self) -> u64 {
            self.reads.load(Ordering::Relaxed)
        }
    }

    /// A store with one blob of `len` bytes and its leaves in it.
    async fn setup(dir: &Path, len: usize) -> (Arc<Counting>, Vec<u8>, BlobId, BlobId) {
        let store = Arc::new(Counting {
            inner: PosixStore::open(dir.join("store")).unwrap(),
            reads: AtomicU64::new(0),
            corrupt: AtomicBool::new(false),
        });
        let bytes: Vec<u8> = (0..len).map(|i| (i * 13 + i / 4093) as u8).collect();
        let src = dir.join("src");
        std::fs::write(&src, &bytes).unwrap();
        let leaves = Leaves::of_file(&src).unwrap();
        let blob = leaves.blob();
        store.put(blob, &src).await.unwrap();
        let lsrc = dir.join("leaves");
        std::fs::write(&lsrc, leaves.to_bytes()).unwrap();
        let lid = BlobId::of(&leaves.to_bytes());
        store.put(lid, &lsrc).await.unwrap();
        (store, bytes, blob, lid)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reads_fetch_only_what_they_need_once_and_the_rest_fills_in() {
        let dir = crate::tests::scratch();
        let c = CHUNK as usize;
        let len = 40 * c + 1234;
        let (store, bytes, blob, leaves) = setup(&dir, len).await;
        let cache = Cache::open(dir.join("l1"), 1 << 30).unwrap();
        let lazy = cache.lazy(store.clone(), blob, len as u64, leaves).await.unwrap();
        let after_open = store.reads();
        assert_eq!(after_open, 1, "only the leaves");

        // Eight readers of the same few bytes share one fetch of 1 MiB.
        let reads = (0..8).map(|_| lazy.read_at(10 * CHUNK + 5, 100));
        for got in futures::future::join_all(reads).await {
            assert_eq!(got.unwrap(), bytes[10 * c + 5..10 * c + 105]);
        }
        assert_eq!(store.reads() - after_open, 1);
        assert_eq!(lazy.progress().have, DEMAND);
        // What was read ahead costs nothing.
        assert_eq!(lazy.read_at(13 * CHUNK, c).await.unwrap(), bytes[13 * c..14 * c]);
        assert_eq!(store.reads() - after_open, 1);
        // A read across chunks, and the short last chunk.
        assert_eq!(lazy.read_at(2 * CHUNK - 3, 6).await.unwrap(), bytes[2 * c - 3..2 * c + 3]);
        assert_eq!(lazy.read_at(len as u64 - 10, 10).await.unwrap(), bytes[len - 10..]);
        let e = lazy.read_at(len as u64 - 10, 11).await.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof);
        assert!(!cache.has(blob));
        // The trace has each chunk once, in the order reads first wanted it.
        assert_eq!(lazy.trace(), [10, 13, 1, 2, 40]);

        lazy.fill_rest(&[30, 31]).await.unwrap();
        assert_eq!(lazy.trace(), [10, 13, 1, 2, 40], "the background fill is not in it");
        assert!(lazy.is_complete() && cache.has(blob));
        let p = lazy.progress();
        assert_eq!((p.have, p.chunks, p.bytes), (41, 41, len as u64));
        assert_eq!(std::fs::read(dir.join("l1").join(blob.to_string())).unwrap(), bytes);
        assert!(!dir.join("l1").join(format!("{blob}.part")).exists());
        assert!(!dir.join("l1").join(format!("{blob}.map")).exists());
        // Reads after the rename still work, and a second caller shares the same fill.
        assert_eq!(lazy.read_at(0, 10).await.unwrap(), bytes[..10]);
        let again = cache.lazy(store.clone(), blob, len as u64, leaves).await.unwrap();
        assert!(again.is_complete());
        drop((lazy, again));
        let again = cache.lazy(store.clone(), blob, len as u64, leaves).await.unwrap();
        assert!(again.is_complete());
        assert_eq!(again.read_at(20 * CHUNK, 7).await.unwrap(), bytes[20 * c..20 * c + 7]);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bad_bytes_are_refused_and_a_restart_keeps_what_was_flushed() {
        let dir = crate::tests::scratch();
        let c = CHUNK as usize;
        let len = 12 * c;
        let (store, bytes, blob, leaves) = setup(&dir, len).await;
        let cache = Cache::open(dir.join("l1"), 1 << 30).unwrap();
        let lazy = cache.lazy(store.clone(), blob, len as u64, leaves).await.unwrap();

        store.corrupt.store(true, Ordering::Relaxed);
        let e = lazy.read_at(0, 10).await.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::InvalidData);
        assert_eq!(lazy.progress().have, 0);
        // The next read tries again.
        store.corrupt.store(false, Ordering::Relaxed);
        assert_eq!(lazy.read_at(0, 10).await.unwrap(), bytes[..10]);
        assert_eq!(lazy.read_at(8 * CHUNK, 10).await.unwrap(), bytes[8 * c..8 * c + 10]);
        assert_eq!(lazy.progress().have, 2 * DEMAND);
        tokio::time::sleep(FLUSH_AFTER * 4).await;
        drop(lazy);
        drop(cache);

        // A new cache on the same directory picks up the 8 chunks and fetches the other 4.
        let cache = Cache::open(dir.join("l1"), 1 << 30).unwrap();
        let lazy = cache.lazy(store.clone(), blob, len as u64, leaves).await.unwrap();
        assert_eq!(lazy.progress().have, 2 * DEMAND);
        let before = store.reads();
        // get() on a blob being filled lazily finishes the fill rather than starting its own.
        let held = cache.get(store.as_ref(), blob).await.unwrap();
        assert_eq!(std::fs::read(held.path()).unwrap(), bytes);
        assert_eq!(lazy.progress().bytes, 4 * CHUNK);
        assert!(store.reads() - before <= 4);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn leaves_for_another_blob_are_refused() {
        let dir = crate::tests::scratch();
        let len = 3 * CHUNK as usize;
        let (store, _, blob, _) = setup(&dir, len).await;
        let other = dir.join("other");
        let mut wrong = Leaves::of_file(&store.inner.path(blob)).unwrap().to_bytes();
        wrong[0] ^= 1;
        std::fs::write(&other, &wrong).unwrap();
        let wrong_id = BlobId::of(&wrong);
        store.put(wrong_id, &other).await.unwrap();
        let cache = Cache::open(dir.join("l1"), 1 << 30).unwrap();
        let e = cache.lazy(store.clone(), blob, len as u64, wrong_id).await.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::InvalidData);
        assert_eq!(cache.usage().blobs, 0);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
