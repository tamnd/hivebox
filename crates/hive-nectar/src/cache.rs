//! The L1 cache: blobs copied from a [`BlobStore`] onto the node's local disk, so that loop
//! devices and mounts can use them.
//!
//! A blob being fetched is a sparse `<name>.part` file with a `<name>.map` bitmap of the chunks
//! it has. Chunks are written, the file is synced, and only then are their bits set and the map
//! synced, so after a crash every set bit is backed by data on disk and the fetch goes on where it
//! stopped. When every chunk is in, the whole file is checked against its name and renamed to
//! `<name>`. This version fetches whole blobs as soon as they are asked for. Lazy filling, where
//! the kernel asks for chunks as it reads them, comes later and keeps the same files.
//!
//! Blobs in use are pinned and never evicted. The rest go least recently used first when room is
//! needed, and on a restart the order is taken from the files' mtimes.

use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::os::unix::fs::{FileExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use futures::{StreamExt, TryStreamExt};

use crate::BlobId;
use crate::store::{BlobStore, ReadReq, blocking};

/// The unit the cache tracks and fetches in. It matches the default layer chunk size.
pub const CHUNK: u64 = 256 << 10;
/// Bytes asked of the store in one batch.
const BATCH: u64 = 16 << 20;
/// Batches in flight at once for one blob.
const IN_FLIGHT: usize = 4;

/// Blobs on local disk, up to a capacity.
#[derive(Debug)]
pub struct Cache {
    dir: PathBuf,
    capacity: u64,
    state: Mutex<State>,
}

#[derive(Debug, Default)]
struct State {
    blobs: HashMap<BlobId, Entry>,
    used: u64,
    clock: u64,
}

#[derive(Debug)]
struct Entry {
    size: u64,
    ready: bool,
    pins: usize,
    last: u64,
    fill: Arc<tokio::sync::Mutex<()>>,
}

impl Entry {
    fn new(size: u64, ready: bool, last: u64) -> Self {
        Self { size, ready, pins: 0, last, fill: Arc::default() }
    }
}

/// A blob in the cache, pinned there until this is dropped.
#[derive(Debug)]
pub struct Held {
    cache: Arc<Cache>,
    blob: BlobId,
    path: PathBuf,
    size: u64,
}

impl Held {
    /// The blob's file, complete and checked.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Which blob it is.
    #[must_use]
    pub const fn blob(&self) -> BlobId {
        self.blob
    }

    /// Its size in bytes.
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.size
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        let mut st = self.cache.state.lock().expect("cache lock");
        if let Some(e) = st.blobs.get_mut(&self.blob) {
            e.pins -= 1;
        }
    }
}

/// How full the cache is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Usage {
    /// Bytes taken, counting blobs still being fetched at their full size.
    pub used: u64,
    /// The most it may take.
    pub capacity: u64,
    /// Blobs in it, complete or not.
    pub blobs: usize,
    /// Blobs pinned by a [`Held`].
    pub pinned: usize,
}

impl Cache {
    /// Opens the cache in `dir`, making it if needed, and picks up what an earlier run left.
    ///
    /// # Errors
    ///
    /// `dir` cannot be made or read.
    pub fn open(dir: impl Into<PathBuf>, capacity: u64) -> io::Result<Arc<Self>> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        let mut found = Vec::new();
        for f in std::fs::read_dir(&dir)? {
            let f = f?;
            let name = f.file_name();
            let name = name.to_string_lossy();
            let (stem, ready) = match name.strip_suffix(".part") {
                Some(stem) => (stem, false),
                None => (&*name, true),
            };
            let Ok(blob) = stem.parse::<BlobId>() else {
                // A map belongs to a part, and anything else is left from a crash mid-write.
                if !name.ends_with(".map") {
                    let _ = std::fs::remove_file(f.path());
                }
                continue;
            };
            let m = f.metadata()?;
            found.push((m.modified().unwrap_or(SystemTime::UNIX_EPOCH), blob, m.len(), ready));
        }
        found.sort();
        let mut st = State::default();
        for (_, blob, size, ready) in found {
            st.clock += 1;
            st.used += size;
            st.blobs.insert(blob, Entry::new(size, ready, st.clock));
        }
        // Maps whose part is gone are of no use.
        for f in std::fs::read_dir(&dir)? {
            let f = f?;
            let name = f.file_name();
            if let Some(stem) = name.to_string_lossy().strip_suffix(".map")
                && stem.parse::<BlobId>().is_ok_and(|b| !st.blobs.contains_key(&b))
            {
                let _ = std::fs::remove_file(f.path());
            }
        }
        Ok(Arc::new(Self { dir, capacity, state: Mutex::new(st) }))
    }

    /// How full it is.
    ///
    /// # Panics
    ///
    /// Only if another thread panicked while holding the cache's lock.
    #[must_use]
    pub fn usage(&self) -> Usage {
        let st = self.state.lock().expect("cache lock");
        Usage {
            used: st.used,
            capacity: self.capacity,
            blobs: st.blobs.len(),
            pinned: st.blobs.values().filter(|e| e.pins > 0).count(),
        }
    }

    /// Whether `blob` is here and complete.
    ///
    /// # Panics
    ///
    /// Only if another thread panicked while holding the cache's lock.
    #[must_use]
    pub fn has(&self, blob: BlobId) -> bool {
        self.state.lock().expect("cache lock").blobs.get(&blob).is_some_and(|e| e.ready)
    }

    /// Gets `blob` into the cache from `store`, unless it is there already, and pins it. Callers
    /// asking for the same blob at once share one fetch.
    ///
    /// # Errors
    ///
    /// The store fails, what it sends does not hash to `blob`, or there is no room without
    /// evicting pinned blobs, which is `StorageFull`.
    ///
    /// # Panics
    ///
    /// Only if another thread panicked while holding the cache's lock.
    pub async fn get(self: &Arc<Self>, store: &dyn BlobStore, blob: BlobId) -> io::Result<Held> {
        let known = self.pin(blob);
        let (size, fill) = match known {
            Some(v) => v,
            None => {
                let size = store.stat(blob).await?.size;
                self.reserve(blob, size)?
            }
        };
        let held = Held { cache: self.clone(), blob, path: self.dir.join(blob.to_string()), size };
        if self.has(blob) {
            touch(held.path.clone());
            return Ok(held);
        }
        let _one = fill.lock().await;
        if self.has(blob) {
            return Ok(held);
        }
        match self.fetch(store, blob, size).await {
            Ok(()) => {
                let mut st = self.state.lock().expect("cache lock");
                if let Some(e) = st.blobs.get_mut(&blob) {
                    e.ready = true;
                }
                Ok(held)
            }
            Err(e) => {
                drop(held);
                self.forget_if_unused(blob);
                Err(e)
            }
        }
    }

    /// Pins a blob the cache knows, and says how big it is.
    fn pin(&self, blob: BlobId) -> Option<(u64, Arc<tokio::sync::Mutex<()>>)> {
        let mut st = self.state.lock().expect("cache lock");
        st.clock += 1;
        let now = st.clock;
        let e = st.blobs.get_mut(&blob)?;
        e.pins += 1;
        e.last = now;
        Some((e.size, e.fill.clone()))
    }

    /// Makes room for a new blob, evicting what it must, and pins it.
    fn reserve(&self, blob: BlobId, size: u64) -> io::Result<(u64, Arc<tokio::sync::Mutex<()>>)> {
        let mut victims = Vec::new();
        let got = {
            let mut st = self.state.lock().expect("cache lock");
            // Another caller may have added it while this one asked the store.
            st.clock += 1;
            let now = st.clock;
            if let Some(e) = st.blobs.get_mut(&blob) {
                e.pins += 1;
                e.last = now;
                Ok((e.size, e.fill.clone()))
            } else {
                let mut idle: Vec<(u64, BlobId)> = st
                    .blobs
                    .iter()
                    .filter(|(_, e)| e.pins == 0)
                    .map(|(b, e)| (e.last, *b))
                    .collect();
                idle.sort_unstable();
                let mut idle = idle.into_iter();
                while st.used + size > self.capacity {
                    let Some((_, b)) = idle.next() else { break };
                    let e = st.blobs.remove(&b).expect("it was just listed");
                    st.used -= e.size;
                    victims.push(b);
                }
                if st.used + size > self.capacity {
                    Err(io::Error::new(
                        io::ErrorKind::StorageFull,
                        format!(
                            "the cache has {} of {} bytes pinned and cannot take {size} more",
                            st.used, self.capacity
                        ),
                    ))
                } else {
                    st.used += size;
                    let mut e = Entry::new(size, false, now);
                    e.pins = 1;
                    let fill = e.fill.clone();
                    st.blobs.insert(blob, e);
                    Ok((size, fill))
                }
            }
        };
        for b in victims {
            self.remove_files(b);
        }
        got
    }

    /// Drops a blob that failed to arrive, unless someone else is still waiting on it.
    fn forget_if_unused(&self, blob: BlobId) {
        let gone = {
            let mut st = self.state.lock().expect("cache lock");
            match st.blobs.get(&blob) {
                Some(e) if e.pins == 0 && !e.ready => {
                    let size = e.size;
                    st.blobs.remove(&blob);
                    st.used -= size;
                    true
                }
                _ => false,
            }
        };
        if gone {
            self.remove_files(blob);
        }
    }

    fn remove_files(&self, blob: BlobId) {
        let name = blob.to_string();
        for suffix in ["", ".part", ".map"] {
            let _ = std::fs::remove_file(self.dir.join(format!("{name}{suffix}")));
        }
    }

    /// Fetches every chunk the part file lacks, checks the whole, and renames it into place.
    async fn fetch(&self, store: &dyn BlobStore, blob: BlobId, size: u64) -> io::Result<()> {
        let name = blob.to_string();
        let part_path = self.dir.join(format!("{name}.part"));
        let map_path = self.dir.join(format!("{name}.map"));
        let (part, map, missing) = {
            let (part_path, map_path) = (part_path.clone(), map_path.clone());
            blocking(move || open_part(&part_path, &map_path, size)).await?
        };
        let (part, map) = (Arc::new(part), Arc::new(Mutex::new(map)));
        let ideal = (store.caps().ideal_io as u64).max(CHUNK) / CHUNK * CHUNK;
        let batches = plan(&missing, size, ideal);
        futures::stream::iter(batches)
            .map(|batch| store.read_vectored(blob, batch))
            .buffer_unordered(IN_FLIGHT)
            .map_err(|e| io::Error::new(e.kind(), format!("fetching {blob}: {e}")))
            .try_for_each(|reqs| {
                let (part, map) = (part.clone(), map.clone());
                blocking(move || {
                    for r in &reqs {
                        part.write_all_at(&r.buf, r.offset)?;
                    }
                    part.sync_data()?;
                    let mut map = map.lock().expect("map lock");
                    for r in &reqs {
                        let first = r.offset / CHUNK;
                        for c in first..(r.offset + r.buf.len() as u64).div_ceil(CHUNK) {
                            map.set(c);
                        }
                    }
                    map.flush()
                })
            })
            .await?;
        blocking(move || {
            let got = crate::store::hash_file(&part_path)?;
            if got != blob {
                let _ = std::fs::remove_file(&part_path);
                let _ = std::fs::remove_file(&map_path);
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("the store sent bytes for {blob} that hash to {got}"),
                ));
            }
            std::fs::set_permissions(&part_path, std::fs::Permissions::from_mode(0o444))?;
            let done = part_path.with_extension("");
            std::fs::rename(&part_path, &done)?;
            File::open(done.parent().expect("cache files have a parent"))?.sync_all()?;
            let _ = std::fs::remove_file(&map_path);
            Ok(())
        })
        .await
    }
}

/// Which chunks a part file has, kept on disk as a little-endian chunk size followed by one bit
/// per chunk.
#[derive(Debug)]
struct Bitmap {
    file: File,
    bits: Vec<u8>,
}

impl Bitmap {
    const HEAD: usize = 4;

    fn has(&self, chunk: u64) -> bool {
        self.bits[Self::HEAD + (chunk / 8) as usize] & (1 << (chunk % 8)) != 0
    }

    fn set(&mut self, chunk: u64) {
        self.bits[Self::HEAD + (chunk / 8) as usize] |= 1 << (chunk % 8);
    }

    fn flush(&self) -> io::Result<()> {
        self.file.write_all_at(&self.bits, 0)?;
        self.file.sync_data()
    }
}

/// Opens or makes a part file and its map, and lists the chunks it still needs.
fn open_part(part_path: &Path, map_path: &Path, size: u64) -> io::Result<(File, Bitmap, Vec<u64>)> {
    let chunks = size.div_ceil(CHUNK);
    let len = Bitmap::HEAD + chunks.div_ceil(8) as usize;
    let part =
        File::options().read(true).write(true).create(true).truncate(false).open(part_path)?;
    let file =
        File::options().read(true).write(true).create(true).truncate(false).open(map_path)?;
    let mut bits = std::fs::read(map_path)?;
    let fresh = part.metadata()?.len() != size
        || bits.len() != len
        || bits[..Bitmap::HEAD] != (CHUNK as u32).to_le_bytes();
    if fresh {
        // Nothing here can be trusted, so the fetch starts over.
        part.set_len(0)?;
        part.set_len(size)?;
        bits = vec![0; len];
        bits[..Bitmap::HEAD].copy_from_slice(&(CHUNK as u32).to_le_bytes());
    }
    let map = Bitmap { file, bits };
    if fresh {
        map.file.set_len(len as u64)?;
        map.flush()?;
    }
    let missing = (0..chunks).filter(|&c| !map.has(c)).collect();
    Ok((part, map, missing))
}

/// Groups missing chunks into reads of up to `ideal` bytes, joining neighbours, and the reads into
/// batches of about [`BATCH`] bytes.
fn plan(missing: &[u64], size: u64, ideal: u64) -> Vec<Vec<ReadReq>> {
    let mut reads: Vec<(u64, u64)> = Vec::new();
    for &c in missing {
        let (start, end) = (c * CHUNK, ((c + 1) * CHUNK).min(size));
        match reads.last_mut() {
            Some((s, e)) if *e == start && end - *s <= ideal => *e = end,
            _ => reads.push((start, end)),
        }
    }
    let mut batches = Vec::new();
    let mut batch = Vec::new();
    let mut bytes = 0;
    for (s, e) in reads {
        let len = usize::try_from(e - s).expect("reads are at most ideal_io");
        batch.push(ReadReq { offset: s, buf: vec![0; len] });
        bytes += e - s;
        if bytes >= BATCH {
            batches.push(std::mem::take(&mut batch));
            bytes = 0;
        }
    }
    if !batch.is_empty() {
        batches.push(batch);
    }
    batches
}

/// Marks a blob as just used, for the eviction order after a restart. Losing this is harmless.
fn touch(path: PathBuf) {
    tokio::task::spawn_blocking(move || {
        if let Ok(f) = File::open(path) {
            let _ = f.set_modified(SystemTime::now());
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::PosixStore;

    fn blob(store_dir: &Path, n: usize, seed: u8) -> (Vec<u8>, BlobId, PathBuf) {
        let bytes: Vec<u8> =
            (0..n).map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed)).collect();
        let id = BlobId::of(&bytes);
        let path = store_dir.join(format!("src-{seed}"));
        std::fs::write(&path, &bytes).unwrap();
        (bytes, id, path)
    }

    #[test]
    fn neighbours_are_read_together_up_to_the_ideal_size() {
        let size = 10 * CHUNK - 5;
        let batches = plan(&[0, 1, 2, 3, 4, 6, 9], size, 4 * CHUNK);
        let reads: Vec<(u64, usize)> =
            batches.iter().flatten().map(|r| (r.offset / CHUNK, r.buf.len())).collect();
        let c = CHUNK as usize;
        assert_eq!(reads, vec![(0, 4 * c), (4, c), (6, c), (9, c - 5)]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_fetches_once_evicts_the_oldest_and_keeps_what_is_pinned() {
        let dir = crate::tests::scratch();
        let store = PosixStore::open(dir.join("store")).unwrap();
        let n = 3 * CHUNK as usize + 100;
        let (a_bytes, a, a_src) = blob(&dir, n, 1);
        let (_, b, b_src) = blob(&dir, n, 2);
        let (_, c, c_src) = blob(&dir, n, 3);
        for (id, src) in [(a, &a_src), (b, &b_src), (c, &c_src)] {
            store.put(id, src).await.unwrap();
        }
        let cache = Cache::open(dir.join("l1"), 2 * n as u64 + 10).unwrap();

        // Two at once share one fetch.
        let (x, y) = tokio::join!(cache.get(&store, a), cache.get(&store, a));
        let (x, y) = (x.unwrap(), y.unwrap());
        assert_eq!(std::fs::read(x.path()).unwrap(), a_bytes);
        assert_eq!(cache.usage().pinned, 1);
        drop((x, y));

        let held_b = cache.get(&store, b).await.unwrap();
        // a is the oldest and not pinned, so c pushes it out.
        let held_c = cache.get(&store, c).await.unwrap();
        assert!(!cache.has(a) && cache.has(b) && cache.has(c));
        assert!(!dir.join("l1").join(a.to_string()).exists());
        // With b and c pinned there is no room for a.
        let e = cache.get(&store, a).await.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::StorageFull);
        drop(held_c);
        drop(held_b);
        assert_eq!(cache.usage().pinned, 0);

        // A new cache on the same directory knows what is there.
        drop(cache);
        let cache = Cache::open(dir.join("l1"), 2 * n as u64 + 10).unwrap();
        assert!(cache.has(b) && cache.has(c));
        assert_eq!(cache.usage().used, 2 * n as u64);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_fetch_picks_up_where_a_crash_left_it_and_bad_bytes_are_refused() {
        let dir = crate::tests::scratch();
        let store = PosixStore::open(dir.join("store")).unwrap();
        let n = 5 * CHUNK as usize;
        let (bytes, id, src) = blob(&dir, n, 7);
        store.put(id, &src).await.unwrap();
        let l1 = dir.join("l1");
        std::fs::create_dir_all(&l1).unwrap();

        // A part file with chunks 0 and 3 in and marked, as a crash would leave it.
        let part = l1.join(format!("{id}.part"));
        let mut map = vec![0u8; 5];
        map[..4].copy_from_slice(&(CHUNK as u32).to_le_bytes());
        map[4] = 0b1001;
        let f = File::create(&part).unwrap();
        f.set_len(n as u64).unwrap();
        let c = CHUNK as usize;
        f.write_all_at(&bytes[..c], 0).unwrap();
        f.write_all_at(&bytes[3 * c..4 * c], 3 * CHUNK).unwrap();
        std::fs::write(l1.join(format!("{id}.map")), &map).unwrap();
        let cache = Cache::open(&l1, 1 << 30).unwrap();
        assert!(!cache.has(id));
        let held = cache.get(&store, id).await.unwrap();
        assert_eq!(std::fs::read(held.path()).unwrap(), bytes);
        assert!(!part.exists() && !l1.join(format!("{id}.map")).exists());
        drop(held);

        // A chunk marked as in but holding the wrong bytes fails the final check.
        let (_, other, other_src) = blob(&dir, n, 9);
        store.put(other, &other_src).await.unwrap();
        let part = l1.join(format!("{other}.part"));
        File::create(&part).unwrap().set_len(n as u64).unwrap();
        map[4] = 0b1;
        std::fs::write(l1.join(format!("{other}.map")), &map).unwrap();
        let cache = Cache::open(&l1, 1 << 30).unwrap();
        let e = cache.get(&store, other).await.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::InvalidData);
        assert!(!part.exists() && !cache.has(other));
        assert_eq!(cache.usage().blobs, 1);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
