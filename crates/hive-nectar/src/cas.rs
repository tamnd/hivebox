//! Content addressed chunks: a big blob kept in the store as the chunks it is cut into, each a
//! blob of its own named by its BLAKE3 hash, and a recipe that lists them. Blobs that share content
//! share the chunks it falls in, so the store keeps that content once, and an import only sends
//! the chunks the store does not have yet.
//!
//! The cuts come from the content rather than from fixed offsets. A layer's data blob packs file
//! contents one after another on 4 KiB boundaries, so the same file in two layers sits at offsets
//! that differ by some multiple of 4 KiB, and fixed windows would cut it differently in each. A cut
//! goes where a gear hash of the last 64 bytes has its top bits zero (FastCDC, with the stricter
//! mask before the average size and the looser one after it), so it moves with the content.
//! Chunks are 64 KiB on average by default, see [`Cuts`].
//!
//! [`put`] cuts a file, stores the chunks the store lacks and stores the recipe. [`Chunked`] is a
//! store that reads the blobs it has recipes for from their chunks and passes everything else to
//! the store under it, so the cache and the lazy filler read a chunked blob as they read any other.

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::future::BoxFuture;
use futures::{StreamExt, TryStreamExt, stream};

use crate::BlobId;
use crate::store::{BlobCaps, BlobStat, BlobStore, PutReceipt, ReadReq, blocking};

/// Chunks checked for or stored at once.
const AT_ONCE: usize = 16;

/// What the recipe starts with: the blob's name, its size and the average its cuts aimed at.
const HEAD: usize = 44;
/// One chunk in a recipe: its name and its length.
const ENTRY: usize = 36;

/// A random number for every byte value, the same on every build.
static GEAR: [u64; 256] = gear();

const fn gear() -> [u64; 256] {
    let mut t = [0; 256];
    let mut x: u64 = 0x6869_7665_626f_7821;
    let mut i = 0;
    while i < 256 {
        // splitmix64
        x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = x;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        t[i] = z ^ (z >> 31);
        i += 1;
    }
    t
}

/// Where blobs are cut: chunks average a power of two from 16 KiB to 1 MiB, and are a quarter to
/// four times that. Smaller chunks find more shared content and make more blobs. Across four
/// python bases and six django checkouts, 64 KiB kept 69% of the data and 256 KiB kept 77%.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cuts {
    bits: u32,
}

impl Default for Cuts {
    /// 64 KiB on average.
    fn default() -> Self {
        Self { bits: 16 }
    }
}

impl Cuts {
    /// Cuts that average `avg` bytes.
    ///
    /// # Errors
    ///
    /// `InvalidInput` if `avg` is not a power of two from 16 KiB to 1 MiB.
    pub fn new(avg: usize) -> io::Result<Self> {
        if !avg.is_power_of_two() || !(16 << 10..=1 << 20).contains(&avg) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("chunks must average a power of two from 16 KiB to 1 MiB, not {avg}"),
            ));
        }
        Ok(Self { bits: avg.trailing_zeros() })
    }

    /// The size chunks come out at on average.
    #[must_use]
    pub const fn avg(self) -> usize {
        1 << self.bits
    }

    /// No chunk is cut shorter than this, except the last.
    #[must_use]
    pub const fn min(self) -> usize {
        1 << (self.bits - 2)
    }

    /// No chunk is longer than this.
    #[must_use]
    pub const fn max(self) -> usize {
        1 << (self.bits + 2)
    }

    /// The length of the first chunk of `data`, which is either all that is left of the blob or
    /// at least [`Cuts::max`] bytes of it.
    #[must_use]
    pub fn cut(self, data: &[u8]) -> usize {
        let n = data.len();
        let min = self.min();
        if n <= min {
            return n;
        }
        let (avg, max) = (n.min(self.avg()), n.min(self.max()));
        // Before the average a cut needs two more of the hash's top bits zero than the average
        // would, and after it two fewer, which keeps most chunks near the average.
        let strict = !0u64 << (62 - self.bits);
        let loose = !0u64 << (66 - self.bits);
        // The hash only sees the last 64 bytes, so starting it 64 bytes early makes the first
        // place a cut can go depend on the content alone.
        let mut h: u64 = 0;
        for &b in &data[min - 64..min] {
            h = (h << 1).wrapping_add(GEAR[usize::from(b)]);
        }
        let mut i = min;
        while i < avg {
            h = (h << 1).wrapping_add(GEAR[usize::from(data[i])]);
            i += 1;
            if h & strict == 0 {
                return i;
            }
        }
        while i < max {
            h = (h << 1).wrapping_add(GEAR[usize::from(data[i])]);
            i += 1;
            if h & loose == 0 {
                return i;
            }
        }
        max
    }
}

/// The chunks a blob is stored as, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recipe {
    blob: BlobId,
    cuts: Cuts,
    chunks: Vec<BlobId>,
    /// Where each chunk ends in the blob.
    ends: Vec<u64>,
}

impl Recipe {
    /// The blob it is the recipe of.
    #[must_use]
    pub const fn blob(&self) -> BlobId {
        self.blob
    }

    /// The cuts it was made with.
    #[must_use]
    pub const fn cuts(&self) -> Cuts {
        self.cuts
    }

    /// The blob's size in bytes.
    #[must_use]
    pub fn size(&self) -> u64 {
        self.ends.last().copied().unwrap_or(0)
    }

    /// The chunks, in order. A chunk that comes up more than once is listed each time.
    #[must_use]
    pub fn chunks(&self) -> &[BlobId] {
        &self.chunks
    }

    /// The bytes it is stored as: the blob's name, its size as a little endian `u64` and the
    /// average of its cuts as a little endian `u32`, then each chunk's name and its length as a
    /// little endian `u32`.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEAD + ENTRY * self.chunks.len());
        out.extend_from_slice(self.blob.as_bytes());
        out.extend_from_slice(&self.size().to_le_bytes());
        out.extend_from_slice(&u32::try_from(self.cuts.avg()).unwrap_or(0).to_le_bytes());
        let mut start = 0;
        for (c, &end) in self.chunks.iter().zip(&self.ends) {
            out.extend_from_slice(c.as_bytes());
            out.extend_from_slice(&u32::try_from(end - start).unwrap_or(u32::MAX).to_le_bytes());
            start = end;
        }
        out
    }

    /// A recipe read back.
    ///
    /// # Errors
    ///
    /// `InvalidData` if it is cut short or its chunks do not add up to its size.
    pub fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        let bad = |what: &str| io::Error::new(io::ErrorKind::InvalidData, format!("recipe {what}"));
        if bytes.len() < HEAD || !(bytes.len() - HEAD).is_multiple_of(ENTRY) {
            return Err(bad("has a bad length"));
        }
        let name = |b: &[u8]| {
            let mut a = [0; 32];
            a.copy_from_slice(b);
            BlobId::from_bytes(a)
        };
        let blob = name(&bytes[..32]);
        let size = u64::from_le_bytes(bytes[32..40].try_into().unwrap_or_default());
        let avg = u32::from_le_bytes(bytes[40..HEAD].try_into().unwrap_or_default());
        let cuts = Cuts::new(avg as usize).map_err(|_| bad(&format!("has an average of {avg}")))?;
        let (mut chunks, mut ends, mut end) = (Vec::new(), Vec::new(), 0u64);
        for e in bytes[HEAD..].as_chunks::<ENTRY>().0 {
            let len = u32::from_le_bytes(e[32..].try_into().unwrap_or_default());
            if len == 0 {
                return Err(bad("has an empty chunk"));
            }
            end += u64::from(len);
            chunks.push(name(&e[..32]));
            ends.push(end);
        }
        if end != size {
            return Err(bad(&format!("chunks add up to {end} bytes, not {size}")));
        }
        Ok(Self { blob, cuts, chunks, ends })
    }

    /// Where chunk `i` starts in the blob.
    fn start(&self, i: usize) -> u64 {
        if i == 0 { 0 } else { self.ends[i - 1] }
    }

    /// Fills `reqs` from the chunks in `store`, all at once.
    ///
    /// # Errors
    ///
    /// A chunk cannot be read, or a request runs past the end of the blob.
    pub async fn read(
        &self,
        store: &dyn BlobStore,
        mut reqs: Vec<ReadReq>,
    ) -> io::Result<Vec<ReadReq>> {
        // What to read from each chunk, and where in which request it goes.
        let mut wanted: HashMap<BlobId, Vec<(u64, usize, usize, usize)>> = HashMap::new();
        for (r, req) in reqs.iter().enumerate() {
            let end = req.offset + req.buf.len() as u64;
            if end > self.size() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("{} is {} bytes, read to {end}", self.blob, self.size()),
                ));
            }
            let mut at = req.offset;
            let mut i = self.ends.partition_point(|&e| e <= at);
            while at < end {
                let (start, stop) = (self.start(i), self.ends[i].min(end));
                let len = usize::try_from(stop - at).map_err(io::Error::other)?;
                let into = usize::try_from(at - req.offset).map_err(io::Error::other)?;
                wanted.entry(self.chunks[i]).or_default().push((at - start, len, r, into));
                at = stop;
                i += 1;
            }
        }
        let got: Vec<_> = stream::iter(wanted)
            .map(|(chunk, pieces)| async move {
                let rs = pieces
                    .iter()
                    .map(|&(offset, len, ..)| ReadReq { offset, buf: vec![0; len] })
                    .collect();
                Ok::<_, io::Error>((store.read_vectored(chunk, rs).await?, pieces))
            })
            .buffer_unordered(AT_ONCE)
            .try_collect()
            .await?;
        for (read, pieces) in got {
            for (r, (_, len, req, into)) in read.into_iter().zip(pieces) {
                reqs[req].buf[into..into + len].copy_from_slice(&r.buf);
            }
        }
        Ok(reqs)
    }
}

/// Reads the recipe named `id` from `store`.
///
/// # Errors
///
/// The store does not have it, or it is not a recipe.
pub async fn load(store: &dyn BlobStore, id: BlobId) -> io::Result<Recipe> {
    let size = usize::try_from(store.stat(id).await?.size).map_err(io::Error::other)?;
    let reqs = store.read_vectored(id, vec![ReadReq { offset: 0, buf: vec![0; size] }]).await?;
    if BlobId::of(&reqs[0].buf) != id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("recipe {id} does not match its name"),
        ));
    }
    Recipe::from_bytes(&reqs[0].buf)
}

/// What [`put`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Put {
    /// The recipe's name.
    pub recipe: BlobId,
    /// Chunks the blob was cut into.
    pub chunks: u64,
    /// Of those, the ones the store did not have, counting each once.
    pub new_chunks: u64,
    /// The bytes of the new chunks.
    pub new_bytes: u64,
}

/// Cuts the file at `path`, which must be the blob named `blob`, into chunks, stores the ones
/// `store` does not have yet and then the recipe, staging files in `work`.
///
/// # Errors
///
/// The file cannot be read, does not hash to `blob`, or the store fails.
pub async fn put(
    store: &dyn BlobStore,
    blob: BlobId,
    path: &Path,
    cuts: Cuts,
    work: &Path,
) -> io::Result<Put> {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let src = path.to_path_buf();
    let recipe = blocking(move || cut_file(&src, cuts)).await?;
    if recipe.blob != blob {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{} hashes to {}, not {blob}", path.display(), recipe.blob),
        ));
    }
    let mut first = HashMap::new();
    for (i, c) in recipe.chunks.iter().enumerate() {
        first.entry(*c).or_insert(i);
    }
    let file = Arc::new(File::open(path)?);
    // The chunks the store lacks are written out first and then stored all at once, so the
    // store can make thousands of them durable together.
    let staged: Vec<Option<(BlobId, std::path::PathBuf, u64)>> = stream::iter(first)
        .map(|(chunk, i)| {
            let (file, start) = (file.clone(), recipe.start(i));
            let len = recipe.ends[i] - start;
            async move {
                if store.stat(chunk).await.is_ok() {
                    return Ok(None);
                }
                let tmp = work.join(format!(
                    "{chunk}.{}.{}",
                    std::process::id(),
                    SEQ.fetch_add(1, Ordering::Relaxed)
                ));
                let tmp2 = tmp.clone();
                blocking(move || {
                    let mut buf = vec![0; usize::try_from(len).map_err(io::Error::other)?];
                    file.read_exact_at(&mut buf, start)?;
                    std::fs::write(&tmp2, &buf)
                })
                .await?;
                Ok::<_, io::Error>(Some((chunk, tmp, len)))
            }
        })
        .buffer_unordered(AT_ONCE)
        .try_collect()
        .await?;
    let staged: Vec<_> = staged.into_iter().flatten().collect();
    let put = store.put_many(staged.iter().map(|(c, tmp, _)| (*c, tmp.clone())).collect()).await;
    let tmps: Vec<_> = staged.iter().map(|(_, tmp, _)| tmp.clone()).collect();
    let _ = blocking(move || {
        for tmp in tmps {
            let _ = std::fs::remove_file(tmp);
        }
        Ok(())
    })
    .await;
    let stored: Vec<Option<u64>> =
        put?.iter().zip(&staged).map(|(r, (_, _, len))| (!r.existed).then_some(*len)).collect();
    let id = crate::trace::put_bytes(store, &recipe.to_bytes(), work).await?;
    let new: Vec<u64> = stored.into_iter().flatten().collect();
    Ok(Put {
        recipe: id,
        chunks: recipe.chunks.len() as u64,
        new_chunks: new.len() as u64,
        new_bytes: new.iter().sum(),
    })
}

/// The recipe of the file at `path`, read once.
fn cut_file(path: &Path, cuts: Cuts) -> io::Result<Recipe> {
    let max = cuts.max();
    let mut f = File::open(path)?;
    let mut buf = vec![0; 2 * max];
    let (mut start, mut end, mut eof) = (0, 0, false);
    let mut whole = blake3::Hasher::new();
    let (mut chunks, mut ends, mut at) = (Vec::new(), Vec::new(), 0u64);
    loop {
        while !eof && end - start < max {
            if end == buf.len() {
                buf.copy_within(start..end, 0);
                (start, end) = (0, end - start);
            }
            match f.read(&mut buf[end..]) {
                Ok(0) => eof = true,
                Ok(n) => end += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        if start == end {
            break;
        }
        let n = cuts.cut(&buf[start..end]);
        let chunk = &buf[start..start + n];
        whole.update(chunk);
        chunks.push(BlobId::of(chunk));
        at += n as u64;
        ends.push(at);
        start += n;
    }
    Ok(Recipe { blob: whole.finalize().into(), cuts, chunks, ends })
}

/// A store that reads the blobs it has recipes for from their chunks, and passes everything else
/// to the store under it.
pub struct Chunked {
    inner: Arc<dyn BlobStore>,
    recipes: HashMap<BlobId, Arc<Recipe>>,
}

impl std::fmt::Debug for Chunked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Chunked").field("recipes", &self.recipes.len()).finish_non_exhaustive()
    }
}

impl Chunked {
    /// `inner` with the recipes named in `recipes` loaded from it.
    ///
    /// # Errors
    ///
    /// A recipe cannot be loaded.
    pub async fn open(
        inner: Arc<dyn BlobStore>,
        recipes: impl IntoIterator<Item = BlobId>,
    ) -> io::Result<Self> {
        let mut map = HashMap::new();
        for id in recipes {
            let r = load(&*inner, id).await?;
            map.insert(r.blob, Arc::new(r));
        }
        Ok(Self { inner, recipes: map })
    }
}

impl BlobStore for Chunked {
    fn caps(&self) -> BlobCaps {
        self.inner.caps()
    }

    fn read_vectored(
        &self,
        blob: BlobId,
        reqs: Vec<ReadReq>,
    ) -> BoxFuture<'_, io::Result<Vec<ReadReq>>> {
        match self.recipes.get(&blob) {
            Some(r) => Box::pin(async move { r.read(&*self.inner, reqs).await }),
            None => self.inner.read_vectored(blob, reqs),
        }
    }

    fn put<'a>(&'a self, blob: BlobId, src: &'a Path) -> BoxFuture<'a, io::Result<PutReceipt>> {
        self.inner.put(blob, src)
    }

    fn put_many(
        &self,
        blobs: Vec<(BlobId, std::path::PathBuf)>,
    ) -> BoxFuture<'_, io::Result<Vec<PutReceipt>>> {
        self.inner.put_many(blobs)
    }

    fn stat(&self, blob: BlobId) -> BoxFuture<'_, io::Result<BlobStat>> {
        match self.recipes.get(&blob) {
            Some(r) => {
                let size = r.size();
                Box::pin(async move { Ok(BlobStat { size }) })
            }
            None => self.inner.stat(blob),
        }
    }

    fn delete(&self, blob: BlobId) -> BoxFuture<'_, io::Result<()>> {
        self.inner.delete(blob)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PosixStore;
    use crate::tests::scratch;

    /// Bytes that look random and are the same on every run.
    fn noise(seed: u64, len: usize) -> Vec<u8> {
        let mut x = seed;
        (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect()
    }

    #[test]
    fn cuts_stay_in_bounds_and_follow_the_content() {
        let data = noise(1, 8 << 20);
        for avg in [16 << 10, 64 << 10, 256 << 10] {
            let c = Cuts::new(avg).unwrap();
            let mut lens = Vec::new();
            let mut at = 0;
            while at < data.len() {
                let n = c.cut(&data[at..(at + c.max()).min(data.len())]);
                lens.push(n);
                at += n;
            }
            let (body, last) = lens.split_at(lens.len() - 1);
            assert!(body.iter().all(|&n| (c.min()..=c.max()).contains(&n)), "{lens:?}");
            assert!(last[0] <= c.max());
            let got = data.len() / lens.len();
            assert!((avg / 2..avg * 2).contains(&got), "average {got} for {avg}");
        }
        for bad in [0, 8 << 10, 100_000, 2 << 20] {
            assert!(Cuts::new(bad).is_err(), "{bad}");
        }
        let c = Cuts::new(256 << 10).unwrap();
        // The same bytes 12 KiB later are cut in the same places once the cuts meet again.
        let mut shifted = noise(2, 12 << 10);
        shifted.extend_from_slice(&data);
        let cuts = |d: &[u8]| {
            let (mut at, mut out) = (0, Vec::new());
            while at < d.len() {
                at += c.cut(&d[at..(at + c.max()).min(d.len())]);
                out.push(at);
            }
            out
        };
        let a = cuts(&data);
        let b: Vec<usize> = cuts(&shifted).into_iter().map(|c| c - (12 << 10)).collect();
        let shared = a.iter().filter(|c| b.contains(c)).count();
        assert!(shared + 3 >= a.len(), "{shared} of {} cuts shared", a.len());
    }

    #[test]
    fn a_recipe_reads_back_and_refuses_bad_bytes() {
        let dir = scratch();
        let data = noise(3, 3 << 20);
        std::fs::write(dir.join("a"), &data).unwrap();
        let r = cut_file(&dir.join("a"), Cuts::default()).unwrap();
        assert_eq!(r.blob, BlobId::of(&data));
        assert_eq!(r.size(), data.len() as u64);
        let bytes = r.to_bytes();
        assert_eq!(Recipe::from_bytes(&bytes).unwrap(), r);
        assert!(Recipe::from_bytes(&bytes[..bytes.len() - 1]).is_err());
        let mut wrong = bytes.clone();
        wrong[32] ^= 1;
        assert!(Recipe::from_bytes(&wrong).is_err());
        assert_eq!(cut_file(&dir.join("a"), Cuts::default()).unwrap(), r);
        assert_eq!(r.cuts(), Cuts::default());
        let mut odd = bytes.clone();
        odd[40] = 3;
        assert!(Recipe::from_bytes(&odd).is_err());
        std::fs::write(dir.join("empty"), b"").unwrap();
        let empty = cut_file(&dir.join("empty"), Cuts::default()).unwrap();
        assert_eq!((empty.size(), empty.chunks().len()), (0, 0));
    }

    #[tokio::test]
    async fn shared_content_is_stored_once_and_reads_back_from_chunks() {
        let dir = scratch();
        let store: Arc<dyn BlobStore> = Arc::new(PosixStore::open(dir.join("store")).unwrap());
        let work = dir.join("work");
        std::fs::create_dir_all(&work).unwrap();
        // Two blobs with the same 4 MiB in them, 20 KiB apart, as a file that moved in a layer.
        let shared = noise(4, 4 << 20);
        let mut a = noise(5, 100 << 10);
        a.extend_from_slice(&shared);
        let mut b = noise(6, 120 << 10);
        b.extend_from_slice(&shared);
        b.extend_from_slice(&noise(7, 300 << 10));
        let (pa, pb) = (dir.join("a"), dir.join("b"));
        std::fs::write(&pa, &a).unwrap();
        std::fs::write(&pb, &b).unwrap();
        let (ida, idb) = (BlobId::of(&a), BlobId::of(&b));

        let first = put(&*store, ida, &pa, Cuts::default(), &work).await.unwrap();
        assert_eq!(first.new_bytes, a.len() as u64);
        let second = put(&*store, idb, &pb, Cuts::default(), &work).await.unwrap();
        assert!(second.new_bytes < (2 << 20), "{second:?}");
        assert!(second.new_chunks < second.chunks);
        let again = put(&*store, idb, &pb, Cuts::default(), &work).await.unwrap();
        assert_eq!((again.recipe, again.new_chunks), (second.recipe, 0));
        assert!(put(&*store, ida, &pb, Cuts::default(), &work).await.is_err());
        assert_eq!(std::fs::read_dir(&work).unwrap().count(), 0);

        let chunked = Chunked::open(store.clone(), [first.recipe, second.recipe]).await.unwrap();
        assert!(store.stat(idb).await.is_err());
        assert_eq!(chunked.stat(idb).await.unwrap().size, b.len() as u64);
        let spans = [(0, 10), (100 << 10, 600 << 10), (b.len() as u64 - 5, 5), (3 << 20, 1)];
        let reqs =
            spans.iter().map(|&(o, l)| ReadReq { offset: o, buf: vec![0; l as usize] }).collect();
        let got = chunked.read_vectored(idb, reqs).await.unwrap();
        for (r, &(o, l)) in got.iter().zip(&spans) {
            assert_eq!(r.buf, b[o as usize..(o + l) as usize]);
        }
        let past = vec![ReadReq { offset: b.len() as u64 - 1, buf: vec![0; 2] }];
        let err = chunked.read_vectored(idb, past).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        // A whole fetch through the cache checks the blob against its name.
        let cache = crate::Cache::open(dir.join("cache"), 64 << 20).unwrap();
        let held = cache.get(&chunked, ida).await.unwrap();
        assert_eq!(std::fs::read(held.path()).unwrap(), a);
    }
}
