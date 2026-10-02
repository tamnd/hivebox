//! The BLAKE3 chaining value of every chunk of a blob, so a chunk can be checked on its own as it
//! arrives rather than only once the whole blob is in.
//!
//! A blob's name is the root of a BLAKE3 tree, and a [`CHUNK`] aligned run of 256 KiB is a whole
//! subtree of it, so the chaining values of the chunks merge back up to the name. That makes the
//! list check itself: [`Leaves::from_bytes`] refuses a list that does not merge to the blob it is
//! for, and after that each chunk is checked against its own value. The importer stores the list
//! as a blob of its own, 32 bytes per chunk, and the layer names it.

use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

use blake3::hazmat::{
    ChainingValue, HasherExt, Mode, merge_subtrees_non_root, merge_subtrees_root,
};

use crate::BlobId;
use crate::cache::CHUNK;

/// The chaining values of a blob's chunks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Leaves {
    blob: BlobId,
    size: u64,
    cvs: Vec<ChainingValue>,
}

impl Leaves {
    /// The leaves of the file at `path`, read once.
    ///
    /// # Errors
    ///
    /// It cannot be read.
    pub fn of_file(path: &Path) -> io::Result<Self> {
        let mut f = File::open(path)?;
        let mut buf = vec![0; CHUNK as usize];
        let mut cvs = Vec::new();
        let mut whole = blake3::Hasher::new();
        let mut size = 0;
        loop {
            let n = read_full(&mut f, &mut buf)?;
            if n == 0 && size > 0 {
                break;
            }
            whole.update(&buf[..n]);
            cvs.push(leaf(size, &buf[..n]));
            size += n as u64;
            if n < buf.len() {
                break;
            }
        }
        Ok(Self { blob: whole.finalize().into(), size, cvs })
    }

    /// The leaves stored for `blob` of `size` bytes, as [`Leaves::to_bytes`] wrote them.
    ///
    /// # Errors
    ///
    /// `InvalidData` if they are the wrong length or do not merge to `blob`.
    pub fn from_bytes(blob: BlobId, size: u64, bytes: &[u8]) -> io::Result<Self> {
        let bad = |why: &str| {
            io::Error::new(io::ErrorKind::InvalidData, format!("leaves of {blob}: {why}"))
        };
        let want = size.div_ceil(CHUNK).max(1);
        if bytes.len() as u64 != want * 32 {
            return Err(bad("the wrong length for the blob"));
        }
        let cvs = bytes.as_chunks::<32>().0.to_vec();
        let leaves = Self { blob, size, cvs };
        // A blob of one chunk has no tree, and its chunk is checked against the name itself.
        if leaves.cvs.len() > 1 && leaves.root() != blob {
            return Err(bad("they do not hash to the blob"));
        }
        Ok(leaves)
    }

    /// The bytes they are stored as.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        self.cvs.concat()
    }

    /// The blob they are the leaves of.
    #[must_use]
    pub const fn blob(&self) -> BlobId {
        self.blob
    }

    /// How many chunks the blob has.
    #[must_use]
    pub fn chunks(&self) -> u64 {
        self.cvs.len() as u64
    }

    /// Whether `data` is chunk `chunk` of the blob.
    #[must_use]
    pub fn check(&self, chunk: u64, data: &[u8]) -> bool {
        let start = chunk * CHUNK;
        let want = CHUNK.min(self.size.saturating_sub(start));
        let Some(cv) = self.cvs.get(usize::try_from(chunk).unwrap_or(usize::MAX)) else {
            return false;
        };
        if data.len() as u64 != want {
            return false;
        }
        if self.cvs.len() == 1 {
            return BlobId::from(blake3::hash(data)) == self.blob;
        }
        leaf(start, data) == *cv
    }

    /// The root the leaves merge to, for more than one leaf.
    fn root(&self) -> BlobId {
        let k = split(self.cvs.len());
        let (l, r) = self.cvs.split_at(k);
        merge_subtrees_root(&subtree(l), &subtree(r), Mode::Hash).into()
    }
}

/// The chaining value of the chunk at `offset`. An empty blob has no tree and is checked
/// against its name, so its one leaf is all zeros.
fn leaf(offset: u64, data: &[u8]) -> ChainingValue {
    if data.is_empty() {
        return [0; 32];
    }
    let mut h = blake3::Hasher::new();
    h.set_input_offset(offset);
    h.update(data);
    h.finalize_non_root()
}

fn subtree(cvs: &[ChainingValue]) -> ChainingValue {
    if let [one] = cvs {
        return *one;
    }
    let (l, r) = cvs.split_at(split(cvs.len()));
    merge_subtrees_non_root(&subtree(l), &subtree(r), Mode::Hash)
}

/// How many leaves go left: the largest power of two below `n`, as in the BLAKE3 tree.
fn split(n: usize) -> usize {
    debug_assert!(n > 1);
    1 << (usize::BITS - 1 - (n - 1).leading_zeros())
}

fn read_full(f: &mut File, buf: &mut [u8]) -> io::Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        match f.read(&mut buf[n..])? {
            0 => break,
            k => n += k,
        }
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaves_merge_to_the_name_and_check_each_chunk() {
        let dir = crate::tests::scratch();
        let c = CHUNK as usize;
        // One byte, one chunk, a chunk and a bit, and sizes that make uneven trees.
        for len in [1, c, c + 1, 3 * c, 5 * c - 7, 8 * c, 13 * c + 100] {
            let bytes: Vec<u8> = (0..len).map(|i| (i * 7 + i / 1000) as u8).collect();
            let path = dir.join("b");
            std::fs::write(&path, &bytes).unwrap();
            let blob = BlobId::of(&bytes);
            let leaves = Leaves::of_file(&path).unwrap();
            assert_eq!(leaves.blob(), blob, "len {len}");
            let back = Leaves::from_bytes(blob, len as u64, &leaves.to_bytes()).unwrap();
            assert_eq!(back, leaves);
            for (i, chunk) in bytes.chunks(c).enumerate() {
                assert!(leaves.check(i as u64, chunk), "len {len} chunk {i}");
                let mut bad = chunk.to_vec();
                bad[0] ^= 1;
                assert!(!leaves.check(i as u64, &bad), "len {len} chunk {i} flipped");
            }
            assert!(!leaves.check(leaves.chunks(), &[]));
            if leaves.chunks() > 1 {
                let mut wrong = leaves.to_bytes();
                wrong[5] ^= 1;
                let e = Leaves::from_bytes(blob, len as u64, &wrong).unwrap_err();
                assert_eq!(e.kind(), io::ErrorKind::InvalidData);
            }
        }
        // An empty blob still has one leaf.
        let path = dir.join("empty");
        std::fs::write(&path, b"").unwrap();
        let leaves = Leaves::of_file(&path).unwrap();
        assert_eq!((leaves.chunks(), leaves.blob()), (1, BlobId::of(b"")));
        std::fs::remove_dir_all(dir).unwrap();
    }
}
