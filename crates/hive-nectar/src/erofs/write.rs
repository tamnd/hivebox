//! Building a layer from a tar stream in process, which is what [`Writer`] does instead of running
//! `mkfs.erofs`.
//!
//! The layer has the shape [`super::Mkfs`] gives it: 4 KiB blocks, extended inodes, directories
//! and symlinks in the metadata blob, often in their inode's own block, file contents only in the
//! data blob, cut into chunks that the inode lists, identical chunks stored once, and AUFS
//! whiteouts turned into overlayfs ones. The data blob holds the same bytes `mkfs.erofs` would
//! write, except that a chunk of zeros is a hole and takes no space. A file whose chunks lie one
//! after another has a single chunk as large as the file. The xattr name filter is left out, and a
//! directory a tar only implies gets an mtime of 0, as [`super::prepare`] arranges for `mkfs.erofs`.
//! A PAX mtime keeps its nanoseconds, which `mkfs.erofs` 1.7 drops. The metadata blob can be a
//! block or a few bigger, since inodes are placed in one pass and a gap left at the end of a block
//! is not filled later.
//!
//! The tar is read once, and each file's contents go to the data blob as they come. The metadata
//! blob is laid out once the whole tree is known: the superblock and the one device slot, then
//! every inode, the root's first and then each directory's entries in turn, so the inodes of one
//! directory sit together, and then the directory blocks that did not fit next to their inode.

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{self, BufRead, Read, Write};
use std::path::Path;

use super::{
    Built, COMPAT_SB_CHKSUM, INCOMPAT_CHUNKED_FILE, INCOMPAT_DEVICE_TABLE, MAGIC, SUPER_OFFSET,
    check_chunk_size, crc32c,
};

const BLOCK_BITS: u32 = 12;
const BLOCK: usize = 1 << BLOCK_BITS;
const COMPAT_MTIME: u32 = 0x2;
/// The device slot follows the superblock, and the inodes follow the slot.
const SLOT_OFFSET: usize = SUPER_OFFSET + 128;
const INODES_OFFSET: usize = SLOT_OFFSET + 128;
const INODE_SIZE: usize = 64;
const XATTR_HEADER: usize = 12;
const DIRENT_SIZE: usize = 12;
const NULL_ADDR: u32 = u32::MAX;
const CHUNK_INDEXES: u32 = 0x20;
const NAME_MAX: usize = 255;

const FLAT_PLAIN: u16 = 0;
const FLAT_INLINE: u16 = 2;
const CHUNK_BASED: u16 = 4;

const S_IFMT: u32 = 0o170_000;
const S_IFREG: u32 = 0o100_000;
const S_IFDIR: u32 = 0o040_000;
const S_IFLNK: u32 = 0o120_000;
const S_IFCHR: u32 = 0o020_000;
const S_IFBLK: u32 = 0o060_000;
const S_IFIFO: u32 = 0o010_000;

/// The xattr name prefixes EROFS stores as an index, longest match first where one is a prefix of
/// another.
const PREFIXES: [(&[u8], u8); 5] = [
    (b"user.", 1),
    (b"system.posix_acl_access", 2),
    (b"system.posix_acl_default", 3),
    (b"trusted.", 4),
    (b"security.", 6),
];
const TRUSTED: u8 = 4;

/// Builds layers without `mkfs.erofs`.
#[derive(Clone, Copy, Debug)]
pub struct Writer {
    chunk_size: u32,
}

impl Writer {
    /// A writer that cuts file contents into chunks of `chunk_size` bytes.
    ///
    /// # Errors
    ///
    /// `chunk_size` is not a power of two from 4 KiB to 64 MiB.
    pub fn new(chunk_size: u32) -> io::Result<Self> {
        check_chunk_size(chunk_size)?;
        Ok(Self { chunk_size })
    }

    /// The chunk size layers are built with.
    #[must_use]
    pub const fn chunk_size(&self) -> u32 {
        self.chunk_size
    }

    /// Builds a layer from the tar stream `src`, which may be gzipped, into `meta` and `data` in
    /// `dir`, as [`super::Mkfs::build`] does. The whole of `src` is read.
    ///
    /// # Errors
    ///
    /// `src` is not a tar, it has a path, link or xattr EROFS cannot hold, or a write fails.
    pub fn build(&self, src: impl Read, dir: &Path) -> io::Result<Built> {
        let built = Built { meta: dir.join("meta"), data: dir.join("data") };
        let mut src = io::BufReader::with_capacity(1 << 20, src);
        let gzipped = src.fill_buf()?.starts_with(&[0x1f, 0x8b]);
        let mut plain: Box<dyn Read + '_> = if gzipped {
            Box::new(flate2::bufread::MultiGzDecoder::new(&mut src))
        } else {
            Box::new(&mut src)
        };
        let mut data = Data::new(File::create(&built.data)?, self.chunk_size);
        let mut tree = Tree::new();
        {
            let mut archive = tar::Archive::new(&mut plain);
            let mut global = Pax::default();
            for entry in archive.entries()? {
                tree.add(&mut entry?, &mut global, &mut data)?;
            }
        }
        // Whoever hashes `src` needs all of it, padding and anything after the gzip stream too.
        io::copy(&mut plain, &mut io::sink())?;
        drop(plain);
        io::copy(&mut src, &mut io::sink())?;
        let data_blocks = data.finish()?;
        let meta = tree.lay_out(data_blocks)?;
        std::fs::write(&built.meta, meta)?;
        Ok(built)
    }
}

/// What a PAX header says about an entry, over what its tar header says.
#[derive(Clone, Default)]
struct Pax {
    mtime: Option<(i64, u32)>,
    xattrs: Vec<(Vec<u8>, Vec<u8>)>,
}

impl Pax {
    fn read(&mut self, ext: tar::PaxExtensions<'_>) -> io::Result<()> {
        for ext in ext {
            let ext = ext?;
            let (key, value) = (ext.key_bytes(), ext.value_bytes());
            if key == b"mtime" {
                self.mtime = Some(pax_time(value).ok_or_else(|| bad("a PAX mtime is not a time"))?);
            } else if let Some(name) = key.strip_prefix(b"SCHILY.xattr.") {
                self.xattrs.push((name.to_vec(), value.to_vec()));
            } else if let Some(name) = key.strip_prefix(b"LIBARCHIVE.xattr.") {
                let name =
                    unescape(name).ok_or_else(|| bad("a PAX xattr name is badly escaped"))?;
                let value = base64(value).ok_or_else(|| bad("a PAX xattr is not base64"))?;
                self.xattrs.push((name, value));
            }
        }
        Ok(())
    }
}

/// A PAX time, `[-]seconds[.fraction]`, as seconds and nanoseconds.
fn pax_time(v: &[u8]) -> Option<(i64, u32)> {
    let v = std::str::from_utf8(v).ok()?;
    let (secs, frac) = v.split_once('.').unwrap_or((v, ""));
    let mut s: i64 = secs.parse().ok()?;
    if !frac.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let digits = &frac[..frac.len().min(9)];
    let mut ns: u32 = format!("{digits:0<9}").parse().ok()?;
    if secs.starts_with('-') && ns > 0 {
        s -= 1;
        ns = 1_000_000_000 - ns;
    }
    Some((s, ns))
}

/// A `%XX` escaped string, as libarchive writes xattr names.
fn unescape(s: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(s.len());
    let mut i = 0;
    while i < s.len() {
        if s[i] == b'%' {
            let hex = std::str::from_utf8(s.get(i + 1..i + 3)?).ok()?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(s[i]);
            i += 1;
        }
    }
    Some(out)
}

/// Standard base64, padded or not.
fn base64(s: &[u8]) -> Option<Vec<u8>> {
    let digit = |c: u8| match c {
        b'A'..=b'Z' => Some(c - b'A'),
        b'a'..=b'z' => Some(c - b'a' + 26),
        b'0'..=b'9' => Some(c - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    };
    let s = s.strip_suffix(b"==").or_else(|| s.strip_suffix(b"=")).unwrap_or(s);
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let (mut acc, mut bits) = (0u32, 0);
    for &c in s {
        acc = acc << 6 | u32::from(digit(c)?);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Some(out)
}

/// A number from a tar header, where a field left blank reads as 0, as `mkfs.erofs` reads it.
fn blank(n: io::Result<u64>, field: &[u8]) -> io::Result<u64> {
    match n {
        Err(_) if field.iter().all(|&b| b == 0 || b == b' ') => Ok(0),
        n => n,
    }
}

fn bad(what: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, what.into())
}

/// The data blob as it is written: each new chunk goes to the end, padded to a block.
struct Data {
    out: io::BufWriter<File>,
    blocks: u64,
    /// Where each chunk written so far starts, by its hash.
    seen: HashMap<blake3::Hash, u32>,
    buf: Vec<u8>,
}

impl Data {
    fn new(file: File, chunk_size: u32) -> Self {
        Self {
            out: io::BufWriter::with_capacity(1 << 20, file),
            blocks: 0,
            seen: HashMap::new(),
            buf: vec![0; chunk_size as usize],
        }
    }

    /// Writes the contents `r` has, and returns their size, the chunk size as a power of two, and
    /// where each chunk starts.
    fn file(&mut self, r: &mut dyn Read) -> io::Result<(u64, u32, Vec<u32>)> {
        let chunk_bits = self.buf.len().trailing_zeros();
        let per = (self.buf.len() / BLOCK) as u64;
        let mut chunks = Vec::new();
        let mut size = 0u64;
        loop {
            let n = fill(r, &mut self.buf)?;
            if n == 0 {
                break;
            }
            size += n as u64;
            let bytes = &self.buf[..n];
            let addr = if zero(bytes) {
                NULL_ADDR
            } else {
                let hash = blake3::hash(bytes);
                match self.seen.get(&hash) {
                    Some(&addr) => addr,
                    None => {
                        let addr = u32::try_from(self.blocks)
                            .ok()
                            .filter(|&a| a != NULL_ADDR)
                            .ok_or_else(|| bad("the layer has over 16 TiB of data"))?;
                        self.out.write_all(bytes)?;
                        let pad = n.next_multiple_of(BLOCK) - n;
                        self.out.write_all(&[0; BLOCK][..pad])?;
                        self.blocks += (n + pad) as u64 / BLOCK as u64;
                        self.seen.insert(hash, addr);
                        addr
                    }
                }
            };
            chunks.push(addr);
            if n < self.buf.len() {
                break;
            }
        }
        // Chunks that lie one after another read the same as one chunk the size of the file, and
        // the kernel then maps the file in one go.
        let run = chunks.len() > 1
            && chunks
                .iter()
                .zip(0..)
                .all(|(&a, k)| a != NULL_ADDR && u64::from(a) == u64::from(chunks[0]) + k * per);
        if run {
            let bits = (64 - (size - 1).leading_zeros()).clamp(chunk_bits, BLOCK_BITS + 31);
            return Ok((size, bits, vec![chunks[0]]));
        }
        Ok((size, chunk_bits, chunks))
    }

    fn finish(self) -> io::Result<u32> {
        let file = self.out.into_inner().map_err(io::IntoInnerError::into_error)?;
        drop(file);
        u32::try_from(self.blocks).map_err(|_| bad("the layer has over 16 TiB of data"))
    }
}

/// Reads into `buf` until it is full or `r` ends, and returns how much it read.
fn fill(r: &mut dyn Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        match r.read(&mut buf[n..]) {
            Ok(0) => break,
            Ok(k) => n += k,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(n)
}

fn zero(bytes: &[u8]) -> bool {
    bytes.chunks(BLOCK).all(|b| b.iter().fold(0, |acc, &x| acc | x) == 0)
}

/// One inode.
struct Node {
    /// The type and permission bits.
    mode: u32,
    uid: u32,
    gid: u32,
    mtime: (i64, u32),
    /// By name index and the rest of the name.
    xattrs: BTreeMap<(u8, Vec<u8>), Vec<u8>>,
    body: Body,
}

enum Body {
    File {
        size: u64,
        chunk_bits: u32,
        chunks: Vec<u32>,
    },
    Dir(BTreeMap<Vec<u8>, usize>),
    Symlink(Vec<u8>),
    /// A device, with its number as `new_encode_dev` packs it, or a fifo.
    Special(u32),
}

impl Node {
    /// A directory that only shows up as the parent of something.
    fn implied() -> Self {
        Self {
            mode: S_IFDIR | 0o755,
            uid: 0,
            gid: 0,
            mtime: (0, 0),
            xattrs: BTreeMap::new(),
            body: Body::Dir(BTreeMap::new()),
        }
    }

    const fn file_type(&self) -> u8 {
        match self.mode & S_IFMT {
            S_IFREG => 1,
            S_IFDIR => 2,
            S_IFCHR => 3,
            S_IFBLK => 4,
            S_IFIFO => 5,
            S_IFLNK => 7,
            _ => 0,
        }
    }

    const fn is_whiteout(&self) -> bool {
        self.mode & S_IFMT == S_IFCHR && matches!(self.body, Body::Special(0))
    }

    fn set_xattrs(&mut self, xattrs: &[(Vec<u8>, Vec<u8>)]) -> io::Result<()> {
        for (name, value) in xattrs {
            let (index, rest) = PREFIXES
                .iter()
                .find_map(|(p, i)| Some((*i, name.strip_prefix(*p)?)))
                .ok_or_else(|| {
                    bad(format!(
                        "EROFS has no place for the xattr {}",
                        String::from_utf8_lossy(name)
                    ))
                })?;
            if rest.len() > NAME_MAX || value.len() > usize::from(u16::MAX) {
                return Err(bad(format!(
                    "the xattr {} is too long",
                    String::from_utf8_lossy(name)
                )));
            }
            self.xattrs.insert((index, rest.to_vec()), value.clone());
        }
        Ok(())
    }
}

/// The tree a tar makes, as later entries change what earlier ones made.
struct Tree {
    nodes: Vec<Node>,
}

const ROOT: usize = 0;

/// The parts of a path in a tar, with `.` and empty ones dropped and `..` taking one away. A path
/// cannot climb above the root.
fn parts(path: &[u8]) -> Vec<&[u8]> {
    let mut out = Vec::new();
    for part in path.split(|&b| b == b'/') {
        match part {
            b"" | b"." => {}
            b".." => {
                out.pop();
            }
            part => out.push(part),
        }
    }
    out
}

impl Tree {
    fn new() -> Self {
        Self { nodes: vec![Node::implied()] }
    }

    fn children(&mut self, dir: usize) -> &mut BTreeMap<Vec<u8>, usize> {
        match &mut self.nodes[dir].body {
            Body::Dir(children) => children,
            _ => unreachable!("only directories are walked into"),
        }
    }

    /// The directory at `parts`, made along with any above it that do not exist yet.
    fn dir(&mut self, parts: &[&[u8]], path: &[u8]) -> io::Result<usize> {
        let mut at = ROOT;
        for &part in parts {
            if part.len() > NAME_MAX {
                return Err(bad(format!("{} has a name over 255 bytes", show(path))));
            }
            let found = self.children(at).get(part).copied();
            at = match found {
                Some(next) if matches!(self.nodes[next].body, Body::Dir(_)) => next,
                Some(_) => {
                    return Err(bad(format!("{} is under something not a directory", show(path))));
                }
                None => {
                    let next = self.nodes.len();
                    self.nodes.push(Node::implied());
                    self.children(at).insert(part.to_vec(), next);
                    next
                }
            };
        }
        Ok(at)
    }

    /// The node at `parts`, without making anything.
    fn find(&self, parts: &[&[u8]]) -> Option<usize> {
        let mut at = ROOT;
        for &part in parts {
            match &self.nodes[at].body {
                Body::Dir(children) => at = *children.get(part)?,
                _ => return None,
            }
        }
        Some(at)
    }

    /// Adds what one tar entry says. `global` holds what PAX global headers said so far.
    fn add<R: Read>(
        &mut self,
        entry: &mut tar::Entry<'_, R>,
        global: &mut Pax,
        data: &mut Data,
    ) -> io::Result<()> {
        let kind = entry.header().entry_type();
        if kind.is_pax_global_extensions() {
            if let Some(ext) = entry.pax_extensions()? {
                global.read(ext)?;
            }
            return Ok(());
        }
        let mut pax = global.clone();
        if let Some(ext) = entry.pax_extensions()? {
            pax.read(ext)?;
        }
        let path = entry.path_bytes().into_owned();
        let h = entry.header();
        let id = |n: u64| {
            u32::try_from(n).map_err(|_| bad(format!("{} has an id over 32 bits", show(&path))))
        };
        let raw = h.as_old();
        let (uid, gid) = (id(blank(h.uid(), &raw.uid)?)?, id(blank(h.gid(), &raw.gid)?)?);
        let mtime = match pax.mtime {
            Some(t) => t,
            None => (i64::try_from(blank(h.mtime(), &raw.mtime)?).unwrap_or(i64::MAX), 0),
        };
        let perm = (blank(h.mode().map(u64::from), &raw.mode)? & 0o7777) as u32;
        let mut all = parts(&path);
        let Some(name) = all.pop() else {
            // Some tars list the root as `./`.
            if !kind.is_dir() {
                return Err(bad(format!("{} is the root and not a directory", show(&path))));
            }
            let root = &mut self.nodes[ROOT];
            (root.mode, root.uid, root.gid, root.mtime) = (S_IFDIR | perm, uid, gid, mtime);
            return root.set_xattrs(&pax.xattrs);
        };
        let dir = self.dir(&all, &path)?;
        if kind.is_hard_link() {
            let to = entry.link_name_bytes().unwrap_or_default().into_owned();
            let target = self.find(&parts(&to)).ok_or_else(|| {
                bad(format!("{} links to {}, which is not there", show(&path), show(&to)))
            })?;
            if matches!(self.nodes[target].body, Body::Dir(_)) {
                return Err(bad(format!("{} is a hard link to a directory", show(&path))));
            }
            self.children(dir).insert(name.to_vec(), target);
            return Ok(());
        }
        // The AUFS whiteouts OCI layers use, which overlayfs says with an xattr and a device.
        if name == b".wh..wh..opq" {
            let d = &mut self.nodes[dir];
            d.xattrs.insert((TRUSTED, b"overlay.opaque".to_vec()), b"y".to_vec());
            return Ok(());
        }
        let (name, whiteout) = match name.strip_prefix(b".wh.") {
            Some(rest) => (rest, true),
            None => (name, false),
        };
        if name.is_empty() || name.len() > NAME_MAX {
            return Err(bad(format!("{} has an empty name or one over 255 bytes", show(&path))));
        }
        let was = self.children(dir).get(name).copied();
        if !whiteout
            && kind.is_dir()
            && let Some(was) = was
            && matches!(self.nodes[was].body, Body::Dir(_))
        {
            let d = &mut self.nodes[was];
            (d.mode, d.uid, d.gid, d.mtime) = (S_IFDIR | perm, uid, gid, mtime);
            return d.set_xattrs(&pax.xattrs);
        }
        let (mode, body) = if whiteout {
            (S_IFCHR, Body::Special(0))
        } else if kind.is_file() || kind.is_contiguous() || kind.is_gnu_sparse() {
            let (size, chunk_bits, chunks) = data.file(entry)?;
            (S_IFREG | perm, Body::File { size, chunk_bits, chunks })
        } else if kind.is_dir() {
            (S_IFDIR | perm, Body::Dir(BTreeMap::new()))
        } else if kind.is_symlink() {
            let to = entry.link_name_bytes().unwrap_or_default().into_owned();
            if to.len() >= BLOCK {
                return Err(bad(format!("{} links to a path over 4 KiB", show(&path))));
            }
            (S_IFLNK | perm, Body::Symlink(to))
        } else if kind.is_character_special() || kind.is_block_special() {
            let h = entry.header();
            let (major, minor) = (h.device_major()?.unwrap_or(0), h.device_minor()?.unwrap_or(0));
            let ty = if kind.is_character_special() { S_IFCHR } else { S_IFBLK };
            (ty | perm, Body::Special(encode_dev(major, minor)))
        } else if kind.is_fifo() {
            (S_IFIFO | perm, Body::Special(0))
        } else {
            // Volume labels and the like, which `mkfs.erofs` skips too.
            return Ok(());
        };
        let mut node = Node { mode, uid, gid, mtime, xattrs: BTreeMap::new(), body };
        node.set_xattrs(&pax.xattrs)?;
        let at = self.nodes.len();
        self.nodes.push(node);
        self.children(dir).insert(name.to_vec(), at);
        Ok(())
    }

    /// The entries of the directory `at`, whose parent is `parent`, sorted by name, with the
    /// node ids in `nids`, or 0 for all of them when there are none yet.
    fn entries(&self, at: usize, parent: usize, nids: &[u64]) -> Vec<(&[u8], u64, u8)> {
        let Body::Dir(children) = &self.nodes[at].body else { return Vec::new() };
        let nid = |i: usize| nids.get(i).copied().unwrap_or(0);
        let mut out = Vec::with_capacity(children.len() + 2);
        out.push((&b"."[..], nid(at), 2));
        out.push((&b".."[..], nid(parent), 2));
        for (name, &c) in children {
            out.push((name.as_slice(), nid(c), self.nodes[c].file_type()));
        }
        out.sort_unstable_by(|a, b| a.0.cmp(b.0));
        out
    }

    /// Lays the metadata blob out and returns it.
    fn lay_out(&self, data_blocks: u32) -> io::Result<Vec<u8>> {
        // Every node the root reaches, each directory's children together, and how many names
        // each one has.
        let n = self.nodes.len();
        let mut order = vec![ROOT];
        let mut placed = vec![false; n];
        let mut links = vec![0u32; n];
        placed[ROOT] = true;
        let mut i = 0;
        while i < order.len() {
            if let Body::Dir(children) = &self.nodes[order[i]].body {
                for &c in children.values() {
                    links[c] += 1;
                    if !placed[c] {
                        placed[c] = true;
                        order.push(c);
                    }
                }
            }
            i += 1;
        }
        // Each inode's xattrs and what follows it, before anything has a place.
        let mut plans: Vec<Plan> = Vec::with_capacity(order.len());
        for &at in &order {
            let node = &self.nodes[at];
            let mut xattrs = node.xattrs.clone();
            if let Body::Dir(children) = &node.body
                && children.values().any(|&c| self.nodes[c].is_whiteout())
            {
                // Overlayfs then reads the directory through its merge code, which hides the
                // whiteouts, even where no lower layer has it.
                xattrs.insert((TRUSTED, b"overlay.origin".to_vec()), Vec::new());
            }
            let xattrs = ibody(&xattrs);
            let size = match &node.body {
                Body::File { size, .. } => *size,
                Body::Dir(_) => dirents(&self.entries(at, ROOT, &[])).len() as u64,
                Body::Symlink(to) => to.len() as u64,
                Body::Special(_) => 0,
            };
            plans.push(Plan { node: at, xattrs, size, ..Plan::default() });
        }
        // Places for the inodes. A directory or symlink keeps its last partial block right after
        // its inode when the two fit in one block, which is also the only way an inode and its
        // xattrs ever sit across a block boundary.
        let mut pos = INODES_OFFSET;
        let mut nids = vec![0u64; n];
        for plan in &mut plans {
            let node = &self.nodes[plan.node];
            let head = INODE_SIZE + plan.xattrs.len();
            let size = usize::try_from(plan.size).unwrap_or(usize::MAX);
            let (layout, tail, blocks) = match &node.body {
                Body::File { chunks, .. } if !chunks.is_empty() => (CHUNK_BASED, 0, 0),
                Body::Dir(_) | Body::Symlink(_) => {
                    let tail = size % BLOCK;
                    if size > 0 && tail == 0 || head + tail > BLOCK {
                        (FLAT_PLAIN, 0, size.div_ceil(BLOCK))
                    } else {
                        (FLAT_INLINE, tail, size / BLOCK)
                    }
                }
                _ => (FLAT_PLAIN, 0, 0),
            };
            let record = head + tail;
            if record <= BLOCK && pos % BLOCK + record > BLOCK {
                pos = pos.next_multiple_of(BLOCK);
            }
            plan.at = pos;
            nids[plan.node] = (pos / 32) as u64;
            pos += record;
            if let Body::File { chunks, .. } = &node.body
                && layout == CHUNK_BASED
            {
                pos = pos.next_multiple_of(8) + 8 * chunks.len();
            }
            pos = pos.next_multiple_of(32);
            (plan.layout, plan.tail, plan.blocks) = (layout, tail, blocks);
        }
        // Then the blocks of directories and symlinks that are not all next to their inode.
        let mut block = pos.div_ceil(BLOCK);
        for plan in &mut plans {
            if plan.blocks > 0 {
                plan.raw = u32::try_from(block).map_err(|_| bad("the metadata is over 16 TiB"))?;
                block += plan.blocks;
            }
        }
        let meta_blocks = u32::try_from(block).map_err(|_| bad("the metadata is over 16 TiB"))?;
        let mut meta = vec![0u8; block * BLOCK];
        let mut parents = vec![ROOT; n];
        for &at in &order {
            if let Body::Dir(children) = &self.nodes[at].body {
                for &c in children.values() {
                    if matches!(self.nodes[c].body, Body::Dir(_)) {
                        parents[c] = at;
                    }
                }
            }
        }
        for (ino, plan) in plans.iter().enumerate() {
            let node = &self.nodes[plan.node];
            let content = match &node.body {
                Body::Dir(_) => dirents(&self.entries(plan.node, parents[plan.node], &nids)),
                Body::Symlink(to) => to.clone(),
                _ => Vec::new(),
            };
            let nlink = match &node.body {
                Body::Dir(children) => {
                    2 + children
                        .values()
                        .filter(|&&c| matches!(self.nodes[c].body, Body::Dir(_)))
                        .count() as u32
                }
                _ => links[plan.node],
            };
            let iu = match &node.body {
                Body::File { chunk_bits, chunks, .. } if !chunks.is_empty() => {
                    CHUNK_INDEXES | (chunk_bits - BLOCK_BITS)
                }
                Body::File { .. } => 0,
                Body::Special(rdev) => *rdev,
                Body::Dir(_) | Body::Symlink(_) => {
                    if plan.blocks > 0 {
                        plan.raw
                    } else {
                        NULL_ADDR
                    }
                }
            };
            let icount = if plan.xattrs.is_empty() {
                0
            } else {
                u16::try_from((plan.xattrs.len() - XATTR_HEADER) / 4 + 1)
                    .map_err(|_| bad("an inode has over 256 KiB of xattrs"))?
            };
            let ino = u32::try_from(ino + 1).map_err(|_| bad("the layer has over 4G inodes"))?;
            let rec = &mut meta[plan.at..];
            put16(rec, 0, 1 | plan.layout << 1);
            put16(rec, 2, icount);
            put16(rec, 4, node.mode as u16);
            rec[8..16].copy_from_slice(&plan.size.to_le_bytes());
            put32(rec, 16, iu);
            put32(rec, 20, ino);
            put32(rec, 24, node.uid);
            put32(rec, 28, node.gid);
            rec[32..40].copy_from_slice(&node.mtime.0.to_le_bytes());
            put32(rec, 40, node.mtime.1);
            put32(rec, 44, nlink);
            let mut off = INODE_SIZE;
            rec[off..off + plan.xattrs.len()].copy_from_slice(&plan.xattrs);
            off += plan.xattrs.len();
            if let Body::File { chunks, .. } = &node.body
                && plan.layout == CHUNK_BASED
            {
                let mut at = (plan.at + off).next_multiple_of(8) - plan.at;
                for &c in chunks {
                    // The device is the data blob, the first after this one.
                    put16(rec, at + 2, 1);
                    put32(rec, at + 4, c);
                    at += 8;
                }
            }
            let full = plan.blocks * BLOCK;
            if plan.blocks > 0 {
                let from = plan.raw as usize * BLOCK;
                let len = full.min(content.len());
                meta[from..from + len].copy_from_slice(&content[..len]);
            }
            if plan.tail > 0 {
                let from = plan.at + off;
                meta[from..from + plan.tail].copy_from_slice(&content[full..]);
            }
        }
        // The superblock, and the slot for the data blob.
        let root = u16::try_from(nids[ROOT]).map_err(|_| bad("the root inode is too far in"))?;
        let sb = &mut meta[SUPER_OFFSET..];
        put32(sb, 0, MAGIC);
        put32(sb, 8, COMPAT_SB_CHKSUM | COMPAT_MTIME);
        sb[12] = BLOCK_BITS as u8;
        put16(sb, 14, root);
        sb[16..24].copy_from_slice(&(order.len() as u64).to_le_bytes());
        put32(sb, 36, meta_blocks);
        put32(sb, 80, INCOMPAT_CHUNKED_FILE | INCOMPAT_DEVICE_TABLE);
        put16(sb, 86, 1);
        put16(sb, 88, (SLOT_OFFSET / 128) as u16);
        let slot = &mut meta[SLOT_OFFSET..];
        put32(slot, 64, data_blocks);
        // Where the data would start if the two blobs were one file, as `mkfs.erofs` sets it.
        put32(slot, 68, meta_blocks);
        let crc = crc32c(!0, &meta[SUPER_OFFSET..BLOCK]);
        put32(&mut meta[SUPER_OFFSET..], 4, crc);
        Ok(meta)
    }
}

/// Where one inode goes and what follows it.
#[derive(Default)]
struct Plan {
    node: usize,
    xattrs: Vec<u8>,
    size: u64,
    at: usize,
    layout: u16,
    /// Bytes of the last block that sit right after the inode.
    tail: usize,
    /// Whole blocks elsewhere in the metadata blob, starting at `raw`.
    blocks: usize,
    raw: u32,
}

/// The xattr body of an inode: a header with no shared xattrs, then each one, padded to 4 bytes.
fn ibody(xattrs: &BTreeMap<(u8, Vec<u8>), Vec<u8>>) -> Vec<u8> {
    if xattrs.is_empty() {
        return Vec::new();
    }
    let mut out = vec![0; XATTR_HEADER];
    for ((index, name), value) in xattrs {
        out.push(name.len() as u8);
        out.push(*index);
        out.extend_from_slice(&(value.len() as u16).to_le_bytes());
        out.extend_from_slice(name);
        out.extend_from_slice(value);
        out.resize(out.len().next_multiple_of(4), 0);
    }
    out
}

/// A directory's blocks for `entries`, sorted by name, the last one only as long as it needs to
/// be. Each block has the entries that fit, then their names.
fn dirents(entries: &[(&[u8], u64, u8)]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut start = 0;
    while start < entries.len() {
        let mut end = start;
        let mut used = 0;
        while end < entries.len() && used + DIRENT_SIZE + entries[end].0.len() <= BLOCK {
            used += DIRENT_SIZE + entries[end].0.len();
            end += 1;
        }
        let base = out.len();
        let mut nameoff = DIRENT_SIZE * (end - start);
        for (name, nid, ty) in &entries[start..end] {
            out.extend_from_slice(&nid.to_le_bytes());
            out.extend_from_slice(&(nameoff as u16).to_le_bytes());
            out.push(*ty);
            out.push(0);
            nameoff += name.len();
        }
        for (name, ..) in &entries[start..end] {
            out.extend_from_slice(name);
        }
        if end < entries.len() {
            out.resize(base + BLOCK, 0);
        }
        start = end;
    }
    out
}

/// A device number as the kernel's `new_encode_dev` packs it.
const fn encode_dev(major: u32, minor: u32) -> u32 {
    (minor & 0xff) | (major & 0xfff) << 8 | (minor & !0xff) << 12
}

fn put16(b: &mut [u8], at: usize, v: u16) {
    b[at..at + 2].copy_from_slice(&v.to_le_bytes());
}

fn put32(b: &mut [u8], at: usize, v: u32) {
    b[at..at + 4].copy_from_slice(&v.to_le_bytes());
}

fn show(path: &[u8]) -> String {
    String::from_utf8_lossy(path).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::erofs::superblock;
    use std::path::PathBuf;

    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir()
                .join(format!("hive-nectar-write-{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// One inode as read back from a layer.
    #[derive(Debug, PartialEq, Eq)]
    struct Seen {
        mode: u32,
        uid: u32,
        gid: u32,
        mtime: (i64, u32),
        nlink: u32,
        rdev: u32,
        nid: u64,
        xattrs: Vec<(u8, Vec<u8>, Vec<u8>)>,
        /// File contents or a symlink's target.
        body: Vec<u8>,
    }

    /// A small EROFS reader for what [`Writer`] makes, by path, with `""` for the root.
    fn read(meta: &[u8], data: &[u8]) -> BTreeMap<String, Seen> {
        let sb = &meta[SUPER_OFFSET..];
        let mut out = BTreeMap::new();
        let root = u64::from(u16::from_le_bytes([sb[14], sb[15]]));
        let mut todo = vec![(String::new(), root)];
        while let Some((path, nid)) = todo.pop() {
            let at = usize::try_from(nid * 32).unwrap();
            let i = &meta[at..];
            let u16_at = |o: usize| u16::from_le_bytes([i[o], i[o + 1]]);
            let u32_at = |o: usize| u32::from_le_bytes(i[o..o + 4].try_into().unwrap());
            assert_eq!(u16_at(0) & 1, 1, "{path} has an extended inode");
            let layout = u16_at(0) >> 1 & 7;
            let icount = usize::from(u16_at(2));
            let mode = u32::from(u16_at(4));
            let size = usize::try_from(u64::from_le_bytes(i[8..16].try_into().unwrap())).unwrap();
            let iu = u32_at(16);
            let xsize = if icount == 0 { 0 } else { XATTR_HEADER + (icount - 1) * 4 };
            let mut xattrs = Vec::new();
            let mut x = INODE_SIZE + XATTR_HEADER;
            while x < INODE_SIZE + xsize {
                let (len, index) = (usize::from(i[x]), i[x + 1]);
                let vlen = usize::from(u16_at(x + 2));
                let name = i[x + 4..x + 4 + len].to_vec();
                let value = i[x + 4 + len..x + 4 + len + vlen].to_vec();
                xattrs.push((index, name, value));
                x = (x + 4 + len + vlen).next_multiple_of(4);
            }
            let after = INODE_SIZE + xsize;
            let body = match layout {
                CHUNK_BASED => {
                    let bits = (iu & 0x1f) + BLOCK_BITS;
                    let chunk = 1usize << bits;
                    let first = (at + after).next_multiple_of(8) - at;
                    let mut body = Vec::new();
                    for k in 0..size.div_ceil(chunk) {
                        let addr = u32_at(first + 8 * k + 4);
                        assert_eq!(u16_at(first + 8 * k + 2), 1, "{path} reads from the data blob");
                        let len = chunk.min(size - body.len());
                        if addr == NULL_ADDR {
                            body.resize(body.len() + len, 0);
                        } else {
                            let from = addr as usize * BLOCK;
                            body.extend_from_slice(&data[from..from + len]);
                        }
                    }
                    body
                }
                FLAT_INLINE => {
                    let full = size / BLOCK * BLOCK;
                    let mut body = Vec::new();
                    if full > 0 {
                        let from = iu as usize * BLOCK;
                        body.extend_from_slice(&meta[from..from + full]);
                    }
                    let tail = &i[after..after + size - full];
                    assert!(
                        (at + after) % BLOCK + tail.len() <= BLOCK,
                        "{path} has its tail in one block"
                    );
                    body.extend_from_slice(tail);
                    body
                }
                _ if size == 0 => Vec::new(),
                _ => meta[iu as usize * BLOCK..iu as usize * BLOCK + size].to_vec(),
            };
            let seen = Seen {
                mode,
                uid: u32_at(24),
                gid: u32_at(28),
                mtime: (i64::from_le_bytes(i[32..40].try_into().unwrap()), u32_at(40)),
                nlink: u32_at(44),
                rdev: if layout == FLAT_PLAIN
                    && mode & S_IFMT != S_IFDIR
                    && mode & S_IFMT != S_IFLNK
                {
                    iu
                } else {
                    0
                },
                nid,
                xattrs,
                body: if mode & S_IFMT == S_IFDIR { Vec::new() } else { body.clone() },
            };
            if mode & S_IFMT == S_IFDIR {
                let mut names = Vec::new();
                for block in body.chunks(BLOCK) {
                    let count = usize::from(u16::from_le_bytes([block[8], block[9]])) / DIRENT_SIZE;
                    for k in 0..count {
                        let d = &block[k * DIRENT_SIZE..];
                        let child = u64::from_le_bytes(d[..8].try_into().unwrap());
                        let from = usize::from(u16::from_le_bytes([d[8], d[9]]));
                        let to = if k + 1 < count {
                            usize::from(u16::from_le_bytes([d[20], d[21]]))
                        } else {
                            block.len()
                        };
                        let name = block[from..to]
                            .iter()
                            .take_while(|&&b| b != 0)
                            .copied()
                            .collect::<Vec<u8>>();
                        names.push(name.clone());
                        if name != b"." && name != b".." {
                            let name = String::from_utf8(name).unwrap();
                            let p = if path.is_empty() { name } else { format!("{path}/{name}") };
                            todo.push((p, child));
                        }
                    }
                }
                let mut sorted = names.clone();
                sorted.sort();
                assert_eq!(names, sorted, "{path} lists its entries in order");
            }
            out.insert(path, seen);
        }
        out
    }

    fn header(kind: tar::EntryType, mode: u32, size: u64) -> tar::Header {
        let mut h = tar::Header::new_gnu();
        h.set_entry_type(kind);
        h.set_mode(mode);
        h.set_uid(1000);
        h.set_gid(100);
        h.set_mtime(1_700_000_000);
        h.set_size(size);
        h
    }

    fn noise(seed: u64, len: usize) -> Vec<u8> {
        let mut x = seed;
        (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                (x >> 24) as u8
            })
            .collect()
    }

    /// Builds `tar` with chunks of `chunk` bytes, and returns the two blobs.
    fn build(name: &str, tar: &[u8], chunk: u32) -> io::Result<(Vec<u8>, Vec<u8>)> {
        let s = Scratch::new(name);
        let built = Writer::new(chunk)?.build(tar, &s.0)?;
        let sb = superblock(&built.meta).unwrap();
        let (meta, data) = (std::fs::read(&built.meta)?, std::fs::read(&built.data)?);
        assert_eq!(sb.blocks as usize * BLOCK, meta.len());
        assert_eq!(sb.data_blocks as usize * BLOCK, data.len());
        let mut head = meta[SUPER_OFFSET..BLOCK].to_vec();
        let crc = u32::from_le_bytes(head[4..8].try_into().unwrap());
        head[4..8].fill(0);
        assert_eq!(crc32c(!0, &head), crc);
        Ok((meta, data))
    }

    #[test]
    fn a_tar_reads_back_the_same_from_the_layer() {
        let mut b = tar::Builder::new(Vec::new());
        let big = noise(1, 700_000);
        let mut gappy = noise(2, 300_000);
        gappy[4096..200_000].fill(0);
        let long = format!("usr/lib/{}/name", "x".repeat(120));
        b.append_data(&mut header(tar::EntryType::Directory, 0o750, 0), "etc/", &[][..]).unwrap();
        b.append_data(
            &mut header(tar::EntryType::Regular, 0o644, 5),
            "etc/hostname",
            &b"hive\n"[..],
        )
        .unwrap();
        b.append_data(&mut header(tar::EntryType::Regular, 0o755, 700_000), "bin/big", &big[..])
            .unwrap();
        b.append_data(&mut header(tar::EntryType::Regular, 0o644, 700_000), "bin/again", &big[..])
            .unwrap();
        b.append_data(&mut header(tar::EntryType::Regular, 0o644, 300_000), "gappy", &gappy[..])
            .unwrap();
        b.append_data(&mut header(tar::EntryType::Regular, 0o600, 0), "empty", &[][..]).unwrap();
        b.append_data(&mut header(tar::EntryType::Regular, 0o644, 3), &long, &b"abc"[..]).unwrap();
        b.append_link(&mut header(tar::EntryType::Symlink, 0o777, 0), "bin/sh", "big").unwrap();
        b.append_link(&mut header(tar::EntryType::Link, 0o644, 0), "bin/hard", "bin/big").unwrap();
        let mut dev = header(tar::EntryType::Char, 0o666, 0);
        dev.set_device_major(1).unwrap();
        dev.set_device_minor(300).unwrap();
        b.append_data(&mut dev, "dev/null", &[][..]).unwrap();
        b.append_data(&mut header(tar::EntryType::Fifo, 0o644, 0), "dev/fifo", &[][..]).unwrap();
        b.append_data(&mut header(tar::EntryType::Regular, 0, 0), "opt/.wh.gone", &[][..]).unwrap();
        b.append_data(&mut header(tar::EntryType::Regular, 0, 0), "srv/.wh..wh..opq", &[][..])
            .unwrap();
        b.append_pax_extensions([
            ("SCHILY.xattr.security.capability", &b"\x01\x02"[..]),
            ("SCHILY.xattr.user.note", &b"hello"[..]),
            ("mtime", &b"1700000000.25"[..]),
        ])
        .unwrap();
        b.append_data(&mut header(tar::EntryType::Regular, 0o644, 1), "srv/caps", &b"c"[..])
            .unwrap();
        for k in 0..400 {
            let name = format!("many/file-with-a-longish-name-{k:04}");
            b.append_data(&mut header(tar::EntryType::Regular, 0o644, 1), name, &b"m"[..]).unwrap();
        }
        let tar = b.into_inner().unwrap();
        let (meta, data) = build("round", &tar, 64 << 10).unwrap();
        let got = read(&meta, &data);

        let root = &got[""];
        assert_eq!((root.mode, root.mtime, root.nlink, root.nid), (S_IFDIR | 0o755, (0, 0), 9, 40));
        let etc = &got["etc"];
        assert_eq!(
            (etc.mode, etc.uid, etc.gid, etc.mtime),
            (S_IFDIR | 0o750, 1000, 100, (1_700_000_000, 0))
        );
        assert_eq!(got["etc/hostname"].body, b"hive\n");
        assert_eq!(got["bin/big"].body, big);
        assert_eq!(got["bin/big"].nlink, 2);
        assert_eq!(got["bin/hard"], got["bin/big"]);
        assert_eq!(got["bin/again"].body, big);
        assert_eq!(got["gappy"].body, gappy);
        assert_eq!(got["empty"].body, b"");
        assert_eq!(got[&long].body, b"abc");
        assert_eq!(got["bin/sh"].mode, S_IFLNK | 0o777);
        assert_eq!(got["bin/sh"].body, b"big");
        assert_eq!(got["dev/null"].mode, S_IFCHR | 0o666);
        assert_eq!(got["dev/null"].rdev, 300 & 0xff | 1 << 8 | (300 & !0xff) << 12);
        assert_eq!(got["dev/fifo"].mode, S_IFIFO | 0o644);
        // Whiteouts as overlayfs has them.
        assert_eq!((got["opt/gone"].mode, got["opt/gone"].rdev), (S_IFCHR, 0));
        assert_eq!(got["opt"].xattrs, vec![(TRUSTED, b"overlay.origin".to_vec(), Vec::new())]);
        assert!(!got.contains_key("srv/.wh..wh..opq"));
        assert_eq!(got["srv"].xattrs, vec![(TRUSTED, b"overlay.opaque".to_vec(), b"y".to_vec())]);
        let caps = &got["srv/caps"];
        assert_eq!(caps.mtime, (1_700_000_000, 250_000_000));
        assert_eq!(
            caps.xattrs,
            vec![(1, b"note".to_vec(), b"hello".to_vec()), (6, b"capability".to_vec(), vec![1, 2])]
        );
        // The big directory is over a block, so its first block is elsewhere in the metadata.
        assert_eq!(got.keys().filter(|p| p.starts_with("many/")).count(), 400);
        assert_eq!(got["many/file-with-a-longish-name-0399"].body, b"m");
        // The 700 KB file once, the gappy one less its two zero chunks, and a block for each small
        // file, with the 400 alike stored once.
        assert_eq!(data.len() / BLOCK, 700_000usize.div_ceil(BLOCK) + 16 + 16 + 10 + 4);
    }

    #[test]
    fn the_same_tar_builds_the_same_bytes() {
        let mut b = tar::Builder::new(Vec::new());
        b.append_data(
            &mut header(tar::EntryType::Regular, 0o644, 9000),
            "a/b/c",
            &noise(3, 9000)[..],
        )
        .unwrap();
        let tar = b.into_inner().unwrap();
        let one = build("same1", &tar, 4096).unwrap();
        let two = build("same2", &tar, 4096).unwrap();
        assert_eq!(one, two);
    }

    #[test]
    fn blank_header_fields_read_as_zero() {
        let mut b = tar::Builder::new(Vec::new());
        let mut h = tar::Header::new_gnu();
        h.set_entry_type(tar::EntryType::Regular);
        h.set_size(1);
        b.append_data(&mut h, "f", &b"f"[..]).unwrap();
        let (meta, data) = build("blank", &b.into_inner().unwrap(), 4096).unwrap();
        let f = &read(&meta, &data)["f"];
        assert_eq!((f.mode, f.uid, f.gid, f.mtime), (S_IFREG, 0, 0, (0, 0)));
    }

    #[test]
    fn later_entries_win() {
        let mut b = tar::Builder::new(Vec::new());
        b.append_data(&mut header(tar::EntryType::Regular, 0o644, 3), "x", &b"old"[..]).unwrap();
        b.append_data(&mut header(tar::EntryType::Regular, 0o644, 3), "x", &b"new"[..]).unwrap();
        b.append_data(&mut header(tar::EntryType::Regular, 0o644, 1), "d/f", &b"f"[..]).unwrap();
        b.append_data(&mut header(tar::EntryType::Directory, 0o700, 0), "d", &[][..]).unwrap();
        b.append_data(&mut header(tar::EntryType::Regular, 0o644, 1), "e/f", &b"f"[..]).unwrap();
        b.append_data(&mut header(tar::EntryType::Regular, 0o644, 1), "e", &b"e"[..]).unwrap();
        b.append_data(&mut header(tar::EntryType::Directory, 0o711, 0), "./", &[][..]).unwrap();
        let tar = b.into_inner().unwrap();
        let (meta, data) = build("later", &tar, 4096).unwrap();
        let got = read(&meta, &data);
        assert_eq!(got["x"].body, b"new");
        assert_eq!(got["d"].mode, S_IFDIR | 0o700);
        assert_eq!(got["d/f"].body, b"f");
        assert_eq!(got["e"].body, b"e");
        assert!(!got.contains_key("e/f"));
        assert_eq!(got[""].mode, S_IFDIR | 0o711);
        assert_eq!(got.len(), 5);
    }

    #[test]
    fn tars_erofs_cannot_hold_are_refused() {
        let refused = |name: &str, f: &dyn Fn(&mut tar::Builder<Vec<u8>>)| {
            let mut b = tar::Builder::new(Vec::new());
            f(&mut b);
            build(name, &b.into_inner().unwrap(), 4096).unwrap_err().kind()
        };
        let kind = refused("missing", &|b| {
            b.append_link(&mut header(tar::EntryType::Link, 0o644, 0), "a", "nothing").unwrap();
        });
        assert_eq!(kind, io::ErrorKind::InvalidData);
        let kind = refused("under", &|b| {
            b.append_data(&mut header(tar::EntryType::Regular, 0o644, 0), "f", &[][..]).unwrap();
            b.append_data(&mut header(tar::EntryType::Regular, 0o644, 0), "f/g", &[][..]).unwrap();
        });
        assert_eq!(kind, io::ErrorKind::InvalidData);
        let kind = refused("xattr", &|b| {
            b.append_pax_extensions([("SCHILY.xattr.odd.name", &b"v"[..])]).unwrap();
            b.append_data(&mut header(tar::EntryType::Regular, 0o644, 0), "f", &[][..]).unwrap();
        });
        assert_eq!(kind, io::ErrorKind::InvalidData);
    }

    #[test]
    fn pax_values_parse() {
        assert_eq!(pax_time(b"12"), Some((12, 0)));
        assert_eq!(pax_time(b"12.5"), Some((12, 500_000_000)));
        assert_eq!(pax_time(b"12.0000000019"), Some((12, 1)));
        assert_eq!(pax_time(b"-1.25"), Some((-2, 750_000_000)));
        assert_eq!(pax_time(b"x"), None);
        assert_eq!(base64(b"aGl2ZQ=="), Some(b"hive".to_vec()));
        assert_eq!(base64(b"aGl2ZWJveA"), Some(b"hivebox".to_vec()));
        assert_eq!(unescape(b"user.a%3Db"), Some(b"user.a=b".to_vec()));
        assert_eq!(unescape(b"bad%4"), None);
    }
}
