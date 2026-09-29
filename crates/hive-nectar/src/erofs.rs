//! Turning tar streams into EROFS layers with `mkfs.erofs`, and the few superblock fields we check
//! or fix afterwards.
//!
//! A layer is two files. The metadata blob has the superblock, inodes, directories, xattrs and
//! chunk indexes, and every node that may run the image keeps it. The data blob has file contents
//! in whole chunks, laid out as EROFS's one extra device, and is what the cache fetches. File data
//! never sits inline in the metadata blob, so the split is clean.
//!
//! Builds are deterministic, which is what lets identical layers dedup by name. The UUID is fixed,
//! every inode is the extended kind so it keeps its own mtime, and after the build the superblock's
//! build time is zeroed and its checksum redone. [`prepare`] makes sure no directory is left for
//! `mkfs.erofs` to invent with the time of the build. Owners stay as the image has them, and the node
//! maps them with an idmapped mount instead.

use std::collections::BTreeSet;
use std::fs::File;
use std::io::{self, BufRead, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use rustix::fs::{FileType, Mode, OFlags};

/// Where the superblock starts.
const SUPER_OFFSET: usize = 1024;
const MAGIC: u32 = 0xE0F5_E1E2;
const COMPAT_SB_CHKSUM: u32 = 0x1;
const INCOMPAT_CHUNKED_FILE: u32 = 0x4;
const INCOMPAT_DEVICE_TABLE: u32 = 0x8;

/// The chunk size layers are built with unless asked otherwise. It is also what the cache fetches
/// in, so it trades fetch round trips against bytes fetched that nobody reads.
pub const DEFAULT_CHUNK_SIZE: u32 = 256 << 10;

/// A `mkfs.erofs` that is new enough.
#[derive(Clone, Debug)]
pub struct Mkfs {
    program: PathBuf,
    chunk_size: u32,
}

/// The two files of a built layer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Built {
    /// The metadata blob.
    pub meta: PathBuf,
    /// The data blob.
    pub data: PathBuf,
}

/// What we read back from a metadata blob's superblock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Superblock {
    /// The block size.
    pub block_size: u32,
    /// Blocks in the metadata blob itself.
    pub blocks: u32,
    /// Blocks the data device must have.
    pub data_blocks: u32,
    /// Inodes in the filesystem.
    pub inodes: u64,
}

impl Mkfs {
    /// Checks that `program` is `mkfs.erofs` 1.7 or newer, which is the first with tar input and
    /// forced extended inodes.
    ///
    /// # Errors
    ///
    /// It cannot be run, is too old, or `chunk_size` is not a power of two from 4 KiB to 64 MiB.
    pub fn new(program: impl Into<PathBuf>, chunk_size: u32) -> io::Result<Self> {
        let program = program.into();
        if !chunk_size.is_power_of_two() || !(4 << 10..=64 << 20).contains(&chunk_size) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("chunk size {chunk_size} is not a power of two from 4 KiB to 64 MiB"),
            ));
        }
        // 1.8 answers -V with its version. 1.7 has no -V and complains, but then prints the same
        // line before its usage.
        let out = Command::new(&program)
            .arg("-V")
            .output()
            .map_err(|e| io::Error::new(e.kind(), format!("running {}: {e}", program.display())))?;
        let text = String::from_utf8_lossy(&out.stdout) + String::from_utf8_lossy(&out.stderr);
        let version = text
            .lines()
            .filter(|l| l.starts_with("mkfs.erofs"))
            .find_map(|l| l.split_whitespace().last().and_then(parse_version));
        match version {
            Some(v) if v >= (1, 7) => Ok(Self { program, chunk_size }),
            Some((major, minor)) => Err(io::Error::other(format!(
                "{} is erofs-utils {major}.{minor}, and 1.7 or newer is needed",
                program.display()
            ))),
            None => Err(io::Error::other(format!("{} is not mkfs.erofs", program.display()))),
        }
    }

    /// The chunk size layers are built with.
    #[must_use]
    pub const fn chunk_size(&self) -> u32 {
        self.chunk_size
    }

    /// Builds a layer from the tar stream `src`, which may be gzipped, into `meta` and `data` in
    /// `dir`. The stream goes through [`prepare`] into a pipe that `mkfs.erofs` reads, so nothing
    /// is written but the layer. AUFS whiteouts, which is how OCI layers delete files, become
    /// overlayfs ones. The whole of `src` is read, so a reader that hashes what passes through it
    /// sees every byte.
    ///
    /// # Errors
    ///
    /// `src` is not a tar, `mkfs.erofs` fails, or what it made is not a chunked layer with one
    /// data device.
    pub fn build(&self, src: impl Read, dir: &Path) -> io::Result<Built> {
        let built = Built { meta: dir.join("meta"), data: dir.join("data") };
        let fifo = dir.join("layer.tar");
        let log = dir.join("mkfs.log");
        rustix::fs::mknodat(rustix::fs::CWD, &fifo, FileType::Fifo, Mode::from_raw_mode(0o600), 0)?;
        let mut child = Command::new(&self.program)
            .args(["--quiet", "--tar=f", "--aufs", "-Enoinline_data,force-inode-extended"])
            .arg("-U00000000-0000-0000-0000-000000000000")
            .arg(format!("--chunksize={}", self.chunk_size))
            .arg(format!("--blobdev={}", built.data.display()))
            .arg(&built.meta)
            .arg(&fifo)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            // A file, not a pipe, so mkfs.erofs can never block on it while we block on the fifo.
            .stderr(File::create(&log)?)
            .spawn()?;
        let fed = feed(&mut child, &fifo, src);
        if fed.as_ref().is_err_and(|e| e.kind() != io::ErrorKind::BrokenPipe) {
            // The tar was bad, and mkfs.erofs is waiting for the rest of it.
            let _ = child.kill();
        }
        let status = child.wait()?;
        let said = std::fs::read_to_string(&log).unwrap_or_default();
        let _ = std::fs::remove_file(&fifo);
        let _ = std::fs::remove_file(&log);
        match fed {
            Err(e) if e.kind() != io::ErrorKind::BrokenPipe => return Err(e),
            _ if !status.success() => {
                return Err(io::Error::other(format!(
                    "mkfs.erofs failed with {status}: {}",
                    said.trim()
                )));
            }
            Err(e) => return Err(e),
            Ok(()) => {}
        }
        let sb = normalize(&built.meta)?;
        let data = std::fs::metadata(&built.data)?.len();
        if data != u64::from(sb.data_blocks) * u64::from(sb.block_size) {
            return Err(io::Error::other(format!(
                "the superblock wants {} data blocks, and mkfs.erofs wrote {data} bytes",
                sb.data_blocks
            )));
        }
        Ok(built)
    }
}

/// Opens the fifo once `mkfs.erofs` has it open for reading, and writes the prepared tar into it.
fn feed(child: &mut Child, fifo: &Path, src: impl Read) -> io::Result<()> {
    let flags = OFlags::WRONLY | OFlags::CLOEXEC;
    // Opening a fifo to write blocks until someone opens it to read, which a mkfs.erofs that died
    // first never does, so this polls instead.
    let fd = loop {
        match rustix::fs::open(fifo, flags | OFlags::NONBLOCK, Mode::empty()) {
            Ok(fd) => break fd,
            Err(rustix::io::Errno::NXIO) => {
                if child.try_wait()?.is_some() {
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "mkfs.erofs exited before reading its input",
                    ));
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            Err(e) => return Err(e.into()),
        }
    };
    rustix::fs::fcntl_setfl(&fd, flags)?;
    let out = prepare(src, io::BufWriter::with_capacity(1 << 20, File::from(fd)))?;
    out.into_inner().map_err(io::IntoInnerError::into_error)?;
    Ok(())
}

/// Copies the tar stream `src`, gunzipping it if it is gzipped, to a plain tar in `out` that
/// lists every directory it has anything in. `mkfs.erofs` makes a directory a tar only implies,
/// such as the root, which no docker layer lists, with the build time as its mtime, and that would
/// make every build of the layer different. The missing ones are added at the end, owned by root
/// with mode 0755 as `mkfs.erofs` would make them, and with an mtime of 0. The whole of `src` is
/// read. Returns `out`, flushed.
///
/// # Errors
///
/// `src` is not a tar, it has more than a MiB of padding after its end, or `out` fails.
pub fn prepare<W: Write>(src: impl Read, out: W) -> io::Result<W> {
    let mut src = io::BufReader::with_capacity(1 << 20, src);
    let gzipped = src.fill_buf()?.starts_with(&[0x1f, 0x8b]);
    let mut plain: Box<dyn Read + '_> = if gzipped {
        Box::new(flate2::bufread::MultiGzDecoder::new(&mut src))
    } else {
        Box::new(&mut src)
    };
    // The end of the tar is only known once the parser has read past it, so the copy runs a MiB
    // behind the parser and the end-of-archive blocks are never sent.
    let mut tee = Tee { from: &mut plain, to: Lagged { out, buf: Vec::new(), sent: 0 } };
    let mut seen = BTreeSet::new();
    let mut implied = BTreeSet::new();
    let mut end = 0;
    {
        let mut archive = tar::Archive::new(&mut tee);
        for entry in archive.entries()? {
            let mut entry = entry?;
            let path = clean(&entry.path()?);
            // What the entry takes in the stream, which a PAX size overrides. It is not
            // `size()`, which for a sparse file is the size it unpacks to.
            let mut stored = entry.header().entry_size()?;
            if let Some(pax) = entry.pax_extensions()? {
                for ext in pax {
                    let ext = ext?;
                    if ext.key() == Ok("size")
                        && let Some(n) = ext.value().ok().and_then(|v| v.parse().ok())
                    {
                        stored = n;
                    }
                }
            }
            end = entry.raw_file_position() + stored.div_ceil(512) * 512;
            let mut parent = path.as_str();
            while let Some((up, _)) = parent.rsplit_once('/') {
                implied.insert(up.to_owned());
                parent = up;
            }
            implied.insert(String::new());
            if entry.header().entry_type().is_dir() {
                seen.insert(path);
            }
        }
    }
    // Whoever hashes `src` needs all of it, padding and anything after the gzip stream too.
    io::copy(&mut tee.from, &mut io::sink())?;
    let lagged = tee.to;
    drop(plain);
    io::copy(&mut src, &mut io::sink())?;
    let mut b = tar::Builder::new(lagged.finish(end)?);
    for dir in implied.difference(&seen) {
        let mut h = tar::Header::new_ustar();
        h.set_entry_type(tar::EntryType::Directory);
        h.set_mode(0o755);
        h.set_uid(0);
        h.set_gid(0);
        h.set_mtime(0);
        h.set_size(0);
        let name = if dir.is_empty() { "./".to_owned() } else { format!("{dir}/") };
        b.append_data(&mut h, name, io::empty())?;
    }
    let mut out = b.into_inner()?;
    out.flush()?;
    Ok(out)
}

/// A path in a tar as `a/b/c`, with no leading `./` or `/` and no trailing `/`.
fn clean(path: &Path) -> String {
    let parts: Vec<_> = path
        .components()
        .filter_map(|c| match c {
            std::path::Component::Normal(p) => Some(p.to_string_lossy()),
            _ => None,
        })
        .collect();
    parts.join("/")
}

/// Passes reads through and writes a copy of them.
struct Tee<'a, R: Read + ?Sized, W: Write> {
    from: &'a mut R,
    to: W,
}

impl<R: Read + ?Sized, W: Write> Read for Tee<'_, R, W> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.from.read(buf)?;
        self.to.write_all(&buf[..n])?;
        Ok(n)
    }
}

/// How far [`Lagged`] runs behind.
const LAG: usize = 1 << 20;

/// A writer that holds back the last [`LAG`] bytes, until it is told where the stream ends.
struct Lagged<W> {
    out: W,
    buf: Vec<u8>,
    sent: u64,
}

impl<W: Write> Write for Lagged<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(bytes);
        if self.buf.len() >= 2 * LAG {
            let n = self.buf.len() - LAG;
            self.out.write_all(&self.buf[..n])?;
            self.buf.drain(..n);
            self.sent += n as u64;
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<W: Write> Lagged<W> {
    /// Sends what is held up to `end` and drops the rest.
    fn finish(mut self, end: u64) -> io::Result<W> {
        let keep = end
            .checked_sub(self.sent)
            .and_then(|k| usize::try_from(k).ok())
            .filter(|k| *k <= self.buf.len())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "the tar has over a MiB of padding after its end",
                )
            })?;
        self.out.write_all(&self.buf[..keep])?;
        Ok(self.out)
    }
}

fn parse_version(s: &str) -> Option<(u32, u32)> {
    let mut parts = s.split('.');
    Some((parts.next()?.parse().ok()?, parts.next()?.parse().ok()?))
}

/// Reads the superblock of the metadata blob at `path`.
///
/// # Errors
///
/// It is not an EROFS metadata blob with chunked files and exactly one data device.
pub fn superblock(path: &Path) -> io::Result<Superblock> {
    let mut f = File::open(path)?;
    let mut head = vec![0; 4096];
    f.read_exact(&mut head)?;
    let sb = parse(&head)?;
    let devt = usize::from(u16_at(&head, SUPER_OFFSET + 88)) * 128;
    let mut slot = [0; 128];
    f.seek(SeekFrom::Start(devt as u64))?;
    f.read_exact(&mut slot)?;
    Ok(Superblock { data_blocks: u32_at(&slot, 64), ..sb })
}

fn parse(head: &[u8]) -> io::Result<Superblock> {
    let bad =
        |what: &str| io::Error::new(io::ErrorKind::InvalidData, format!("not a layer: {what}"));
    let sb = &head[SUPER_OFFSET..];
    if u32_at(sb, 0) != MAGIC {
        return Err(bad("no EROFS magic"));
    }
    let bits = sb[12];
    if !(9..=12).contains(&bits) {
        return Err(bad("an odd block size"));
    }
    let incompat = u32_at(sb, 80);
    let wanted = INCOMPAT_CHUNKED_FILE | INCOMPAT_DEVICE_TABLE;
    if incompat & wanted != wanted || u16_at(sb, 86) != 1 {
        return Err(bad("the data is not in one extra device"));
    }
    Ok(Superblock {
        block_size: 1 << bits,
        blocks: u32_at(sb, 36),
        data_blocks: 0,
        inodes: u64::from_le_bytes(sb[16..24].try_into().expect("8 bytes")),
    })
}

/// Zeroes the build time in the superblock of the metadata blob at `path` and redoes its
/// checksum. Every inode carries its own mtime, so nothing in the filesystem changes.
fn normalize(path: &Path) -> io::Result<Superblock> {
    let mut f = File::options().read(true).write(true).open(path)?;
    let mut head = vec![0; 4096];
    f.read_exact(&mut head)?;
    parse(&head)?;
    let bits = head[SUPER_OFFSET + 12];
    let end = 1usize << bits;
    let sb = &mut head[SUPER_OFFSET..end];
    sb[24..36].fill(0);
    if u32_at(sb, 8) & COMPAT_SB_CHKSUM != 0 {
        sb[4..8].fill(0);
        let crc = crc32c(!0, sb);
        sb[4..8].copy_from_slice(&crc.to_le_bytes());
    }
    f.seek(SeekFrom::Start(SUPER_OFFSET as u64))?;
    f.write_all(&head[SUPER_OFFSET..end])?;
    f.sync_all()?;
    drop(f);
    superblock(path)
}

fn u16_at(b: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([b[at], b[at + 1]])
}

fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().expect("4 bytes"))
}

/// CRC32C the way the kernel's `crc32c()` does it: seeded by the caller, with no final inversion.
fn crc32c(mut crc: u32, bytes: &[u8]) -> u32 {
    const TABLE: [u32; 256] = {
        let mut t = [0; 256];
        let mut i = 0;
        while i < 256 {
            let mut c = i as u32;
            let mut k = 0;
            while k < 8 {
                c = if c & 1 == 1 { (c >> 1) ^ 0x82F6_3B78 } else { c >> 1 };
                k += 1;
            }
            t[i] = c;
            i += 1;
        }
        t
    };
    for &b in bytes {
        crc = TABLE[((crc ^ u32::from(b)) & 0xff) as usize] ^ (crc >> 8);
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32c_matches_the_published_check_value() {
        // The standard check: CRC32C of "123456789" with the usual inversions is 0xE3069283.
        assert_eq!(!crc32c(!0, b"123456789"), 0xE306_9283);
    }

    #[test]
    fn versions_are_read_from_the_banner() {
        assert_eq!(parse_version("1.7.1"), Some((1, 7)));
        assert_eq!(parse_version("1.8"), Some((1, 8)));
        assert_eq!(parse_version("[options]"), None);
    }

    #[test]
    fn prepare_lists_every_directory_a_gzipped_tar_only_implies() {
        let mut b =
            tar::Builder::new(flate2::write::GzEncoder::new(Vec::new(), Default::default()));
        let mut add = |name: &str, kind: tar::EntryType, data: &[u8]| {
            let mut h = tar::Header::new_gnu();
            h.set_entry_type(kind);
            h.set_mode(0o644);
            h.set_mtime(1_700_000_000);
            h.set_size(data.len() as u64);
            b.append_data(&mut h, name, data).unwrap();
        };
        add("etc/", tar::EntryType::Directory, &[]);
        add("etc/hostname", tar::EntryType::Regular, b"hive\n");
        // Long enough for a GNU long name entry, and three directories deep with none listed.
        let long = format!("usr/lib/{}/f", "x".repeat(120));
        add(&long, tar::EntryType::Regular, &[7; 1000]);
        let gz = b.into_inner().unwrap().finish().unwrap();
        let out = prepare(&gz[..], Vec::new()).unwrap();

        let mut got = Vec::new();
        let mut archive = tar::Archive::new(&out[..]);
        for e in archive.entries().unwrap() {
            let mut e = e.unwrap();
            let mut body = Vec::new();
            e.read_to_end(&mut body).unwrap();
            got.push((
                e.path().unwrap().display().to_string(),
                e.header().mtime().unwrap(),
                body.len(),
            ));
        }
        let x = "x".repeat(120);
        assert_eq!(
            got,
            vec![
                ("etc/".into(), 1_700_000_000, 0),
                ("etc/hostname".into(), 1_700_000_000, 5),
                (long.clone(), 1_700_000_000, 1000),
                ("./".into(), 0, 0),
                ("usr/".into(), 0, 0),
                ("usr/lib/".into(), 0, 0),
                (format!("usr/lib/{x}/"), 0, 0),
            ]
        );
    }

    #[test]
    fn a_bad_chunk_size_is_refused_before_anything_runs() {
        assert!(Mkfs::new("/nonexistent/mkfs.erofs", 3 << 10).is_err());
        assert!(Mkfs::new("/nonexistent/mkfs.erofs", 128 << 20).is_err());
    }
}
