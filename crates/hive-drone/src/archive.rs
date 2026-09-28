//! `fs.upload` and `fs.download`: whole trees moved as tar archives.
//!
//! Unpacking trusts nothing in the archive. The destination is opened once under the drone's
//! roots, and every entry resolves beneath it with `openat2` and `RESOLVE_IN_ROOT`, so an
//! absolute name, a `..`, or a symlink an earlier entry made all stay inside it. The final name
//! is always made with the `*at` calls and `O_NOFOLLOW`, and owners in the archive are ignored.
//! Packing walks the tree the same way `fs.list` does, never following a symlink.

use crate::Config;
use crate::fs::{
    CHUNK, Owner, Part, Resolved, blocking, fstat, io_err, kind, make_dirs_beneath, open_beneath,
    os, pipe_in, statat,
};
use bytes::{Buf, Bytes};
use hive_proto::drone::Stream;
use hive_proto::drone::api::{FileKind, FsPath, FsUpload, FsUploadResult};
use hive_types::{Error, Reason};
use rustix::fd::{AsFd, BorrowedFd, OwnedFd};
use rustix::fs::{AtFlags, Dir, Mode, OFlags, Statx, Timespec, Timestamps, UTIME_OMIT};
use rustix::io::Errno;
use std::ffi::{CString, OsStr};
use std::io::{self, BufWriter, Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path, PathBuf};
use tar::{EntryType, Header};
use tokio::sync::mpsc;

/// Unpacks the archive that follows the request on `stream`.
pub(crate) async fn upload(
    cfg: &Config,
    req: FsUpload,
    stream: &mut Stream,
) -> Result<FsUploadResult, Error> {
    let cfg = cfg.clone();
    pipe_in(stream, move |rx| {
        let input = Incoming { rx, now: Bytes::new(), ended: false };
        unpack(&cfg, &req, input)
    })
    .await
}

// The stream as a reader, for the tar parser on the blocking pool.
struct Incoming {
    rx: mpsc::Receiver<Part>,
    now: Bytes,
    ended: bool,
}

impl Read for Incoming {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        while self.now.is_empty() {
            if self.ended {
                return Ok(0);
            }
            match self.rx.blocking_recv() {
                Some(Part::Data(b)) => self.now = b,
                Some(Part::End) => self.ended = true,
                // The stream failed, and the caller reports how.
                None => return Err(io::Error::from(io::ErrorKind::ConnectionAborted)),
            }
        }
        let n = buf.len().min(self.now.len());
        buf[..n].copy_from_slice(&self.now[..n]);
        self.now.advance(n);
        Ok(n)
    }
}

fn unpack(cfg: &Config, req: &FsUpload, input: Incoming) -> Result<FsUploadResult, Error> {
    let path = &req.path;
    let at = Resolved::new(cfg, path)?;
    let owner = Owner::new(req.uid.or(cfg.uid), req.gid.or(cfg.gid));
    let dest = if req.make_parents {
        at.make_dirs(&at.rel, 0o755, owner)
    } else {
        at.open(&at.rel, OFlags::PATH | OFlags::DIRECTORY, Mode::empty())
    }
    .map_err(|e| os(e, path))?;
    let dest = dest.as_fd();
    let mut done = FsUploadResult::default();
    // Directory modes and times go on at the end, so a read-only directory in the archive
    // doesn't stop what goes in it and adding to a directory doesn't undo its time.
    let mut dirs = Vec::new();
    // Archives are grouped by directory, so the last parent is usually the next one too.
    let mut parent: Option<(PathBuf, OwnedFd)> = None;
    let mut buf = vec![0u8; CHUNK];
    let mut archive = tar::Archive::new(input);
    for entry in archive.entries().map_err(|e| broken(&e, path))? {
        let mut entry = entry.map_err(|e| broken(&e, path))?;
        let name = entry.path().map_err(|e| broken(&e, path))?;
        let Some(rel) = clean(&name) else {
            done.skipped += 1;
            continue;
        };
        let shown = format!("{}/{}", path.trim_end_matches('/'), rel.display());
        let fail = |e: Errno| os(e, &shown);
        let header = entry.header();
        let mode = header.mode().unwrap_or(0o644) & 0o7777;
        let mtime = header.mtime().ok();
        let ty = header.entry_type();
        let Some(file_name) = rel.file_name() else {
            // The destination itself, often the "./" an archive starts with. It is left alone.
            continue;
        };
        let up = rel.parent().unwrap_or(Path::new(""));
        let dir = match &parent {
            Some((p, fd)) if p == up => fd,
            _ => {
                let fd = make_dirs_beneath(dest, up, 0o755, owner).map_err(fail)?;
                &parent.insert((up.to_path_buf(), fd)).1
            }
        };
        match ty {
            EntryType::Directory => {
                make_dirs_beneath(dest, &rel, 0o700, owner).map_err(fail)?;
                dirs.push((rel, mode, mtime, shown));
            }
            EntryType::Regular | EntryType::Continuous | EntryType::GNUSparse => {
                clear(dir, file_name).map_err(fail)?;
                let flags = OFlags::WRONLY
                    | OFlags::CREATE
                    | OFlags::EXCL
                    | OFlags::NOFOLLOW
                    | OFlags::NOCTTY
                    | OFlags::CLOEXEC;
                let fd = rustix::fs::openat(dir, file_name, flags, Mode::from_raw_mode(0o600))
                    .map_err(fail)?;
                owner.apply(&fd).map_err(fail)?;
                let mut file = std::fs::File::from(fd);
                done.bytes += copy(&mut entry, &mut file, &mut buf, &shown)?;
                // A write by anyone but root clears setuid, so the bits go on after the data.
                rustix::fs::fchmod(&file, Mode::from_raw_mode(mode)).map_err(fail)?;
                if let Some(t) = mtime {
                    rustix::fs::futimens(&file, &times(t)).map_err(fail)?;
                }
            }
            EntryType::Symlink => {
                let target = entry.link_name().map_err(|e| broken(&e, path))?;
                let Some(target) = target else {
                    return Err(bad_entry(&shown, "a symlink with no target"));
                };
                clear(dir, file_name).map_err(fail)?;
                rustix::fs::symlinkat(target.as_os_str(), dir, file_name).map_err(fail)?;
                owner.apply_to_link(dir, file_name).map_err(fail)?;
            }
            EntryType::Link => {
                let target = entry.link_name().map_err(|e| broken(&e, path))?;
                let Some(target) = target.as_deref().and_then(clean) else {
                    done.skipped += 1;
                    continue;
                };
                let (Some(tname), tup) =
                    (target.file_name(), target.parent().unwrap_or(Path::new("")))
                else {
                    return Err(bad_entry(&shown, "a hard link to the destination itself"));
                };
                let tdir = open_beneath(dest, tup, OFlags::PATH | OFlags::DIRECTORY, Mode::empty())
                    .map_err(fail)?;
                clear(dir, file_name).map_err(fail)?;
                rustix::fs::linkat(&tdir, tname, dir, file_name, AtFlags::empty()).map_err(fail)?;
            }
            // Extended headers that the parser did not fold into an entry carry nothing to make.
            EntryType::XGlobalHeader | EntryType::XHeader => continue,
            _ => {
                done.skipped += 1;
                continue;
            }
        }
        done.entries += 1;
    }
    for (rel, mode, mtime, shown) in dirs.into_iter().rev() {
        let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW;
        let fd = match open_beneath(dest, &rel, flags, Mode::empty()) {
            Ok(fd) => fd,
            // A later entry put something else there.
            Err(Errno::NOTDIR | Errno::LOOP | Errno::NOENT) => continue,
            Err(e) => return Err(os(e, &shown)),
        };
        rustix::fs::fchmod(&fd, Mode::from_raw_mode(mode)).map_err(|e| os(e, &shown))?;
        if let Some(t) = mtime {
            rustix::fs::futimens(&fd, &times(t)).map_err(|e| os(e, &shown))?;
        }
    }
    Ok(done)
}

// The entry name relative to the destination, or None when it has a `..` in it. Leading
// slashes and `.` parts are dropped, the way tar does.
fn clean(name: &Path) -> Option<PathBuf> {
    let mut out = PathBuf::new();
    for part in name.components() {
        match part {
            Component::Normal(p) => out.push(p),
            Component::ParentDir => return None,
            Component::RootDir | Component::CurDir | Component::Prefix(_) => {}
        }
    }
    Some(out)
}

// Makes room for a new entry. A directory in the way is an error, the same as with tar.
fn clear(dir: &OwnedFd, name: &OsStr) -> Result<(), Errno> {
    match rustix::fs::unlinkat(dir, name, AtFlags::empty()) {
        Ok(()) | Err(Errno::NOENT) => Ok(()),
        Err(e) => Err(e),
    }
}

// Copies an entry into a file in big writes. The parser hands data over a frame at a time, so
// the buffer is filled first.
fn copy(
    from: &mut impl Read,
    to: &mut std::fs::File,
    buf: &mut [u8],
    shown: &str,
) -> Result<u64, Error> {
    let mut total = 0;
    loop {
        let mut filled = 0;
        while filled < buf.len() {
            match from.read(&mut buf[filled..]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(broken(&e, shown)),
            }
        }
        if filled == 0 {
            return Ok(total);
        }
        to.write_all(&buf[..filled]).map_err(|e| io_err(&e, shown))?;
        total += filled as u64;
    }
}

fn times(mtime: u64) -> Timestamps {
    Timestamps {
        last_access: Timespec { tv_sec: 0, tv_nsec: UTIME_OMIT },
        last_modification: Timespec {
            tv_sec: i64::try_from(mtime).unwrap_or(i64::MAX),
            tv_nsec: 0,
        },
    }
}

fn broken(e: &io::Error, path: &str) -> Error {
    if e.kind() == io::ErrorKind::ConnectionAborted {
        return Error::new(Reason::Internal, "the upload was cut off");
    }
    Error::new(Reason::InvalidArgument, format!("{path}: the archive is broken: {e}"))
}

fn bad_entry(shown: &str, what: &str) -> Error {
    Error::new(Reason::InvalidArgument, format!("{shown}: {what}"))
}

/// Packs `req.path` and sends it on `stream` as a tar archive. A directory's entries are named
/// relative to it, and anything else goes in under its own name. Errors that come before the
/// first byte are returned without anything sent.
pub(crate) async fn download(cfg: &Config, req: FsPath, stream: &mut Stream) -> Result<(), Error> {
    let cfg = cfg.clone();
    let path = req.path;
    let shown = path.clone();
    let top = blocking(move || Top::new(&cfg, &shown)).await?;
    let (tx, mut rx) = mpsc::channel::<Bytes>(4);
    let packer = tokio::task::spawn_blocking(move || {
        let out = BufWriter::with_capacity(CHUNK, Outgoing { tx });
        pack(top, &path, out)
    });
    let mut sent = Ok(());
    while let Some(chunk) = rx.recv().await {
        if let Err(e) = stream.send(chunk).await {
            sent = Err(e);
            break;
        }
    }
    // The packer stops at its next write once nothing is listening.
    drop(rx);
    let packed = packer.await.map_err(|e| Error::new(Reason::Internal, e.to_string()))?;
    sent?;
    packed
}

// What a download starts from.
enum Top {
    // A directory, opened for reading.
    Dir(OwnedFd),
    // Anything else, as its parent and its name.
    Entry(OwnedFd, CString),
}

impl Top {
    fn new(cfg: &Config, path: &str) -> Result<Self, Error> {
        let at = Resolved::new(cfg, path)?;
        let read_dir = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW;
        let Some((up, name)) = at.split() else {
            let fd = at.open(&at.rel, read_dir, Mode::empty()).map_err(|e| os(e, path))?;
            return Ok(Self::Dir(fd));
        };
        let dir = at
            .open(up, OFlags::PATH | OFlags::DIRECTORY, Mode::empty())
            .map_err(|e| os(e, path))?;
        let st = statat(&dir, name, false).map_err(|e| os(e, path))?;
        if kind(&st) == FileKind::Dir {
            let fd = rustix::fs::openat(&dir, name, read_dir | OFlags::CLOEXEC, Mode::empty())
                .map_err(|e| os(e, path))?;
            return Ok(Self::Dir(fd));
        }
        let name = CString::new(name.as_bytes()).map_err(|_| os(Errno::INVAL, path))?;
        Ok(Self::Entry(dir, name))
    }
}

// The archive as it is written, handed to the async side in big pieces.
struct Outgoing {
    tx: mpsc::Sender<Bytes>,
}

impl Write for Outgoing {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.tx
            .blocking_send(Bytes::copy_from_slice(buf))
            .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

type Builder = tar::Builder<BufWriter<Outgoing>>;

fn pack(top: Top, path: &str, out: BufWriter<Outgoing>) -> Result<(), Error> {
    let mut tar = tar::Builder::new(out);
    match top {
        Top::Dir(fd) => add_dir(&mut tar, fd, Path::new(""), path)?,
        Top::Entry(dir, name) => {
            let name = OsStr::from_bytes(name.as_bytes());
            add(&mut tar, dir.as_fd(), name, Path::new(name), path)?;
        }
    }
    let out = tar.into_inner().map_err(|e| io_err(&e, path))?;
    out.into_inner().map_err(|e| io_err(e.error(), path))?;
    Ok(())
}

// Adds what is in a directory, sorted by name so the same tree always packs the same way.
fn add_dir(tar: &mut Builder, fd: OwnedFd, rel: &Path, base: &str) -> Result<(), Error> {
    let shown = |rel: &Path| {
        if rel.as_os_str().is_empty() {
            base.to_string()
        } else {
            format!("{}/{}", base.trim_end_matches('/'), rel.display())
        }
    };
    let mut entries = Dir::new(fd).map_err(|e| os(e, &shown(rel)))?;
    let mut names = Vec::new();
    while let Some(entry) = entries.read() {
        let entry = entry.map_err(|e| os(e, &shown(rel)))?;
        let name = entry.file_name();
        if name != c"." && name != c".." {
            names.push(name.to_owned());
        }
    }
    names.sort_unstable();
    let here = entries.fd().map_err(|e| os(e, &shown(rel)))?;
    for name in names {
        let name = OsStr::from_bytes(name.as_bytes());
        add(tar, here, name, &rel.join(name), base)?;
    }
    Ok(())
}

fn add(
    tar: &mut Builder,
    dir: BorrowedFd<'_>,
    name: &OsStr,
    rel: &Path,
    base: &str,
) -> Result<(), Error> {
    let shown = format!("{}/{}", base.trim_end_matches('/'), rel.display());
    let fail = |e: Errno| os(e, &shown);
    let st = match statat(dir, name, false) {
        Ok(st) => st,
        // Gone since the directory was read.
        Err(Errno::NOENT) => return Ok(()),
        Err(e) => return Err(fail(e)),
    };
    let mut header = header_for(&st);
    match kind(&st) {
        FileKind::File => {
            let flags = OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::NOCTTY;
            let fd = match rustix::fs::openat(dir, name, flags | OFlags::CLOEXEC, Mode::empty()) {
                Ok(fd) => fd,
                // Removed, or swapped for a symlink, since it was looked at.
                Err(Errno::NOENT | Errno::LOOP) => return Ok(()),
                Err(e) => return Err(fail(e)),
            };
            // Describe what was opened, which may not be what was looked at a moment ago.
            let st = fstat(&fd).map_err(fail)?;
            if kind(&st) != FileKind::File {
                return Ok(());
            }
            header = header_for(&st);
            header.set_entry_type(EntryType::Regular);
            header.set_size(st.stx_size);
            let data = Exact { file: std::fs::File::from(fd), left: st.stx_size };
            tar.append_data(&mut header, rel, data).map_err(|e| io_err(&e, &shown))?;
        }
        FileKind::Dir => {
            let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
            let fd = match rustix::fs::openat(dir, name, flags, Mode::empty()) {
                Ok(fd) => fd,
                Err(Errno::NOENT | Errno::NOTDIR | Errno::LOOP) => return Ok(()),
                Err(e) => return Err(fail(e)),
            };
            header.set_entry_type(EntryType::Directory);
            tar.append_data(&mut header, rel, io::empty()).map_err(|e| io_err(&e, &shown))?;
            add_dir(tar, fd, rel, base)?;
        }
        FileKind::Symlink => {
            let target = match rustix::fs::readlinkat(dir, name, Vec::new()) {
                Ok(t) => t,
                Err(Errno::NOENT | Errno::INVAL) => return Ok(()),
                Err(e) => return Err(fail(e)),
            };
            header.set_entry_type(EntryType::Symlink);
            let target = Path::new(OsStr::from_bytes(target.as_bytes()));
            tar.append_link(&mut header, rel, target).map_err(|e| io_err(&e, &shown))?;
        }
        // Devices, pipes and sockets have no content to carry.
        _ => {}
    }
    Ok(())
}

fn header_for(st: &Statx) -> Header {
    let mut h = Header::new_gnu();
    h.set_mode(u32::from(st.stx_mode) & 0o7777);
    h.set_uid(u64::from(st.stx_uid));
    h.set_gid(u64::from(st.stx_gid));
    h.set_mtime(u64::try_from(st.stx_mtime.tv_sec).unwrap_or(0));
    h.set_size(0);
    h
}

// A file's content at exactly the size the header gave. A file that shrinks while it is read is
// padded with zeros and one that grows is cut, because anything else breaks the archive.
struct Exact {
    file: std::fs::File,
    left: u64,
}

impl Read for Exact {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let want = usize::try_from(self.left).unwrap_or(usize::MAX).min(buf.len());
        if want == 0 {
            return Ok(0);
        }
        let n = match self.file.read(&mut buf[..want])? {
            0 => {
                buf[..want].fill(0);
                want
            }
            n => n,
        };
        self.left -= n as u64;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_stay_relative_and_never_climb() {
        assert_eq!(clean(Path::new("a/b")), Some(PathBuf::from("a/b")));
        assert_eq!(clean(Path::new("/etc/passwd")), Some(PathBuf::from("etc/passwd")));
        assert_eq!(clean(Path::new("./x/./y/")), Some(PathBuf::from("x/y")));
        assert_eq!(clean(Path::new("./")), Some(PathBuf::new()));
        assert_eq!(clean(Path::new("a/../../b")), None);
        assert_eq!(clean(Path::new("..")), None);
    }
}
