//! File operations for the node, from `spec/09_guest_agent.md`, section 3.
//!
//! The drone usually runs as root and the paths come from whoever drives the cell, so every path
//! is resolved with `openat2` and `RESOLVE_IN_ROOT` under one of the configured roots. A symlink
//! or a `..` can't lead out of the root, and neither can `/proc/self/root` style magic links.
//! Anything that changes a directory entry works on a parent directory opened that way plus a
//! final name, with the `*at` calls, so swapping a path for a symlink halfway through a call
//! can't point the drone somewhere else. The work is blocking, so it runs on tokio's blocking
//! pool.

use crate::Config;
use bytes::{Bytes, BytesMut};
use hive_proto::drone::Stream;
use hive_proto::drone::api::{
    FileInfo, FileKind, FsChmod, FsList, FsListResult, FsMkdir, FsPath, FsRead, FsRename, FsWrite,
};
use hive_rt::{OsRng, Rng};
use hive_types::{Error, Reason};
use rustix::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use rustix::fs::{
    AtFlags, Dir, FileType, Mode, OFlags, RenameFlags, ResolveFlags, Statx, StatxFlags,
};
use rustix::io::Errno;
use rustix::process::{Gid, Uid};
use std::ffi::{CStr, OsStr, OsString};
use std::io::Write;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;

/// How much the fs methods read or write at a time.
pub(crate) const CHUNK: usize = 256 * 1024;
/// The most entries one `fs.list` answer holds.
pub(crate) const MAX_ENTRIES: usize = 100_000;

/// Reads a file and streams it on `stream`. Errors that come before the first byte, like a
/// missing file, are returned without anything sent.
pub(crate) async fn read(cfg: &Config, req: FsRead, stream: &mut Stream) -> Result<(), Error> {
    let cfg = cfg.clone();
    let FsRead { path, offset, length } = req;
    let shown = path.clone();
    let file = blocking(move || open_for_read(&cfg, &shown)).await?;
    let (tx, mut rx) = mpsc::channel::<Result<Bytes, Error>>(4);
    let end = if length == 0 { u64::MAX } else { offset.saturating_add(length) };
    let mut at = offset;
    tokio::task::spawn_blocking(move || {
        while at < end {
            let want = usize::try_from(end - at).unwrap_or(usize::MAX).min(CHUNK);
            let mut buf = BytesMut::zeroed(want);
            let got = match file.read_at(&mut buf, at) {
                Ok(0) => return,
                Ok(n) => n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    let _ = tx.blocking_send(Err(io_err(&e, &path)));
                    return;
                }
            };
            buf.truncate(got);
            at += got as u64;
            // The receiver is gone when the node dropped the call.
            if tx.blocking_send(Ok(buf.freeze())).is_err() {
                return;
            }
        }
    });
    while let Some(chunk) = rx.recv().await {
        stream.send(chunk?).await?;
    }
    Ok(())
}

fn open_for_read(cfg: &Config, path: &str) -> Result<std::fs::File, Error> {
    let at = Resolved::new(cfg, path)?;
    let flags = OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOCTTY;
    let fd = at.open(&at.rel, flags, Mode::empty()).map_err(|e| os(e, path))?;
    let st = fstat(&fd).map_err(|e| os(e, path))?;
    match kind(&st) {
        FileKind::File => Ok(fd.into()),
        FileKind::Dir => Err(os(Errno::ISDIR, path)),
        _ => Err(Error::new(Reason::InvalidArgument, format!("{path} is not a regular file"))),
    }
}

/// What a blocking task that consumes a stream gets: data, then the end. A channel that closes
/// without the end means the call failed, and the task should throw its work away.
pub(crate) enum Part {
    Data(Bytes),
    End,
}

/// Writes a file from the request and whatever data follows it on `stream`.
pub(crate) async fn write(
    cfg: &Config,
    mut req: FsWrite,
    stream: &mut Stream,
) -> Result<FileInfo, Error> {
    let first = std::mem::take(&mut req.data);
    let cfg = cfg.clone();
    pipe_in(stream, move |mut rx| write_blocking(&cfg, &req, first, || rx.blocking_recv())).await
}

/// Runs `work` on the blocking pool and feeds it what arrives on `stream` until the end.
pub(crate) async fn pipe_in<T: Send + 'static>(
    stream: &mut Stream,
    work: impl FnOnce(mpsc::Receiver<Part>) -> Result<T, Error> + Send + 'static,
) -> Result<T, Error> {
    let (tx, rx) = mpsc::channel::<Part>(8);
    let worker = tokio::task::spawn_blocking(move || work(rx));
    let fed = loop {
        match stream.recv().await {
            Ok(Some(chunk)) => {
                if tx.send(Part::Data(chunk)).await.is_err() {
                    // The worker failed and says why below.
                    break Ok(());
                }
            }
            Ok(None) => {
                let _ = tx.send(Part::End).await;
                break Ok(());
            }
            // Dropping the sender without an end makes the worker throw its work away.
            Err(e) => break Err(e),
        }
    };
    drop(tx);
    let done = worker.await.map_err(|e| Error::new(Reason::Internal, e.to_string()))?;
    fed?;
    done
}

fn write_blocking(
    cfg: &Config,
    req: &FsWrite,
    first: Bytes,
    mut next: impl FnMut() -> Option<Part>,
) -> Result<FileInfo, Error> {
    let path = &req.path;
    let at = Resolved::new(cfg, path)?;
    let (parent, name) = at.split().ok_or_else(|| os(Errno::ISDIR, path))?;
    let owner = Owner::new(req.uid.or(cfg.uid), req.gid.or(cfg.gid));
    let dir = if req.make_parents {
        at.make_dirs(parent, 0o755, owner)
    } else {
        at.open(parent, OFlags::PATH | OFlags::DIRECTORY, Mode::empty())
    }
    .map_err(|e| os(e, path))?;
    let existing = match statat(&dir, name, false) {
        Ok(st) => Some(st),
        Err(Errno::NOENT) => None,
        Err(e) => return Err(os(e, path)),
    };
    let existing_kind = existing.as_ref().map(kind);
    if existing_kind == Some(FileKind::Dir) {
        return Err(os(Errno::ISDIR, path));
    }
    let mut feed = |file: &mut std::fs::File| -> Result<(), Error> {
        let fail = |e: std::io::Error| io_err(&e, path);
        file.write_all(&first).map_err(fail)?;
        loop {
            match next() {
                Some(Part::Data(b)) => file.write_all(&b).map_err(fail)?,
                Some(Part::End) => return Ok(()),
                None => return Err(Error::new(Reason::Internal, "the write was cut off")),
            }
        }
    };

    if req.append || existing_kind == Some(FileKind::Symlink) {
        let mode = if req.mode == 0 { 0o644 } else { req.mode & 0o7777 };
        let how = if req.append { OFlags::APPEND } else { OFlags::TRUNC };
        let flags = OFlags::WRONLY | OFlags::CREATE | OFlags::NOCTTY | how;
        let fd = at.open(&at.rel, flags, Mode::from_raw_mode(mode)).map_err(|e| os(e, path))?;
        if existing.is_none() {
            owner.apply(&fd).map_err(|e| os(e, path))?;
        }
        let mut file = std::fs::File::from(fd);
        feed(&mut file)?;
        // A write by anyone but root clears setuid, so the bits go on last.
        if existing.is_none() || req.mode != 0 {
            rustix::fs::fchmod(&file, Mode::from_raw_mode(mode)).map_err(|e| os(e, path))?;
        }
        return fstat(&file)
            .map(|st| info(path.clone(), &st, String::new()))
            .map_err(|e| os(e, path));
    }

    // Everything else goes to a new file that replaces the old one at the end.
    let tmp = temp_name(name);
    let flags = OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC;
    let fd = rustix::fs::openat(&dir, tmp.as_os_str(), flags, Mode::from_raw_mode(0o600))
        .map_err(|e| os(e, path))?;
    let mut guard = Unlink { dir: &dir, name: Some(tmp.as_os_str()) };
    let (owner, mode) = match &existing {
        Some(st) => (
            Owner::new(Some(st.stx_uid), Some(st.stx_gid)),
            if req.mode == 0 { u32::from(st.stx_mode) & 0o7777 } else { req.mode & 0o7777 },
        ),
        None => (owner, if req.mode == 0 { 0o644 } else { req.mode & 0o7777 }),
    };
    owner.apply(&fd).map_err(|e| os(e, path))?;
    let mut file = std::fs::File::from(fd);
    feed(&mut file)?;
    // Changing the owner clears setuid and setgid, and so does a write by anyone but root, so
    // the bits go on last.
    rustix::fs::fchmod(&file, Mode::from_raw_mode(mode)).map_err(|e| os(e, path))?;
    rustix::fs::renameat(&dir, tmp.as_os_str(), &dir, name).map_err(|e| os(e, path))?;
    guard.name = None;
    fstat(&file).map(|st| info(path.clone(), &st, String::new())).map_err(|e| os(e, path))
}

// Removes a half written temporary file unless the write got as far as renaming it.
struct Unlink<'a> {
    dir: &'a OwnedFd,
    name: Option<&'a OsStr>,
}

impl Drop for Unlink<'_> {
    fn drop(&mut self) {
        if let Some(name) = self.name {
            let _ = rustix::fs::unlinkat(self.dir, name, AtFlags::empty());
        }
    }
}

fn temp_name(name: &OsStr) -> OsString {
    // Keep it under NAME_MAX with room for the suffix. Cutting at a byte count can split a UTF-8
    // character, which Linux does not mind.
    let bytes = name.as_bytes();
    let mut out = b".".to_vec();
    out.extend_from_slice(&bytes[..bytes.len().min(200)]);
    let r = OsRng.secret();
    let n = u64::from_le_bytes([r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7]]);
    out.extend_from_slice(format!(".hive-{n:016x}").as_bytes());
    OsString::from_vec(out)
}

/// Describes a path.
pub(crate) async fn stat(cfg: &Config, req: FsPath) -> Result<FileInfo, Error> {
    let cfg = cfg.clone();
    blocking(move || {
        let path = &req.path;
        let at = Resolved::new(&cfg, path)?;
        match at.split() {
            Some((parent, name)) if !req.follow => {
                let dir = at
                    .open(parent, OFlags::PATH | OFlags::DIRECTORY, Mode::empty())
                    .map_err(|e| os(e, path))?;
                describe(&dir, name, path.clone()).map_err(|e| os(e, path))
            }
            _ => {
                let fd = at.open(&at.rel, OFlags::PATH, Mode::empty()).map_err(|e| os(e, path))?;
                fstat(&fd).map(|st| info(path.clone(), &st, String::new())).map_err(|e| os(e, path))
            }
        }
    })
    .await
}

/// Lists a directory, going `depth` levels down.
pub(crate) async fn list(cfg: &Config, req: FsList) -> Result<FsListResult, Error> {
    let cfg = cfg.clone();
    blocking(move || {
        let path = &req.path;
        let at = Resolved::new(&cfg, path)?;
        let dir = at
            .open(&at.rel, OFlags::RDONLY | OFlags::DIRECTORY, Mode::empty())
            .map_err(|e| os(e, path))?;
        let mut out = Vec::new();
        let truncated = walk(dir, path, req.depth.max(1), &mut out).map_err(|e| os(e, path))?;
        out.sort_unstable_by(|a, b| a.path.cmp(&b.path));
        Ok(FsListResult { entries: out, truncated })
    })
    .await
}

// Adds the entries under `dir` to `out`. True when it stopped at MAX_ENTRIES.
fn walk(dir: OwnedFd, base: &str, depth: u32, out: &mut Vec<FileInfo>) -> Result<bool, Errno> {
    let mut entries = Dir::new(dir)?;
    let mut subdirs = Vec::new();
    while let Some(entry) = entries.read() {
        let entry = entry?;
        let name = entry.file_name();
        if name == c"." || name == c".." {
            continue;
        }
        if out.len() >= MAX_ENTRIES {
            return Ok(true);
        }
        let path = join(base, name);
        let fd = entries.fd()?;
        let found = match describe(fd, OsStr::from_bytes(name.to_bytes()), path) {
            Ok(i) => i,
            // Gone between reading the directory and looking at the entry.
            Err(Errno::NOENT) => continue,
            Err(e) => return Err(e),
        };
        if depth > 1 && found.kind == FileKind::Dir as i32 {
            subdirs.push((name.to_owned(), found.path.clone()));
        }
        out.push(found);
    }
    for (name, path) in subdirs {
        let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        match rustix::fs::openat(entries.fd()?, &name, flags, Mode::empty()) {
            Ok(sub) => {
                if walk(sub, &path, depth - 1, out)? {
                    return Ok(true);
                }
            }
            // Removed, swapped for something else, or closed to us since it was listed.
            Err(Errno::NOENT | Errno::NOTDIR | Errno::LOOP | Errno::ACCESS) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(false)
}

pub(crate) fn join(base: &str, name: &CStr) -> String {
    let name = name.to_string_lossy();
    if base.ends_with('/') { format!("{base}{name}") } else { format!("{base}/{name}") }
}

/// Makes a directory.
pub(crate) async fn mkdir(cfg: &Config, req: FsMkdir) -> Result<FileInfo, Error> {
    let cfg = cfg.clone();
    blocking(move || {
        let path = &req.path;
        let at = Resolved::new(&cfg, path)?;
        let mode = if req.mode == 0 { 0o755 } else { req.mode & 0o7777 };
        let owner = Owner::new(req.uid.or(cfg.uid), req.gid.or(cfg.gid));
        let fd = if req.parents {
            at.make_dirs(&at.rel, mode, owner).map_err(|e| os(e, path))?
        } else {
            let (parent, name) = at.split().ok_or_else(|| os(Errno::EXIST, path))?;
            let dir = at
                .open(parent, OFlags::PATH | OFlags::DIRECTORY, Mode::empty())
                .map_err(|e| os(e, path))?;
            make_dir(&dir, name, mode, owner).map_err(|e| os(e, path))?
        };
        fstat(&fd).map(|st| info(path.clone(), &st, String::new())).map_err(|e| os(e, path))
    })
    .await
}

// Makes one directory with exactly `mode`, whatever the umask, and returns it opened.
pub(crate) fn make_dir(
    dir: &OwnedFd,
    name: &OsStr,
    mode: u32,
    owner: Owner,
) -> Result<OwnedFd, Errno> {
    rustix::fs::mkdirat(dir, name, Mode::from_raw_mode(mode))?;
    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let fd = rustix::fs::openat(dir, name, flags, Mode::empty())?;
    owner.apply(&fd)?;
    rustix::fs::fchmod(&fd, Mode::from_raw_mode(mode))?;
    Ok(fd)
}

/// Removes a file, a symlink, an empty directory, or with `recursive` any directory.
pub(crate) async fn remove(cfg: &Config, req: FsPath) -> Result<(), Error> {
    let cfg = cfg.clone();
    blocking(move || {
        let path = &req.path;
        let at = Resolved::new(&cfg, path)?;
        let Some((parent, name)) = at.split() else {
            return Err(Error::new(Reason::InvalidArgument, format!("{path} is a root")));
        };
        let dir = at
            .open(parent, OFlags::PATH | OFlags::DIRECTORY, Mode::empty())
            .map_err(|e| os(e, path))?;
        let st = statat(&dir, name, false).map_err(|e| os(e, path))?;
        let done = match (kind(&st), req.recursive) {
            (FileKind::Dir, true) => remove_tree(dir.as_fd(), name),
            (FileKind::Dir, false) => rustix::fs::unlinkat(&dir, name, AtFlags::REMOVEDIR),
            _ => rustix::fs::unlinkat(&dir, name, AtFlags::empty()),
        };
        done.map_err(|e| os(e, path))
    })
    .await
}

// Removes the directory `name` in `dir` and everything under it, without following symlinks.
fn remove_tree(dir: BorrowedFd<'_>, name: &OsStr) -> Result<(), Errno> {
    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let fd = rustix::fs::openat(dir, name, flags, Mode::empty())?;
    let mut entries = Dir::new(fd)?;
    // Read the names first, since removing entries while reading can skip some.
    let mut names = Vec::new();
    while let Some(entry) = entries.read() {
        let entry = entry?;
        let n = entry.file_name();
        if n != c"." && n != c".." {
            names.push((n.to_owned(), entry.file_type()));
        }
    }
    let here = entries.fd()?;
    for (n, t) in names {
        let n = OsStr::from_bytes(n.to_bytes());
        let t = if t == FileType::Unknown {
            match statat(here, n, false) {
                Ok(st) => FileType::from_raw_mode(st.stx_mode.into()),
                Err(Errno::NOENT) => continue,
                Err(e) => return Err(e),
            }
        } else {
            t
        };
        let done = if t == FileType::Directory {
            remove_tree(here, n)
        } else {
            rustix::fs::unlinkat(here, n, AtFlags::empty())
        };
        match done {
            Ok(()) | Err(Errno::NOENT) => {}
            Err(e) => return Err(e),
        }
    }
    match rustix::fs::unlinkat(dir, name, AtFlags::REMOVEDIR) {
        Ok(()) | Err(Errno::NOENT) => Ok(()),
        Err(e) => Err(e),
    }
}

/// Moves a path.
pub(crate) async fn rename(cfg: &Config, req: FsRename) -> Result<FileInfo, Error> {
    let cfg = cfg.clone();
    blocking(move || {
        let from = Resolved::new(&cfg, &req.from)?;
        let to = Resolved::new(&cfg, &req.to)?;
        let (fp, fname) = from.split().ok_or_else(|| os(Errno::BUSY, &req.from))?;
        let (tp, tname) = to.split().ok_or_else(|| os(Errno::BUSY, &req.to))?;
        let dirs = OFlags::PATH | OFlags::DIRECTORY;
        let fdir = from.open(fp, dirs, Mode::empty()).map_err(|e| os(e, &req.from))?;
        let tdir = to.open(tp, dirs, Mode::empty()).map_err(|e| os(e, &req.to))?;
        let flags = if req.overwrite { RenameFlags::empty() } else { RenameFlags::NOREPLACE };
        rustix::fs::renameat_with(&fdir, fname, &tdir, tname, flags).map_err(|e| {
            let which = if e == Errno::EXIST { &req.to } else { &req.from };
            os(e, which)
        })?;
        describe(&tdir, tname, req.to.clone()).map_err(|e| os(e, &req.to))
    })
    .await
}

/// Changes permission bits.
pub(crate) async fn chmod(cfg: &Config, req: FsChmod) -> Result<FileInfo, Error> {
    let cfg = cfg.clone();
    blocking(move || {
        let path = &req.path;
        let at = Resolved::new(&cfg, path)?;
        let fd = at.open(&at.rel, OFlags::PATH, Mode::empty()).map_err(|e| os(e, path))?;
        // An O_PATH descriptor can't be passed to fchmod, and opening the file for real could
        // block on a pipe or wake a device. Going through the descriptor's own entry in /proc is
        // how the C library does it, and it can't be raced, since it names this open file.
        let own = format!("/proc/self/fd/{}", fd.as_raw_fd());
        match rustix::fs::chmod(&own, Mode::from_raw_mode(req.mode & 0o7777)) {
            Ok(()) => {}
            Err(Errno::NOENT) if !Path::new("/proc/self/fd").exists() => {
                return Err(Error::new(Reason::Internal, "fs.chmod needs /proc to be mounted"));
            }
            Err(e) => return Err(os(e, path)),
        }
        fstat(&fd).map(|st| info(path.clone(), &st, String::new())).map_err(|e| os(e, path))
    })
    .await
}

/// A path split into the root it falls under and the rest.
pub(crate) struct Resolved {
    pub(crate) root: OwnedFd,
    /// Relative to the root, and empty for the root itself.
    pub(crate) rel: PathBuf,
}

impl Resolved {
    pub(crate) fn new(cfg: &Config, path: &str) -> Result<Self, Error> {
        if path.is_empty() {
            return Err(Error::new(Reason::InvalidArgument, "an empty path"));
        }
        let full = cfg.workdir.join(path);
        let root = cfg
            .roots
            .iter()
            .filter(|r| full.starts_with(r))
            .max_by_key(|r| r.components().count())
            .ok_or_else(|| {
                Error::new(Reason::PolicyDenied, format!("{path} is outside the drone's roots"))
            })?;
        let rel = full.strip_prefix(root).unwrap_or(Path::new("")).to_path_buf();
        let flags = OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC;
        let host = match &cfg.base {
            Some(base) => base.join(root.strip_prefix("/").unwrap_or(root)),
            None => root.clone(),
        };
        let root = rustix::fs::open(&host, flags, Mode::empty()).map_err(|e| {
            Error::new(Reason::Internal, format!("the root {}: {}", root.display(), e))
        })?;
        Ok(Self { root, rel })
    }

    /// The parent directory and the final name, or None for the root itself.
    pub(crate) fn split(&self) -> Option<(&Path, &OsStr)> {
        Some((self.rel.parent()?, self.rel.file_name()?))
    }

    pub(crate) fn open(&self, rel: &Path, flags: OFlags, mode: Mode) -> Result<OwnedFd, Errno> {
        open_beneath(self.root.as_fd(), rel, flags, mode)
    }

    // Opens `rel` as a directory, making it and any missing parents first.
    pub(crate) fn make_dirs(&self, rel: &Path, mode: u32, owner: Owner) -> Result<OwnedFd, Errno> {
        make_dirs_beneath(self.root.as_fd(), rel, mode, owner)
    }
}

/// Opens `rel` as if `root` were `/`, so no symlink or `..` in it leads out of `root`.
pub(crate) fn open_beneath(
    root: BorrowedFd<'_>,
    rel: &Path,
    flags: OFlags,
    mode: Mode,
) -> Result<OwnedFd, Errno> {
    let rel = if rel.as_os_str().is_empty() { Path::new(".") } else { rel };
    let how = ResolveFlags::IN_ROOT | ResolveFlags::NO_MAGICLINKS;
    let mut tries = 0;
    loop {
        match rustix::fs::openat2(root, rel, flags | OFlags::CLOEXEC, mode, how) {
            // The kernel gives up when a rename races the walk. Trying again is what it asks.
            Err(Errno::AGAIN) if tries < 16 => tries += 1,
            other => return other,
        }
    }
}

/// Opens `rel` under `root` as a directory, making it and any missing parents first.
pub(crate) fn make_dirs_beneath(
    root: BorrowedFd<'_>,
    rel: &Path,
    mode: u32,
    owner: Owner,
) -> Result<OwnedFd, Errno> {
    let dirs = OFlags::PATH | OFlags::DIRECTORY;
    match open_beneath(root, rel, dirs, Mode::empty()) {
        Err(Errno::NOENT) => {}
        other => return other,
    }
    let (Some(parent), Some(name)) = (rel.parent(), rel.file_name()) else {
        return Err(Errno::NOENT);
    };
    let up = make_dirs_beneath(root, parent, mode, owner)?;
    match make_dir(&up, name, mode, owner) {
        Ok(_) | Err(Errno::EXIST) => open_beneath(root, rel, dirs, Mode::empty()),
        Err(e) => Err(e),
    }
}

/// Who new files and directories belong to. Only the ids that differ from the drone's own are
/// set, so a drone that isn't root can still create files.
#[derive(Clone, Copy)]
pub(crate) struct Owner {
    uid: Option<Uid>,
    gid: Option<Gid>,
}

impl Owner {
    pub(crate) fn new(uid: Option<u32>, gid: Option<u32>) -> Self {
        let uid = uid.map(Uid::from_raw).filter(|&u| u != rustix::process::geteuid());
        let gid = gid.map(Gid::from_raw).filter(|&g| g != rustix::process::getegid());
        Self { uid, gid }
    }

    pub(crate) fn apply(self, fd: impl AsFd) -> Result<(), Errno> {
        if self.uid.is_none() && self.gid.is_none() {
            return Ok(());
        }
        rustix::fs::fchown(fd, self.uid, self.gid)
    }

    /// Sets the owner of the symlink `name` in `dir`, not of what it points to.
    pub(crate) fn apply_to_link(self, dir: impl AsFd, name: &OsStr) -> Result<(), Errno> {
        if self.uid.is_none() && self.gid.is_none() {
            return Ok(());
        }
        rustix::fs::chownat(dir, name, self.uid, self.gid, AtFlags::SYMLINK_NOFOLLOW)
    }
}

fn describe(dir: impl AsFd, name: impl AsRef<OsStr>, path: String) -> Result<FileInfo, Errno> {
    let name = name.as_ref();
    let st = statat(&dir, name, false)?;
    let target = if kind(&st) == FileKind::Symlink {
        let t = rustix::fs::readlinkat(&dir, name, Vec::new())?;
        t.to_string_lossy().into_owned()
    } else {
        String::new()
    };
    Ok(info(path, &st, target))
}

pub(crate) fn statat(
    dir: impl AsFd,
    name: impl AsRef<OsStr>,
    follow: bool,
) -> Result<Statx, Errno> {
    let flags = if follow { AtFlags::empty() } else { AtFlags::SYMLINK_NOFOLLOW };
    rustix::fs::statx(dir, name.as_ref(), flags, StatxFlags::BASIC_STATS)
}

pub(crate) fn fstat(fd: impl AsFd) -> Result<Statx, Errno> {
    rustix::fs::statx(fd, c"", AtFlags::EMPTY_PATH, StatxFlags::BASIC_STATS)
}

pub(crate) fn kind(st: &Statx) -> FileKind {
    match FileType::from_raw_mode(st.stx_mode.into()) {
        FileType::RegularFile => FileKind::File,
        FileType::Directory => FileKind::Dir,
        FileType::Symlink => FileKind::Symlink,
        _ => FileKind::Other,
    }
}

pub(crate) fn info(path: String, st: &Statx, symlink_target: String) -> FileInfo {
    let t = st.stx_mtime;
    FileInfo {
        path,
        kind: kind(st) as i32,
        size: st.stx_size,
        mode: u32::from(st.stx_mode) & 0o7777,
        uid: st.stx_uid,
        gid: st.stx_gid,
        modified_unix_nanos: t
            .tv_sec
            .saturating_mul(1_000_000_000)
            .saturating_add(i64::from(t.tv_nsec)),
        symlink_target,
    }
}

pub(crate) async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, Error> + Send + 'static,
) -> Result<T, Error> {
    tokio::task::spawn_blocking(f).await.map_err(|e| Error::new(Reason::Internal, e.to_string()))?
}

pub(crate) fn io_err(e: &std::io::Error, path: &str) -> Error {
    os(Errno::from_io_error(e).unwrap_or(Errno::IO), path)
}

/// A failed call as a `FILE_ERROR` with the errno's name.
pub(crate) fn os(e: Errno, path: &str) -> Error {
    if e == Errno::NOSYS {
        return Error::new(Reason::Internal, "the kernel has no openat2, which needs Linux 5.6");
    }
    Error::file(errno_name(e), format!("{path}: {}", std::io::Error::from(e)))
}

fn errno_name(e: Errno) -> String {
    let name = match e {
        Errno::NOENT => "ENOENT",
        Errno::EXIST => "EEXIST",
        Errno::NOTDIR => "ENOTDIR",
        Errno::ISDIR => "EISDIR",
        Errno::ACCESS => "EACCES",
        Errno::PERM => "EPERM",
        Errno::NOTEMPTY => "ENOTEMPTY",
        Errno::NOSPC => "ENOSPC",
        Errno::DQUOT => "EDQUOT",
        Errno::ROFS => "EROFS",
        Errno::LOOP => "ELOOP",
        Errno::XDEV => "EXDEV",
        Errno::NAMETOOLONG => "ENAMETOOLONG",
        Errno::INVAL => "EINVAL",
        Errno::BUSY => "EBUSY",
        Errno::FBIG => "EFBIG",
        Errno::TXTBSY => "ETXTBSY",
        Errno::MLINK => "EMLINK",
        Errno::MFILE => "EMFILE",
        Errno::NFILE => "ENFILE",
        Errno::IO => "EIO",
        Errno::NOTSUP => "EOPNOTSUPP",
        other => return format!("E{}", other.raw_os_error()),
    };
    name.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temp_names_are_hidden_and_distinct() {
        let a = temp_name(OsStr::new("x.txt"));
        let b = temp_name(OsStr::new("x.txt"));
        assert_ne!(a, b);
        assert!(a.to_string_lossy().starts_with(".x.txt.hive-"));
        let long = "n".repeat(255);
        assert!(temp_name(OsStr::new(&long)).len() <= 255);
    }

    #[test]
    fn joins_keep_one_slash() {
        assert_eq!(join("/", c"etc"), "/etc");
        assert_eq!(join("/etc", c"hosts"), "/etc/hosts");
        assert_eq!(join("src/", c"main.rs"), "src/main.rs");
    }

    #[test]
    fn errno_names_are_the_linux_ones() {
        assert_eq!(errno_name(Errno::NOENT), "ENOENT");
        assert_eq!(errno_name(Errno::NOTEMPTY), "ENOTEMPTY");
        assert_eq!(errno_name(Errno::from_raw_os_error(133)), "E133");
    }
}
