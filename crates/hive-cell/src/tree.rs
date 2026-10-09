//! Copies a directory tree as it is, for a cell that starts from what another one wrote. Owners,
//! modes, times, extended attributes, hard links, symlinks and device nodes all come along, so an
//! overlay upper copied this way, whiteouts and opaque directories included, reads the same over
//! the same lowers. A file's data is shared with a reflink where the filesystem has them, as XFS
//! and btrfs do, and copied in the kernel where it does not.
//!
//! [`copy`] wants a source that holds still. [`Precopy`] copies one that is still changing and
//! then brings the copy up to date once it stops, which only redoes what changed, so the source
//! has to be stopped for a walk of the tree rather than a whole copy.

use rustix::fs::{AtFlags, CWD, FileType, Mode, Timespec, Timestamps, XattrFlags};
use rustix::io::Errno;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::fs::{self, File, Metadata, OpenOptions, Permissions};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// What [`copy`] made, or what [`Precopy::finish`] left.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Copied {
    /// Everything under the destination: files, directories, links and nodes.
    pub entries: u64,
    /// The bytes of the regular files, each counted once however many links it has.
    pub bytes: u64,
    /// The regular files whose data was shared with the source rather than copied.
    pub reflinked: u64,
}

/// The most threads one pass uses for what is not a directory.
const THREADS: usize = 4;

/// Copies what is in the directory `from` into the directory `to`, which should be empty. `to`
/// keeps its own owner and mode and gets the extended attributes of `from`. Symlinks are copied,
/// never followed. A copy that fails partway leaves what it made so far.
///
/// The directories are made first, on the calling thread, and then the files, links and nodes on
/// up to four, since for a tree of many small files the time goes on system calls, not bytes.
///
/// # Errors
///
/// Something cannot be read or made, or an owner or attribute cannot be set, which needs root for
/// files that are not the caller's and for the `trusted` attributes overlay keeps.
pub fn copy(from: &Path, to: &Path) -> io::Result<Copied> {
    let mut pass = Pass::new(Kind::Once);
    pass.run(from, to)?;
    Ok(pass.done)
}

/// A copy of a tree that may change while it is made, brought up to date by [`Precopy::finish`]
/// once the tree holds still.
#[derive(Debug)]
pub struct Precopy {
    from: PathBuf,
    to: PathBuf,
    /// What each source entry was when it was copied, by where it went.
    seen: HashMap<PathBuf, Stamp>,
    /// Entries changed since this are copied again whatever their stamp says, since a change in
    /// the same clock tick as the stamp was taken leaves the stamp as it was.
    racy: (i64, i64),
    reflinked: u64,
}

impl Precopy {
    /// Copies `from` into the empty directory `to` while `from` may still change. Entries that go
    /// away during the copy are left out, for [`Precopy::finish`] to settle.
    ///
    /// # Errors
    ///
    /// As for [`copy`], except for entries that are gone by the time they are copied.
    pub fn start(from: &Path, to: &Path) -> io::Result<Self> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        let racy = (now.as_secs() as i64 - 1, i64::from(now.subsec_nanos()));
        let mut pass = Pass::new(Kind::Live);
        pass.run(from, to)?;
        Ok(Self {
            from: from.to_path_buf(),
            to: to.to_path_buf(),
            seen: pass.seen,
            racy,
            reflinked: pass.done.reflinked,
        })
    }

    /// Brings the copy up to date with `from`, which must not change while this runs. Each entry
    /// whose inode, type or change time is not what it was when copied is copied again, and each
    /// that is gone is removed, so only a walk of the tree and what changed take time.
    ///
    /// # Errors
    ///
    /// As for [`copy`]. The `reflinked` it counts takes in both passes, so a file shared in the
    /// first and changed before this one counts twice.
    pub fn finish(self) -> io::Result<Copied> {
        let mut pass = Pass::new(Kind::Finish { seen: self.seen, racy: self.racy });
        pass.run(&self.from, &self.to)?;
        pass.done.reflinked += self.reflinked;
        Ok(pass.done)
    }
}

/// What an entry was when it was copied. An inode's change time moves on any write to it or to
/// its owner, mode, attributes or links.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Stamp {
    dev: u64,
    ino: u64,
    mode: u32,
    ctime: (i64, i64),
}

impl Stamp {
    fn of(meta: &Metadata) -> Self {
        Self {
            dev: meta.dev(),
            ino: meta.ino(),
            mode: meta.mode() & 0o170_000,
            ctime: (meta.ctime(), meta.ctime_nsec()),
        }
    }
}

#[derive(Debug)]
enum Kind {
    /// One copy of a tree that holds still.
    Once,
    /// The first copy of a tree that may change, which keeps stamps.
    Live,
    /// The pass that brings a first copy up to date.
    Finish { seen: HashMap<PathBuf, Stamp>, racy: (i64, i64) },
}

/// One walk of a source tree.
struct Pass {
    mode: Kind,
    done: Copied,
    /// Stamps of what was copied, kept by a [`Kind::Live`] pass.
    seen: HashMap<PathBuf, Stamp>,
    /// Inodes with more than one link already counted in `done.bytes`.
    counted: HashSet<(u64, u64)>,
}

impl Pass {
    fn new(mode: Kind) -> Self {
        Self { mode, done: Copied::default(), seen: HashMap::new(), counted: HashSet::new() }
    }

    fn live(&self) -> bool {
        matches!(self.mode, Kind::Live)
    }

    /// Whether the copy of what had `meta` is still good.
    fn fresh(&self, d: &Path, meta: &Metadata) -> bool {
        match &self.mode {
            Kind::Finish { seen, racy } => {
                let stamp = Stamp::of(meta);
                seen.get(d) == Some(&stamp) && stamp.ctime < *racy
            }
            _ => false,
        }
    }

    fn run(&mut self, from: &Path, to: &Path) -> io::Result<()> {
        if matches!(self.mode, Kind::Finish { .. }) {
            exact_xattrs(from, to).map_err(|e| at(from, e))?;
        } else {
            xattrs(from, to).map_err(|e| at(from, e))?;
        }
        // A directory's times change as entries are made in it, so they are set once all are made.
        let mut dirs = Vec::new();
        let mut jobs = Vec::new();
        // Each file with more than one link is copied once, and its other links made after.
        let mut firsts: HashMap<(u64, u64), PathBuf> = HashMap::new();
        let mut links = Vec::new();
        let mut stack = vec![(from.to_path_buf(), to.to_path_buf())];
        while let Some((src, dst)) = stack.pop() {
            let listing = match fs::read_dir(&src) {
                Err(e) if self.live() && e.kind() == io::ErrorKind::NotFound => continue,
                r => r.map_err(|e| at(&src, e))?,
            };
            // What the copy has that the source no longer does goes.
            let mut stale = HashSet::new();
            if matches!(self.mode, Kind::Finish { .. }) {
                for entry in fs::read_dir(&dst).map_err(|e| at(&dst, e))? {
                    stale.insert(entry.map_err(|e| at(&dst, e))?.file_name());
                }
            }
            for entry in listing {
                let entry = entry.map_err(|e| at(&src, e))?;
                let name = entry.file_name();
                let had = stale.remove(&name);
                let (s, d) = (entry.path(), dst.join(&name));
                let meta = match fs::symlink_metadata(&s) {
                    Err(e) if self.live() && e.kind() == io::ErrorKind::NotFound => continue,
                    r => r.map_err(|e| at(&s, e))?,
                };
                self.done.entries += 1;
                if self.live() {
                    self.seen.insert(d.clone(), Stamp::of(&meta));
                }
                let fresh = had && self.fresh(&d, &meta);
                if meta.is_dir() {
                    if had && !(fresh || was_dir(&self.mode, &d)) {
                        remove(&d).map_err(|e| at(&d, e))?;
                    }
                    if had && (fresh || was_dir(&self.mode, &d)) {
                        if !fresh {
                            exact_attrs(&s, &d, &meta).map_err(|e| at(&s, e))?;
                        }
                    } else {
                        fs::create_dir(&d)
                            .and_then(|()| attrs(&s, &d, &meta))
                            .map_err(|e| at(&s, e))?;
                    }
                    dirs.push((d.clone(), meta));
                    stack.push((s, d));
                    continue;
                }
                if meta.is_file() {
                    let key = (meta.dev(), meta.ino());
                    if meta.nlink() < 2 || self.counted.insert(key) {
                        self.done.bytes += meta.len();
                    }
                }
                if fresh {
                    if meta.nlink() > 1 {
                        firsts.entry((meta.dev(), meta.ino())).or_insert(d);
                    }
                    continue;
                }
                if had {
                    remove(&d).map_err(|e| at(&d, e))?;
                }
                if meta.nlink() > 1 {
                    match firsts.entry((meta.dev(), meta.ino())) {
                        Entry::Occupied(first) => {
                            links.push((first.get().clone(), d));
                            continue;
                        }
                        Entry::Vacant(v) => {
                            v.insert(d.clone());
                        }
                    }
                }
                jobs.push((s, d, meta));
            }
            for name in stale {
                let d = dst.join(name);
                remove(&d).map_err(|e| at(&d, e))?;
            }
        }
        let c = Copier {
            live: self.live(),
            next: AtomicUsize::new(0),
            reflink: AtomicBool::new(true),
            reflinked: AtomicU64::new(0),
        };
        let threads = THREADS.min(jobs.len().div_ceil(64));
        if threads > 1 {
            std::thread::scope(|scope| {
                let all: Vec<_> = (0..threads).map(|_| scope.spawn(|| c.run(&jobs))).collect();
                // The scope joins any left once one has failed.
                all.into_iter()
                    .try_for_each(|t| t.join().unwrap_or_else(|p| std::panic::resume_unwind(p)))
            })?;
        } else {
            c.run(&jobs)?;
        }
        for (first, d) in &links {
            match fs::hard_link(first, d) {
                // The first link went away while a live pass copied it.
                Err(e) if self.live() && e.kind() == io::ErrorKind::NotFound => {}
                r => r.map_err(|e| at(d, e))?,
            }
        }
        for (d, meta) in dirs.iter().rev() {
            set_times(d, meta).map_err(|e| at(d, e))?;
        }
        self.done.reflinked += c.reflinked.into_inner();
        Ok(())
    }
}

/// Whether the copy at `d` was made as a directory, so it can stay one.
fn was_dir(mode: &Kind, d: &Path) -> bool {
    match mode {
        Kind::Finish { seen, .. } => seen.get(d).is_some_and(|s| s.mode == 0o040_000),
        _ => false,
    }
}

/// Removes whatever is at `d`.
fn remove(d: &Path) -> io::Result<()> {
    match fs::symlink_metadata(d) {
        Ok(m) if m.is_dir() => fs::remove_dir_all(d),
        Ok(_) => fs::remove_file(d),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// What the threads of one pass share.
struct Copier {
    /// Whether the source may change, so that an entry gone before it is copied is skipped.
    live: bool,
    /// The next job to take.
    next: AtomicUsize,
    /// Whether to try a reflink, until the filesystem says it has none.
    reflink: AtomicBool,
    reflinked: AtomicU64,
}

impl Copier {
    /// Takes jobs until there are none left or one fails.
    fn run(&self, jobs: &[(PathBuf, PathBuf, Metadata)]) -> io::Result<()> {
        loop {
            let Some((s, d, meta)) = jobs.get(self.next.fetch_add(1, Ordering::Relaxed)) else {
                return Ok(());
            };
            match self.entry(s, d, meta) {
                Ok(()) => {}
                Err(e) if self.live && e.kind() == io::ErrorKind::NotFound => {
                    // Gone, or replaced, partway: the finishing pass sees it is not as stamped.
                    let _ = fs::remove_file(d);
                }
                Err(e) => {
                    // The others stop at their next job.
                    self.next.store(jobs.len(), Ordering::Relaxed);
                    return Err(at(s, e));
                }
            }
        }
    }

    fn entry(&self, s: &Path, d: &Path, meta: &Metadata) -> io::Result<()> {
        let kind = meta.file_type();
        if kind.is_file() {
            return self.file(s, d, meta);
        }
        if kind.is_symlink() {
            std::os::unix::fs::symlink(fs::read_link(s)?, d)?;
            std::os::unix::fs::lchown(d, Some(meta.uid()), Some(meta.gid()))?;
            xattrs(s, d)?;
        } else {
            // A device node, which is what an overlay whiteout is, a pipe or a socket.
            let mode = meta.mode();
            rustix::fs::mknodat(
                CWD,
                d,
                FileType::from_raw_mode(mode),
                Mode::from_raw_mode(mode),
                meta.rdev(),
            )?;
            attrs(s, d, meta)?;
        }
        set_times(d, meta)
    }

    fn file(&self, s: &Path, d: &Path, meta: &Metadata) -> io::Result<()> {
        let mut src = File::open(s)?;
        let mut dst = OpenOptions::new().write(true).create_new(true).mode(0o600).open(d)?;
        let mut shared = false;
        if self.reflink.load(Ordering::Relaxed) {
            match rustix::fs::ioctl_ficlone(&dst, &src) {
                Ok(()) => shared = true,
                Err(Errno::OPNOTSUPP | Errno::XDEV | Errno::INVAL) => {
                    self.reflink.store(false, Ordering::Relaxed);
                }
                Err(_) => {}
            }
        }
        if shared {
            self.reflinked.fetch_add(1, Ordering::Relaxed);
        } else {
            io::copy(&mut src, &mut dst)?;
        }
        // The owner first, since a new owner clears set-id bits and file capabilities.
        std::os::unix::fs::fchown(&dst, Some(meta.uid()), Some(meta.gid()))?;
        xattrs(s, d)?;
        dst.set_permissions(Permissions::from_mode(meta.mode() & 0o7777))?;
        rustix::fs::futimens(&dst, &times(meta))?;
        Ok(())
    }
}

/// Gives `d` the owner, extended attributes and mode of `s`, in that order.
fn attrs(s: &Path, d: &Path, meta: &Metadata) -> io::Result<()> {
    std::os::unix::fs::lchown(d, Some(meta.uid()), Some(meta.gid()))?;
    xattrs(s, d)?;
    fs::set_permissions(d, Permissions::from_mode(meta.mode() & 0o7777))
}

fn xattrs(s: &Path, d: &Path) -> io::Result<()> {
    let names = read_all(|b| rustix::fs::llistxattr(s, b))?;
    for name in names.split(|&b| b == 0).filter(|n| !n.is_empty()) {
        let name = OsStr::from_bytes(name);
        let value = match read_all(|b| rustix::fs::lgetxattr(s, name, b)) {
            Ok(v) => v,
            // Gone since it was listed.
            Err(e) if e.raw_os_error() == Some(Errno::NODATA.raw_os_error()) => continue,
            Err(e) => return Err(e),
        };
        match rustix::fs::lsetxattr(d, name, &value, XattrFlags::empty()) {
            // The destination's filesystem has no such attributes, such as the labels of a
            // security module it does not run.
            Ok(()) | Err(Errno::OPNOTSUPP) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// As [`attrs`], and also drops the extended attributes `d` has that `s` does not.
fn exact_attrs(s: &Path, d: &Path, meta: &Metadata) -> io::Result<()> {
    std::os::unix::fs::lchown(d, Some(meta.uid()), Some(meta.gid()))?;
    exact_xattrs(s, d)?;
    fs::set_permissions(d, Permissions::from_mode(meta.mode() & 0o7777))
}

/// As [`xattrs`], and also drops the extended attributes `d` has that `s` does not.
fn exact_xattrs(s: &Path, d: &Path) -> io::Result<()> {
    xattrs(s, d)?;
    let names = |p: &Path| -> io::Result<HashSet<OsString>> {
        let all = read_all(|b| rustix::fs::llistxattr(p, b))?;
        let all = all.split(|&b| b == 0).filter(|n| !n.is_empty());
        Ok(all.map(|n| OsStr::from_bytes(n).to_os_string()).collect())
    };
    let keep = names(s)?;
    for name in names(d)?.difference(&keep) {
        match rustix::fs::lremovexattr(d, name.as_os_str()) {
            Ok(()) | Err(Errno::NODATA | Errno::OPNOTSUPP) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// The whole of a list or value the kernel hands out by size, asking again if it grew.
fn read_all(mut f: impl FnMut(&mut [u8]) -> rustix::io::Result<usize>) -> io::Result<Vec<u8>> {
    loop {
        let n = f(&mut [])?;
        if n == 0 {
            return Ok(Vec::new());
        }
        let mut buf = vec![0; n];
        match f(&mut buf) {
            Ok(n) => {
                buf.truncate(n);
                return Ok(buf);
            }
            Err(Errno::RANGE) => {}
            Err(e) => return Err(e.into()),
        }
    }
}

fn times(meta: &Metadata) -> Timestamps {
    Timestamps {
        last_access: Timespec { tv_sec: meta.atime(), tv_nsec: meta.atime_nsec() },
        last_modification: Timespec { tv_sec: meta.mtime(), tv_nsec: meta.mtime_nsec() },
    }
}

fn set_times(d: &Path, meta: &Metadata) -> io::Result<()> {
    Ok(rustix::fs::utimensat(CWD, d, &times(meta), AtFlags::SYMLINK_NOFOLLOW)?)
}

fn at(path: &Path, e: io::Error) -> io::Error {
    io::Error::new(e.kind(), format!("{}: {e}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::{Precopy, copy};
    use std::fs;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    use std::path::Path;

    fn xattr(p: &Path, name: &str) -> Option<Vec<u8>> {
        let mut buf = [0u8; 64];
        rustix::fs::lgetxattr(p, name, &mut buf[..]).ok().map(|n| buf[..n].to_vec())
    }

    #[test]
    fn a_copied_tree_is_the_same_tree() {
        let root = std::env::temp_dir().join(format!("hive-tree-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let (a, b) = (root.join("a"), root.join("b"));
        fs::create_dir_all(a.join("d/e")).unwrap();
        fs::create_dir(&b).unwrap();
        fs::write(a.join("d/e/f"), vec![7u8; 100_000]).unwrap();
        fs::set_permissions(a.join("d/e/f"), fs::Permissions::from_mode(0o4751)).unwrap();
        fs::hard_link(a.join("d/e/f"), a.join("g")).unwrap();
        std::os::unix::fs::symlink("d/e/f", a.join("s")).unwrap();
        std::os::unix::fs::symlink("/nowhere", a.join("dangling")).unwrap();
        rustix::fs::mknodat(
            rustix::fs::CWD,
            a.join("pipe"),
            rustix::fs::FileType::Fifo,
            rustix::fs::Mode::from_raw_mode(0o640),
            0,
        )
        .unwrap();
        let root_user = rustix::process::geteuid().is_root();
        if root_user {
            // An overlay whiteout and an opaque directory, as overlay leaves them in an upper.
            rustix::fs::mknodat(
                rustix::fs::CWD,
                a.join("d/gone"),
                rustix::fs::FileType::CharacterDevice,
                rustix::fs::Mode::empty(),
                0,
            )
            .unwrap();
            rustix::fs::lsetxattr(
                a.join("d/e"),
                "trusted.overlay.opaque",
                b"y",
                rustix::fs::XattrFlags::empty(),
            )
            .unwrap();
            std::os::unix::fs::lchown(a.join("s"), Some(1_000_123), Some(1_000_456)).unwrap();
        }
        let user = rustix::fs::lsetxattr(
            a.join("g"),
            "user.note",
            b"kept",
            rustix::fs::XattrFlags::empty(),
        )
        .is_ok();
        let old = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_000_000);
        fs::File::options().write(true).open(a.join("d/e/f")).unwrap().set_modified(old).unwrap();
        fs::File::open(a.join("d")).unwrap().set_modified(old).unwrap();

        let done = copy(&a, &b).unwrap();
        assert_eq!(done.entries, if root_user { 8 } else { 7 });
        assert_eq!(done.bytes, 100_000);
        assert_eq!(fs::read(b.join("g")).unwrap(), vec![7u8; 100_000]);
        let (f, g) = (fs::metadata(b.join("d/e/f")).unwrap(), fs::metadata(b.join("g")).unwrap());
        assert_eq!((f.ino(), f.nlink()), (g.ino(), 2));
        assert_eq!(f.mode() & 0o7777, 0o4751);
        assert_eq!(f.mtime(), 1_000_000);
        assert_eq!(fs::metadata(b.join("d")).unwrap().mtime(), 1_000_000);
        assert_eq!(fs::read_link(b.join("s")).unwrap(), Path::new("d/e/f"));
        assert_eq!(fs::read_link(b.join("dangling")).unwrap(), Path::new("/nowhere"));
        let pipe = fs::symlink_metadata(b.join("pipe")).unwrap();
        assert_eq!(pipe.mode(), 0o10640);
        if user {
            assert_eq!(xattr(&b.join("g"), "user.note").as_deref(), Some(&b"kept"[..]));
        }
        if root_user {
            let gone = fs::symlink_metadata(b.join("d/gone")).unwrap();
            assert_eq!((gone.mode() & 0o170000, gone.rdev()), (0o020000, 0));
            assert_eq!(xattr(&b.join("d/e"), "trusted.overlay.opaque").as_deref(), Some(&b"y"[..]));
            let s = fs::symlink_metadata(b.join("s")).unwrap();
            assert_eq!((s.uid(), s.gid()), (1_000_123, 1_000_456));
        }
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_tree_of_many_files_is_copied_on_threads() {
        let root = std::env::temp_dir().join(format!("hive-tree-many-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let (a, b) = (root.join("a"), root.join("b"));
        for i in 0..600 {
            let d = a.join(format!("d{}", i % 13));
            fs::create_dir_all(&d).unwrap();
            fs::write(d.join(format!("f{i}")), i.to_string().repeat(i)).unwrap();
            if i % 10 == 0 {
                fs::hard_link(d.join(format!("f{i}")), a.join(format!("link{i}"))).unwrap();
            }
        }
        fs::create_dir(&b).unwrap();
        let done = copy(&a, &b).unwrap();
        assert_eq!(done.entries, 13 + 600 + 60);
        let want: usize = (0..600).map(|i: usize| i.to_string().len() * i).sum();
        assert_eq!(done.bytes, want as u64);
        for i in (0..600).step_by(10) {
            let f = b.join(format!("d{}", i % 13)).join(format!("f{i}"));
            let (f, l) = (fs::metadata(f).unwrap(), fs::metadata(b.join(format!("link{i}"))).unwrap());
            assert_eq!((f.ino(), f.nlink(), f.len()), (l.ino(), 2, l.len()));
        }
        assert_eq!(fs::read(b.join("d1/f599")).unwrap(), "599".repeat(599).as_bytes());
        fs::remove_dir_all(&root).unwrap();
    }

    /// Everything about a tree that a copy should keep, by path.
    fn look(root: &Path) -> std::collections::BTreeMap<std::path::PathBuf, String> {
        let mut out = std::collections::BTreeMap::new();
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            for e in fs::read_dir(&dir).unwrap() {
                let p = e.unwrap().path();
                let m = fs::symlink_metadata(&p).unwrap();
                let what = if m.is_dir() {
                    stack.push(p.clone());
                    String::new()
                } else if m.is_symlink() {
                    format!("{:?}", fs::read_link(&p).unwrap())
                } else if m.is_file() {
                    format!("{} {:?}", m.nlink(), fs::read(&p).unwrap())
                } else {
                    m.rdev().to_string()
                };
                let attr = xattr(&p, "user.note");
                let key = p.strip_prefix(root).unwrap().to_path_buf();
                let line = format!("{:o} {} {} {} {what} {attr:?}", m.mode(), m.uid(), m.gid(), m.mtime());
                out.insert(key, line);
            }
        }
        out
    }

    #[test]
    fn a_precopy_is_brought_up_to_date_once_the_tree_stops() {
        for racy in [false, true] {
            let root = std::env::temp_dir().join(format!("hive-tree-pre-{}-{racy}", std::process::id()));
            let _ = fs::remove_dir_all(&root);
            let (a, b) = (root.join("a"), root.join("b"));
            fs::create_dir_all(a.join("sub")).unwrap();
            fs::create_dir_all(a.join("old/deep")).unwrap();
            fs::create_dir(&b).unwrap();
            for f in ["keep", "change", "gone", "swap", "mode", "sub/x", "old/deep/y", "linked", "attr"] {
                fs::write(a.join(f), f).unwrap();
            }
            fs::hard_link(a.join("linked"), a.join("sub/link2")).unwrap();
            let user = rustix::fs::lsetxattr(
                a.join("attr"),
                "user.note",
                b"kept",
                rustix::fs::XattrFlags::empty(),
            )
            .is_ok();
            let old = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_000_000);
            fs::File::options().write(true).open(a.join("keep")).unwrap().set_modified(old).unwrap();

            let mut pre = Precopy::start(&a, &b).unwrap();
            let kept = fs::metadata(b.join("keep")).unwrap().ino();
            // Past the clock tick the stamps were taken in.
            std::thread::sleep(std::time::Duration::from_millis(50));
            fs::write(a.join("change"), "changed").unwrap();
            fs::remove_file(a.join("gone")).unwrap();
            fs::remove_file(a.join("swap")).unwrap();
            fs::create_dir(a.join("swap")).unwrap();
            fs::write(a.join("swap/in"), "in").unwrap();
            fs::hard_link(a.join("linked"), a.join("link3")).unwrap();
            fs::set_permissions(a.join("mode"), fs::Permissions::from_mode(0o700)).unwrap();
            fs::write(a.join("sub/new"), "new").unwrap();
            fs::remove_dir_all(a.join("old")).unwrap();
            if user {
                rustix::fs::lremovexattr(a.join("attr"), "user.note").unwrap();
            }
            if !racy {
                // As if the first pass ran long ago, so only the changes above are redone.
                pre.racy = (i64::MAX, 0);
            }
            pre.finish().unwrap();
            assert_eq!(look(&b), look(&a));
            let l = fs::metadata(b.join("link3")).unwrap();
            assert_eq!((l.ino(), l.nlink()), (fs::metadata(b.join("sub/link2")).unwrap().ino(), 3));
            // What did not change is left as the first pass made it, unless it changed too
            // recently to tell.
            assert_eq!(fs::metadata(b.join("keep")).unwrap().ino() == kept, !racy);
            fs::remove_dir_all(&root).unwrap();
        }
    }
}
