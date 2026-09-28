//! `fs.watch`: changes under a directory, from inotify.
//!
//! The directory is opened under the drone's roots like any other path, and the watch is added
//! through the opened descriptor's entry in `/proc/self/fd`, so it lands on that directory and
//! not on whatever the path names by the time the kernel looks. A recursive watch adds every
//! directory below it, and adds new ones as they appear. Whatever was made in a new directory
//! before its watch was in place is reported as created, so nothing made quickly is missed.
//!
//! The kernel's queue is finite. When it overflows the node gets an overflow event, since
//! anything may have changed after that and the node should look again.

use crate::Config;
use crate::fs::{Resolved, blocking, fstat, info, open_beneath, os, statat};
use crate::server::LINGER;
use bytes::Bytes;
use hive_proto::drone::api::{FsEvent, FsEventKind, FsEvents, FsPath};
use hive_proto::drone::frame::MAX_PAYLOAD;
use hive_proto::drone::msg::Status;
use hive_proto::drone::{RecvHalf, SendHalf, Stream};
use hive_types::{Error, Reason};
use prost::Message;
use rustix::fd::{AsFd, AsRawFd, OwnedFd};
use rustix::fs::inotify::{self, CreateFlags, ReadFlags, WatchFlags};
use rustix::fs::{Dir, FileType, Mode, OFlags};
use rustix::io::Errno;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::mem::MaybeUninit;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::Interest;
use tokio::io::unix::AsyncFd;

/// What a watch listens for on each directory.
const MASK: WatchFlags = WatchFlags::CREATE
    .union(WatchFlags::MODIFY)
    .union(WatchFlags::ATTRIB)
    .union(WatchFlags::DELETE)
    .union(WatchFlags::MOVED_FROM)
    .union(WatchFlags::MOVED_TO)
    .union(WatchFlags::DELETE_SELF)
    .union(WatchFlags::MOVE_SELF)
    .union(WatchFlags::ONLYDIR);

/// How much is read from inotify at a time. Each event is 16 bytes and a name.
const READ_BUF: usize = 64 * 1024;

/// Serves one `fs.watch` call until the node ends it or the directory goes away.
pub(crate) async fn run(cfg: &Config, req: FsPath, stream: Stream) {
    let cfg = cfg.clone();
    let (mut tx, mut rx) = stream.split();
    let started = blocking(move || Watch::new(&cfg, req)).await;
    let (mut watch, first) = match started {
        Ok(w) => w,
        Err(e) => return tx.reset(Status::from(e)).await,
    };
    let ino = match AsyncFd::with_interest(watch.ino.clone(), Interest::READABLE) {
        Ok(fd) => fd,
        Err(e) => {
            let e = Error::new(Reason::Internal, format!("inotify: {e}"));
            return tx.reset(Status::from(e)).await;
        }
    };
    if tx.send(first).await.is_err() {
        return;
    }
    let mut buf = vec![MaybeUninit::<u8>::uninit(); READ_BUF];
    loop {
        tokio::select! {
            ready = ino.readable() => {
                let mut guard = match ready {
                    Ok(g) => g,
                    Err(e) => return fail(tx, e).await,
                };
                let mut raw = Vec::new();
                // An Err here is the WouldBlock that ends a drain, which also clears readiness.
                if let Ok(Err(e)) = guard.try_io(|fd| drain(fd.get_ref(), &mut buf, &mut raw)) {
                    return fail(tx, e).await;
                }
                drop(guard);
                let events = if watch.needs_blocking(&raw) {
                    let moved = blocking(move || {
                        let events = watch.handle(raw);
                        Ok((watch, events))
                    })
                    .await;
                    match moved {
                        Ok((w, events)) => {
                            watch = w;
                            events
                        }
                        Err(e) => return tx.reset(Status::from(e)).await,
                    }
                } else {
                    watch.handle(raw)
                };
                for frame in frames(events) {
                    if tx.send(frame).await.is_err() {
                        return;
                    }
                }
                if watch.gone() {
                    break;
                }
            }
            got = rx.recv() => match got {
                // The node sends nothing on a watch but its end.
                Ok(Some(_)) => {}
                Ok(None) => {
                    let _ = tx.finish().await;
                    return;
                }
                Err(_) => return,
            },
        }
    }
    if tx.finish().await.is_ok() {
        linger(rx).await;
    }
}

async fn fail(tx: SendHalf, e: std::io::Error) {
    let e = Error::new(Reason::Internal, format!("inotify: {e}"));
    tx.reset(Status::from(e)).await;
}

async fn linger(mut rx: RecvHalf) {
    let _ =
        tokio::time::timeout(LINGER, async { while let Ok(Some(_)) = rx.recv().await {} }).await;
}

// One event as read, before it is turned into what the node sees.
struct Raw {
    wd: i32,
    mask: ReadFlags,
    name: Option<Vec<u8>>,
}

// Reads every event that is waiting. Ends with the WouldBlock that clears the readiness.
fn drain(
    fd: &Arc<OwnedFd>,
    buf: &mut [MaybeUninit<u8>],
    out: &mut Vec<Raw>,
) -> std::io::Result<()> {
    let mut reader = inotify::Reader::new(fd.as_fd(), buf);
    loop {
        match reader.next() {
            Ok(ev) => out.push(Raw {
                wd: ev.wd(),
                mask: ev.events(),
                name: ev.file_name().map(|n| n.to_bytes().to_vec()),
            }),
            Err(Errno::AGAIN) => return Err(std::io::ErrorKind::WouldBlock.into()),
            Err(Errno::INTR) => {}
            Err(e) => return Err(e.into()),
        }
    }
}

struct Watch {
    ino: Arc<OwnedFd>,
    // The root the watched directory is under, and where it is in it. Holding the directory
    // itself open would keep the kernel from ever reporting it deleted.
    root: OwnedFd,
    top: PathBuf,
    base: String,
    recursive: bool,
    // Each watched directory, relative to the top.
    dirs: HashMap<i32, PathBuf>,
    // The top went away, so nothing more will come.
    ended: bool,
}

impl Watch {
    fn new(cfg: &Config, req: FsPath) -> Result<(Self, Bytes), Error> {
        let path = &req.path;
        let at = Resolved::new(cfg, path)?;
        let st = at
            .open(&at.rel, OFlags::PATH | OFlags::DIRECTORY, Mode::empty())
            .and_then(fstat)
            .map_err(|e| os(e, path))?;
        let ino =
            inotify::init(CreateFlags::NONBLOCK | CreateFlags::CLOEXEC).map_err(|e| os(e, path))?;
        let mut watch = Self {
            ino: Arc::new(ino),
            root: at.root,
            top: at.rel,
            base: req.path.clone(),
            recursive: req.recursive,
            dirs: HashMap::new(),
            ended: false,
        };
        let mut ignored = Vec::new();
        watch.add(Path::new(""), false, &mut ignored).map_err(|e| os(e, path))?;
        let first = info(req.path, &st, String::new()).encode_to_vec().into();
        Ok((watch, first))
    }

    // Watches `rel` and, for a recursive watch, every directory under it. With `report`, what
    // is found under it goes into `events` as created.
    fn add(&mut self, rel: &Path, report: bool, events: &mut Vec<FsEvent>) -> Result<(), Errno> {
        let mut todo = vec![rel.to_path_buf()];
        let mut first = true;
        while let Some(rel) = todo.pop() {
            match self.add_one(&rel, report, events) {
                Ok(subdirs) => todo.extend(subdirs),
                // The top must work, and what is below it may vanish while it is walked.
                Err(e) if first => return Err(e),
                Err(Errno::NOENT | Errno::NOTDIR | Errno::LOOP | Errno::ACCESS) => {}
                Err(Errno::NOSPC) => {
                    // Out of watches, from fs.inotify.max_user_watches.
                    events.push(overflow());
                    return Ok(());
                }
                Err(e) => return Err(e),
            }
            first = false;
        }
        Ok(())
    }

    fn add_one(
        &mut self,
        rel: &Path,
        report: bool,
        events: &mut Vec<FsEvent>,
    ) -> Result<Vec<PathBuf>, Errno> {
        let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW;
        let fd = open_beneath(self.root.as_fd(), &self.top.join(rel), flags, Mode::empty())?;
        let own = format!("/proc/self/fd/{}", fd.as_raw_fd());
        let wd = inotify::add_watch(&*self.ino, own, MASK)?;
        self.dirs.insert(wd, rel.to_path_buf());
        if !self.recursive && !report {
            return Ok(Vec::new());
        }
        let mut subdirs = Vec::new();
        let mut entries = Dir::new(fd)?;
        while let Some(entry) = entries.read() {
            let entry = entry?;
            let name = entry.file_name();
            if name == c"." || name == c".." {
                continue;
            }
            let name = OsStr::from_bytes(name.to_bytes());
            let is_dir = match entry.file_type() {
                FileType::Directory => true,
                FileType::Unknown => statat(entries.fd()?, name, false)
                    .is_ok_and(|st| FileType::from_raw_mode(st.stx_mode.into()).is_dir()),
                _ => false,
            };
            let below = rel.join(name);
            if report {
                events.push(self.event(FsEventKind::Create, &below, is_dir));
            }
            if is_dir && self.recursive {
                subdirs.push(below);
            }
        }
        Ok(subdirs)
    }

    // Whether handling `raw` may walk new directories, which is too slow for the runtime.
    fn needs_blocking(&self, raw: &[Raw]) -> bool {
        self.recursive
            && raw.iter().any(|r| {
                r.mask.contains(ReadFlags::ISDIR)
                    && r.mask.intersects(ReadFlags::CREATE | ReadFlags::MOVED_TO)
            })
    }

    fn handle(&mut self, raw: Vec<Raw>) -> Vec<FsEvent> {
        let mut events = Vec::new();
        for r in raw {
            if r.mask.contains(ReadFlags::QUEUE_OVERFLOW) {
                events.push(overflow());
                continue;
            }
            if r.mask.contains(ReadFlags::IGNORED) {
                if self.dirs.remove(&r.wd).is_some_and(|rel| rel.as_os_str().is_empty()) {
                    self.ended = true;
                }
                continue;
            }
            let Some(dir) = self.dirs.get(&r.wd) else {
                // A watch dropped since, whose last events were still queued.
                continue;
            };
            let is_dir = r.mask.contains(ReadFlags::ISDIR);
            let rel = match &r.name {
                Some(n) => dir.join(OsStr::from_bytes(n)),
                None => dir.clone(),
            };
            let top = rel.as_os_str().is_empty();
            let m = r.mask;
            let kind = if m.intersects(ReadFlags::CREATE | ReadFlags::MOVED_TO) {
                FsEventKind::Create
            } else if m.contains(ReadFlags::MODIFY) {
                FsEventKind::Write
            } else if m.contains(ReadFlags::DELETE) {
                FsEventKind::Remove
            } else if m.contains(ReadFlags::MOVED_FROM) {
                FsEventKind::Rename
            } else if m.contains(ReadFlags::ATTRIB) {
                FsEventKind::Chmod
            } else if m.contains(ReadFlags::DELETE_SELF) && top {
                self.ended = true;
                FsEventKind::Remove
            } else if m.contains(ReadFlags::MOVE_SELF) && top {
                // Paths under it no longer mean anything, so the watch ends here.
                self.ended = true;
                FsEventKind::Rename
            } else {
                // A directory below the top going away, which its parent already reported.
                continue;
            };
            events.push(self.event(kind, &rel, is_dir || top));
            if !self.recursive || !is_dir {
                continue;
            }
            if kind == FsEventKind::Create {
                // Only a new directory can have things in it that were made before its watch.
                let report = m.contains(ReadFlags::CREATE);
                if self.add(&rel, report, &mut events).is_err() {
                    // It couldn't be watched, so changes in it may be missed.
                    events.push(overflow());
                }
            } else if kind == FsEventKind::Rename {
                self.forget(&rel);
            }
        }
        dedup(events)
    }

    // Drops the watches on `rel` and below, when a directory moves away. If it moved somewhere
    // else in the tree, it gets new watches when it arrives there.
    fn forget(&mut self, rel: &Path) {
        let gone: Vec<i32> =
            self.dirs.iter().filter(|(_, d)| d.starts_with(rel)).map(|(&wd, _)| wd).collect();
        for wd in gone {
            self.dirs.remove(&wd);
            let _ = inotify::remove_watch(&*self.ino, wd);
        }
    }

    fn event(&self, kind: FsEventKind, rel: &Path, is_dir: bool) -> FsEvent {
        let path = if rel.as_os_str().is_empty() {
            self.base.clone()
        } else if self.base.ends_with('/') {
            format!("{}{}", self.base, rel.display())
        } else {
            format!("{}/{}", self.base, rel.display())
        };
        FsEvent { kind: kind as i32, path, is_dir }
    }

    // Whether the watch is over: the top went away and no directory is left to hear from.
    fn gone(&self) -> bool {
        self.ended || self.dirs.is_empty()
    }
}

fn overflow() -> FsEvent {
    FsEvent { kind: FsEventKind::Overflow as i32, path: String::new(), is_dir: false }
}

// Drops an event that repeats the one just before it for the same path, like the many writes a
// big file gets.
fn dedup(events: Vec<FsEvent>) -> Vec<FsEvent> {
    let mut last: HashMap<String, i32> = HashMap::new();
    let mut out = Vec::with_capacity(events.len());
    for e in events {
        if e.kind != FsEventKind::Overflow as i32 && last.get(&e.path) == Some(&e.kind) {
            continue;
        }
        last.insert(e.path.clone(), e.kind);
        out.push(e);
    }
    out
}

// Packs events into batches that each fit in one frame.
fn frames(events: Vec<FsEvent>) -> Vec<Bytes> {
    let mut out = Vec::new();
    let mut batch = FsEvents::default();
    let mut size = 0;
    for e in events {
        // The event, its tag and a length prefix of up to three bytes.
        let n = e.encoded_len() + 4;
        if size + n > MAX_PAYLOAD && !batch.events.is_empty() {
            out.push(std::mem::take(&mut batch).encode_to_vec().into());
            size = 0;
        }
        size += n;
        batch.events.push(e);
    }
    if !batch.events.is_empty() {
        out.push(batch.encode_to_vec().into());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(kind: FsEventKind, path: &str) -> FsEvent {
        FsEvent { kind: kind as i32, path: path.into(), is_dir: false }
    }

    #[test]
    fn repeated_writes_collapse() {
        let got = dedup(vec![
            ev(FsEventKind::Create, "/a"),
            ev(FsEventKind::Write, "/a"),
            ev(FsEventKind::Write, "/a"),
            ev(FsEventKind::Write, "/b"),
            ev(FsEventKind::Write, "/a"),
            ev(FsEventKind::Remove, "/a"),
            ev(FsEventKind::Create, "/a"),
        ]);
        let kinds: Vec<_> = got.iter().map(|e| (e.kind, e.path.as_str())).collect();
        assert_eq!(
            kinds,
            [
                (FsEventKind::Create as i32, "/a"),
                (FsEventKind::Write as i32, "/a"),
                (FsEventKind::Write as i32, "/b"),
                (FsEventKind::Remove as i32, "/a"),
                (FsEventKind::Create as i32, "/a"),
            ]
        );
    }

    #[test]
    fn batches_fit_in_a_frame() {
        let long = "x".repeat(4000);
        let events: Vec<_> =
            (0..100).map(|i| ev(FsEventKind::Create, &format!("/{long}/{i}"))).collect();
        let frames = frames(events);
        assert!(frames.len() > 1);
        let mut seen = 0;
        for f in &frames {
            assert!(f.len() <= MAX_PAYLOAD);
            seen += FsEvents::decode(f.clone()).unwrap().events.len();
        }
        assert_eq!(seen, 100);
    }
}
