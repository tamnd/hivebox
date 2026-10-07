//! The audit log. Every control op, exec, file write, policy change and snapshot becomes an
//! [`AuditEvent`], and a node's events go into one chain: each line carries the hash of the line
//! before it, so a line edited, dropped, added or moved breaks the chain from there on.
//!
//! The chain is cut into one file per hour, named for the hour of its first event in UTC, such as
//! `2026-10-07T06.log`. When an hour is done, a seal next to it records how many events it holds
//! and the hash of its last line, its root, which is what gets published to the keeper. The first
//! line of an hour points back at the root of the hour before, so the hours form one chain too.
//!
//! [`AuditLog`] appends from a thread of its own, writing whatever has queued up as one batch and
//! syncing the file once per batch, so a busy node pays for one sync per batch and not per event.
//! [`AuditLog::record`] returns once the event is queued, and [`AuditLog::flush`] waits until
//! everything queued so far is on disk. [`verify`] walks a node's files and says where the chain
//! breaks, if it does.

use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;

/// The hash the first line of a node's chain points back at.
pub const GENESIS: [u8; 32] = [0; 32];

/// The most events one batch writes before it syncs.
const BATCH: usize = 4096;

/// How many events may wait for the writer before [`AuditLog::record`] blocks.
const QUEUE: usize = 1 << 16;

/// One thing that happened, as the audit log keeps it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEvent {
    /// When, in nanoseconds since the Unix epoch.
    pub ts: u64,
    /// Who asked: an API key's id, a service's SPIFFE id, or `local` for the comb's own socket.
    pub principal: String,
    /// The project the call was for.
    pub project: String,
    /// The cell it touched, or empty for a call that touches none.
    pub cell: String,
    /// What was asked, such as `cell.create` or `exec.run`.
    pub op: String,
    /// The BLAKE3 hash of the call's arguments in hex, so the log shows what was asked without
    /// holding secrets or file contents.
    pub args: String,
    /// `ok`, or the error the call got.
    pub result: String,
    /// The trace id of the call, or empty.
    pub trace: String,
}

/// Hashes `args` the way [`AuditEvent::args`] holds them.
pub fn digest(args: &[u8]) -> String {
    hex(blake3::hash(args).as_bytes())
}

/// One line of a log file.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Line {
    seq: u64,
    node: String,
    event: AuditEvent,
    prev: String,
    hash: String,
}

/// What a sealed hour holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Seal {
    /// The node whose chain it is.
    pub node: String,
    /// The hour, as its file is named.
    pub hour: String,
    /// The sequence number of its first event.
    pub first_seq: u64,
    /// How many events it holds.
    pub count: u64,
    /// The hash its first line points back at, in hex.
    pub prev: String,
    /// The hash of its last line, in hex.
    pub root: String,
}

fn hash(prev: &[u8; 32], seq: u64, node: &str, event: &AuditEvent) -> [u8; 32] {
    // Serializing a struct writes its fields in a fixed order, so the same event always hashes
    // the same, however the line that held it was spaced.
    hash_body(prev, seq, node, &serde_json::to_vec(event).unwrap_or_default())
}

/// The hash of a line, given its event as JSON.
fn hash_body(prev: &[u8; 32], seq: u64, node: &str, body: &[u8]) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(prev);
    h.update(&seq.to_le_bytes());
    h.update(&(node.len() as u64).to_le_bytes());
    h.update(node.as_bytes());
    h.update(body);
    *h.finalize().as_bytes()
}

fn hex32(b: &[u8; 32]) -> [u8; 64] {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = [0u8; 64];
    for (i, x) in b.iter().enumerate() {
        out[2 * i] = DIGITS[usize::from(x >> 4)];
        out[2 * i + 1] = DIGITS[usize::from(x & 15)];
    }
    out
}

fn hex(b: &[u8; 32]) -> String {
    String::from_utf8_lossy(&hex32(b)).into_owned()
}

fn unhex(s: &str) -> Option<[u8; 32]> {
    let b = s.as_bytes();
    if b.len() != 64 {
        return None;
    }
    let nibble = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    };
    let mut out = [0u8; 32];
    for (i, o) in out.iter_mut().enumerate() {
        *o = nibble(b[2 * i])? << 4 | nibble(b[2 * i + 1])?;
    }
    Some(out)
}

/// The hour `ts` falls in, in UTC, as a file is named for it: `2026-10-07T06`.
pub fn hour_of(ts: u64) -> String {
    let secs = ts / 1_000_000_000;
    let (days, rest) = (secs / 86_400, secs % 86_400);
    // Howard Hinnant's days to civil date, for days since 1970-01-01.
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}T{:02}", rest / 3600)
}

/// Where a chain breaks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Broken {
    /// The file.
    pub file: PathBuf,
    /// The line, counting from 1, or 0 for the file as a whole.
    pub line: usize,
    /// What is wrong there.
    pub why: String,
}

impl std::fmt::Display for Broken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.line == 0 {
            write!(f, "{}: {}", self.file.display(), self.why)
        } else {
            write!(f, "{}:{}: {}", self.file.display(), self.line, self.why)
        }
    }
}

impl std::error::Error for Broken {}

/// What a whole chain holds, once it checks out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verified {
    /// The node whose chain it is, or empty for a chain with no events yet.
    pub node: String,
    /// How many hour files.
    pub hours: usize,
    /// How many events.
    pub events: u64,
    /// The hash of the last line, or [`GENESIS`] for an empty chain.
    pub root: [u8; 32],
    /// Whether the last hour is sealed. The hour still being written is not, and the lines at its
    /// end can only be checked against a root published elsewhere.
    pub sealed: bool,
}

impl Verified {
    /// The root in hex.
    pub fn root_hex(&self) -> String {
        hex(&self.root)
    }
}

/// The hour files in `dir`, oldest first.
fn hours(dir: &Path) -> io::Result<Vec<String>> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir)? {
        let name = e?.file_name();
        if let Some(h) = name.to_str().and_then(|n| n.strip_suffix(".log")) {
            out.push(h.to_string());
        }
    }
    out.sort();
    Ok(out)
}

fn read_seal(dir: &Path, hour: &str) -> io::Result<Option<Seal>> {
    match std::fs::read(dir.join(format!("{hour}.seal"))) {
        Ok(b) => serde_json::from_slice(&b).map(Some).map_err(io::Error::other),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Where a walk over one hour file got to.
struct Walk {
    seq: u64,
    prev: [u8; 32],
    node: Option<String>,
    count: u64,
    /// The byte length of the lines that checked out.
    good: u64,
}

/// Checks the lines of one hour file, carrying on from `walk`. A line that fails stops it with
/// the reason, and `walk` says how far it got.
fn walk_file(path: &Path, walk: &mut Walk) -> io::Result<Option<(usize, String)>> {
    let mut r = BufReader::new(File::open(path)?);
    let mut buf = Vec::new();
    let mut n = 0;
    loop {
        buf.clear();
        let len = r.read_until(b'\n', &mut buf)?;
        if len == 0 {
            return Ok(None);
        }
        n += 1;
        if buf.last() != Some(&b'\n') {
            return Ok(Some((n, "the line has no end".into())));
        }
        let line: Line = match serde_json::from_slice(&buf) {
            Ok(l) => l,
            Err(e) => return Ok(Some((n, format!("not an audit line: {e}")))),
        };
        if line.seq != walk.seq {
            return Ok(Some((n, format!("sequence {} where {} was due", line.seq, walk.seq))));
        }
        if let Some(node) = &walk.node
            && *node != line.node
        {
            return Ok(Some((n, format!("node {} in the chain of {node}", line.node))));
        }
        if unhex(&line.prev) != Some(walk.prev) {
            return Ok(Some((n, "it does not point back at the line before".into())));
        }
        let h = hash(&walk.prev, line.seq, &line.node, &line.event);
        if unhex(&line.hash) != Some(h) {
            return Ok(Some((n, "its hash does not match what it holds".into())));
        }
        walk.node = Some(line.node);
        walk.seq += 1;
        walk.prev = h;
        walk.count += 1;
        walk.good += len as u64;
    }
}

/// Checks a node's whole chain in `dir`: every line's hash, every link to the line before, the
/// sequence numbers, and every seal. Only the last hour may be unsealed.
///
/// # Errors
///
/// `Ok(Err(..))` says where the chain breaks, and `Err` is a file that could not be read.
pub fn verify(dir: &Path) -> io::Result<Result<Verified, Broken>> {
    let all = hours(dir)?;
    let mut walk = Walk { seq: 0, prev: GENESIS, node: None, count: 0, good: 0 };
    let mut events = 0;
    let mut sealed = false;
    for (i, hour) in all.iter().enumerate() {
        let path = dir.join(format!("{hour}.log"));
        let broken = |line, why: String| Broken { file: path.clone(), line, why };
        let (first_seq, first_prev) = (walk.seq, walk.prev);
        walk.count = 0;
        if let Some((line, why)) = walk_file(&path, &mut walk)? {
            return Ok(Err(broken(line, why)));
        }
        events += walk.count;
        let seal = read_seal(dir, hour)?;
        sealed = seal.is_some();
        match seal {
            None if i + 1 < all.len() => {
                return Ok(Err(broken(0, "an hour before the last has no seal".into())));
            }
            None => {}
            Some(s) => {
                let want = Seal {
                    node: walk.node.clone().unwrap_or_default(),
                    hour: hour.clone(),
                    first_seq,
                    count: walk.count,
                    prev: hex(&first_prev),
                    root: hex(&walk.prev),
                };
                if s != want {
                    return Ok(Err(broken(0, format!("the seal says {s:?}, the lines {want:?}"))));
                }
            }
        }
    }
    Ok(Ok(Verified {
        node: walk.node.unwrap_or_default(),
        hours: all.len(),
        events,
        root: walk.prev,
        sealed,
    }))
}

/// How much an [`AuditLog`] has written.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AuditStats {
    /// Events on disk.
    pub events: u64,
    /// Syncs, one per batch.
    pub syncs: u64,
    /// Hours sealed.
    pub sealed: u64,
    /// Events that were queued and never reached the disk, because a write failed.
    pub lost: u64,
}

#[derive(Default)]
struct Counters {
    events: AtomicU64,
    syncs: AtomicU64,
    sealed: AtomicU64,
    lost: AtomicU64,
}

enum Msg {
    Event(AuditEvent),
    Flush(SyncSender<io::Result<()>>),
}

/// A node's audit log, appended to from a thread of its own.
pub struct AuditLog {
    tx: Option<SyncSender<Msg>>,
    thread: Option<JoinHandle<()>>,
    counters: Arc<Counters>,
    root: Arc<Mutex<(u64, [u8; 32])>>,
}

impl std::fmt::Debug for AuditLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditLog").field("stats", &self.stats()).finish_non_exhaustive()
    }
}

/// The open end of the chain.
struct Writer {
    dir: PathBuf,
    node: String,
    /// The node's name as a JSON string, quotes and all.
    node_json: String,
    seq: u64,
    prev: [u8; 32],
    /// The hour being written, its first sequence number and the hash it started from.
    hour: Option<(String, u64, [u8; 32])>,
    out: Option<BufWriter<File>>,
    /// Hours this writer sealed.
    sealed: u64,
}

impl Writer {
    /// Finds the end of the chain in `dir`. A last line cut short by a crash is cut off, and an
    /// hour left unsealed by a crash at the turn of the hour is sealed now.
    fn open(dir: &Path, node: &str) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let all = hours(dir)?;
        let mut walk = Walk { seq: 0, prev: GENESIS, node: None, count: 0, good: 0 };
        let mut w = Self {
            dir: dir.to_path_buf(),
            node: node.to_string(),
            node_json: serde_json::to_string(node).map_err(io::Error::other)?,
            seq: 0,
            prev: GENESIS,
            hour: None,
            out: None,
            sealed: 0,
        };
        for (i, hour) in all.iter().enumerate() {
            let path = dir.join(format!("{hour}.log"));
            let (first_seq, first_prev) = (walk.seq, walk.prev);
            walk.count = 0;
            walk.good = 0;
            let last = i + 1 == all.len();
            if let Some((line, why)) = walk_file(&path, &mut walk)? {
                // Only a torn last line of the last hour is a crash. Anything else is damage the
                // log must not write past.
                let lines = BufReader::new(File::open(&path)?).split(b'\n').count();
                if !(last && line == lines) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        Broken { file: path, line, why }.to_string(),
                    ));
                }
                OpenOptions::new().write(true).open(&path)?.set_len(walk.good)?;
            }
            if let Some(n) = &walk.node
                && n != node
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{} holds the chain of node {n}, not {node}", dir.display()),
                ));
            }
            if last {
                w.hour = Some((hour.clone(), first_seq, first_prev));
            } else if read_seal(dir, hour)?.is_none() {
                w.seal(hour, first_seq, walk.count, &first_prev, &walk.prev)?;
            }
        }
        w.seq = walk.seq;
        w.prev = walk.prev;
        if let Some((hour, _, _)) = &w.hour {
            // A new hour's file is made before the hour before it is sealed, so the last hour
            // never has a seal unless someone else wrote one.
            if read_seal(dir, hour)?.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{}: the last hour, {hour}, is sealed already", dir.display()),
                ));
            }
            let f = OpenOptions::new().append(true).open(dir.join(format!("{hour}.log")))?;
            w.out = Some(BufWriter::with_capacity(1 << 20, f));
        }
        Ok(w)
    }

    fn seal(
        &mut self,
        hour: &str,
        first_seq: u64,
        count: u64,
        prev: &[u8; 32],
        root: &[u8; 32],
    ) -> io::Result<()> {
        let seal = Seal {
            node: self.node.clone(),
            hour: hour.to_string(),
            first_seq,
            count,
            prev: hex(prev),
            root: hex(root),
        };
        let path = self.dir.join(format!("{hour}.seal"));
        let tmp = path.with_extension("seal.tmp");
        let mut f = File::create(&tmp)?;
        f.write_all(&serde_json::to_vec(&seal).map_err(io::Error::other)?)?;
        f.sync_all()?;
        std::fs::rename(&tmp, &path)?;
        File::open(&self.dir)?.sync_all()?;
        self.sealed += 1;
        Ok(())
    }

    /// Starts the file for `hour`, then syncs and seals the hour before it. Done in that order, a
    /// crash in between leaves an unsealed hour that is not the last, which [`Writer::open`]
    /// seals.
    fn next_hour(&mut self, hour: String) -> io::Result<()> {
        if let Some(out) = &mut self.out {
            out.flush()?;
            out.get_ref().sync_data()?;
        }
        let path = self.dir.join(format!("{hour}.log"));
        let f = OpenOptions::new().create(true).append(true).open(&path)?;
        File::open(&self.dir)?.sync_all()?;
        self.out = Some(BufWriter::with_capacity(1 << 20, f));
        if let Some((done, first_seq, first_prev)) = self.hour.replace((hour, self.seq, self.prev))
        {
            self.seal(&done, first_seq, self.seq - first_seq, &first_prev, &self.prev.clone())?;
        }
        Ok(())
    }

    /// Drops whatever is buffered and not written, without writing it.
    fn discard(&mut self) {
        if let Some(out) = self.out.take() {
            drop(out.into_parts());
        }
    }

    fn append(&mut self, event: AuditEvent) -> io::Result<()> {
        let hour = hour_of(event.ts);
        // An event whose clock went back stays in the hour being written, so the files keep
        // their order.
        if self.hour.as_ref().is_none_or(|(h, _, _)| hour > *h) {
            self.next_hour(hour)?;
        }
        // The event is serialized once, for the hash and for the line, which reads back as a
        // `Line`.
        let body = serde_json::to_vec(&event).map_err(io::Error::other)?;
        let h = hash_body(&self.prev, self.seq, &self.node, &body);
        let out = self.out.as_mut().ok_or_else(|| io::Error::other("no hour open"))?;
        write!(out, "{{\"seq\":{},\"node\":{},\"event\":", self.seq, self.node_json)?;
        out.write_all(&body)?;
        out.write_all(b",\"prev\":\"")?;
        out.write_all(&hex32(&self.prev))?;
        out.write_all(b"\",\"hash\":\"")?;
        out.write_all(&hex32(&h))?;
        out.write_all(b"\"}\n")?;
        self.seq += 1;
        self.prev = h;
        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        if let Some(out) = &mut self.out {
            out.flush()?;
            out.get_ref().sync_data()?;
        }
        Ok(())
    }
}

impl AuditLog {
    /// Opens node `node`'s chain in `dir`, or starts one, and starts the thread that writes it.
    ///
    /// # Errors
    ///
    /// The directory cannot be read or written, the chain in it is broken anywhere but a torn
    /// last line, or it is another node's chain.
    pub fn open(dir: &Path, node: &str) -> io::Result<Self> {
        let mut w = Writer::open(dir, node)?;
        let counters = Arc::new(Counters::default());
        counters.events.store(w.seq, Ordering::Relaxed);
        let root = Arc::new(Mutex::new((w.seq, w.prev)));
        let (tx, rx) = mpsc::sync_channel(QUEUE);
        let (c, r) = (counters.clone(), root.clone());
        let thread = std::thread::Builder::new()
            .name("hive-audit".into())
            .spawn(move || run(&mut w, &rx, &c, &r))?;
        Ok(Self { tx: Some(tx), thread: Some(thread), counters, root })
    }

    /// Queues `event`. It blocks only while the writer is a whole queue behind.
    pub fn record(&self, event: AuditEvent) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(Msg::Event(event));
        }
    }

    /// Waits until every event queued so far is on disk.
    ///
    /// # Errors
    ///
    /// The last write or sync failed, or the writer is gone.
    pub fn flush(&self) -> io::Result<()> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.tx
            .as_ref()
            .ok_or_else(|| io::Error::other("the audit log is closed"))?
            .send(Msg::Flush(tx))
            .map_err(|_| io::Error::other("the audit writer is gone"))?;
        rx.recv().map_err(|_| io::Error::other("the audit writer is gone"))?
    }

    /// The number of events on disk and the hash of the last of them.
    pub fn root(&self) -> (u64, [u8; 32]) {
        *self.root.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// How much the log has written.
    pub fn stats(&self) -> AuditStats {
        AuditStats {
            events: self.counters.events.load(Ordering::Relaxed),
            syncs: self.counters.syncs.load(Ordering::Relaxed),
            sealed: self.counters.sealed.load(Ordering::Relaxed),
            lost: self.counters.lost.load(Ordering::Relaxed),
        }
    }
}

impl Drop for AuditLog {
    fn drop(&mut self) {
        // Closing the queue lets the writer finish what is in it and sync.
        self.tx = None;
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

fn run(w: &mut Writer, rx: &Receiver<Msg>, counters: &Counters, root: &Mutex<(u64, [u8; 32])>) {
    let mut waiting = Vec::new();
    let mut failed: Option<String> = None;
    while let Ok(first) = rx.recv() {
        let mut next = Some(first);
        let mut n = 0;
        while let Some(msg) = next.take() {
            match msg {
                Msg::Event(e) => {
                    n += 1;
                    if let Err(e) = w.append(e) {
                        failed = Some(e.to_string());
                        recover(w, counters);
                    }
                }
                Msg::Flush(tx) => waiting.push(tx),
            }
            if n < BATCH {
                next = rx.try_recv().ok();
            }
        }
        if let Err(e) = w.sync() {
            failed = Some(e.to_string());
            recover(w, counters);
        }
        counters.syncs.fetch_add(1, Ordering::Relaxed);
        publish(w, counters, root);
        // A failure is kept until a flush can report it.
        if !waiting.is_empty() {
            let failed = failed.take();
            for tx in waiting.drain(..) {
                let _ = tx.send(failed.clone().map_or(Ok(()), |e| Err(io::Error::other(e))));
            }
        }
    }
    if w.sync().is_err() {
        recover(w, counters);
    }
    publish(w, counters, root);
}

fn publish(w: &Writer, counters: &Counters, root: &Mutex<(u64, [u8; 32])>) {
    counters.events.store(w.seq, Ordering::Relaxed);
    counters.sealed.store(w.sealed, Ordering::Relaxed);
    *root.lock().unwrap_or_else(PoisonError::into_inner) = (w.seq, w.prev);
}

/// After a failed write, finds the end of the chain on disk again, so the next line follows the
/// last one that made it and not one that did not.
fn recover(w: &mut Writer, counters: &Counters) {
    w.discard();
    if let Ok(mut fresh) = Writer::open(&w.dir, &w.node) {
        counters.lost.fetch_add(w.seq.saturating_sub(fresh.seq), Ordering::Relaxed);
        fresh.sealed += w.sealed;
        *w = fresh;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: u64 = 3_600_000_000_000;
    /// 2026-10-07 06:00 UTC.
    const T0: u64 = 1_791_352_800 * 1_000_000_000;

    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let p = std::env::temp_dir().join(format!("hive-audit-{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&p);
            Self(p)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn event(ts: u64, i: u64) -> AuditEvent {
        AuditEvent {
            ts,
            principal: "key:k1".into(),
            project: "p".into(),
            cell: format!("c{i}"),
            op: "exec.run".into(),
            args: digest(format!("argv {i}").as_bytes()),
            result: "ok".into(),
            trace: String::new(),
        }
    }

    /// Writes `per_hour` events in each of `hours` hours and closes the log.
    fn fill(dir: &Path, hours: u64, per_hour: u64) {
        let log = AuditLog::open(dir, "n1").unwrap();
        for h in 0..hours {
            for i in 0..per_hour {
                log.record(event(T0 + h * HOUR + i, h * per_hour + i));
            }
        }
        log.flush().unwrap();
    }

    fn lines(path: &Path) -> Vec<String> {
        std::fs::read_to_string(path).unwrap().lines().map(String::from).collect()
    }

    fn put(path: &Path, lines: &[String]) {
        std::fs::write(path, lines.iter().map(|l| format!("{l}\n")).collect::<String>()).unwrap();
    }

    #[test]
    fn hours_are_named_in_utc() {
        assert_eq!(hour_of(0), "1970-01-01T00");
        assert_eq!(hour_of(T0), "2026-10-07T06");
        assert_eq!(hour_of(T0 + 18 * HOUR), "2026-10-08T00");
        // 2024-02-29 23:59:59 UTC.
        assert_eq!(hour_of(1_709_251_199 * 1_000_000_000), "2024-02-29T23");
    }

    #[test]
    fn a_chain_over_three_hours_checks_out() {
        let d = Scratch::new("ok");
        fill(&d.0, 3, 50);
        let v = verify(&d.0).unwrap().unwrap();
        assert_eq!((v.node.as_str(), v.hours, v.events, v.sealed), ("n1", 3, 150, false));
        assert!(d.0.join("2026-10-07T06.seal").exists());
        assert!(d.0.join("2026-10-07T07.seal").exists());
        assert!(!d.0.join("2026-10-07T08.seal").exists());
        // A log opened again carries on the same chain.
        let log = AuditLog::open(&d.0, "n1").unwrap();
        assert_eq!(log.root(), (150, v.root));
        log.record(event(T0 + 2 * HOUR + 99, 150));
        log.flush().unwrap();
        drop(log);
        assert_eq!(verify(&d.0).unwrap().unwrap().events, 151);
    }

    #[test]
    fn an_edited_line_breaks_the_chain_there() {
        let d = Scratch::new("edit");
        fill(&d.0, 2, 20);
        let path = d.0.join("2026-10-07T06.log");
        let mut l = lines(&path);
        l[7] = l[7].replace("\"result\":\"ok\"", "\"result\":\"denied\"");
        put(&path, &l);
        let b = verify(&d.0).unwrap().unwrap_err();
        assert_eq!((b.file, b.line), (path, 8), "{}", b.why);
    }

    #[test]
    fn a_dropped_or_moved_line_breaks_the_chain() {
        let d = Scratch::new("drop");
        fill(&d.0, 1, 20);
        let path = d.0.join("2026-10-07T06.log");
        let all = lines(&path);
        let mut l = all.clone();
        l.remove(5);
        put(&path, &l);
        assert_eq!(verify(&d.0).unwrap().unwrap_err().line, 6);
        let mut l = all.clone();
        l.swap(3, 4);
        put(&path, &l);
        assert_eq!(verify(&d.0).unwrap().unwrap_err().line, 4);
    }

    #[test]
    fn a_rewritten_tail_does_not_match_the_seal() {
        let d = Scratch::new("tail");
        fill(&d.0, 2, 20);
        let path = d.0.join("2026-10-07T06.log");
        let mut l = lines(&path);
        l.truncate(15);
        put(&path, &l);
        let b = verify(&d.0).unwrap().unwrap_err();
        assert_eq!(b.line, 0, "{}", b.why);
        assert!(b.why.contains("seal"), "{}", b.why);
    }

    #[test]
    fn a_torn_last_line_is_cut_off_on_open() {
        let d = Scratch::new("torn");
        fill(&d.0, 1, 10);
        let path = d.0.join("2026-10-07T06.log");
        let mut text = std::fs::read(&path).unwrap();
        text.extend_from_slice(b"{\"seq\":10,\"node\":\"n1\",\"ev");
        std::fs::write(&path, &text).unwrap();
        assert_eq!(verify(&d.0).unwrap().unwrap_err().line, 11);
        let log = AuditLog::open(&d.0, "n1").unwrap();
        assert_eq!(log.root().0, 10);
        log.record(event(T0 + 5, 10));
        log.flush().unwrap();
        drop(log);
        assert_eq!(verify(&d.0).unwrap().unwrap().events, 11);
    }

    #[test]
    fn damage_before_the_end_stops_the_log_from_opening() {
        let d = Scratch::new("damage");
        fill(&d.0, 1, 10);
        let path = d.0.join("2026-10-07T06.log");
        let mut l = lines(&path);
        l[2] = l[2].replace("exec.run", "exec.rum");
        put(&path, &l);
        let e = AuditLog::open(&d.0, "n1").unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::InvalidData);
        assert!(AuditLog::open(&Scratch::new("other").0, "n1").is_ok());
        let d2 = Scratch::new("node");
        fill(&d2.0, 1, 3);
        assert!(AuditLog::open(&d2.0, "n2").is_err());
    }

    #[test]
    fn an_hour_left_unsealed_by_a_crash_is_sealed_on_open() {
        let d = Scratch::new("unsealed");
        fill(&d.0, 2, 10);
        std::fs::remove_file(d.0.join("2026-10-07T06.seal")).unwrap();
        assert_eq!(verify(&d.0).unwrap().unwrap_err().line, 0);
        drop(AuditLog::open(&d.0, "n1").unwrap());
        assert_eq!(verify(&d.0).unwrap().unwrap().events, 20);
    }

    #[test]
    fn an_event_whose_clock_went_back_stays_in_the_open_hour() {
        let d = Scratch::new("back");
        let log = AuditLog::open(&d.0, "n1").unwrap();
        log.record(event(T0 + HOUR, 0));
        log.record(event(T0, 1));
        log.flush().unwrap();
        drop(log);
        let v = verify(&d.0).unwrap().unwrap();
        assert_eq!((v.hours, v.events), (1, 2));
        assert_eq!(lines(&d.0.join("2026-10-07T07.log")).len(), 2);
    }

    #[test]
    fn records_from_many_threads_all_land_in_order() {
        let d = Scratch::new("threads");
        let log = Arc::new(AuditLog::open(&d.0, "n1").unwrap());
        let threads: Vec<_> = (0..8)
            .map(|t| {
                let log = log.clone();
                std::thread::spawn(move || {
                    for i in 0..500 {
                        log.record(event(T0 + i, t * 1000 + i));
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        log.flush().unwrap();
        let s = log.stats();
        assert_eq!(s.events, 4000);
        assert!(s.syncs <= 4000);
        drop(log);
        assert_eq!(verify(&d.0).unwrap().unwrap().events, 4000);
    }
}
