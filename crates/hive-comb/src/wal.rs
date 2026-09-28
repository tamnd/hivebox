//! The write ahead log. Every cell's current record lives here, and a transition is on disk before
//! the comb acts on it, so a restart finds every cell where it was left.
//!
//! It is one append only file of frames. A frame is a put, which carries a cell's whole record, or
//! a delete. Replaying the file from the start gives the current record of every cell. One thread
//! owns the file and commits in groups: it takes every write that is waiting, up to
//! [`MAX_BATCH`], appends them in one `write` and syncs once, so under load a sync is shared by
//! hundreds of cells and when idle a write waits for nothing but its own sync. When the file
//! grows to twice what the live records need, the thread writes the live records to a new file
//! and renames it into place.
//!
//! A frame is a 4 byte length, a kind byte, the payload and an 8 byte BLAKE3 checksum over the
//! rest. A crash can leave a torn frame at the end, which replay finds by its length or checksum
//! and cuts off. Everything before it was synced, so nothing acknowledged is lost.

use bytes::Bytes;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{mpsc, oneshot};

/// Most writes committed with one sync.
pub(crate) const MAX_BATCH: usize = 256;
/// Largest record. A length above this in a frame header means the header is garbage.
pub(crate) const MAX_RECORD: usize = 1 << 20;
/// The file is not compacted below this size, however much of it is dead.
const COMPACT_FLOOR: u64 = 4 << 20;

const PUT: u8 = 1;
const DELETE: u8 = 2;
const HEAD: usize = 5;
const SUM: usize = 8;
const KEY: usize = 16;
const FILE: &str = "cells.wal";
const TMP: &str = "cells.wal.tmp";

/// Counters for tests and benchmarks.
#[derive(Debug, Default)]
pub(crate) struct Stats {
    /// Writes committed.
    pub(crate) writes: AtomicU64,
    /// Syncs done, one per group.
    pub(crate) syncs: AtomicU64,
    /// Times the file was rewritten with only the live records.
    pub(crate) compactions: AtomicU64,
}

struct Op {
    key: u128,
    value: Option<Bytes>,
    // Set on the last op, from close. The writer stops once the ops before it are on disk.
    last: bool,
    done: oneshot::Sender<Result<(), String>>,
}

/// The handle every lifecycle actor writes through. Cloning it is cheap.
#[derive(Clone, Debug)]
pub(crate) struct Wal {
    tx: mpsc::Sender<Op>,
    stats: Arc<Stats>,
}

/// What [`Wal::open`] found on disk.
#[derive(Debug, Default)]
pub(crate) struct Replay {
    /// The current value of every key.
    pub(crate) records: HashMap<u128, Bytes>,
    /// Bytes cut off the end because the last frame was torn or damaged.
    pub(crate) cut: u64,
}

impl Wal {
    /// Opens the log in `dir`, making it if needed, and replays it. The writer thread runs until
    /// every handle is dropped.
    pub(crate) fn open(dir: &Path) -> io::Result<(Self, Replay)> {
        std::fs::create_dir_all(dir)?;
        // A compaction that died before its rename leaves this behind, and the old file is whole.
        match std::fs::remove_file(dir.join(TMP)) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
            _ => {}
        }
        let path = dir.join(FILE);
        let mut file =
            OpenOptions::new().read(true).append(true).create(true).truncate(false).open(&path)?;
        let mut raw = Vec::new();
        file.read_to_end(&mut raw)?;
        let (live, good) = replay(&raw);
        let cut = (raw.len() - good) as u64;
        if cut > 0 {
            file.set_len(good as u64)?;
            file.sync_all()?;
        }
        sync_dir(dir)?;
        let records = live.iter().map(|(&k, v)| (k, v.clone())).collect();
        let stats = Arc::new(Stats::default());
        let (tx, rx) = mpsc::channel(4 * MAX_BATCH);
        let writer = Writer {
            dir: dir.to_path_buf(),
            file,
            len: good as u64,
            live_len: live.values().map(|v| frame_len(v.len())).sum(),
            live,
            stats: stats.clone(),
            failed: None,
        };
        std::thread::Builder::new().name("comb-wal".into()).spawn(move || writer.run(rx))?;
        Ok((Self { tx, stats }, Replay { records, cut }))
    }

    /// Sets the record for `key`. Returns once it is on disk.
    pub(crate) async fn put(&self, key: u128, value: Bytes) -> io::Result<()> {
        if value.len() > MAX_RECORD - KEY {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "record too large"));
        }
        self.send(key, Some(value)).await
    }

    /// Removes the record for `key`. Returns once that is on disk.
    pub(crate) async fn delete(&self, key: u128) -> io::Result<()> {
        self.send(key, None).await
    }

    pub(crate) fn stats(&self) -> &Stats {
        &self.stats
    }

    /// Stops the writer once everything sent so far is on disk. Writes after this fail, so a new
    /// writer can take over the file.
    pub(crate) async fn close(&self) {
        let (done, rx) = oneshot::channel();
        if self.tx.send(Op { key: 0, value: None, last: true, done }).await.is_ok() {
            let _ = rx.await;
        }
    }

    async fn send(&self, key: u128, value: Option<Bytes>) -> io::Result<()> {
        let (done, rx) = oneshot::channel();
        let gone = || io::Error::other("the WAL writer has stopped");
        self.tx.send(Op { key, value, last: false, done }).await.map_err(|_| gone())?;
        rx.await.map_err(|_| gone())?.map_err(io::Error::other)
    }
}

struct Writer {
    dir: PathBuf,
    file: File,
    len: u64,
    live: HashMap<u128, Bytes>,
    live_len: u64,
    stats: Arc<Stats>,
    // Once a write or a sync fails, what is on disk is unknown, so every later write fails too
    // and the comb has to restart and replay.
    failed: Option<String>,
}

impl Writer {
    fn run(mut self, mut rx: mpsc::Receiver<Op>) {
        let mut batch = Vec::with_capacity(MAX_BATCH);
        let mut buf = Vec::new();
        while let Some(op) = rx.blocking_recv() {
            let mut last = None;
            if op.last {
                last = Some(op);
            } else {
                batch.push(op);
            }
            while last.is_none() && batch.len() < MAX_BATCH {
                match rx.try_recv() {
                    Ok(op) if op.last => last = Some(op),
                    Ok(op) => batch.push(op),
                    Err(_) => break,
                }
            }
            if batch.is_empty() {
                if let Some(op) = last {
                    let _ = op.done.send(Ok(()));
                }
                return;
            }
            let result = match &self.failed {
                Some(e) => Err(e.clone()),
                None => self.commit(&batch, &mut buf).map_err(|e| {
                    let e = format!("the WAL write failed: {e}");
                    self.failed = Some(e.clone());
                    e
                }),
            };
            for op in batch.drain(..) {
                let _ = op.done.send(result.clone());
            }
            if let Some(op) = last {
                let _ = op.done.send(Ok(()));
                return;
            }
            if self.failed.is_none()
                && self.len > COMPACT_FLOOR
                && self.len > 2 * self.live_len
                && let Err(e) = self.compact()
            {
                self.failed = Some(format!("the WAL compaction failed: {e}"));
            }
        }
    }

    fn commit(&mut self, batch: &[Op], buf: &mut Vec<u8>) -> io::Result<()> {
        buf.clear();
        for op in batch {
            encode(buf, op.key, op.value.as_deref());
        }
        self.file.write_all(buf)?;
        self.file.sync_data()?;
        self.len += buf.len() as u64;
        for op in batch {
            let old = match &op.value {
                Some(v) => {
                    self.live_len += frame_len(v.len());
                    self.live.insert(op.key, v.clone())
                }
                None => self.live.remove(&op.key),
            };
            if let Some(old) = old {
                self.live_len -= frame_len(old.len());
            }
        }
        self.stats.writes.fetch_add(batch.len() as u64, Ordering::Relaxed);
        self.stats.syncs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn compact(&mut self) -> io::Result<()> {
        let tmp = self.dir.join(TMP);
        let mut buf = Vec::with_capacity(self.live_len as usize);
        for (&k, v) in &self.live {
            encode(&mut buf, k, Some(v));
        }
        let mut file = File::create(&tmp)?;
        file.write_all(&buf)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&tmp, self.dir.join(FILE))?;
        sync_dir(&self.dir)?;
        self.file = OpenOptions::new().append(true).open(self.dir.join(FILE))?;
        self.len = buf.len() as u64;
        self.stats.compactions.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

fn frame_len(value: usize) -> u64 {
    (HEAD + KEY + value + SUM) as u64
}

fn checksum(frame: &[u8]) -> [u8; SUM] {
    let mut out = [0; SUM];
    out.copy_from_slice(&blake3::hash(frame).as_bytes()[..SUM]);
    out
}

fn encode(buf: &mut Vec<u8>, key: u128, value: Option<&[u8]>) {
    let start = buf.len();
    let body = value.unwrap_or_default();
    let len = u32::try_from(KEY + body.len()).expect("records are capped well below 4 GiB");
    buf.extend_from_slice(&len.to_le_bytes());
    buf.push(if value.is_some() { PUT } else { DELETE });
    buf.extend_from_slice(&key.to_le_bytes());
    buf.extend_from_slice(body);
    let sum = checksum(&buf[start..]);
    buf.extend_from_slice(&sum);
}

/// Applies every whole frame in `raw` and returns the live records and how many bytes were good.
fn replay(raw: &[u8]) -> (HashMap<u128, Bytes>, usize) {
    let mut live = HashMap::new();
    let mut at = 0;
    while let Some(rest) = raw.get(at..)
        && rest.len() >= HEAD
    {
        let len = u32::from_le_bytes(rest[..4].try_into().expect("four bytes")) as usize;
        let kind = rest[4];
        if !(KEY..=MAX_RECORD).contains(&len)
            || !matches!(kind, PUT | DELETE)
            || (kind == DELETE && len != KEY)
            || rest.len() < HEAD + len + SUM
        {
            break;
        }
        let (frame, sum) = rest[..HEAD + len + SUM].split_at(HEAD + len);
        if checksum(frame) != sum {
            break;
        }
        let key = u128::from_le_bytes(frame[HEAD..HEAD + KEY].try_into().expect("sixteen bytes"));
        if kind == PUT {
            live.insert(key, Bytes::copy_from_slice(&frame[HEAD + KEY..]));
        } else {
            live.remove(&key);
        }
        at += HEAD + len + SUM;
    }
    (live, at)
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frames(ops: &[(u128, Option<&[u8]>)]) -> Vec<u8> {
        let mut buf = Vec::new();
        for &(k, v) in ops {
            encode(&mut buf, k, v);
        }
        buf
    }

    #[test]
    fn replay_applies_puts_and_deletes_in_order() {
        let raw = frames(&[
            (1, Some(b"a")),
            (2, Some(b"b")),
            (1, Some(b"a2")),
            (2, None),
            (3, Some(b"")),
        ]);
        let (live, good) = replay(&raw);
        assert_eq!(good, raw.len());
        assert_eq!(live.len(), 2);
        assert_eq!(&live[&1][..], b"a2");
        assert_eq!(&live[&3][..], b"");
    }

    #[test]
    fn a_torn_tail_is_cut_at_every_length() {
        let whole = frames(&[(1, Some(b"first")), (2, Some(b"second"))]);
        let first = frame_len(5) as usize;
        for cut in 0..whole.len() {
            let (live, good) = replay(&whole[..cut]);
            let want = if cut >= first { first } else { 0 };
            assert_eq!(good, want, "cut at {cut}");
            assert_eq!(live.len(), usize::from(cut >= first));
        }
    }

    #[test]
    fn a_damaged_frame_ends_the_replay() {
        let mut raw = frames(&[(1, Some(b"one")), (2, Some(b"two")), (3, Some(b"three"))]);
        let second = frame_len(3) as usize;
        for byte in second..second + frame_len(3) as usize {
            raw[byte] ^= 0x40;
            let (live, good) = replay(&raw);
            assert_eq!(good, second, "flipped byte {byte}");
            assert_eq!(live.keys().copied().collect::<Vec<_>>(), [1]);
            raw[byte] ^= 0x40;
        }
        assert_eq!(replay(&raw).1, raw.len());
    }

    /// A fresh directory that is removed afterwards.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new() -> Self {
            static N: AtomicU64 = AtomicU64::new(0);
            let n = N.fetch_add(1, Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!("hive-wal-{}-{n}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            Self(dir)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn value(k: u128, round: u32) -> Bytes {
        Bytes::from(format!("cell {k} round {round}").repeat(8))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_writes_share_syncs_and_survive_a_reopen() {
        let s = Scratch::new();
        let (wal, replay) = Wal::open(&s.0).unwrap();
        assert!(replay.records.is_empty());
        let tasks: Vec<_> = (0..512u128)
            .map(|k| {
                let wal = wal.clone();
                tokio::spawn(async move {
                    for round in 0..4 {
                        wal.put(k, value(k, round)).await.unwrap();
                    }
                    if k % 3 == 0 {
                        wal.delete(k).await.unwrap();
                    }
                })
            })
            .collect();
        for t in tasks {
            t.await.unwrap();
        }
        let writes = wal.stats().writes.load(Ordering::Relaxed);
        let syncs = wal.stats().syncs.load(Ordering::Relaxed);
        assert_eq!(writes, 512 * 4 + 171);
        assert!(syncs < writes / 4, "{syncs} syncs for {writes} writes");
        drop(wal);

        let (_, replay) = Wal::open(&s.0).unwrap();
        assert_eq!(replay.cut, 0);
        assert_eq!(replay.records.len(), 512 - 171);
        for k in (0..512).filter(|k| k % 3 != 0) {
            assert_eq!(replay.records[&k], value(k, 3));
        }
    }

    #[tokio::test]
    async fn a_torn_tail_on_disk_is_cut_and_writing_goes_on() {
        let s = Scratch::new();
        let (wal, _) = Wal::open(&s.0).unwrap();
        wal.put(1, value(1, 0)).await.unwrap();
        wal.put(2, value(2, 0)).await.unwrap();
        drop(wal);
        // Half of a third frame, as a crash in the middle of a write would leave it.
        let mut half = Vec::new();
        encode(&mut half, 3, Some(&value(3, 0)));
        half.truncate(half.len() / 2);
        let mut f = OpenOptions::new().append(true).open(s.0.join(FILE)).unwrap();
        f.write_all(&half).unwrap();
        drop(f);

        let (wal, replay) = Wal::open(&s.0).unwrap();
        assert_eq!(replay.cut, half.len() as u64);
        assert_eq!(replay.records.len(), 2);
        wal.put(4, value(4, 0)).await.unwrap();
        drop(wal);
        let (_, replay) = Wal::open(&s.0).unwrap();
        assert_eq!(replay.cut, 0);
        let mut keys: Vec<_> = replay.records.keys().copied().collect();
        keys.sort_unstable();
        assert_eq!(keys, [1, 2, 4]);
    }

    #[tokio::test]
    async fn compaction_keeps_only_the_live_records() {
        let s = Scratch::new();
        let (wal, _) = Wal::open(&s.0).unwrap();
        let big = Bytes::from(vec![7; 64 << 10]);
        // Rewriting the same few keys piles up dead frames until the file gets compacted.
        for round in 0..200u8 {
            for k in 0..4 {
                let mut v = big.to_vec();
                v[0] = round;
                wal.put(k, v.into()).await.unwrap();
            }
        }
        wal.delete(3).await.unwrap();
        assert!(wal.stats().compactions.load(Ordering::Relaxed) > 0);
        let size = std::fs::metadata(s.0.join(FILE)).unwrap().len();
        assert!(size < 2 * COMPACT_FLOOR, "{size} bytes after compaction");
        drop(wal);

        // A compaction that died before its rename leaves a file that has to be ignored.
        std::fs::write(s.0.join(TMP), b"half written").unwrap();
        let (_, replay) = Wal::open(&s.0).unwrap();
        assert!(!s.0.join(TMP).exists());
        assert_eq!(replay.records.len(), 3);
        for k in 0..3 {
            assert_eq!(replay.records[&k][0], 199);
        }
    }

    #[tokio::test]
    async fn records_over_the_cap_are_refused() {
        let s = Scratch::new();
        let (wal, _) = Wal::open(&s.0).unwrap();
        let e = wal.put(1, Bytes::from(vec![0; MAX_RECORD])).await.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::InvalidInput);
        wal.put(1, Bytes::from(vec![0; MAX_RECORD - KEY])).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn close_waits_for_earlier_writes_and_refuses_later_ones() {
        let s = Scratch::new();
        let (wal, _) = Wal::open(&s.0).unwrap();
        let writes: Vec<_> = (0..64u128)
            .map(|k| {
                let wal = wal.clone();
                tokio::spawn(async move { wal.put(k, Bytes::from_static(b"v")).await.is_ok() })
            })
            .collect();
        tokio::task::yield_now().await;
        wal.close().await;
        assert!(wal.put(100, Bytes::new()).await.is_err());
        let mut done = 0;
        for w in writes {
            done += usize::from(w.await.unwrap());
        }
        // Whatever was acknowledged is on disk, and a second writer can open the file at once.
        let (_, replay) = Wal::open(&s.0).unwrap();
        assert_eq!(replay.records.len(), done);
        assert_eq!(replay.cut, 0);
        wal.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "a measurement, not a test"]
    async fn throughput() {
        let s = Scratch::new();
        let (wal, _) = Wal::open(&s.0).unwrap();
        let record = Bytes::from(vec![1; 400]);
        for writers in [1u128, 16, 64, 256, 1024] {
            let per = (4096 / writers).max(64);
            let syncs = wal.stats().syncs.load(Ordering::Relaxed);
            let t = std::time::Instant::now();
            let tasks: Vec<_> = (0..writers)
                .map(|k| {
                    let (wal, record) = (wal.clone(), record.clone());
                    tokio::spawn(async move {
                        let mut lat = Vec::with_capacity(per as usize);
                        for _ in 0..per {
                            let t = std::time::Instant::now();
                            wal.put(k, record.clone()).await.unwrap();
                            lat.push(t.elapsed());
                        }
                        lat
                    })
                })
                .collect();
            let mut lat = Vec::new();
            for task in tasks {
                lat.extend(task.await.unwrap());
            }
            let took = t.elapsed();
            lat.sort_unstable();
            let n = lat.len();
            let syncs = wal.stats().syncs.load(Ordering::Relaxed) - syncs;
            println!(
                "{writers} writers: {:.0} writes/s, {:.1} per sync, p50 {:?}, p99 {:?}",
                n as f64 / took.as_secs_f64(),
                n as f64 / syncs as f64,
                lat[n / 2],
                lat[n * 99 / 100],
            );
        }
    }
}
