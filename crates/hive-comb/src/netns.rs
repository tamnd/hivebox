//! The pool of network namespaces, from `spec/08_node_agent.md`, section 3.
//!
//! Each cell gets a namespace of its own at `<dir>/cell-<n>`, made ahead of time by a background
//! task with loopback up. That is the whole of the `none` profile. Namespaces are never reused: one
//! a cell has used can still hold its `TIME_WAIT` sockets and any sysctl it changed, and the next
//! tenant would see both.

use hive_cell::netns;
use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::sync::{Notify, Semaphore};
use tokio_util::sync::CancellationToken;

/// Namespaces the refill task makes in one go. Each takes the kernel's RTNL lock once, so a batch
/// is kept small enough not to hold up other network changes on the host for long.
const BATCH: usize = 32;
/// Removals running at once. On some kernels each unmount waits out an RCU grace period of 20 to
/// 40 ms, but concurrent ones share it, so one at a time removes about 30 a second and 64 at once
/// over 1,000. The cap keeps a burst of them from taking every blocking thread tokio has.
const REAPERS: usize = 64;

#[derive(Debug)]
pub(crate) struct Namespaces {
    dir: PathBuf,
    depth: usize,
    next: AtomicU64,
    ready: Mutex<Vec<PathBuf>>,
    refill: Notify,
    reapers: Semaphore,
}

impl Namespaces {
    /// Uses `dir` for namespaces, keeping `depth` of them ready.
    pub(crate) fn init(dir: &Path, depth: usize) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let next = names(dir)?.iter().filter_map(|p| number(p)).max().map_or(0, |n| n + 1);
        Ok(Self {
            dir: dir.to_path_buf(),
            depth,
            next: AtomicU64::new(next),
            ready: Mutex::new(Vec::with_capacity(depth)),
            refill: Notify::new(),
            reapers: Semaphore::new(REAPERS),
        })
    }

    fn ready(&self) -> MutexGuard<'_, Vec<PathBuf>> {
        self.ready.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn fresh(&self, n: usize) -> Vec<PathBuf> {
        let first = self.next.fetch_add(n as u64, Ordering::Relaxed);
        (first..first + n as u64).map(|i| self.dir.join(format!("cell-{i}"))).collect()
    }

    /// A namespace for a new cell. Blocks for about a millisecond if none is ready.
    pub(crate) fn take(&self) -> io::Result<PathBuf> {
        let (ready, left) = {
            let mut ready = self.ready();
            (ready.pop(), ready.len())
        };
        if left < self.depth / 2 {
            self.refill.notify_one();
        }
        if let Some(ns) = ready {
            return Ok(ns);
        }
        let path = self.fresh(1).pop().expect("asked for one");
        netns::create(std::slice::from_ref(&path)).pop().expect("one result per path")?;
        Ok(path)
    }

    /// Namespaces ready for new cells.
    pub(crate) fn depth(&self) -> usize {
        self.ready().len()
    }

    /// Removes every namespace that is not in `keep`. Returns how many were removed.
    pub(crate) async fn sweep(&self, keep: &HashSet<PathBuf>) -> io::Result<usize> {
        let gone: Vec<PathBuf> =
            names(&self.dir)?.into_iter().filter(|p| !keep.contains(p)).collect();
        let n = gone.len();
        let results = futures::future::join_all(gone.into_iter().map(|p| self.remove(p))).await;
        results.into_iter().collect::<io::Result<Vec<()>>>()?;
        Ok(n)
    }

    /// Removes the namespace at `path`, off the async threads.
    pub(crate) async fn remove(&self, path: PathBuf) -> io::Result<()> {
        let _turn = self.reapers.acquire().await.map_err(io::Error::other)?;
        tokio::task::spawn_blocking(move || netns::remove(&path)).await.map_err(io::Error::other)?
    }

    /// Keeps the pool topped up until `stop` fires.
    pub(crate) async fn refill(self: Arc<Self>, stop: CancellationToken) {
        let this = self.clone();
        crate::pool::refill("network namespace", move || this.fill_once(), &self.refill, &stop)
            .await;
    }

    /// Makes up to one batch of namespaces if the pool is short. Returns true if it is full.
    fn fill_once(&self) -> io::Result<bool> {
        let short = self.depth.saturating_sub(self.depth());
        if short == 0 {
            return Ok(true);
        }
        let paths = self.fresh(short.min(BATCH));
        let results = netns::create(&paths);
        let mut failed = None;
        for (path, made) in paths.into_iter().zip(results) {
            match made {
                Ok(()) => self.ready().push(path),
                Err(e) => failed = Some(e),
            }
        }
        match failed {
            Some(e) => Err(io::Error::new(e.kind(), format!("in {}: {e}", self.dir.display()))),
            None => Ok(false),
        }
    }
}

fn names(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if number(&path).is_some() {
            out.push(path);
        }
    }
    Ok(out)
}

fn number(p: &Path) -> Option<u64> {
    p.file_name()?.to_str()?.strip_prefix("cell-")?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A directory for namespaces, or `None` when not running as root. What is left in it is
    /// removed at the end.
    struct Dir(PathBuf);

    impl Dir {
        fn new() -> Option<Self> {
            static N: AtomicU64 = AtomicU64::new(0);
            if !rustix_root() {
                eprintln!("skipped: needs root");
                return None;
            }
            let n = N.fetch_add(1, Ordering::Relaxed);
            Some(Self(std::env::temp_dir().join(format!("hive-ns-{}-{n}", std::process::id()))))
        }
    }

    impl Drop for Dir {
        fn drop(&mut self) {
            if let Ok(names) = names(&self.0) {
                for p in names {
                    let _ = netns::remove(&p);
                }
            }
            let _ = std::fs::remove_dir(&self.0);
        }
    }

    fn rustix_root() -> bool {
        std::fs::read_to_string("/proc/self/status").is_ok_and(|s| {
            s.lines()
                .find_map(|l| l.strip_prefix("Uid:"))
                .and_then(|l| l.split_whitespace().nth(1).map(|u| u == "0"))
                .unwrap_or(false)
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_pool_fills_hands_out_and_sweeps() {
        let Some(d) = Dir::new() else { return };
        let pool = Arc::new(Namespaces::init(&d.0, 6).unwrap());
        // With nothing ready a take makes one on the spot.
        let first = pool.take().unwrap();
        assert!(first.exists());

        let stop = CancellationToken::new();
        let task = tokio::spawn(pool.clone().refill(stop.clone()));
        let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while pool.depth() < 6 {
            assert!(std::time::Instant::now() < until, "only {} ready", pool.depth());
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        let taken: Vec<PathBuf> = (0..4).map(|_| pool.take().unwrap()).collect();
        let mut all: HashSet<PathBuf> = taken.iter().cloned().collect();
        all.insert(first.clone());
        assert_eq!(all.len(), 5, "every take is a different namespace");
        stop.cancel();
        task.await.unwrap();
        drop(pool);

        // The next comb keeps the two that cells hold and removes the rest.
        let pool = Namespaces::init(&d.0, 6).unwrap();
        let keep = HashSet::from([first.clone(), taken[0].clone()]);
        let left = names(&d.0).unwrap().len();
        assert_eq!(pool.sweep(&keep).await.unwrap(), left - 2);
        let mut now = names(&d.0).unwrap();
        now.sort();
        let mut want: Vec<PathBuf> = keep.into_iter().collect();
        want.sort();
        assert_eq!(now, want);
        // New names never reuse an old one.
        let fresh = pool.take().unwrap();
        assert!(number(&fresh) > taken.iter().map(|p| number(p)).max().unwrap());
        pool.remove(fresh.clone()).await.unwrap();
        assert!(!fresh.exists());
    }
}
