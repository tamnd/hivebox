//! The node's cgroup tree and its leaves, from `spec/08_node_agent.md`, sections 3 and 6.
//!
//! The tree is `<root>/<class>.slice/cell-<n>`, with one slice per QoS class carrying that class's
//! CPU weight, and one leaf per cell carrying the cell's own limits. A background task makes leaves
//! ahead of time, so a create does not wait on `mkdir`, which takes up to a quarter of a millisecond
//! on a busy host. Leaves are never reused: a used one still has its `cpu.stat` and `memory.peak`, so
//! it is killed and removed with its cell.

use hive_cell::cgroup;
use hive_types::{Qos, Resources};
use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// Controllers the comb cannot work without.
const NEEDED: [&str; 3] = ["cpu", "memory", "pids"];
/// Controllers it turns on if the kernel has them.
const WANTED: [&str; 1] = ["io"];
/// Leaves the refill task makes in one go before it looks at the other classes.
const BATCH: usize = 32;
/// The limits spare leaves are made with. Most cells ask for the defaults, so taking a spare usually
/// writes nothing.
const PRESET: Resources = Resources::DEFAULT;
/// How long a removal waits for the killed processes to go.
const DRAIN: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub(crate) struct Cgroups {
    classes: [Class; 3],
    depth: usize,
    next: AtomicU64,
    refill: Notify,
}

#[derive(Debug)]
struct Class {
    dir: PathBuf,
    ready: Mutex<Vec<PathBuf>>,
}

impl Class {
    fn ready(&self) -> std::sync::MutexGuard<'_, Vec<PathBuf>> {
        self.ready.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Cgroups {
    /// Makes the tree under `root`, or takes over the one a previous comb left, keeping `depth`
    /// ready leaves per class.
    pub(crate) fn init(root: &Path, depth: usize) -> io::Result<Self> {
        std::fs::create_dir_all(root)?;
        enable_controllers(root)?;
        let classes = [Qos::Latency, Qos::Standard, Qos::BestEffort].map(|qos| Class {
            dir: root.join(format!("{}.slice", slice_name(qos))),
            ready: Mutex::new(Vec::with_capacity(depth)),
        });
        let mut next = 0;
        for (qos, class) in [Qos::Latency, Qos::Standard, Qos::BestEffort].into_iter().zip(&classes)
        {
            match std::fs::create_dir(&class.dir) {
                Err(e) if e.kind() != io::ErrorKind::AlreadyExists => return Err(e),
                _ => {}
            }
            enable_controllers(&class.dir)?;
            weigh(&class.dir, qos)?;
            for leaf in leaves(&class.dir)? {
                next = next.max(leaf_number(&leaf).map_or(0, |n| n + 1));
            }
        }
        Ok(Self { classes, depth, next: AtomicU64::new(next), refill: Notify::new() })
    }

    fn class(&self, qos: Qos) -> &Class {
        match qos {
            Qos::Latency => &self.classes[0],
            Qos::Standard => &self.classes[1],
            Qos::BestEffort => &self.classes[2],
        }
    }

    /// A leaf for a new cell of class `qos`, with the limits in `r` already set.
    pub(crate) fn take(&self, qos: Qos, r: &Resources) -> io::Result<PathBuf> {
        let class = self.class(qos);
        let (ready, left) = {
            let mut ready = class.ready();
            (ready.pop(), ready.len())
        };
        if left < self.depth / 2 {
            self.refill.notify_one();
        }
        match ready {
            Some(dir) => {
                cgroup::relimit(&dir, Some(&PRESET), r).inspect_err(|_| {
                    let _ = std::fs::remove_dir(&dir);
                })?;
                Ok(dir)
            }
            None => self.make(class, r),
        }
    }

    /// Makes a leaf in `class` with the limits in `r`.
    fn make(&self, class: &Class, r: &Resources) -> io::Result<PathBuf> {
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        let dir = class.dir.join(format!("cell-{n}"));
        std::fs::create_dir(&dir)?;
        cgroup::limit(&dir, r).inspect_err(|_| {
            let _ = std::fs::remove_dir(&dir);
        })?;
        Ok(dir)
    }

    /// Ready leaves per class, latency first.
    pub(crate) fn depths(&self) -> [usize; 3] {
        self.classes.each_ref().map(|c| c.ready().len())
    }

    /// Removes every leaf that is not in `keep`: spares a previous comb made, and leaves whose cell
    /// has no record. Any process still in one is killed. Returns how many were removed.
    pub(crate) async fn sweep(&self, keep: &HashSet<PathBuf>) -> io::Result<usize> {
        let mut gone = Vec::new();
        for class in &self.classes {
            gone.extend(leaves(&class.dir)?.into_iter().filter(|l| !keep.contains(l)));
        }
        let n = gone.len();
        let results = futures::future::join_all(gone.into_iter().map(remove)).await;
        results.into_iter().collect::<io::Result<Vec<()>>>()?;
        Ok(n)
    }

    /// Keeps every class topped up to its depth until `stop` fires.
    pub(crate) async fn refill(self: Arc<Self>, stop: CancellationToken) {
        loop {
            let this = self.clone();
            // Blocking calls, but each only a few microseconds, and at most one batch at a time.
            let full = tokio::task::spawn_blocking(move || this.fill_once()).await.unwrap_or(true);
            if full {
                tokio::select! {
                    () = self.refill.notified() => {}
                    () = stop.cancelled() => return,
                }
            } else if stop.is_cancelled() {
                return;
            }
        }
    }

    /// Makes up to one batch of leaves where they are short. Returns true if every class is full.
    fn fill_once(&self) -> bool {
        let mut full = true;
        for class in &self.classes {
            let short = self.depth.saturating_sub(class.ready().len());
            if short == 0 {
                continue;
            }
            full = false;
            for _ in 0..short.min(BATCH) {
                match self.make(class, &PRESET) {
                    Ok(dir) => class.ready().push(dir),
                    Err(e) => {
                        eprintln!(
                            "hive-comb: making a spare cgroup in {}: {e}",
                            class.dir.display()
                        );
                        return true;
                    }
                }
            }
        }
        full
    }
}

/// Kills whatever is in the cgroup `dir`, waits for it to go, and removes the directory. A cgroup
/// that is already gone is fine.
pub(crate) async fn remove(dir: PathBuf) -> io::Result<()> {
    match cgroup::kill(&dir) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
        Ok(()) => {}
    }
    let mut wait = Duration::from_micros(200);
    let mut waited = Duration::ZERO;
    while cgroup::populated(&dir).unwrap_or(false) {
        if waited >= DRAIN {
            return Err(io::Error::other(format!(
                "{} still has processes {}s after they were killed",
                dir.display(),
                DRAIN.as_secs()
            )));
        }
        tokio::time::sleep(wait).await;
        waited += wait;
        wait = (wait * 2).min(Duration::from_millis(50));
    }
    match std::fs::remove_dir(&dir) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

fn slice_name(qos: Qos) -> &'static str {
    match qos {
        Qos::Latency => "latency",
        Qos::Standard => "standard",
        Qos::BestEffort => "besteffort",
    }
}

/// Sets a class slice's CPU share. Best effort cells only get CPU nobody else wants.
fn weigh(dir: &Path, qos: Qos) -> io::Result<()> {
    match qos {
        Qos::Latency => cgroup::write(dir, "cpu.weight", "1000"),
        Qos::Standard => cgroup::write(dir, "cpu.weight", "100"),
        // `cpu.idle` is from Linux 5.15. Before that the lowest weight is the nearest thing.
        Qos::BestEffort => {
            cgroup::write(dir, "cpu.idle", "1").or_else(|_| cgroup::write(dir, "cpu.weight", "1"))
        }
    }
}

/// Turns on the controllers the comb uses for the children of `dir`.
fn enable_controllers(dir: &Path) -> io::Result<()> {
    let have = std::fs::read_to_string(dir.join("cgroup.controllers"))?;
    let have: HashSet<&str> = have.split_whitespace().collect();
    if let Some(missing) = NEEDED.iter().find(|c| !have.contains(*c)) {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "{} has no {missing} controller, so it has to be turned on in its parent's cgroup.subtree_control",
                dir.display()
            ),
        ));
    }
    let on: Vec<String> = NEEDED
        .iter()
        .chain(&WANTED)
        .filter(|c| have.contains(*c))
        .map(|c| format!("+{c}"))
        .collect();
    cgroup::write(dir, "cgroup.subtree_control", &on.join(" "))
}

fn leaves(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() && leaf_number(&entry.path()).is_some() {
            out.push(entry.path());
        }
    }
    Ok(out)
}

fn leaf_number(p: &Path) -> Option<u64> {
    p.file_name()?.to_str()?.strip_prefix("cell-")?.parse().ok()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::process::{Child, Command};

    /// A cgroup tree of its own under the real root, or `None` when the tests are not running as
    /// root on a cgroup v2 host. Removed with everything in it at the end.
    pub(crate) struct Tree(pub(crate) PathBuf);

    impl Tree {
        pub(crate) fn new() -> Option<Self> {
            static N: AtomicU64 = AtomicU64::new(0);
            let status = std::fs::read_to_string("/proc/self/status").ok()?;
            let uid =
                status.lines().find_map(|l| l.strip_prefix("Uid:"))?.split_whitespace().nth(1)?;
            if uid != "0" || !Path::new("/sys/fs/cgroup/cgroup.controllers").exists() {
                eprintln!("skipped: needs root and cgroup v2");
                return None;
            }
            let n = N.fetch_add(1, Ordering::Relaxed);
            Some(Self(PathBuf::from(format!(
                "/sys/fs/cgroup/hive-test-{}-{n}.slice",
                std::process::id()
            ))))
        }
    }

    impl Drop for Tree {
        fn drop(&mut self) {
            fn clear(dir: &Path) {
                if let Ok(entries) = std::fs::read_dir(dir) {
                    for e in entries.flatten() {
                        if e.file_type().is_ok_and(|t| t.is_dir()) {
                            clear(&e.path());
                        }
                    }
                }
                let _ = cgroup::kill(dir);
                for _ in 0..1000 {
                    if std::fs::remove_dir(dir).is_ok() || !dir.exists() {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            clear(&self.0);
        }
    }

    pub(crate) fn sleeper_in(dir: &Path) -> Child {
        let child = Command::new("sleep").arg("600").spawn().unwrap();
        cgroup::write(dir, "cgroup.procs", &child.id().to_string()).unwrap();
        child
    }

    fn read(dir: &Path, file: &str) -> String {
        std::fs::read_to_string(dir.join(file)).unwrap().trim().to_owned()
    }

    #[tokio::test]
    async fn the_tree_has_a_slice_per_class_with_its_weight() {
        let Some(t) = Tree::new() else { return };
        let c = Cgroups::init(&t.0, 4).unwrap();
        assert_eq!(read(&t.0.join("latency.slice"), "cpu.weight"), "1000");
        assert_eq!(read(&t.0.join("standard.slice"), "cpu.weight"), "100");
        let best = t.0.join("besteffort.slice");
        assert!(read(&best, "cpu.idle") == "1" || read(&best, "cpu.weight") == "1");
        for class in ["latency", "standard", "besteffort"] {
            let on = read(&t.0.join(format!("{class}.slice")), "cgroup.subtree_control");
            for needed in NEEDED {
                assert!(on.contains(needed), "{class}: {on}");
            }
        }
        // A second comb takes the tree over as it is.
        drop(c);
        Cgroups::init(&t.0, 4).unwrap();
    }

    #[tokio::test]
    async fn a_leaf_has_the_cells_limits() {
        let Some(t) = Tree::new() else { return };
        let c = Cgroups::init(&t.0, 0).unwrap();
        let r = Resources { vcpu_milli: 1500, mem_mib: 300, pids: 77, ..Resources::default() };
        let leaf = c.take(Qos::Latency, &r).unwrap();
        assert!(leaf.starts_with(t.0.join("latency.slice")));
        assert_eq!(read(&leaf, "memory.max"), (300u64 << 20).to_string());
        assert_eq!(read(&leaf, "memory.oom.group"), "1");
        assert_eq!(read(&leaf, "cpu.max"), "150000 100000");
        assert_eq!(read(&leaf, "pids.max"), "77");
        let other = c.take(Qos::Latency, &r).unwrap();
        assert_ne!(leaf, other);

        // A spare comes with the usual limits, and a take changes only the ones that differ.
        let c = Cgroups::init(&t.0, 2).unwrap();
        c.fill_once();
        let usual = c.take(Qos::Standard, &Resources::default()).unwrap();
        assert_eq!(read(&usual, "memory.max"), (2048u64 << 20).to_string());
        assert_eq!(read(&usual, "memory.oom.group"), "1");
        assert_eq!(read(&usual, "cpu.max"), "100000 100000");
        let small = Resources { mem_mib: 512, ..Resources::default() };
        let leaf = c.take(Qos::Standard, &small).unwrap();
        assert_eq!(read(&leaf, "memory.max"), (512u64 << 20).to_string());
        assert_eq!(read(&leaf, "cpu.max"), "100000 100000");
        assert_eq!(read(&leaf, "pids.max"), "1024");
    }

    #[tokio::test]
    async fn removing_a_leaf_kills_what_is_in_it() {
        let Some(t) = Tree::new() else { return };
        let c = Cgroups::init(&t.0, 0).unwrap();
        let leaf = c.take(Qos::Standard, &Resources::default()).unwrap();
        let mut kids: Vec<Child> = (0..3).map(|_| sleeper_in(&leaf)).collect();
        assert!(cgroup::populated(&leaf).unwrap());
        remove(leaf.clone()).await.unwrap();
        assert!(!leaf.exists());
        for k in &mut kids {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(k.wait().unwrap().signal(), Some(9));
        }
        // Removing it again is not an error.
        remove(leaf).await.unwrap();
    }

    #[tokio::test]
    async fn the_sweep_removes_what_no_cell_claims() {
        let Some(t) = Tree::new() else { return };
        let c = Cgroups::init(&t.0, 2).unwrap();
        assert!(!c.fill_once());
        assert!(c.fill_once());
        let kept = c.take(Qos::Standard, &Resources::default()).unwrap();
        let orphan = c.take(Qos::BestEffort, &Resources::default()).unwrap();
        let mut stray = sleeper_in(&orphan);
        drop(c);

        // The next comb finds four spares, one leaf with a record and one without.
        let c = Cgroups::init(&t.0, 2).unwrap();
        let removed = c.sweep(&HashSet::from([kept.clone()])).await.unwrap();
        assert_eq!(removed, 5);
        assert!(kept.exists() && !orphan.exists());
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(stray.wait().unwrap().signal(), Some(9));
        // New leaves never reuse an old name.
        let new = c.take(Qos::Standard, &Resources::default()).unwrap();
        assert!(leaf_number(&new) > leaf_number(&orphan));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_refill_task_keeps_every_class_topped_up() {
        let Some(t) = Tree::new() else { return };
        let c = Arc::new(Cgroups::init(&t.0, 40).unwrap());
        let stop = CancellationToken::new();
        let task = tokio::spawn(c.clone().refill(stop.clone()));
        let full = |c: &Cgroups| c.depths() == [40, 40, 40];
        // Generous, for a small host running the other tests at the same time.
        for _ in 0..5000 {
            if full(&c) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        assert!(full(&c), "{:?}", c.depths());
        for _ in 0..30 {
            c.take(Qos::Standard, &Resources::default()).unwrap();
        }
        for _ in 0..5000 {
            if full(&c) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        assert!(full(&c), "{:?}", c.depths());
        stop.cancel();
        task.await.unwrap();
    }

    fn pct(mut v: Vec<Duration>) -> String {
        v.sort();
        let at = |p: usize| v[(v.len() - 1) * p / 100].as_secs_f64() * 1e6;
        format!("p50 {:.0}us p99 {:.0}us", at(50), at(99))
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "a measurement, not a test"]
    async fn speed() {
        use std::time::Instant;
        const N: usize = 1000;
        let Some(t) = Tree::new() else { return };
        let r = Resources::default();
        let c = Cgroups::init(&t.0, N).unwrap();
        let mut made = Vec::new();
        let start = Instant::now();
        while !c.fill_once() {}
        eprintln!("filling {N} spares per class: {:?}", start.elapsed() / 3);

        let (mut warm, mut other, mut cold) = (Vec::new(), Vec::new(), Vec::new());
        for _ in 0..N {
            let t0 = Instant::now();
            made.push(c.take(Qos::Standard, &r).unwrap());
            warm.push(t0.elapsed());
        }
        while !c.fill_once() {}
        let small = Resources { mem_mib: 512, ..r };
        for _ in 0..N {
            let t0 = Instant::now();
            made.push(c.take(Qos::Standard, &small).unwrap());
            other.push(t0.elapsed());
        }
        for _ in 0..N {
            let t0 = Instant::now();
            made.push(c.take(Qos::Standard, &r).unwrap());
            cold.push(t0.elapsed());
        }
        eprintln!("take from the pool: {}", pct(warm));
        eprintln!("same, other memory: {}", pct(other));
        eprintln!("take with mkdir:    {}", pct(cold));

        let mut empty = Vec::new();
        for dir in made.drain(..) {
            let t0 = Instant::now();
            remove(dir).await.unwrap();
            empty.push(t0.elapsed());
        }
        eprintln!("remove, empty:      {}", pct(empty));

        let mut busy = Vec::new();
        for _ in 0..200 {
            let dir = c.take(Qos::Standard, &r).unwrap();
            let mut kid = sleeper_in(&dir);
            let t0 = Instant::now();
            remove(dir).await.unwrap();
            busy.push(t0.elapsed());
            kid.wait().unwrap();
        }
        eprintln!("remove, one process: {}", pct(busy));
    }
}
