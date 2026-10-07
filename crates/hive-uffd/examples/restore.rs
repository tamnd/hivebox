//! Restores a fake VM from a memory file many times and times how long its working set takes to
//! come in, faulting every page one at a time and then with a trace prefetched, first with each
//! page copied into the VM's own memory and then with the file loaded into memory and each page
//! mapped in, as minor fault mode does. Then it restores VMs whose working set shifts from run to
//! run and compares prefetching the first restore's trace with prefetching one merged from every
//! restore so far.
//!
//! ```text
//! cargo run --release -p hive-uffd --example restore -- --dir /var/tmp/uffd --mib 1024 --hot 25 --runs 5
//! ```
//!
//! The working set is clusters of 16 pages, each in it with a chance of `--hot` percent, touched in
//! a shuffled order, which is roughly how a guest wakes up: some runs of neighbours, scattered.
//! The memory file is read once before the runs, so the page cache is warm for all of them, and
//! the numbers are what the server adds, not what the disk does. The private memory column is how
//! much the process's anonymous memory grew while a VM's working set came in, which is the memory
//! each VM restored that way costs on top of the shared file.
//!
//! For the shifting runs, `--varied` percent of the clusters outside the working set form a pool,
//! and each run touches each cluster in the pool with a chance of a half on top of the working set.

#[cfg(target_os = "linux")]
fn main() {
    if let Err(e) = linux::run() {
        eprintln!("restore: {e}");
        std::process::exit(1);
    }
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("restore only runs on Linux");
}

#[cfg(target_os = "linux")]
mod linux {
    use hive_uffd::{Guest, Memory, Merged, Session, Stats, Trace};
    use std::collections::HashSet;
    use std::io::{self, Write};
    use std::os::unix::net::UnixListener;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    const PAGE: usize = 4096;
    const CLUSTER: usize = 16;

    struct Args {
        dir: PathBuf,
        mib: usize,
        hot: u64,
        runs: usize,
        varied: u64,
    }

    fn args() -> Result<Args, String> {
        let mut a = Args {
            dir: PathBuf::from("/var/tmp/hive-uffd"),
            mib: 1024,
            hot: 25,
            runs: 5,
            varied: 10,
        };
        let mut it = std::env::args().skip(1);
        while let Some(arg) = it.next() {
            let v = it.next().ok_or(format!("{arg} needs a value"))?;
            let n = || v.parse::<usize>().map_err(|e| format!("{arg} {v}: {e}"));
            match arg.as_str() {
                "--dir" => a.dir = PathBuf::from(&v),
                "--mib" => a.mib = n()?,
                "--hot" => a.hot = n()? as u64,
                "--runs" => a.runs = n()?,
                "--varied" => a.varied = n()? as u64,
                other => return Err(format!("unknown argument {other}")),
            }
        }
        Ok(a)
    }

    /// A small fixed generator, so every run and every machine picks the same pages.
    struct Lcg(u64);

    impl Lcg {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            self.0 >> 33
        }
    }

    fn working_set(pages: usize, hot: u64) -> Vec<usize> {
        let mut g = Lcg(42);
        let mut clusters: Vec<usize> =
            (0..pages / CLUSTER).filter(|_| g.next() % 100 < hot).collect();
        for i in (1..clusters.len()).rev() {
            clusters.swap(i, (g.next() % (i as u64 + 1)) as usize);
        }
        clusters.into_iter().flat_map(|c| c * CLUSTER..(c + 1) * CLUSTER).collect()
    }

    /// The clusters outside `set` that a run may touch, each with a chance of `varied` percent.
    fn pool(pages: usize, set: &[usize], varied: u64) -> Vec<usize> {
        let hot: HashSet<usize> = set.iter().map(|p| p / CLUSTER).collect();
        let mut g = Lcg(7);
        (0..pages / CLUSTER).filter(|c| !hot.contains(c) && g.next() % 100 < varied).collect()
    }

    /// What run `run` touches: the working set, then about half the pool.
    fn shifted(set: &[usize], pool: &[usize], run: usize) -> Vec<usize> {
        let mut g = Lcg(1000 + run as u64);
        let mut out = set.to_vec();
        for &c in pool {
            if g.next().is_multiple_of(2) {
                out.extend(c * CLUSTER..(c + 1) * CLUSTER);
            }
        }
        out
    }

    fn write_memory(path: &Path, mib: usize) -> io::Result<()> {
        let mut f = io::BufWriter::new(std::fs::File::create(path)?);
        let mut page = vec![0u8; PAGE];
        for p in 0..mib * 256 {
            page.fill((p % 251) as u8 + 1);
            f.write_all(&page)?;
        }
        f.flush()
    }

    struct Run {
        total: Duration,
        private: u64,
        touches: Vec<Duration>,
        stats: Stats,
        trace: Trace,
    }

    /// The process's anonymous memory, in bytes.
    fn anonymous() -> u64 {
        let text = std::fs::read_to_string("/proc/self/smaps_rollup").unwrap_or_default();
        text.lines()
            .find_map(|l| l.strip_prefix("Anonymous:"))
            .and_then(|v| v.trim().trim_end_matches("kB").trim().parse::<u64>().ok())
            .map_or(0, |kib| kib << 10)
    }

    /// Restores one VM and touches `set`. With `mapped`, the VM maps the memory file privately and
    /// takes minor faults, and with no `mapped`, its memory is anonymous and every page is copied.
    fn restore(
        dir: &Path,
        memory: &Arc<Memory>,
        set: &[usize],
        trace: Option<&Trace>,
        mapped: bool,
    ) -> io::Result<Run> {
        let sock = dir.join("uffd.sock");
        let _ = std::fs::remove_file(&sock);
        let listener = UnixListener::bind(&sock)?;
        let (served, trace) = (memory.clone(), trace.cloned());
        let server = std::thread::spawn(move || -> io::Result<Session> {
            let (stream, _) = listener.accept()?;
            let mut s = Session::accept(stream, served)?;
            if let Some(t) = &trace {
                s.prefetch(t)?;
            }
            s.serve()?;
            Ok(s)
        });
        let before = anonymous();
        let began = Instant::now();
        let guest = if mapped {
            Guest::connect_mapped(&sock, memory)?
        } else {
            Guest::connect(&sock, set.iter().max().map_or(0, |m| m + 1).max(1) * PAGE)?
        };
        let mut touches = Vec::with_capacity(set.len());
        let mut sum = 0u64;
        for &p in set {
            let t = Instant::now();
            sum += u64::from(guest.touch(p * PAGE));
            touches.push(t.elapsed());
        }
        let total = began.elapsed();
        let private = anonymous().saturating_sub(before);
        assert!(sum > 0);
        drop(guest);
        let s = server.join().map_err(|_| io::Error::other("the server panicked"))??;
        Ok(Run { total, private, touches, stats: s.stats(), trace: s.trace() })
    }

    /// The same touches on fresh anonymous memory with no server, which is the kernel's own cost
    /// of a fault and the floor for any of this.
    fn anon(len: usize, set: &[usize]) -> (Duration, u64, Vec<Duration>) {
        let before = anonymous();
        let mut v: Vec<u8> = Vec::with_capacity(len);
        let base = v.as_mut_ptr();
        let began = Instant::now();
        let mut touches = Vec::with_capacity(set.len());
        for &p in set {
            let t = Instant::now();
            // SAFETY: the page is inside the vector's capacity, and a write needs no initialized
            // memory.
            unsafe { base.add(p * PAGE).write_volatile(1) };
            touches.push(t.elapsed());
        }
        let took = began.elapsed();
        (took, anonymous().saturating_sub(before), touches)
    }

    fn pct(v: &mut [Duration], p: f64) -> f64 {
        v.sort_unstable();
        let i = ((v.len() as f64 - 1.0) * p).round() as usize;
        v.get(i).map_or(0.0, |d| d.as_secs_f64() * 1e6)
    }

    fn median(mut v: Vec<f64>) -> f64 {
        v.sort_by(f64::total_cmp);
        v[v.len() / 2]
    }

    pub(super) fn run() -> Result<(), String> {
        let a = args()?;
        let e = |e: io::Error| e.to_string();
        std::fs::create_dir_all(&a.dir).map_err(e)?;
        let path = a.dir.join("mem");
        write_memory(&path, a.mib).map_err(e)?;
        let pages = a.mib * 256;
        let set = working_set(pages, a.hot);
        let memory = Arc::new(Memory::open(&path).map_err(e)?);
        // Warms the page cache, so every run reads the file from memory.
        let _ = std::fs::read(&path).map_err(e)?;
        let t = Instant::now();
        let loaded = Arc::new(Memory::load(&path).map_err(e)?);
        let load = t.elapsed();
        println!(
            "memory {} MiB, working set {} pages ({} MiB, {:.1}%), {} runs each",
            a.mib,
            set.len(),
            (set.len() * PAGE) >> 20,
            set.len() as f64 * 100.0 / pages as f64,
            a.runs
        );
        println!("loading the file into memory took {:.1} ms", load.as_secs_f64() * 1e3);
        println!(
            "| mode | working set in, ms | touch p50 us | touch p99 us | faults | prefetched MiB | private MiB |"
        );
        println!("|---|---|---|---|---|---|---|");
        let mut trace = None;
        for mode in ["anon", "fault", "prefetch", "mapped", "mapped-prefetch"] {
            let (mut totals, mut touches, mut privates) = (vec![], vec![], vec![]);
            let (mut faults, mut prefetched) = (0, 0);
            let mapped = mode.starts_with("mapped");
            let prefetch = mode.ends_with("prefetch");
            for _ in 0..a.runs {
                if mode == "anon" {
                    let (t, private, mut v) = anon(pages * PAGE, &set);
                    totals.push(t.as_secs_f64() * 1e3);
                    privates.push(private as f64);
                    touches.append(&mut v);
                    continue;
                }
                let from = if mapped { &loaded } else { &memory };
                let r = restore(&a.dir, from, &set, trace.as_ref().filter(|_| prefetch), mapped)
                    .map_err(e)?;
                totals.push(r.total.as_secs_f64() * 1e3);
                privates.push(r.private as f64);
                touches.extend(r.touches);
                faults = r.stats.faults;
                prefetched = r.stats.prefetched >> 20;
                if trace.is_none() {
                    trace = Some(r.trace);
                }
            }
            println!(
                "| {mode} | {:.1} | {:.2} | {:.2} | {} | {} | {:.1} |",
                median(totals),
                pct(&mut touches, 0.5),
                pct(&mut touches, 0.99),
                if mode == "anon" { "-".into() } else { faults.to_string() },
                if prefetch { prefetched.to_string() } else { "-".into() },
                median(privates) / f64::from(1 << 20)
            );
        }
        let pool = pool(pages, &set, a.varied);
        println!();
        println!(
            "shifting runs, mapped: the working set plus about half of a pool of {} pages",
            pool.len() * CLUSTER
        );
        println!(
            "| run | touched | first: missed | first: extra | first: in, ms | merged: pages | merged: missed | merged: extra | merged: in, ms |"
        );
        println!("|---|---|---|---|---|---|---|---|---|");
        // The touched pages a trace left out, and the pages it filled in that went untouched.
        let compare = |trace: Option<&Trace>, touched: &[usize]| {
            let has: HashSet<u64> = trace.iter().flat_map(|t| t.0.iter().copied()).collect();
            let want: HashSet<u64> = touched.iter().map(|&p| (p * PAGE) as u64).collect();
            (want.difference(&has).count(), has.difference(&want).count())
        };
        let (mut first, mut merged) = (None::<Trace>, Merged::new(None, 2));
        for run in 0..a.runs * 2 {
            let touched = shifted(&set, &pool, run);
            let one = restore(&a.dir, &loaded, &touched, first.as_ref(), true).map_err(e)?;
            let size = merged.trace().map_or(0, |t| t.0.len());
            let (missed1, extra1) = compare(first.as_ref(), &touched);
            let (missed2, extra2) = compare(merged.trace(), &touched);
            let two = restore(&a.dir, &loaded, &touched, merged.trace(), true).map_err(e)?;
            if first.is_none() {
                first = Some(one.trace);
            }
            merged.add(&two.trace);
            println!(
                "| {} | {} | {missed1} | {extra1} | {:.1} | {size} | {missed2} | {extra2} | {:.1} |",
                run + 1,
                touched.len(),
                one.total.as_secs_f64() * 1e3,
                two.total.as_secs_f64() * 1e3
            );
        }
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(a.dir.join("uffd.sock"));
        Ok(())
    }
}
