//! What scout costs at a few cluster sizes: taking one report, and building and publishing a
//! snapshot after every node reported once. Times are the thread's CPU time, since wall time on
//! a busy host counts the moments it takes the thread away.
//!
//! ```text
//! cargo run --release -p hive-scout --example bench
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use hive_scout::{NodeReport, Scout};
use hive_types::Backend;
use hive_waggle::{BackendSet, LayerBloom};
use rustix::time::{ClockId, clock_gettime};

fn cpu() -> Duration {
    let t = clock_gettime(ClockId::ThreadCPUTime);
    Duration::new(t.tv_sec as u64, t.tv_nsec as u32)
}

fn at(v: &mut [Duration], q: f64) -> f64 {
    v.sort();
    v[((v.len() - 1) as f64 * q) as usize].as_secs_f64() * 1e6
}

fn main() {
    println!(
        "| nodes | reports | report p50 us | report p99 us | snapshots | snapshot p50 us | snapshot p99 us |"
    );
    println!("|---|---|---|---|---|---|---|");
    for nodes in [16u16, 160, 1000, 4000] {
        let mut scout = Scout::new();
        let mut bloom = LayerBloom::default();
        for i in 0..64u8 {
            bloom.insert(blake3::hash(&[i]).as_bytes());
        }
        let (mut per_report, mut per_snap) = (Vec::new(), Vec::new());
        let start = Instant::now();
        let mut round = 0u64;
        while start.elapsed() < Duration::from_secs(3) || per_snap.len() < 50 {
            round += 1;
            let now = Duration::from_secs(round);
            for node in 0..nodes {
                let report = NodeReport {
                    node,
                    epoch: 1,
                    seq: round,
                    addr: Arc::from("10.0.0.1:7400"),
                    healthy: true,
                    backends: BackendSet::of(&[Backend::Container, Backend::Microvm]),
                    cpu_milli: 96_000,
                    cpu_committed_milli: 40_000,
                    mem_admit_mib: 384 * 1024,
                    mem_committed_mib: (u64::from(node) * 7919 + round) % (384 * 1024),
                    cells: (u32::from(node) * 31) % 2000,
                    max_cells: 4096,
                    pool_depth: 64,
                    create_rate: 12.0,
                    burst_cap: 300,
                    // One report in ten carries the layers, as when a pull lands.
                    layers: (round % 10 == 1).then(|| bloom.clone()),
                    top_projects: vec![(1, 300), (2, 120), (3, 40), (4, 12)],
                };
                let c = cpu();
                std::hint::black_box(scout.apply(report, now));
                per_report.push(cpu() - c);
            }
            let c = cpu();
            std::hint::black_box(scout.tick(now));
            per_snap.push(cpu() - c);
        }
        let reports = per_report.len();
        let snaps = per_snap.len();
        println!(
            "| {nodes} | {reports} | {:.2} | {:.2} | {snaps} | {:.1} | {:.1} |",
            at(&mut per_report, 0.5),
            at(&mut per_report, 0.99),
            at(&mut per_snap, 0.5),
            at(&mut per_snap, 0.99),
        );
    }
}
