//! How long a placement takes on clusters of a few sizes, half full with some layers cached.
//! Wall time counts the moments the host takes the thread away, so the thread's CPU time is shown
//! next to it. Then how long one rebalance round takes on clusters with every share from empty
//! to full.
//!
//! ```text
//! cargo run --release -p hive-waggle --example bench
//! ```

use std::time::{Duration, Instant};

use hive_types::{Backend, Resources};
use hive_waggle::{ClusterView, NodeView, PlaceReq, Placer, Rebalancer};
use rustix::time::{ClockId, clock_gettime};

fn cpu() -> Duration {
    let t = clock_gettime(ClockId::ThreadCPUTime);
    Duration::new(t.tv_sec as u64, t.tv_nsec as u32)
}

fn main() {
    let layers: Vec<[u8; 32]> = (0..8u8).map(|i| *blake3::hash(&[i]).as_bytes()).collect();
    println!(
        "| nodes | cells a batch | placements | wall p50 us | wall p99 us | cpu p50 us | cpu p99 us | cpu max us | per cpu second |"
    );
    println!("|---|---|---|---|---|---|---|---|---|");
    for nodes in [16u16, 160, 1000] {
        for n in [1u32, 32, 1000, 32_000] {
            let mut view = ClusterView {
                nodes: (0..nodes).map(|i| NodeView::empty(i, 96_000, 384 * 1024)).collect(),
            };
            for (i, node) in view.nodes.iter_mut().enumerate() {
                node.mem_committed_mib = (i as u64 * 7919) % node.mem_admit_mib;
                node.cells = (i as u32 * 31) % 2000;
                node.top_projects = vec![(1, node.cells / 3), (2, node.cells / 5)];
                for d in layers.iter().skip(i % 3) {
                    node.layers.insert(d);
                }
            }
            let req = PlaceReq {
                backend: Backend::Container,
                resources: Resources { vcpu_milli: 1000, mem_mib: 512, ..Resources::default() },
                n,
                layers: &layers,
                image: None,
                project: 1,
                affinity: None,
                exclude: &[],
            };
            let mut placer = Placer::new(1);
            let mut took = Vec::new();
            let mut used = Vec::new();
            let start = Instant::now();
            let mut now = Duration::from_secs(1);
            while start.elapsed() < Duration::from_secs(2) || took.len() < 100 {
                // A fresh report from every node every 10 placements, as scout would send.
                now += Duration::from_millis(10);
                if took.len() % 10 == 0 {
                    for node in &mut view.nodes {
                        node.report += 1;
                    }
                }
                let (t, c) = (Instant::now(), cpu());
                let p = placer.place(&view, &req, now);
                used.push(cpu() - c);
                took.push(t.elapsed());
                std::hint::black_box(p);
            }
            took.sort();
            used.sort();
            let at =
                |v: &[Duration], q: f64| v[((v.len() - 1) as f64 * q) as usize].as_secs_f64() * 1e6;
            let per = used.len() as f64 / used.iter().sum::<Duration>().as_secs_f64();
            println!(
                "| {nodes} | {n} | {} | {:.1} | {:.1} | {:.1} | {:.1} | {:.1} | {:.0} |",
                took.len(),
                at(&took, 0.5),
                at(&took, 0.99),
                at(&used, 0.5),
                at(&used, 0.99),
                at(&used, 1.0),
                per
            );
        }
    }
    rebalance();
}

fn rebalance() {
    println!();
    println!("| nodes | hot | moves | cells moved | cpu p50 us | cpu max us |");
    println!("|---|---|---|---|---|---|");
    for nodes in [1000u16, 10_000, 50_000] {
        // Node i is (37 i mod 100)% full, so some are hot and some empty, with up to 31 idle cells.
        let view = ClusterView {
            nodes: (0..nodes)
                .map(|i| {
                    let admit = 64_000;
                    let used = admit * (u64::from(i) * 37 % 100) / 100;
                    let idle = u32::from(i % 32);
                    NodeView {
                        mem_committed_mib: used,
                        idle_cells: idle,
                        idle_mem_mib: (u64::from(idle) * 512).min(used),
                        ..NodeView::empty(i, 64_000, admit)
                    }
                })
                .collect(),
        };
        let mut r = Rebalancer::new();
        let mut used = Vec::new();
        let mut plan = r.plan(&view);
        for _ in 0..50 {
            let c = cpu();
            plan = r.plan(&view);
            used.push(cpu() - c);
        }
        used.sort_unstable();
        println!(
            "| {nodes} | {} | {} | {} | {:.1} | {:.1} |",
            plan.hot,
            plan.moves.len(),
            plan.cells(),
            used[used.len() / 2].as_secs_f64() * 1e6,
            used[used.len() - 1].as_secs_f64() * 1e6
        );
    }
}
