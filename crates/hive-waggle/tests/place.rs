//! Placement against made up clusters.

use std::collections::HashMap;
use std::time::Duration;

use hive_types::{Backend, Resources};
use hive_waggle::{BackendSet, ClusterView, NodeView, PlaceReq, Placement, Placer};

fn res(vcpu_milli: u32, mem_mib: u32) -> Resources {
    Resources { vcpu_milli, mem_mib, ..Resources::default() }
}

fn req(n: u32, r: Resources) -> PlaceReq<'static> {
    PlaceReq {
        backend: Backend::Container,
        resources: r,
        n,
        layers: &[],
        project: 1,
        affinity: None,
        exclude: &[],
    }
}

fn cluster(nodes: u16) -> ClusterView {
    ClusterView { nodes: (0..nodes).map(|i| NodeView::empty(i, 64_000, 256 * 1024)).collect() }
}

fn total(p: &Placement) -> u32 {
    p.nodes.iter().map(|(_, n)| n).sum()
}

const T0: Duration = Duration::from_secs(100);

/// A small generator so the test needs no crate for it.
struct Gen(u64);

impl Gen {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

#[test]
fn no_node_is_ever_given_more_than_it_has_room_for() {
    for seed in 1..=300u64 {
        let mut g = Gen(seed);
        let n = 1 + g.below(40) as u16;
        let mut view = cluster(n);
        for node in &mut view.nodes {
            node.mem_admit_mib = 1024 * (1 + g.below(64));
            node.mem_committed_mib = g.below(node.mem_admit_mib + 1);
            node.max_cells = 1 + g.below(200) as u32;
            node.cells = g.below(u64::from(node.max_cells) + 1) as u32;
            node.burst_cap = 1 + g.below(300) as u32;
            node.healthy = g.below(10) > 0;
        }
        let mut placer = Placer::new(seed);
        let mut given: HashMap<u16, (u32, u64)> = HashMap::new();
        // Several batches with no report in between, so only the overlay keeps them apart.
        for _ in 0..5 {
            let r = res(500, 256 * (1 + g.below(8) as u32));
            let want = 1 + g.below(2000) as u32;
            let p = placer.place(&view, &req(want, r), T0);
            assert_eq!(total(&p) + p.unplaced, want, "seed {seed}");
            for &(node, cells) in &p.nodes {
                let e = given.entry(node).or_default();
                e.0 += cells;
                e.1 += u64::from(cells) * u64::from(r.mem_mib);
            }
        }
        for (node, (cells, mem)) in given {
            let v = &view.nodes[usize::from(node)];
            assert!(v.healthy, "seed {seed}: node {node} is down");
            assert!(v.cells + cells <= v.max_cells, "seed {seed}: node {node} has too many cells");
            assert!(
                v.mem_committed_mib + mem <= v.mem_admit_mib,
                "seed {seed}: node {node} memory"
            );
        }
    }
}

#[test]
fn a_batch_fills_the_room_there_is_and_says_what_did_not_fit() {
    let mut view = cluster(4);
    for node in &mut view.nodes {
        node.max_cells = 350;
    }
    let p = Placer::new(1).place(&view, &req(1500, res(100, 64)), T0);
    assert_eq!(total(&p), 1400);
    assert_eq!(p.unplaced, 100);
    assert!(p.nodes.iter().all(|&(_, n)| n == 350));
}

#[test]
fn burst_caps_spread_a_batch_and_only_a_bigger_one_goes_past_them() {
    let view = cluster(4);
    // Each node takes its burst cap of 300 first, so 1,200 go out evenly.
    let p = Placer::new(2).place(&view, &req(1200, res(100, 64)), T0);
    assert!(p.nodes.iter().all(|&(_, n)| n == 300), "{:?}", p.nodes);
    // Past the caps the combs queue what is left rather than it being turned away.
    let mut placer = Placer::new(2);
    let p = placer.place(&view, &req(2000, res(100, 64)), T0);
    assert_eq!(total(&p), 2000);
    assert!(p.nodes.iter().all(|&(_, n)| n >= 300), "{:?}", p.nodes);
    // One node alone, as in a small cluster, still takes more than its cap.
    let one = cluster(1);
    let p = Placer::new(3).place(&one, &req(700, res(100, 64)), T0);
    assert_eq!(total(&p), 700);
}

#[test]
fn down_excluded_and_wrong_backend_nodes_get_nothing() {
    let mut view = cluster(6);
    view.nodes[0].healthy = false;
    view.nodes[1].backends = BackendSet::of(&[Backend::Microvm]);
    view.nodes[2].mem_committed_mib = view.nodes[2].mem_admit_mib;
    let exclude = [3];
    let mut placer = Placer::new(7);
    let p = placer.place(&view, &PlaceReq { exclude: &exclude, ..req(400, res(100, 64)) }, T0);
    let used: Vec<u16> = p.nodes.iter().map(|(n, _)| *n).collect();
    assert!(used.iter().all(|n| *n == 4 || *n == 5), "{used:?}");
    assert_eq!(total(&p), 400);
}

#[test]
fn a_big_batch_spreads_over_many_nodes() {
    let view = cluster(160);
    let mut placer = Placer::new(3);
    let p = placer.place(&view, &req(32_000, res(1000, 512)), T0);
    assert_eq!(total(&p), 32_000);
    // A burst cap of 300 means at least 107 nodes.
    assert!(p.nodes.len() >= 107, "{} nodes", p.nodes.len());
    let most = p.nodes.iter().map(|(_, n)| *n).max().unwrap();
    assert!(most <= 300);
}

#[test]
fn back_to_back_single_cells_do_not_pile_onto_one_node() {
    let view = cluster(8);
    let mut placer = Placer::new(5);
    let mut per: HashMap<u16, u32> = HashMap::new();
    for _ in 0..64 {
        let p = placer.place(&view, &req(1, res(1000, 1024)), T0);
        *per.entry(p.nodes[0].0).or_default() += 1;
    }
    // With the overlay every node ends up with about 8. Without it, all 64 would go to one.
    assert_eq!(per.len(), 8, "{per:?}");
    assert!(per.values().all(|n| (6..=10).contains(n)), "{per:?}");
    assert_eq!(placer.inflight(), 64);
}

#[test]
fn the_overlay_clears_on_a_newer_report_or_after_a_while() {
    let mut view = cluster(1);
    view.nodes[0].max_cells = 10;
    let mut placer = Placer::new(9);
    assert_eq!(total(&placer.place(&view, &req(10, res(100, 64)), T0)), 10);
    // Full until the node says it has them, or the entry runs out.
    assert_eq!(total(&placer.place(&view, &req(1, res(100, 64)), T0)), 0);
    let later = T0 + hive_waggle::INFLIGHT_TTL;
    assert_eq!(total(&placer.place(&view, &req(1, res(100, 64)), later)), 1);
    view.nodes[0].report += 1;
    assert_eq!(total(&placer.place(&view, &req(10, res(100, 64)), later)), 10);
}

#[test]
fn what_a_comb_refused_is_room_again() {
    let mut view = cluster(1);
    view.nodes[0].max_cells = 10;
    let mut placer = Placer::new(11);
    placer.place(&view, &req(10, res(100, 64)), T0);
    placer.refused(0, 4, &res(100, 64));
    assert_eq!(placer.inflight(), 6);
    view.nodes[0].report = 0;
    assert_eq!(total(&placer.place(&view, &req(10, res(100, 64)), T0)), 4);
}

#[test]
fn cached_layers_draw_cells_when_the_cluster_is_quiet_but_not_when_it_is_busy() {
    let layers = [*blake3::hash(b"python:3.12").as_bytes(), *blake3::hash(b"swe").as_bytes()];
    let run = |used: f64| {
        let mut view = cluster(8);
        for node in &mut view.nodes {
            node.mem_committed_mib = (node.mem_admit_mib as f64 * used) as u64;
        }
        // Node 0 has the image but 15% less free memory than the rest.
        for d in &layers {
            view.nodes[0].layers.insert(d);
        }
        view.nodes[0].mem_committed_mib += view.nodes[0].mem_admit_mib * 15 / 100;
        let mut placer = Placer::new(13);
        let p = placer.place(&view, &PlaceReq { layers: &layers, ..req(1, res(1000, 512)) }, T0);
        p.nodes[0].0
    };
    assert_eq!(run(0.1), 0, "quiet: the node with the layers wins");
    assert_ne!(run(0.8), 0, "busy: the node with the most room wins");
}

#[test]
fn affinity_wins_when_the_node_has_room() {
    let view = cluster(50);
    let mut placer = Placer::new(17);
    let p = placer.place(&view, &PlaceReq { affinity: Some(42), ..req(1, res(1000, 512)) }, T0);
    assert_eq!(p.nodes, vec![(42, 1)]);
}

#[test]
fn the_same_seed_and_view_give_the_same_placement() {
    let mut view = cluster(300);
    for (i, node) in view.nodes.iter_mut().enumerate() {
        node.mem_committed_mib = (i as u64 * 7919) % node.mem_admit_mib;
    }
    let a = Placer::new(21).place(&view, &req(50, res(1000, 512)), T0);
    let b = Placer::new(21).place(&view, &req(50, res(1000, 512)), T0);
    let c = Placer::new(22).place(&view, &req(50, res(1000, 512)), T0);
    assert_eq!(a, b);
    assert_ne!(a, c, "another seed samples other nodes");
}
