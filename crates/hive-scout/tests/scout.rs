//! Scout against made up reports.

use std::sync::Arc;
use std::time::Duration;

use hive_scout::{Applied, FORGET_AFTER, NodeReport, STALE_AFTER, Scout};
use hive_types::{Backend, Resources};
use hive_waggle::{BackendSet, LayerBloom, PlaceReq, Placer};

fn report(node: u16, seq: u64) -> NodeReport {
    NodeReport {
        node,
        epoch: 1,
        seq,
        addr: Arc::from(format!("10.0.0.{node}:7400")),
        healthy: true,
        backends: BackendSet::of(&[Backend::Container]),
        cpu_milli: 64_000,
        cpu_committed_milli: 0,
        mem_admit_mib: 100_000,
        mem_committed_mib: 0,
        cells: 0,
        max_cells: 1000,
        pool_depth: 64,
        create_rate: 0.0,
        burst_cap: 300,
        layers: None,
        top_projects: Vec::new(),
    }
}

fn secs(s: u64) -> Duration {
    Duration::from_secs(s)
}

#[test]
fn a_report_shows_in_the_next_snapshot_and_nothing_new_publishes_nothing() {
    let mut scout = Scout::new();
    let mut rx = scout.subscribe();
    assert_eq!(scout.apply(report(3, 1), secs(1)), Applied::Urgent);
    let snap = scout.tick(secs(1)).expect("a new node publishes");
    assert_eq!(snap.version, 1);
    assert_eq!(snap.view.nodes.len(), 1);
    assert_eq!(snap.view.nodes[0].node, 3);
    assert_eq!(snap.addr(3).map(|a| &**a), Some("10.0.0.3:7400"));
    assert!(snap.addr(4).is_none());
    assert!(rx.has_changed().unwrap());
    assert_eq!(rx.borrow_and_update().version, 1);
    assert!(scout.tick(secs(1)).is_none());
    assert_eq!(scout.snapshot().version, 1);
}

#[test]
fn late_and_old_epoch_reports_are_dropped() {
    let mut scout = Scout::new();
    scout.apply(report(0, 5), secs(1));
    assert_eq!(scout.apply(report(0, 5), secs(1)), Applied::Stale);
    assert_eq!(scout.apply(report(0, 4), secs(1)), Applied::Stale);
    // The comb registered again, so its sequence starts over.
    let restarted = NodeReport { epoch: 2, ..report(0, 1) };
    assert_eq!(scout.apply(restarted, secs(2)), Applied::Urgent);
    assert_eq!(scout.apply(NodeReport { epoch: 1, ..report(0, 9) }, secs(2)), Applied::Stale);
    assert_eq!(scout.apply(NodeReport { epoch: 2, ..report(0, 2) }, secs(3)), Applied::Taken);
}

#[test]
fn layers_carry_over_until_they_change_or_the_comb_restarts() {
    let digest = *blake3::hash(b"python:3.12").as_bytes();
    let mut bloom = LayerBloom::default();
    bloom.insert(&digest);
    let mut scout = Scout::new();
    scout.apply(NodeReport { layers: Some(bloom), ..report(0, 1) }, secs(1));
    scout.apply(report(0, 2), secs(2));
    assert!(scout.tick(secs(2)).unwrap().view.nodes[0].layers.contains(&digest));
    // A new epoch is a new comb whose cache may be empty.
    scout.apply(NodeReport { epoch: 2, ..report(0, 1) }, secs(3));
    assert!(!scout.tick(secs(3)).unwrap().view.nodes[0].layers.contains(&digest));
}

#[test]
fn a_quiet_node_goes_down_comes_back_and_is_forgotten() {
    let mut scout = Scout::new();
    scout.apply(report(0, 1), secs(10));
    scout.apply(report(1, 1), secs(10));
    scout.tick(secs(10));
    assert!(scout.tick(secs(10) + STALE_AFTER - Duration::from_millis(1)).is_none());
    scout.apply(report(1, 2), secs(12));
    let snap = scout.tick(secs(10) + STALE_AFTER).expect("node 0 went down");
    assert!(!snap.view.nodes[0].healthy);
    assert!(snap.view.nodes[1].healthy);
    assert_eq!((snap.totals.nodes, snap.totals.healthy), (2, 1));
    // Coming back is urgent, so placement sees it at once.
    assert_eq!(scout.apply(report(0, 2), secs(14)), Applied::Urgent);
    assert!(scout.tick(secs(14)).unwrap().view.nodes[0].healthy);
    scout.apply(report(1, 3), secs(14) + FORGET_AFTER);
    let snap = scout.tick(secs(14) + FORGET_AFTER).unwrap();
    assert_eq!(snap.view.nodes.len(), 1);
    assert_eq!(snap.view.nodes[0].node, 1);
}

#[test]
fn only_big_changes_are_urgent() {
    let mut scout = Scout::new();
    scout.apply(report(0, 1), secs(1));
    // 4% of memory moved.
    assert_eq!(
        scout.apply(NodeReport { mem_committed_mib: 4_000, ..report(0, 2) }, secs(2)),
        Applied::Taken
    );
    // Then 6% more.
    assert_eq!(
        scout.apply(NodeReport { mem_committed_mib: 10_000, ..report(0, 3) }, secs(3)),
        Applied::Urgent
    );
    assert_eq!(
        scout.apply(NodeReport { mem_committed_mib: 10_000, cells: 60, ..report(0, 4) }, secs(4)),
        Applied::Urgent
    );
    assert_eq!(
        scout.apply(
            NodeReport { mem_committed_mib: 10_000, cells: 60, healthy: false, ..report(0, 5) },
            secs(5)
        ),
        Applied::Urgent
    );
}

#[test]
fn project_cells_add_up_over_the_healthy_nodes() {
    let mut scout = Scout::new();
    scout.apply(
        NodeReport { cells: 30, top_projects: vec![(7, 20), (8, 10)], ..report(0, 1) },
        secs(1),
    );
    scout.apply(NodeReport { cells: 5, top_projects: vec![(7, 5)], ..report(1, 1) }, secs(1));
    scout.apply(
        NodeReport { cells: 9, healthy: false, top_projects: vec![(7, 9)], ..report(2, 1) },
        secs(1),
    );
    let snap = scout.tick(secs(1)).unwrap();
    assert_eq!(snap.projects.get(&7), Some(&25));
    assert_eq!(snap.projects.get(&8), Some(&10));
    assert_eq!(snap.totals.cells, 35);
    assert_eq!(snap.totals.mem_admit_mib, 200_000);
}

#[test]
fn a_newer_report_clears_what_waggle_placed_against_the_older_one() {
    let mut scout = Scout::new();
    scout.apply(NodeReport { max_cells: 10, ..report(0, 1) }, secs(1));
    let snap = scout.tick(secs(1)).unwrap();
    let mut placer = Placer::new(1);
    let req = PlaceReq {
        backend: Backend::Container,
        resources: Resources { vcpu_milli: 100, mem_mib: 64, ..Resources::default() },
        n: 10,
        layers: &[],
        project: 1,
        affinity: None,
        exclude: &[],
    };
    assert_eq!(placer.place(&snap.view, &req, secs(1)).unplaced, 0);
    // Until the node reports them, it is full.
    assert_eq!(placer.place(&snap.view, &req, secs(1)).unplaced, 10);
    scout.apply(NodeReport { max_cells: 20, cells: 10, ..report(0, 2) }, secs(2));
    let snap = scout.tick(secs(2)).unwrap();
    assert_eq!(placer.place(&snap.view, &req, secs(2)).unplaced, 0);
}
