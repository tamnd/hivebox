//! The offline rebalancer: which idle cells move off hot nodes, and when it says to burst.

use std::collections::HashMap;

use hive_waggle::{BURST_ROUNDS, Burst, ClusterView, Move, NodeView, Rebalancer};

/// A node that admits 10000 MiB with `used` given out, `idle` cells of it idle at 256 MiB each.
fn node(id: u16, used: u64, idle: u32) -> NodeView {
    NodeView {
        mem_committed_mib: used,
        idle_cells: idle,
        idle_mem_mib: u64::from(idle) * 256,
        ..NodeView::empty(id, 16_000, 10_000)
    }
}

fn view(nodes: Vec<NodeView>) -> ClusterView {
    ClusterView { nodes }
}

#[test]
fn a_hot_node_sends_idle_cells_to_the_emptiest_nodes_until_it_is_level() {
    // 21800 of 40000 is 54.5%, so each node's level is 5450 MiB. Node 1 is 4350 over it, 17
    // cells of 256. Node 2 has room for 13 and node 3 then takes the other 4.
    let v = view(vec![node(1, 9800, 20), node(2, 2000, 0), node(3, 4000, 0), node(4, 6000, 0)]);
    let plan = Rebalancer::new().plan(&v);
    assert_eq!(plan.hot, 1);
    assert!((plan.share - 0.545).abs() < 1e-9);
    assert_eq!(
        plan.moves,
        [
            Move { from: 1, to: 2, cells: 13, mem_mib: 3328 },
            Move { from: 1, to: 3, cells: 4, mem_mib: 1024 },
        ]
    );
    assert_eq!((plan.cells(), plan.mem_mib()), (17, 4352));
    assert!(plan.burst.is_none());
}

#[test]
fn a_hot_node_gives_no_more_than_its_idle_cells() {
    let v = view(vec![node(1, 9800, 3), node(2, 2000, 0)]);
    let plan = Rebalancer::new().plan(&v);
    assert_eq!(plan.moves, [Move { from: 1, to: 2, cells: 3, mem_mib: 768 }]);
}

#[test]
fn nothing_moves_without_idle_cells_or_a_cool_node() {
    let mut r = Rebalancer::new();
    // Hot, but every cell is busy.
    let plan = r.plan(&view(vec![node(1, 9800, 0), node(2, 2000, 0)]));
    assert_eq!((plan.hot, plan.moves.len()), (1, 0));
    // Every node equally hot, so none is below the level to take anything.
    let plan = r.plan(&view((1..=4).map(|i| node(i, 9500, 10)).collect()));
    assert_eq!((plan.hot, plan.moves.len()), (4, 0));
    // Nobody hot.
    let plan = r.plan(&view(vec![node(1, 8000, 10), node(2, 1000, 0)]));
    assert_eq!((plan.hot, plan.moves.len()), (0, 0));
}

#[test]
fn cloud_and_down_nodes_take_nothing() {
    let cloud = NodeView { cloud: true, ..node(5, 0, 0) };
    let down = NodeView { healthy: false, ..node(6, 0, 0) };
    let plan =
        Rebalancer::new().plan(&view(vec![node(1, 9800, 20), node(2, 7000, 0), cloud, down]));
    // On-prem is 16800 of 20000, so node 2 has 1400 MiB of room to its level of 8400.
    assert_eq!(plan.moves, [Move { from: 1, to: 2, cells: 5, mem_mib: 1280 }]);
}

#[test]
fn a_burst_is_said_only_after_the_share_stays_past_the_line() {
    let cloud = NodeView { cloud: true, mem_committed_mib: 1000, ..node(9, 0, 0) };
    // 34000 of 40000 is 85%, 2000 MiB over the 80% line.
    let hot = view(vec![
        node(1, 8500, 4),
        node(2, 8500, 0),
        node(3, 8500, 2),
        node(4, 8500, 0),
        cloud.clone(),
    ]);
    let mut r = Rebalancer::new();
    for _ in 1..BURST_ROUNDS {
        assert!(r.plan(&hot).burst.is_none());
    }
    let want = Burst { rounds: BURST_ROUNDS, over_mib: 2000, idle_mib: 1536, cloud_room_mib: 9000 };
    assert_eq!(r.plan(&hot).burst, Some(want));
    assert_eq!(r.plan(&hot).burst.map(|b| b.rounds), Some(BURST_ROUNDS + 1));
    // One round below the line starts the count again.
    let cool = view(vec![node(1, 1000, 0), node(2, 1000, 0), cloud]);
    assert!(r.plan(&cool).burst.is_none());
    assert!(r.plan(&hot).burst.is_none());
    // Over 1 never bursts.
    let mut never = Rebalancer::new().with_burst_above(1.01);
    for _ in 0..BURST_ROUNDS * 2 {
        assert!(never.plan(&hot).burst.is_none());
    }
}

#[test]
fn no_move_takes_a_node_past_the_level_or_a_hot_node_past_its_idle_cells() {
    // Random clusters of a few hundred nodes, checked against what the plan promises.
    let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
    let mut next = |m: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % m
    };
    for round in 0..200 {
        let n = 2 + next(400) as u16;
        let nodes: Vec<NodeView> = (0..n)
            .map(|i| {
                let admit = 4096 + next(60_000);
                let used = next(admit + 1);
                let idle = next(64) as u32;
                NodeView {
                    mem_committed_mib: used,
                    idle_cells: idle,
                    idle_mem_mib: (u64::from(idle) * (64 + next(2048))).min(used),
                    ..NodeView::empty(i, 16_000, admit)
                }
            })
            .collect();
        let used: u64 = nodes.iter().map(|n| n.mem_committed_mib).sum();
        let total: u64 = nodes.iter().map(|n| n.mem_admit_mib).sum();
        let share = used as f64 / total as f64;
        let plan = Rebalancer::new().plan(&view(nodes.clone()));
        assert_eq!(plan, Rebalancer::new().plan(&view(nodes.clone())), "round {round}");
        let by: HashMap<u16, &NodeView> = nodes.iter().map(|n| (n.node, n)).collect();
        let mut gave: HashMap<u16, (u64, u64)> = HashMap::new();
        let mut took: HashMap<u16, u64> = HashMap::new();
        for m in &plan.moves {
            assert!(m.cells > 0 && m.from != m.to, "round {round}: {m:?}");
            let g = gave.entry(m.from).or_default();
            g.0 += u64::from(m.cells);
            g.1 += m.mem_mib;
            *took.entry(m.to).or_default() += m.mem_mib;
        }
        for (node, (cells, _)) in &gave {
            let n = by[node];
            assert!(n.mem_committed_mib as f64 / n.mem_admit_mib as f64 > 0.9, "round {round}");
            assert!(*cells <= u64::from(n.idle_cells), "round {round}");
        }
        for (node, mem) in &took {
            let n = by[node];
            assert!(!gave.contains_key(node), "round {round}");
            let level = (n.mem_admit_mib as f64 * share) as u64;
            assert!(n.mem_committed_mib + mem <= level, "round {round}: node {node}");
        }
    }
}
