//! Seeds of the simulation, each of which must hold every invariant. These are few enough to
//! run unoptimized, and `hive-sim --seeds N` runs as many as wanted from the release build.

use hive_sim::{Config, Faults, run};

#[test]
fn seeds_hold_the_invariants() {
    let mut bad = Vec::new();
    for seed in 1..=60 {
        let r = run(&Config { seed, ..Config::default() });
        assert!(r.cells > 0, "seed {seed} made no cells");
        if let Some(v) = r.violations.first() {
            bad.push(format!("seed {seed}: {v}"));
        }
    }
    assert!(bad.is_empty(), "{} seeds broke an invariant:\n{}", bad.len(), bad.join("\n"));
}

#[test]
fn without_faults_every_create_is_answered_and_nothing_goes_over() {
    for seed in 1..=20 {
        let r = run(&Config { seed, faults: Faults::none(), ..Config::default() });
        assert!(r.violations.is_empty(), "seed {seed}: {:?}", r.violations);
        assert_eq!(r.failed, 0, "seed {seed}");
        assert_eq!(r.unchecked, 0, "seed {seed}");
        assert_eq!(r.fenced_cells, 0, "seed {seed}");
        assert_eq!((r.moved, r.forgot), (0, 0), "seed {seed}");
    }
}

#[test]
fn a_seed_runs_the_same_every_time() {
    for seed in [7, 42, 1009] {
        let a = run(&Config { seed, ..Config::default() });
        let b = run(&Config { seed, ..Config::default() });
        assert_eq!(a.digest, b.digest, "seed {seed}");
        assert_eq!((a.events, a.cells, a.created), (b.events, b.cells, b.created), "seed {seed}");
    }
}
