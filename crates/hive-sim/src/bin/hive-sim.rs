//! Runs seeds of the simulation and prints the ones that broke an invariant. `--each` prints one
//! line per seed: the seed, its worst quota overshoot, how far it was allowed then, the creates let
//! through unchecked, the failed creates, the fenced cells, the keyed cells made again because
//! their node was out of view or lost, and those made again because a node forgot it turned
//! the key away.
//!
//! ```text
//! hive-sim [--seeds N] [--from S] [--seed S] [--secs N] [--no-faults] [--trace] [--each]
//! ```

use std::process::ExitCode;
use std::time::Instant;

use hive_sim::{Config, Faults, run};

const USAGE: &str =
    "usage: hive-sim [--seeds N] [--from S] [--seed S] [--secs N] [--no-faults] [--trace] [--each]";

fn main() -> ExitCode {
    let mut seeds = 100u64;
    let mut from = 1u64;
    let mut cfg = Config::default();
    let mut each = false;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut num = || args.next().and_then(|v| v.parse::<u64>().ok());
        match a.as_str() {
            "--seeds" => seeds = num().unwrap_or(seeds),
            "--from" => from = num().unwrap_or(from),
            "--seed" => {
                from = num().unwrap_or(from);
                seeds = 1;
            }
            "--secs" => cfg.secs = num().unwrap_or(cfg.secs),
            "--no-faults" => cfg.faults = Faults::none(),
            "--trace" => cfg.trace = true,
            "--each" => each = true,
            _ => {
                eprintln!("{USAGE}");
                return ExitCode::from(2);
            }
        }
    }
    let start = Instant::now();
    let (mut bad, mut events, mut cells, mut unchecked, mut late) = (0u64, 0u64, 0u64, 0u64, 0u64);
    let (mut over, mut over_seed) = (0i64, 0u64);
    for seed in from..from + seeds {
        cfg.seed = seed;
        let r = run(&cfg);
        events += r.events;
        cells += r.cells;
        unchecked += r.unchecked;
        late += r.late;
        if r.overshoot > over {
            (over, over_seed) = (r.overshoot, seed);
        }
        if each {
            println!(
                "{seed} {} {} {} {} {} {} {}",
                r.overshoot, r.allowance, r.unchecked, r.failed, r.fenced_cells, r.moved, r.forgot
            );
        }
        if seeds == 1 {
            println!(
                "seed {seed}: {} events, {} requests, {} created, {} refused, {} failed, {} cells, {} late, {} unchecked, overshoot {}, {} epochs fenced, {} cells fenced, {} moved, {} forgot",
                r.events,
                r.requests,
                r.created,
                r.refused,
                r.failed,
                r.cells,
                r.late,
                r.unchecked,
                r.overshoot,
                r.fenced_epochs,
                r.fenced_cells,
                r.moved,
                r.forgot
            );
            for f in &r.faults {
                println!("fault {f}");
            }
            for l in &r.trace {
                println!("{l}");
            }
        }
        if !r.violations.is_empty() {
            bad += 1;
            println!("seed {seed}: {}", r.violations[0]);
            for v in r.violations.iter().skip(1).take(4) {
                println!("  {v}");
            }
        }
    }
    let secs = start.elapsed().as_secs_f64();
    println!(
        "{seeds} seeds from {from}: {bad} broke an invariant; {cells} cells, {late} late, {unchecked} unchecked, worst overshoot {over} (seed {over_seed}); {events} events in {secs:.1} s"
    );
    if bad > 0 { ExitCode::FAILURE } else { ExitCode::SUCCESS }
}
