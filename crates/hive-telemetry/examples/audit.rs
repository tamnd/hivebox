//! Times the audit log: how many events a second it takes from many threads, what one
//! `record` costs the caller, how long one event takes to be on disk when it is the only one, and
//! how fast a chain verifies. It also reads the writer thread's schedstat, the time it ran on a
//! CPU and the time it sat runnable waiting for one, so the cost an event puts on the writer can
//! be told apart from how busy the machine is.
//!
//! ```text
//! cargo run --release -p hive-telemetry --example audit -- --dir /var/tmp/audit --events 1000000 --threads 8
//! ```

use hive_telemetry::audit::{self, AuditEvent, AuditLog};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct Args {
    dir: PathBuf,
    events: u64,
    threads: u64,
    single: u64,
}

fn args() -> Result<Args, String> {
    let mut a = Args {
        dir: PathBuf::from("/var/tmp/hive-audit"),
        events: 1_000_000,
        threads: 8,
        single: 2000,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let v = it.next().ok_or(format!("{arg} needs a value"))?;
        let n = || v.parse::<u64>().map_err(|e| format!("{arg} {v}: {e}"));
        match arg.as_str() {
            "--dir" => a.dir = PathBuf::from(&v),
            "--events" => a.events = n()?,
            "--threads" => a.threads = n()?.max(1),
            "--single" => a.single = n()?,
            other => return Err(format!("unknown argument {other}")),
        }
    }
    Ok(a)
}

fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_nanos() as u64)
}

/// An event shaped like an exec on a cell.
fn event(i: u64) -> AuditEvent {
    AuditEvent {
        ts: now(),
        principal: "key:3f9a2c".into(),
        project: "rl-swe".into(),
        cell: format!("c-{:016x}", i / 50),
        op: "exec.run".into(),
        args: audit::digest(format!("[\"pytest\",\"-x\",\"tests/test_{i}.py\"]").as_bytes()),
        result: "ok".into(),
        trace: format!("{:032x}", u128::from(i) * 0x9e37_79b9_7f4a_7c15),
    }
}

fn pct(v: &mut [Duration], p: f64) -> f64 {
    v.sort_unstable();
    let i = ((v.len() as f64 - 1.0) * p).round() as usize;
    v.get(i).map_or(0.0, |d| d.as_secs_f64() * 1e6)
}

/// The writer thread's time on a CPU and time waiting for one, from its schedstat.
fn writer_time() -> Option<(Duration, Duration)> {
    for e in std::fs::read_dir("/proc/self/task").ok()? {
        let path = e.ok()?.path();
        if std::fs::read_to_string(path.join("comm")).ok()?.trim() != "hive-audit" {
            continue;
        }
        let text = std::fs::read_to_string(path.join("schedstat")).ok()?;
        let mut it = text.split_whitespace().map(|v| v.parse::<u64>().ok());
        let (run, wait) = (it.next()??, it.next()??);
        return Some((Duration::from_nanos(run), Duration::from_nanos(wait)));
    }
    None
}

fn size(dir: &std::path::Path) -> u64 {
    std::fs::read_dir(dir)
        .map(|it| it.filter_map(|e| e.ok()?.metadata().ok()).map(|m| m.len()).sum())
        .unwrap_or(0)
}

fn run() -> Result<(), String> {
    let a = args()?;
    let e = |e: std::io::Error| e.to_string();
    let _ = std::fs::remove_dir_all(&a.dir);
    let dir = a.dir.join("many");
    let log = Arc::new(AuditLog::open(&dir, "node-1").map_err(e)?);
    let per = a.events / a.threads;
    let began = Instant::now();
    let threads: Vec<_> = (0..a.threads)
        .map(|t| {
            let log = log.clone();
            std::thread::spawn(move || {
                let mut took = Vec::with_capacity(per as usize);
                for i in 0..per {
                    let ev = event(t * per + i);
                    let s = Instant::now();
                    log.record(ev);
                    took.push(s.elapsed());
                }
                took
            })
        })
        .collect();
    let mut took = Vec::new();
    for t in threads {
        took.extend(t.join().map_err(|_| "a thread panicked".to_string())?);
    }
    log.flush().map_err(e)?;
    let all = began.elapsed();
    let st = log.stats();
    let n = per * a.threads;
    if let Some((run, wait)) = writer_time() {
        println!(
            "writer: {:.2} s on a CPU, {:.2} us an event, and {:.2} s waiting for a CPU",
            run.as_secs_f64(),
            run.as_secs_f64() * 1e6 / n as f64,
            wait.as_secs_f64()
        );
    }
    println!(
        "{n} events from {} threads in {:.2} s: {:.0} events/s, {} syncs, {:.0} events a sync, {:.0} bytes an event",
        a.threads,
        all.as_secs_f64(),
        n as f64 / all.as_secs_f64(),
        st.syncs,
        n as f64 / st.syncs.max(1) as f64,
        size(&dir) as f64 / n as f64
    );
    println!(
        "record: p50 {:.2} us, p99 {:.2} us, p99.9 {:.2} us",
        pct(&mut took, 0.5),
        pct(&mut took, 0.99),
        pct(&mut took, 0.999)
    );
    drop(log);

    let t = Instant::now();
    let v = audit::verify(&dir).map_err(e)?.map_err(|b| b.to_string())?;
    let vt = t.elapsed();
    println!(
        "verify: {} events in {:.2} s, {:.0} events/s, root {}",
        v.events,
        vt.as_secs_f64(),
        v.events as f64 / vt.as_secs_f64(),
        &v.root_hex()[..16]
    );

    let dir = a.dir.join("single");
    let log = AuditLog::open(&dir, "node-1").map_err(e)?;
    let mut durable = Vec::with_capacity(a.single as usize);
    for i in 0..a.single {
        let s = Instant::now();
        log.record(event(i));
        log.flush().map_err(e)?;
        durable.push(s.elapsed());
    }
    println!(
        "one event at a time until it is on disk: p50 {:.0} us, p99 {:.0} us over {}",
        pct(&mut durable, 0.5),
        pct(&mut durable, 0.99),
        a.single
    );
    drop(log);
    let _ = std::fs::remove_dir_all(&a.dir);
    Ok(())
}

fn main() {
    if let Err(e) = run() {
        eprintln!("audit: {e}");
        std::process::exit(1);
    }
}
