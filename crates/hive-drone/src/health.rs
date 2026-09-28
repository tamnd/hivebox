//! What the drone reports about the cell.

use hive_proto::drone::api::Health;
use std::time::Duration;

pub(crate) fn read(build: &str, uptime: Duration, processes: u32) -> Health {
    let (mem_total_bytes, mem_available_bytes) = meminfo();
    Health {
        build: build.to_string(),
        uptime_nanos: uptime.as_nanos() as u64,
        load1_milli: load1_milli(),
        mem_total_bytes,
        mem_available_bytes,
        processes,
    }
}

fn load1_milli() -> u64 {
    let Ok(s) = std::fs::read_to_string("/proc/loadavg") else { return 0 };
    let load: f64 = s.split_whitespace().next().and_then(|f| f.parse().ok()).unwrap_or(0.0);
    (load * 1000.0) as u64
}

fn meminfo() -> (u64, u64) {
    let Ok(s) = std::fs::read_to_string("/proc/meminfo") else { return (0, 0) };
    let field = |name: &str| {
        s.lines()
            .find_map(|l| l.strip_prefix(name))
            .and_then(|rest| rest.trim().trim_end_matches("kB").trim().parse::<u64>().ok())
            .map_or(0, |kib| kib * 1024)
    };
    (field("MemTotal:"), field("MemAvailable:"))
}
