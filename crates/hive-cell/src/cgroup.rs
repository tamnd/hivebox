//! Reading a cell's cgroup. Every backend puts its cells in one, so every driver reads it the same
//! way.

use crate::driver::CellMetrics;
use std::path::Path;

/// Reads the numbers in [`CellMetrics`] from the cgroup v2 directory `dir`. A file that is
/// missing, because its controller is off or the cgroup is gone, reads as zero.
#[must_use]
pub fn metrics(dir: &Path) -> CellMetrics {
    let read = |name: &str| std::fs::read_to_string(dir.join(name)).unwrap_or_default();
    let number = |name: &str| read(name).trim().parse().unwrap_or(0);
    CellMetrics {
        cpu_usec: field(&read("cpu.stat"), "usage_usec").unwrap_or(0),
        mem_bytes: number("memory.current"),
        mem_peak: number("memory.peak"),
        pids: number("pids.current"),
    }
}

/// The value of `key` in a flat keyed file such as `cpu.stat` or `memory.events`, where each line
/// is a key, a space and a number.
#[must_use]
pub fn field(text: &str, key: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let (k, v) = line.split_once(' ')?;
        if k == key { v.trim().parse().ok() } else { None }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyed_files_parse() {
        let stat = "usage_usec 1234\nuser_usec 1000\nsystem_usec 234\n";
        assert_eq!(field(stat, "usage_usec"), Some(1234));
        assert_eq!(field(stat, "system_usec"), Some(234));
        assert_eq!(field(stat, "usage"), None);
        assert_eq!(field("oom 0\noom_kill 2", "oom_kill"), Some(2));
    }

    #[test]
    fn a_missing_cgroup_reads_as_zero() {
        assert_eq!(metrics(Path::new("/nonexistent/cgroup")), CellMetrics::default());
    }
}
