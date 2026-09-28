//! A cell's cgroup. Every backend puts its cells in one, so every driver reads, limits, freezes and
//! kills it the same way. These are plain cgroup v2 file operations, each a few microseconds, so
//! they are blocking calls.

use crate::driver::CellMetrics;
use hive_types::Resources;
use std::io;
use std::path::Path;

/// The period `cpu.max` quotas are in, in microseconds.
pub const CPU_PERIOD_USEC: u64 = 100_000;

/// Writes `value` to the control file `file` in the cgroup `dir`.
pub fn write(dir: &Path, file: &str, value: &str) -> io::Result<()> {
    std::fs::write(dir.join(file), value).map_err(|e| {
        io::Error::new(e.kind(), format!("writing {value:?} to {}: {e}", dir.join(file).display()))
    })
}

/// Sets the limits in `r` on the cgroup `dir`: `memory.max`, `cpu.max` and `pids.max`. It also turns
/// on `memory.oom.group`, so the OOM killer takes the whole cell rather than one process in it.
pub fn limit(dir: &Path, r: &Resources) -> io::Result<()> {
    write(dir, "memory.oom.group", "1")?;
    relimit(dir, None, r)
}

/// Changes the limits on a cgroup that [`limit`] set to `was`, writing only the ones that differ.
/// Each write is a syscall that can take a few hundred microseconds on a busy host, so a leaf made
/// ahead of time with the usual limits often needs none. `None` writes all of them.
pub fn relimit(dir: &Path, was: Option<&Resources>, r: &Resources) -> io::Result<()> {
    if was.is_none_or(|w| w.mem_bytes() != r.mem_bytes()) {
        write(dir, "memory.max", &r.mem_bytes().to_string())?;
    }
    if was.is_none_or(|w| cpu_max(w.vcpu_milli) != cpu_max(r.vcpu_milli)) {
        write(dir, "cpu.max", &cpu_max(r.vcpu_milli))?;
    }
    if was.is_none_or(|w| w.pids != r.pids) {
        write(dir, "pids.max", &r.pids.to_string())?;
    }
    Ok(())
}

/// The `cpu.max` line for `vcpu_milli` thousandths of a core. The kernel wants a quota of at least
/// a millisecond per period.
#[must_use]
pub fn cpu_max(vcpu_milli: u32) -> String {
    let quota = (u64::from(vcpu_milli) * CPU_PERIOD_USEC / 1000).max(1000);
    format!("{quota} {CPU_PERIOD_USEC}")
}

/// Kills every process in the cgroup `dir` and its children with `SIGKILL`, all at once.
pub fn kill(dir: &Path) -> io::Result<()> {
    write(dir, "cgroup.kill", "1")
}

/// Whether any process is left in the cgroup `dir` or its children.
pub fn populated(dir: &Path) -> io::Result<bool> {
    let events = std::fs::read_to_string(dir.join("cgroup.events"))?;
    Ok(field(&events, "populated").unwrap_or(0) == 1)
}

/// Freezes or thaws the cgroup `dir`. The kernel finishes freezing in the background, and
/// [`frozen`] says when it has.
pub fn freeze(dir: &Path, on: bool) -> io::Result<()> {
    write(dir, "cgroup.freeze", if on { "1" } else { "0" })
}

/// Whether every process in the cgroup `dir` is frozen.
pub fn frozen(dir: &Path) -> io::Result<bool> {
    let events = std::fs::read_to_string(dir.join("cgroup.events"))?;
    Ok(field(&events, "frozen").unwrap_or(0) == 1)
}

/// How many times the OOM killer has hit the cgroup `dir`.
#[must_use]
pub fn oom_kills(dir: &Path) -> u64 {
    std::fs::read_to_string(dir.join("memory.events"))
        .ok()
        .and_then(|e| field(&e, "oom_kill"))
        .unwrap_or(0)
}

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
    fn cpu_quotas_follow_the_cores() {
        assert_eq!(cpu_max(1000), "100000 100000");
        assert_eq!(cpu_max(2500), "250000 100000");
        assert_eq!(cpu_max(250), "25000 100000");
        // Below the kernel's floor of a millisecond per period.
        assert_eq!(cpu_max(1), "1000 100000");
    }

    #[test]
    fn a_missing_cgroup_reads_as_zero() {
        let gone = Path::new("/nonexistent/cgroup");
        assert_eq!(metrics(gone), CellMetrics::default());
        assert_eq!(oom_kills(gone), 0);
        assert!(populated(gone).is_err());
        assert!(kill(gone).is_err());
    }
}
