//! Telling whether the process that holds a cell is still the one the driver started.
//!
//! A pid can be reused once its process is gone, so a driver keeps the start time it read when
//! it started the process in the handle's `extra`, under [`START`], and checks it with the pid.

use crate::CellHandle;
use std::io;
use std::os::fd::OwnedFd;

/// The key in a handle's `extra` for its process's start time, in clock ticks after boot.
pub const START: &str = "start";

/// Whether the handle's process is still running: the pid is there, is the same process that
/// was started, and has not exited yet.
#[must_use]
pub fn alive(h: &CellHandle) -> bool {
    let Some(pid) = h.pid else { return false };
    let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else { return false };
    let Some((state, start)) = stat_fields(&stat) else { return false };
    let same = h.extra.get(START).is_none_or(|s| s.parse() == Ok(start));
    same && state != 'Z' && state != 'X'
}

/// When the process `pid` started, for [`START`].
#[must_use]
pub fn start_time(pid: u32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    stat_fields(&stat).map(|(_, start)| start)
}

/// A pidfd for `pid`, which can be signalled and waited on without the pid being reused under it.
///
/// # Errors
///
/// When the process is gone.
pub fn pidfd(pid: u32) -> io::Result<OwnedFd> {
    let pid = i32::try_from(pid).ok().and_then(rustix::process::Pid::from_raw);
    let pid = pid.ok_or_else(|| io::Error::other("bad pid"))?;
    Ok(rustix::process::pidfd_open(pid, rustix::process::PidfdFlags::empty())?)
}

/// The state letter and the start time from a `/proc/PID/stat` line.
fn stat_fields(stat: &str) -> Option<(char, u64)> {
    // The command name can hold spaces and brackets, so the fields start after the last `)`.
    let rest = &stat[stat.rfind(')')? + 2..];
    let mut fields = rest.split(' ');
    let state = fields.next()?.chars().next()?;
    // starttime is field 22 of the line, the 20th after the state.
    let start = fields.nth(18)?.parse().ok()?;
    Some((state, start))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stat_lines_with_odd_names_parse() {
        let line = "42 (a) b (c)) S 1 42 42 0 -1 4194560 100 0 0 0 1 2 0 0 20 0 1 0 987654 1000 10";
        assert_eq!(stat_fields(line), Some(('S', 987_654)));
        assert_eq!(stat_fields("garbage"), None);
    }

    #[test]
    fn this_process_is_alive_and_a_stale_start_is_not() {
        let pid = std::process::id();
        let start = start_time(pid).unwrap();
        let mut h = CellHandle {
            id: hive_types::CellId::new(1, 1, 1, 1, 1).unwrap(),
            backend: hive_types::Backend::Fullvm,
            pid: Some(pid),
            channel: crate::GuestChannel::Unix("/x".into()),
            cgroup: "/c".into(),
            netns: None,
            extra: [(START.to_string(), start.to_string())].into(),
        };
        assert!(alive(&h));
        h.extra.insert(START.into(), (start + 1).to_string());
        assert!(!alive(&h));
        assert!(pidfd(pid).is_ok());
    }
}
