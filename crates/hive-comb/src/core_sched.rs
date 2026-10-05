//! Core scheduling, so the two threads of a core never run cells of different CPU classes at once.
//!
//! `cpu.idle` keeps best effort cells off a thread a latency cell wants, but not off the other
//! thread of the same core, where they still slow it down through the caches and execution units
//! the two threads share. With core scheduling the kernel only runs tasks with the same cookie on
//! the threads of a core at the same time, and leaves a thread idle rather than mix them. The comb
//! makes one cookie per class and gives it to every process in a new cell's cgroup right after the
//! cell starts. Whatever the cell starts later is forked from one of those and inherits it.
//!
//! A cookie lives on tasks, and the only way to hand one to another process is to push it from a
//! task that holds it, so each cookie is held by a thread of its own here that does nothing else.
//! After a restart the comb makes new cookies. Cells that were running keep their old ones, which
//! only means they no longer share a core with new cells of their class.
//!
//! The kernel refuses cookies on a host without SMT, where there is nothing to share, and the comb
//! then runs without them.

use std::io;
use std::path::Path;
use std::sync::mpsc;

use hive_types::Qos;
use rustix::process::Pid;
use rustix::thread::{
    CoreSchedulingScope, create_core_scheduling_cookie, gettid, push_core_scheduling_cookie,
};
use tokio::sync::oneshot;

/// Pids to give a cookie to, and where to say how many took it.
type Push = (Vec<Pid>, oneshot::Sender<usize>);

/// The three cookies, latency first.
pub(crate) struct CoreSched {
    holders: [mpsc::Sender<Push>; 3],
}

impl CoreSched {
    /// Makes a cookie per class, each on a thread of its own.
    ///
    /// # Errors
    ///
    /// The kernel will not make a cookie: `ENODEV` on a host without SMT, `EINVAL` on a kernel
    /// older than 5.14 or built without core scheduling.
    pub(crate) fn new() -> io::Result<Self> {
        let mut holders = Vec::with_capacity(3);
        for qos in [Qos::Latency, Qos::Standard, Qos::BestEffort] {
            let (tx, rx) = mpsc::channel::<Push>();
            let (made_tx, made) = mpsc::sync_channel(1);
            let name = format!("core-{}", qos.as_str());
            std::thread::Builder::new().name(name).spawn(move || hold(&made_tx, rx))?;
            made.recv().map_err(|_| io::Error::other("a cookie thread died"))??;
            holders.push(tx);
        }
        let holders = holders.try_into().unwrap_or_else(|_| unreachable!("three classes"));
        Ok(Self { holders })
    }

    /// Gives the cookie of `qos` to every process in `cgroup`, and says how many took it. A process
    /// that is gone by then does not count.
    pub(crate) async fn give(&self, qos: Qos, cgroup: &Path) -> io::Result<usize> {
        let text = std::fs::read_to_string(cgroup.join("cgroup.procs"))?;
        let pids: Vec<Pid> =
            text.lines().filter_map(|l| l.parse().ok()).filter_map(Pid::from_raw).collect();
        let (tx, rx) = oneshot::channel();
        self.holders[index(qos)]
            .send((pids, tx))
            .map_err(|_| io::Error::other("the cookie thread is gone"))?;
        rx.await.map_err(|_| io::Error::other("the cookie thread is gone"))
    }
}

/// Makes a cookie on this thread and says whether that worked, then pushes it to whatever pids
/// come in until the comb goes away.
fn hold(made: &mpsc::SyncSender<io::Result<()>>, rx: mpsc::Receiver<Push>) {
    let ok = create_core_scheduling_cookie(gettid(), CoreSchedulingScope::Thread);
    let failed = ok.is_err();
    let _ = made.send(ok.map_err(io::Error::from));
    if failed {
        return;
    }
    for (pids, done) in rx {
        let push = |&p: &Pid| push_core_scheduling_cookie(p, CoreSchedulingScope::ThreadGroup);
        let _ = done.send(pids.iter().filter(|p| push(p).is_ok()).count());
    }
}

fn index(qos: Qos) -> usize {
    match qos {
        Qos::Latency => 0,
        Qos::Standard => 1,
        Qos::BestEffort => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cgroups::tests::{Tree, sleeper_in};
    use rustix::io::Errno;
    use rustix::thread::core_scheduling_cookie;

    /// The cookie of a process, 0 when it has none.
    fn cookie(pid: u32) -> io::Result<u64> {
        let pid =
            i32::try_from(pid).ok().and_then(Pid::from_raw).ok_or(io::ErrorKind::InvalidInput)?;
        Ok(core_scheduling_cookie(pid, CoreSchedulingScope::Thread)?)
    }

    #[tokio::test]
    async fn cells_of_a_class_share_a_cookie_and_other_classes_do_not() {
        let core = match CoreSched::new() {
            Ok(c) => c,
            Err(e) => {
                // ENODEV on a host without SMT, EINVAL on a kernel without core scheduling.
                let refused = [Errno::NODEV, Errno::INVAL].map(Errno::raw_os_error);
                assert!(refused.contains(&e.raw_os_error().unwrap_or(0)), "{e}");
                eprintln!("skipped: no core scheduling here: {e}");
                return;
            }
        };
        let Some(root) = Tree::new() else { return };
        std::fs::create_dir_all(&root.0).unwrap();
        let mut cells = Vec::new();
        for (i, qos) in [Qos::Latency, Qos::Latency, Qos::BestEffort].into_iter().enumerate() {
            let dir = root.0.join(format!("c{i}"));
            std::fs::create_dir(&dir).unwrap();
            let child = sleeper_in(&dir);
            assert_eq!(core.give(qos, &dir).await.unwrap(), 1);
            cells.push(child);
        }
        let c: Vec<u64> = cells.iter().map(|c| cookie(c.id()).unwrap()).collect();
        assert_ne!(c[0], 0);
        assert_eq!(c[0], c[1]);
        assert_ne!(c[0], c[2]);
        assert_eq!(cookie(std::process::id()).unwrap(), 0, "the comb itself has none");
        for mut c in cells {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}
