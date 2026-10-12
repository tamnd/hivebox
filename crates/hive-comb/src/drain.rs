//! Emptying a node for maintenance, with [`Comb::drain`]: the node is cordoned, its idle cells are
//! stopped at once, and its running ones are given a while to end on their own.

use crate::cell::CellInfo;
use crate::comb::Comb;
use futures::stream::{FuturesUnordered, StreamExt};
use hive_nectar::BlobId;
use hive_types::{Cause, CellId, CellState, Error};
use std::collections::HashSet;
use std::io;
use std::time::{Duration, Instant};
use tokio::sync::broadcast::error::RecvError;

/// Idle cells stopped at once. Each may be taking a snapshot, which reads all it wrote.
const AT_ONCE: usize = 8;
/// How often the cells are looked at again when no event says one changed.
const TICK: Duration = Duration::from_secs(1);

/// What [`Comb::drain`] does.
#[derive(Clone, Copy, Debug)]
pub struct DrainOptions {
    /// How long the running cells get to end on their own.
    pub timeout: Duration,
    /// Stop the cells still running at the timeout, rather than leaving them.
    pub stop_rest: bool,
    /// Take a disk snapshot of each idle cell before stopping it.
    pub snapshot: bool,
}

/// How a cell left a draining node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum How {
    /// It was idle, and the drain stopped it.
    Idle,
    /// It ended on its own while the drain waited.
    Ended,
    /// It was still running at the timeout, and the drain stopped it.
    Stopped,
    /// It was still running at the timeout, and was left running.
    Left,
}

/// One cell the drain is done with.
#[derive(Clone, Debug)]
pub struct Drained {
    /// The cell.
    pub id: CellId,
    /// Its project.
    pub project: String,
    /// How it left.
    pub how: How,
    /// The snapshot taken before an idle cell was stopped, or why it could not be. `None` when
    /// none was asked for, and for a quarantined cell, which has had one taken already.
    pub snapshot: Option<Result<BlobId, Error>>,
}

/// The cells a drain dealt with, by how they left.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DrainTotals {
    /// Idle cells stopped.
    pub idle: u32,
    /// Cells that ended on their own.
    pub ended: u32,
    /// Running cells stopped at the timeout.
    pub stopped: u32,
    /// Running cells left at the timeout.
    pub left: u32,
    /// How long the drain took.
    pub took: Duration,
}

impl DrainTotals {
    fn count(&mut self, how: How) {
        let n = match how {
            How::Idle => &mut self.idle,
            How::Ended => &mut self.ended,
            How::Stopped => &mut self.stopped,
            How::Left => &mut self.left,
        };
        *n += 1;
    }
}

impl Comb {
    /// Cordons the node and empties it. Paused cells are idle and are stopped at once, eight at a
    /// time, each after a disk snapshot when `opts.snapshot` is set. The others are waited on
    /// until `opts.timeout`, and a cell that goes idle meanwhile is stopped then. At the timeout
    /// the cells still running are stopped or left, as `opts.stop_rest` says. Each cell is passed
    /// to `each` once the drain is done with it. Cells it stops end with [`Cause::Drained`]. The
    /// node stays cordoned.
    ///
    /// Each stop runs as a task of its own, so one under way finishes even if the drain is
    /// dropped.
    ///
    /// # Errors
    ///
    /// The node could not be cordoned.
    pub async fn drain(
        &self,
        opts: DrainOptions,
        mut each: impl FnMut(Drained) + Send,
    ) -> io::Result<DrainTotals> {
        let started = Instant::now();
        self.cordon(true)?;
        let deadline = tokio::time::Instant::now() + opts.timeout;
        let mut events = self.subscribe();
        let mut totals = DrainTotals::default();
        // Cells seen live that the drain has not reported yet, of those the ones it is stopping,
        // and the ones it left running.
        let mut live: HashSet<CellId> = HashSet::new();
        let mut mine: HashSet<CellId> = HashSet::new();
        let mut left: HashSet<CellId> = HashSet::new();
        let mut stopping = FuturesUnordered::new();
        let mut timed_out = false;
        loop {
            for info in self.list() {
                if info.status.state.is_terminal() {
                    if !mine.contains(&info.id) && live.remove(&info.id) {
                        totals.count(How::Ended);
                        each(drained(&info, How::Ended, None));
                    }
                    continue;
                }
                if mine.contains(&info.id) || left.contains(&info.id) {
                    continue;
                }
                live.insert(info.id);
                let how = if info.status.state == CellState::Paused {
                    How::Idle
                } else if timed_out && opts.stop_rest {
                    How::Stopped
                } else if timed_out {
                    live.remove(&info.id);
                    left.insert(info.id);
                    totals.count(How::Left);
                    each(drained(&info, How::Left, None));
                    continue;
                } else {
                    continue;
                };
                if stopping.len() >= AT_ONCE {
                    continue;
                }
                mine.insert(info.id);
                let snapshot = opts.snapshot && how == How::Idle && !info.status.quarantined;
                let (id, task) =
                    (info.id, tokio::spawn(self.clone().drain_one(info, how, snapshot)));
                stopping.push(async move { (id, task.await) });
            }
            if live.is_empty() && stopping.is_empty() {
                break;
            }
            tokio::select! {
                Some((id, done)) = stopping.next() => {
                    live.remove(&id);
                    mine.remove(&id);
                    // A stop that panicked is looked at again on the next pass.
                    if let Ok(d) = done {
                        totals.count(d.how);
                        each(d);
                    }
                }
                got = events.recv() => {
                    if matches!(got, Err(RecvError::Closed)) {
                        tokio::time::sleep(TICK).await;
                    }
                }
                () = tokio::time::sleep_until(deadline), if !timed_out => timed_out = true,
                () = tokio::time::sleep(TICK) => {}
            }
        }
        totals.took = started.elapsed();
        Ok(totals)
    }

    /// Stops one cell for a drain, after a snapshot when `snapshot` is set.
    async fn drain_one(self, info: CellInfo, how: How, snapshot: bool) -> Drained {
        let snap = if snapshot { Some(self.snapshot(info.id, None).await) } else { None };
        // A cell that ended meanwhile, or that is gone, is what the drain wanted anyway.
        let _ = self.stop_for(info.id, Cause::Drained, None).await;
        drained(&info, how, snap)
    }
}

fn drained(info: &CellInfo, how: How, snapshot: Option<Result<BlobId, Error>>) -> Drained {
    Drained { id: info.id, project: info.project.clone(), how, snapshot }
}
