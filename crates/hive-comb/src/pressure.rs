//! The memory pressure brake, from `spec/08_node_agent.md`, section 5.
//!
//! The kernel counts how much of the time tasks spend stalled waiting for memory, and `some avg10`
//! is the share of the last ten seconds in which at least one task was. Once that passes the limit,
//! 20% by default, the comb takes no new cells and asks the kernel for memory back from the best
//! effort slice, so the cells already running get the memory and not newcomers. Admits come back
//! once the stall falls under half the limit, so a node sitting near the line does not flap.
//!
//! With cgroups on, the brake reads `memory.pressure` of the comb's own root, which only counts
//! stalls of cells. A host can be short of memory because of something else while the cells barely
//! notice, and then there is nothing to brake for. Without cgroups it reads the whole machine's
//! `/proc/pressure/memory`.

use crate::admit::Admission;
use crate::metrics::Metrics;
use hive_cell::cgroup;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// How often the brake looks. The kernel updates `avg10` every two seconds.
const EVERY: Duration = Duration::from_secs(1);
/// Each look with the brake on asks for this share of the best effort slice's memory back.
const RECLAIM_SHARE: u64 = 8;
/// Less than this in the best effort slice is not worth asking for.
const RECLAIM_FLOOR: u64 = 1 << 20;

/// What the brake reads and what it reclaims from.
#[derive(Debug)]
pub(crate) struct Brake {
    source: PathBuf,
    besteffort: Option<PathBuf>,
    limit: f64,
}

impl Brake {
    /// A brake that goes on past `limit` percent, reading the cells' pressure under `cgroup_root`
    /// or the machine's when there is none.
    pub(crate) fn new(cgroup_root: Option<&Path>, limit: f64) -> Self {
        match cgroup_root {
            Some(root) => Self {
                source: root.join("memory.pressure"),
                besteffort: Some(root.join("besteffort.slice")),
                limit,
            },
            None => {
                Self { source: PathBuf::from("/proc/pressure/memory"), besteffort: None, limit }
            }
        }
    }

    /// Looks every second until `stop`, holding admits in `admission` while the brake is on.
    pub(crate) async fn run(
        self,
        admission: Arc<Admission>,
        metrics: Metrics,
        stop: CancellationToken,
    ) {
        let mut on = false;
        let mut unread = false;
        loop {
            tokio::select! {
                () = stop.cancelled() => return,
                () = tokio::time::sleep(EVERY) => {}
            }
            let text = tokio::fs::read_to_string(&self.source).await;
            let Some(avg10) = text.as_deref().ok().and_then(some_avg10) else {
                if !unread {
                    eprintln!(
                        "hive-comb: cannot read memory pressure from {}, so the brake is off",
                        self.source.display()
                    );
                    unread = true;
                }
                continue;
            };
            metrics.pressure(avg10);
            let was = on;
            on = next(on, avg10, self.limit);
            if on != was {
                admission.hold(on);
                let what = if on { "takes no new cells" } else { "takes new cells again" };
                eprintln!(
                    "hive-comb: cells stalled on memory {avg10:.1}% of the last 10 s, the node {what}"
                );
            }
            if on && let Some(dir) = &self.besteffort {
                metrics.reclaimed(reclaim(dir.clone()).await);
            }
        }
    }
}

/// Whether the brake is on after a look that saw `avg10`, given it was `on` before.
fn next(on: bool, avg10: f64, limit: f64) -> bool {
    if on { avg10 >= limit / 2.0 } else { avg10 > limit }
}

/// The `some avg10` figure from a pressure file, in percent.
fn some_avg10(text: &str) -> Option<f64> {
    let line = text.lines().find(|l| l.starts_with("some "))?;
    line.split_whitespace().find_map(|f| f.strip_prefix("avg10="))?.parse().ok()
}

/// Asks the kernel for a share of `dir`'s memory back. Returns how much it went down by.
async fn reclaim(dir: PathBuf) -> u64 {
    let ask = move || {
        let current = read_u64(&dir.join("memory.current"))?;
        if current < RECLAIM_FLOOR {
            return Some(0);
        }
        // The write fails with EAGAIN when the kernel got back less than asked, and that is still
        // memory back, so what counts is the drop.
        let _ = cgroup::write(&dir, "memory.reclaim", &(current / RECLAIM_SHARE).to_string());
        Some(current.saturating_sub(read_u64(&dir.join("memory.current"))?))
    };
    // A reclaim writes pages out before the write returns, so it stays off the runtime's threads.
    tokio::task::spawn_blocking(ask).await.ok().flatten().unwrap_or(0)
}

fn read_u64(path: &Path) -> Option<u64> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use hive_types::{Backend, CellSpec, Reason, Source};
    use std::collections::BTreeMap;

    const CALM: &str = "some avg10=0.00 avg60=0.00 avg300=0.00 total=1234\n\
                        full avg10=0.00 avg60=0.00 avg300=0.00 total=99\n";

    fn stalled(avg10: f64) -> String {
        format!(
            "some avg10={avg10:.2} avg60=1.50 avg300=0.40 total=987654\n\
             full avg10=3.10 avg60=0.70 avg300=0.10 total=4321\n"
        )
    }

    #[test]
    fn the_some_line_is_read_and_the_full_line_is_not() {
        assert_eq!(some_avg10(CALM), Some(0.0));
        assert_eq!(some_avg10(&stalled(27.45)), Some(27.45));
        assert_eq!(some_avg10("full avg10=50.00 avg60=0 avg300=0 total=1\n"), None);
        assert_eq!(some_avg10(""), None);
    }

    #[test]
    fn the_brake_goes_on_past_the_limit_and_off_under_half_of_it() {
        let seen = [5.0, 20.0, 20.5, 30.0, 12.0, 10.0, 9.99, 15.0, 21.0];
        let mut on = false;
        let states: Vec<bool> = seen
            .iter()
            .map(|&v| {
                on = next(on, v, 20.0);
                on
            })
            .collect();
        assert_eq!(states, [false, false, true, true, true, true, false, false, true]);
    }

    #[tokio::test]
    async fn admits_stop_while_the_cells_stall() {
        let file = std::env::temp_dir().join(format!("hive-psi-{}", std::process::id()));
        std::fs::write(&file, CALM).unwrap();
        let admission =
            Arc::new(Admission::new(1 << 36, 100, &BTreeMap::from([(Backend::Container, 2)])));
        let brake = Brake { source: file.clone(), besteffort: None, limit: 20.0 };
        let stop = CancellationToken::new();
        let metrics = Metrics::default();
        let task = tokio::spawn(brake.run(admission.clone(), metrics.clone(), stop.clone()));
        let spec = CellSpec::new(Source::Image("x".into()), Backend::Container);
        let wait_for = async |held: bool| {
            for _ in 0..50 {
                if admission.held() == held {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            panic!("the brake did not turn {}", if held { "on" } else { "off" });
        };
        let before = admission.reserve(&spec).unwrap();
        std::fs::write(&file, stalled(35.0)).unwrap();
        wait_for(true).await;
        let e = admission.reserve(&spec).unwrap_err();
        assert_eq!(e.reason, Reason::CapacityUnavailable);
        assert!(e.message.contains("stalling on memory"), "{e}");
        // Cells found alive after a restart still count, brake or not.
        let _adopted = admission.adopt(&spec);
        std::fs::write(&file, stalled(10.0)).unwrap();
        tokio::time::sleep(Duration::from_millis(2500)).await;
        assert!(admission.held(), "10% is half the limit, which still holds");
        std::fs::write(&file, stalled(4.0)).unwrap();
        wait_for(false).await;
        drop(before);
        admission.reserve(&spec).unwrap();
        let text = metrics.registry().render();
        assert!(text.contains("hive_memory_stall_basis_points 400"), "{text}");
        stop.cancel();
        task.await.unwrap();
        let _ = std::fs::remove_file(&file);
    }

    #[tokio::test]
    async fn a_real_stall_holds_admits_until_it_passes() {
        let Some(tree) = crate::cgroups::tests::Tree::new() else { return };
        let _pool = crate::cgroups::Cgroups::init(&tree.0, 1).unwrap();
        let leaf = tree.0.join("besteffort.slice/cell-hog");
        std::fs::create_dir(&leaf).unwrap();
        // With no room to reclaim into, a cell over its memory.high is throttled every time it
        // goes back to user space, and the kernel counts that as a memory stall.
        cgroup::write(&leaf, "memory.high", &(16u64 << 20).to_string()).unwrap();
        let _ = cgroup::write(&leaf, "memory.swap.max", "0");
        let admission =
            Arc::new(Admission::new(1 << 36, 100, &BTreeMap::from([(Backend::Container, 2)])));
        let stop = CancellationToken::new();
        let brake = Brake::new(Some(&tree.0), 20.0);
        let task = tokio::spawn(brake.run(admission.clone(), Metrics::default(), stop.clone()));
        let started = std::time::Instant::now();
        let mut hog = std::process::Command::new("sh")
            .arg("-c")
            .arg(format!(
                "echo $$ > {}/cgroup.procs && exec dd if=/dev/zero of=/dev/null bs=64M count=100000",
                leaf.display()
            ))
            .spawn()
            .unwrap();
        let wait_for = async |held: bool| {
            for _ in 0..1200 {
                if admission.held() == held {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            false
        };
        let on = wait_for(true).await;
        let took_on = started.elapsed();
        let _ = hog.kill();
        let _ = hog.wait();
        let killed = std::time::Instant::now();
        let off = on && wait_for(false).await;
        let took_off = killed.elapsed();
        let avg10 = std::fs::read_to_string(tree.0.join("memory.pressure")).unwrap();
        eprintln!(
            "brake on {:.1} s after the hog started, off {:.1} s after it ended, now {}",
            took_on.as_secs_f64(),
            took_off.as_secs_f64(),
            avg10.lines().next().unwrap_or("")
        );
        stop.cancel();
        task.await.unwrap();
        assert!(on, "the brake never went on");
        assert!(off, "the brake never went off");
    }

    #[tokio::test]
    async fn the_best_effort_slice_gives_memory_back() {
        let Some(tree) = crate::cgroups::tests::Tree::new() else { return };
        let _pool = crate::cgroups::Cgroups::init(&tree.0, 1).unwrap();
        let slice = tree.0.join("besteffort.slice");
        let leaf = slice.join("cell-test");
        std::fs::create_dir(&leaf).unwrap();
        // Page cache is what a reclaim can always take back, swap or not, and it is charged to
        // the cgroup that wrote it, so a cell writes a file of its own. It goes on disk and not in
        // /tmp, which may be tmpfs and so not reclaimable without swap.
        let data = PathBuf::from(format!("/var/tmp/hive-psi-data-{}", std::process::id()));
        let mut child = std::process::Command::new("sh")
            .arg("-c")
            .arg(format!(
                "echo $$ > {}/cgroup.procs && dd if=/dev/zero of={} bs=1M count=64 status=none && exec sleep 600",
                leaf.display(),
                data.display()
            ))
            .spawn()
            .unwrap();
        for _ in 0..300 {
            if read_u64(&slice.join("memory.current")).unwrap_or(0) >= 48 << 20 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let before = read_u64(&slice.join("memory.current")).unwrap();
        assert!(before >= 32 << 20, "the slice holds {before} bytes");
        let mut back = 0;
        for _ in 0..8 {
            back += reclaim(slice.clone()).await;
        }
        let after = read_u64(&slice.join("memory.current")).unwrap();
        let _ = child.kill();
        let _ = child.wait();
        let _ = std::fs::remove_file(&data);
        eprintln!("best effort slice {} MiB before, {} MiB after", before >> 20, after >> 20);
        assert!(back > 0 && after < before, "{before} then {after}, {back} back");
    }
}
