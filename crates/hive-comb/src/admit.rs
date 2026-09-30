//! Admission: whether a cell fits on this node, and the reservation that holds its place while it
//! lives.
//!
//! A cell is admitted if the node is under its cell cap and the memory committed to every cell,
//! with this one added, stays under the node's memory times the overcommit factor of this cell's
//! class. So a latency cell only goes in while nothing is overcommitted, and a best effort cell
//! can go in up to three times over, from `spec/08_node_agent.md`, section 5. Creates also wait
//! for a per backend permit, which bounds how many are in flight at once.

use hive_types::{Backend, CellSpec, Error, Qos, Reason};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Debug)]
pub(crate) struct Admission {
    mem_limit: u64,
    max_cells: usize,
    used: Mutex<Used>,
    creates: BTreeMap<Backend, Arc<Semaphore>>,
}

#[derive(Debug, Default)]
struct Used {
    cells: usize,
    mem: u64,
    cpu_milli: u64,
    admitted: u64,
}

/// What the node has given out, for its reports to scout.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Usage {
    /// Cells holding a share.
    pub(crate) cells: usize,
    /// Memory given to them, in bytes.
    pub(crate) mem: u64,
    /// CPU given to them, in thousandths of a core.
    pub(crate) cpu_milli: u64,
    /// Creates admitted since the comb started, which scout turns into a rate.
    pub(crate) admitted: u64,
}

/// A cell's share of the node. Dropping it gives the share back.
#[derive(Debug)]
pub(crate) struct Reservation {
    admission: Arc<Admission>,
    mem: u64,
    cpu_milli: u64,
}

impl Admission {
    pub(crate) fn new(
        mem_limit: u64,
        max_cells: usize,
        creates: &BTreeMap<Backend, usize>,
    ) -> Self {
        let creates = creates.iter().map(|(&b, &n)| (b, Arc::new(Semaphore::new(n)))).collect();
        Self { mem_limit, max_cells, used: Mutex::new(Used::default()), creates }
    }

    fn used(&self) -> std::sync::MutexGuard<'_, Used> {
        self.used.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Takes a share of the node for a cell with `spec`, or says why there is none.
    pub(crate) fn reserve(self: &Arc<Self>, spec: &CellSpec) -> Result<Reservation, Error> {
        let mem = spec.resources.mem_bytes();
        let mut used = self.used();
        if used.cells >= self.max_cells {
            return Err(Error::new(
                Reason::CapacityUnavailable,
                format!("the node already has its limit of {} cells", self.max_cells),
            ));
        }
        let ceiling = self.ceiling(spec.qos);
        if used.mem + mem > ceiling {
            return Err(Error::new(
                Reason::CapacityUnavailable,
                format!(
                    "{} MiB more would pass the node's {} MiB ceiling for {} cells",
                    mem >> 20,
                    ceiling >> 20,
                    spec.qos.as_str()
                ),
            ));
        }
        let cpu_milli = u64::from(spec.resources.vcpu_milli);
        used.cells += 1;
        used.mem += mem;
        used.cpu_milli += cpu_milli;
        used.admitted += 1;
        Ok(Reservation { admission: self.clone(), mem, cpu_milli })
    }

    /// Takes a share for a cell that was already running before a restart. It is there whatever
    /// the limits say, so this never fails.
    pub(crate) fn adopt(self: &Arc<Self>, spec: &CellSpec) -> Reservation {
        let mem = spec.resources.mem_bytes();
        let cpu_milli = u64::from(spec.resources.vcpu_milli);
        let mut used = self.used();
        used.cells += 1;
        used.mem += mem;
        used.cpu_milli += cpu_milli;
        Reservation { admission: self.clone(), mem, cpu_milli }
    }

    /// Waits for a create permit for `backend`.
    pub(crate) async fn create_permit(&self, backend: Backend) -> Option<OwnedSemaphorePermit> {
        self.creates.get(&backend)?.clone().acquire_owned().await.ok()
    }

    fn ceiling(&self, qos: Qos) -> u64 {
        self.mem_limit.saturating_mul(qos.overcommit_tenths()) / 10
    }

    /// Cells and bytes committed right now.
    pub(crate) fn committed(&self) -> (usize, u64) {
        let used = self.used();
        (used.cells, used.mem)
    }

    /// What is given out right now.
    pub(crate) fn usage(&self) -> Usage {
        let used = self.used();
        Usage {
            cells: used.cells,
            mem: used.mem,
            cpu_milli: used.cpu_milli,
            admitted: used.admitted,
        }
    }

    /// The memory a cell of `qos` may be admitted up to, in bytes.
    pub(crate) fn ceiling_for(&self, qos: Qos) -> u64 {
        self.ceiling(qos)
    }

    /// Most cells at once.
    pub(crate) fn max_cells(&self) -> usize {
        self.max_cells
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let mut used = self.admission.used();
        used.cells -= 1;
        used.mem -= self.mem;
        used.cpu_milli -= self.cpu_milli;
    }
}

/// The machine's memory, from `/proc/meminfo`, in bytes.
pub(crate) fn mem_total() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    let line = text.lines().find(|l| l.starts_with("MemTotal:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib * 1024)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hive_types::{Resources, Source};

    fn spec(mem_mib: u32, qos: Qos) -> CellSpec {
        let mut s = CellSpec::new(Source::Image("x".into()), Backend::Container);
        s.resources = Resources { mem_mib, ..Resources::default() };
        s.qos = qos;
        s
    }

    fn admission(mem_mib: u64, cells: usize) -> Arc<Admission> {
        Arc::new(Admission::new(mem_mib << 20, cells, &BTreeMap::from([(Backend::Container, 2)])))
    }

    #[test]
    fn each_class_fills_up_to_its_own_ceiling() {
        let a = admission(1000, 100);
        let latency = a.reserve(&spec(1000, Qos::Latency)).unwrap();
        // Nothing more for latency cells, half as much again for standard, three times for best
        // effort.
        assert_eq!(
            a.reserve(&spec(64, Qos::Latency)).unwrap_err().reason,
            Reason::CapacityUnavailable
        );
        let standard = a.reserve(&spec(500, Qos::Standard)).unwrap();
        assert!(a.reserve(&spec(64, Qos::Standard)).is_err());
        let best = a.reserve(&spec(1500, Qos::BestEffort)).unwrap();
        assert!(a.reserve(&spec(64, Qos::BestEffort)).is_err());
        assert_eq!(a.committed(), (3, 3000 << 20));
        assert_eq!(a.usage().cpu_milli, 3 * 1000);
        assert_eq!(a.usage().admitted, 3);
        drop((latency, standard, best));
        assert_eq!(a.committed(), (0, 0));
        assert_eq!(a.usage(), Usage { admitted: 3, ..Usage::default() });
        a.reserve(&spec(1000, Qos::Latency)).unwrap();
    }

    #[test]
    fn the_cell_cap_holds_whatever_the_memory() {
        let a = admission(1 << 20, 2);
        let one = a.reserve(&spec(64, Qos::BestEffort)).unwrap();
        let _two = a.reserve(&spec(64, Qos::BestEffort)).unwrap();
        let e = a.reserve(&spec(64, Qos::BestEffort)).unwrap_err();
        assert!(e.message.contains("limit of 2 cells"), "{e}");
        drop(one);
        let _three = a.reserve(&spec(64, Qos::BestEffort)).unwrap();
        // Cells found alive after a restart count even past the cap.
        let _adopted = a.adopt(&spec(64, Qos::BestEffort));
        assert_eq!(a.committed().0, 3);
    }

    #[tokio::test]
    async fn creates_wait_for_a_permit() {
        let a = admission(1000, 100);
        let p1 = a.create_permit(Backend::Container).await.unwrap();
        let _p2 = a.create_permit(Backend::Container).await.unwrap();
        let third = a.create_permit(Backend::Container);
        tokio::pin!(third);
        assert!(futures::poll!(third.as_mut()).is_pending());
        drop(p1);
        assert!(third.await.is_some());
        assert!(a.create_permit(Backend::Microvm).await.is_none());
    }

    #[test]
    fn meminfo_parses() {
        if let Some(total) = mem_total() {
            assert!(total > 64 << 20);
        }
    }
}
