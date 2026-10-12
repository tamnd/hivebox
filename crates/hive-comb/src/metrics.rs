//! What the comb counts, from `spec/13_observability_testing_bench.md` section 1.
//!
//! A create is timed stage by stage, so a slow p99 can be traced to the stage that grew and not
//! only seen. The stages follow one create in order: `admit` waits for a create permit, `pool`
//! takes a cgroup and a network namespace, `rootfs` finds the image and mounts its layers,
//! `prepare` has the driver set the cell up, `start` starts it, `handshake` waits for the guest
//! agent, and `wal` is the two records written on the way. `total` is all of it.
//!
//! A stop is timed the same way: `wal` is again the two records, `driver` is the backend taking
//! the cell down, `release` gives back its share of the node and removes its directory, and
//! `total` is all of it.
//!
//! A disk snapshot is timed too: `read` is what the cell wrote being read out, which is how long a
//! running cell stands frozen, `build` makes that a layer and stores it, and `total` is all of it.
//!
//! `hive_cells` counts the live cells by state, and `hive_cells_ended_total` the cells that ended,
//! by their last state and cause. A cell that failed ended for an infrastructure cause, so the
//! share of ended cells that failed is the infra failure rate the SLO is about.

use hive_telemetry::{CounterVec, GaugeVec, HistogramVec, Registry};
use hive_types::{Backend, Cause, CellState};
use std::time::{Duration, SystemTime};

/// The comb's metrics and the registry they are in.
#[derive(Clone, Debug)]
pub struct Metrics {
    registry: Registry,
    create_seconds: HistogramVec,
    creates: CounterVec,
    stop_seconds: HistogramVec,
    snapshot_seconds: HistogramVec,
    exec_seconds: HistogramVec,
    policy_seconds: HistogramVec,
    stall: GaugeVec,
    reclaimed: CounterVec,
    squeezed: CounterVec,
    trimmed: CounterVec,
    cells: GaugeVec,
    ended: CounterVec,
    cordoned: GaugeVec,
    started_at: GaugeVec,
    start_cells: GaugeVec,
}

impl Default for Metrics {
    fn default() -> Self {
        let registry = Registry::new();
        Self {
            create_seconds: registry.histogram(
                "hive_create_seconds",
                "How long each stage of a create took.",
                &["backend", "stage"],
            ),
            creates: registry.counter(
                "hive_create_total",
                "Creates that ended, by how they ended.",
                &["backend", "result"],
            ),
            stop_seconds: registry.histogram(
                "hive_stop_seconds",
                "How long each stage of a stop took.",
                &["backend", "stage"],
            ),
            snapshot_seconds: registry.histogram(
                "hive_snapshot_seconds",
                "How long each stage of a disk snapshot took.",
                &["stage"],
            ),
            exec_seconds: registry.histogram(
                "hive_exec_seconds",
                "Exec calls from the comb's side, from the request to the answer.",
                &["op"],
            ),
            policy_seconds: registry.histogram(
                "hive_policy_seconds",
                "How long each policy plugin took over a create, by its verdict.",
                &["policy", "result"],
            ),
            stall: registry.gauge(
                "hive_memory_stall_basis_points",
                "Share of the last 10 s in which a cell stalled on memory, in hundredths of a percent.",
                &[],
            ),
            reclaimed: registry.counter(
                "hive_memory_reclaimed_bytes_total",
                "Memory the pressure brake took back from best effort cells.",
                &[],
            ),
            squeezed: registry.counter(
                "hive_pressure_pauses_total",
                "Idle cells the pressure brake paused.",
                &[],
            ),
            trimmed: registry.counter(
                "hive_memory_trimmed_bytes_total",
                "Page cache idle running cells gave back.",
                &[],
            ),
            cells: registry.gauge("hive_cells", "Live cells, by state.", &["backend", "state"]),
            ended: registry.counter(
                "hive_cells_ended_total",
                "Cells that ended, by their last state and why they ended.",
                &["backend", "state", "cause"],
            ),
            cordoned: registry.gauge(
                "hive_node_cordoned",
                "1 when the node is cordoned and takes no new cells.",
                &[],
            ),
            started_at: registry.gauge(
                "hive_comb_start_time_seconds",
                "When this comb started, in seconds since the epoch.",
                &[],
            ),
            start_cells: registry.gauge(
                "hive_comb_start_cells",
                "Cells this comb found live in its WAL when it started, by what became of them: taken_back, ended while no comb ran, or lost.",
                &["result"],
            ),
            registry,
        }
    }
}

impl Metrics {
    /// The registry, for the `/metrics` endpoint.
    #[must_use]
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    pub(crate) fn stage(&self, backend: Backend, stage: &str, took: Duration) {
        self.create_seconds.with(&[backend.as_str(), stage]).observe_duration(took);
    }

    pub(crate) fn created(&self, backend: Backend, result: &str) {
        self.creates.with(&[backend.as_str(), result]).inc();
    }

    pub(crate) fn stopped(&self, backend: Backend, stage: &str, took: Duration) {
        self.stop_seconds.with(&[backend.as_str(), stage]).observe_duration(took);
    }

    pub(crate) fn snapshot(&self, stage: &str, took: Duration) {
        self.snapshot_seconds.with(&[stage]).observe_duration(took);
    }

    pub(crate) fn exec(&self, op: &str, took: Duration) {
        self.exec_seconds.with(&[op]).observe_duration(took);
    }

    pub(crate) fn policy(&self, policy: &str, result: &str, took: Duration) {
        self.policy_seconds.with(&[policy, result]).observe_duration(took);
    }

    pub(crate) fn pressure(&self, avg10: f64) {
        self.stall.with(&[]).set((avg10 * 100.0).round() as i64);
    }

    pub(crate) fn reclaimed(&self, bytes: u64) {
        self.reclaimed.with(&[]).add(bytes);
    }

    pub(crate) fn squeezed(&self) {
        self.squeezed.with(&[]).inc();
    }

    pub(crate) fn trimmed(&self, bytes: u64) {
        self.trimmed.with(&[]).add(bytes);
    }

    /// A cell went from `from` to `to`, or showed up in `to` when `from` is `None`. Only a cell
    /// that was live and no longer is counts as ended.
    pub(crate) fn moved(
        &self,
        backend: Backend,
        from: Option<CellState>,
        to: CellState,
        cause: Option<Cause>,
    ) {
        if from == Some(to) {
            return;
        }
        let b = backend.as_str();
        if let Some(f) = from
            && !f.is_terminal()
        {
            self.cells.with(&[b, &f.as_str().to_ascii_lowercase()]).add(-1);
        }
        let state = to.as_str().to_ascii_lowercase();
        if !to.is_terminal() {
            self.cells.with(&[b, &state]).add(1);
        } else if from.is_some_and(|f| !f.is_terminal()) {
            self.ended.with(&[b, &state, cause.map_or("none", Cause::as_str)]).inc();
        }
    }

    pub(crate) fn cordoned(&self, on: bool) {
        self.cordoned.with(&[]).set(i64::from(on));
    }

    pub(crate) fn started(&self, taken_back: u32, ended: u32, lost: u32) {
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
        self.started_at.with(&[]).set(i64::try_from(now.as_secs()).unwrap_or(i64::MAX));
        for (result, n) in [("taken_back", taken_back), ("ended", ended), ("lost", lost)] {
            self.start_cells.with(&[result]).set(i64::from(n));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stages_and_results_show_up_in_the_text_format() {
        let m = Metrics::default();
        m.stage(Backend::Container, "pool", Duration::from_millis(3));
        m.created(Backend::Container, "ok");
        m.exec("run", Duration::from_millis(2));
        m.stopped(Backend::Container, "driver", Duration::from_millis(4));
        m.snapshot("read", Duration::from_millis(5));
        m.trimmed(4096);
        m.policy("rl", "change", Duration::from_micros(80));
        let c = Backend::Container;
        m.moved(c, None, CellState::Pending, None);
        m.moved(c, None, CellState::Running, None);
        m.moved(c, None, CellState::Stopped, Some(Cause::Requested));
        m.moved(c, Some(CellState::Pending), CellState::Failed, Some(Cause::StartFailed));
        m.moved(c, Some(CellState::Running), CellState::Running, None);
        m.moved(c, Some(CellState::Running), CellState::Stopping, Some(Cause::Idle));
        m.moved(c, Some(CellState::Stopping), CellState::Stopped, Some(Cause::Idle));
        m.cordoned(true);
        m.started(4, 1, 0);
        let text = m.registry().render();
        assert!(text.contains(r#"hive_create_seconds_count{backend="container",stage="pool"} 1"#));
        assert!(text.contains(r#"hive_create_total{backend="container",result="ok"} 1"#));
        assert!(text.contains(r#"hive_exec_seconds_count{op="run"} 1"#));
        assert!(text.contains(r#"hive_stop_seconds_count{backend="container",stage="driver"} 1"#));
        assert!(text.contains(r#"hive_snapshot_seconds_count{stage="read"} 1"#));
        assert!(text.contains("hive_memory_trimmed_bytes_total 4096"));
        assert!(text.contains(r#"hive_policy_seconds_count{policy="rl",result="change"} 1"#));
        assert!(text.contains(r#"hive_cells{backend="container",state="pending"} 0"#), "{text}");
        assert!(text.contains(r#"hive_cells{backend="container",state="running"} 0"#), "{text}");
        assert!(text.contains(r#"hive_cells{backend="container",state="stopping"} 0"#), "{text}");
        let ended = |state, cause| {
            format!(
                r#"hive_cells_ended_total{{backend="container",state="{state}",cause="{cause}"}} 1"#
            )
        };
        assert!(text.contains(&ended("failed", "start_failed")), "{text}");
        assert!(text.contains(&ended("stopped", "idle")), "{text}");
        assert!(!text.contains("requested"), "a cell that was never live did not end here");
        assert!(text.contains("hive_node_cordoned 1"));
        assert!(text.contains(r#"hive_comb_start_cells{result="taken_back"} 4"#));
    }
}
