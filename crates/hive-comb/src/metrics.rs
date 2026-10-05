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

use hive_telemetry::{CounterVec, GaugeVec, HistogramVec, Registry};
use hive_types::Backend;
use std::time::Duration;

/// The comb's metrics and the registry they are in.
#[derive(Clone, Debug)]
pub struct Metrics {
    registry: Registry,
    create_seconds: HistogramVec,
    creates: CounterVec,
    stop_seconds: HistogramVec,
    exec_seconds: HistogramVec,
    stall: GaugeVec,
    reclaimed: CounterVec,
    squeezed: CounterVec,
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
            exec_seconds: registry.histogram(
                "hive_exec_seconds",
                "Exec calls from the comb's side, from the request to the answer.",
                &["op"],
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

    pub(crate) fn exec(&self, op: &str, took: Duration) {
        self.exec_seconds.with(&[op]).observe_duration(took);
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
        let text = m.registry().render();
        assert!(text.contains(r#"hive_create_seconds_count{backend="container",stage="pool"} 1"#));
        assert!(text.contains(r#"hive_create_total{backend="container",result="ok"} 1"#));
        assert!(text.contains(r#"hive_exec_seconds_count{op="run"} 1"#));
        assert!(text.contains(r#"hive_stop_seconds_count{backend="container",stage="driver"} 1"#));
    }
}
