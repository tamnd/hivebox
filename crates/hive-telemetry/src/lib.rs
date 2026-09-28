//! One place that sets up logs and metrics for every daemon. It also owns the cardinality guard, because a cell id used as a metric label takes down a Prometheus server long before it takes down anything else.
//!
//! The design is in `spec/13_observability_testing_bench.md`, section 1. Metrics live in a [`Registry`] of labelled families. Label names come from a fixed list ([`ALLOWED_LABELS`]), each family has a cap on how many series it may grow, and anything over the cap is folded into one series whose labels all read `other`. Handles are cheap to clone and updating one is a few atomic adds, so the hot path looks a handle up once and keeps it. Histograms use exponential buckets, eight per doubling, so any quantile read off them is within about nine percent. The registry renders the Prometheus text format, and [`serve`] answers `GET /metrics` with it. OTLP export and tracing spans come in a later change.

#![forbid(unsafe_code)]

mod histogram;
mod log;
mod metrics;
mod serve;

pub use histogram::{Buckets, Histogram, Snapshot, Timer};
pub use log::{LogFormat, init_logs};
pub use metrics::{
    ALLOWED_LABELS, Counter, CounterVec, DEFAULT_MAX_SERIES, Family, Gauge, GaugeVec, HistogramVec,
    Registry, Series,
};
pub use serve::serve;
