//! The registry and the labelled families in it.

use crate::histogram::{Buckets, Histogram};
use std::collections::HashMap;
use std::fmt::{self, Write};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};

/// The only label names a metric may use. There is no cell id here on purpose: with hundreds of
/// thousands of live cells and millions a day, one series per cell would sink the metrics store.
/// Per cell numbers go to the columnar store instead.
pub const ALLOWED_LABELS: &[&str] = &[
    "backend",
    "cause",
    "metric",
    "node",
    "op",
    "path",
    "pool",
    "project",
    "qos",
    "reason",
    "result",
    "stage",
    "state",
    "template_class",
    "unit",
];

/// How many series a family may hold before new label values are folded into `other`.
pub const DEFAULT_MAX_SERIES: usize = 1000;

const OTHER: &str = "other";

/// Something a family holds one of per label set.
pub trait Series: Clone + Send + Sync + 'static {
    /// The Prometheus type name.
    const TYPE: &'static str;
    /// What a family of these is built with.
    type Opts: Clone + Send + Sync + 'static;
    /// A fresh series.
    fn new(opts: &Self::Opts) -> Self;
    /// Appends this series in the text format. `labels` is the rendered label list without
    /// braces, possibly empty.
    fn render(&self, name: &str, labels: &str, out: &mut String);
}

/// A set of series with the same name and label names.
pub struct Family<S: Series> {
    inner: Arc<FamilyInner<S>>,
}

impl<S: Series> Clone for Family<S> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

// Keyed by the encoded label values, holding the values themselves for rendering.
type Table<S> = HashMap<String, (Box<[String]>, S)>;

struct FamilyInner<S: Series> {
    name: String,
    help: String,
    labels: Vec<&'static str>,
    opts: S::Opts,
    max_series: usize,
    series: RwLock<Table<S>>,
    overflow: AtomicU64,
}

impl<S: Series> fmt::Debug for Family<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Family")
            .field("name", &self.inner.name)
            .field("labels", &self.inner.labels)
            .finish()
    }
}

impl<S: Series> Family<S> {
    /// The series for `values`, one per label name in order. Look it up once and keep it, since
    /// this takes a lock and the handle does not.
    ///
    /// # Panics
    ///
    /// If the number of values does not match the number of label names, which is a bug at the
    /// call site.
    pub fn with(&self, values: &[&str]) -> S {
        let inner = &*self.inner;
        assert_eq!(
            values.len(),
            inner.labels.len(),
            "{} takes labels {:?}",
            inner.name,
            inner.labels
        );
        let key = key(values);
        if let Some(s) = inner.series.read().unwrap_or_else(PoisonError::into_inner).get(&key) {
            return s.1.clone();
        }
        let mut series = inner.series.write().unwrap_or_else(PoisonError::into_inner);
        if let Some(s) = series.get(&key) {
            return s.1.clone();
        }
        let (key, values): (String, Box<[String]>) = if series.len() >= inner.max_series {
            inner.overflow.fetch_add(1, Ordering::Relaxed);
            let other = vec![OTHER; values.len()];
            (self::key(&other), other.iter().map(|v| (*v).to_string()).collect())
        } else {
            (key, values.iter().map(|v| (*v).to_string()).collect())
        };
        series.entry(key).or_insert_with(|| (values, S::new(&inner.opts))).1.clone()
    }
}

// Length prefixes keep ["a,b"] and ["a", "b"] apart whatever the values contain.
fn key(values: &[&str]) -> String {
    let mut k = String::with_capacity(values.iter().map(|v| v.len() + 4).sum());
    for v in values {
        let _ = write!(k, "{}:{v}", v.len());
    }
    k
}

trait Render: Send + Sync {
    fn render(&self, out: &mut String);
    fn overflow(&self) -> (&str, u64);
}

impl<S: Series> Render for FamilyInner<S> {
    fn render(&self, out: &mut String) {
        let _ = writeln!(
            out,
            "# HELP {} {}",
            self.name,
            self.help.replace('\\', "\\\\").replace('\n', "\\n")
        );
        let _ = writeln!(out, "# TYPE {} {}", self.name, S::TYPE);
        let series = self.series.read().unwrap_or_else(PoisonError::into_inner);
        let mut rows: Vec<_> = series.values().map(|(values, s)| (values, s)).collect();
        rows.sort_by(|a, b| a.0.cmp(b.0));
        for (values, s) in rows {
            let mut labels = String::new();
            for (i, (k, v)) in self.labels.iter().zip(values.iter()).enumerate() {
                if i > 0 {
                    labels.push(',');
                }
                let _ = write!(labels, "{k}=\"{}\"", escape(v));
            }
            s.render(&self.name, &labels, out);
        }
    }

    fn overflow(&self) -> (&str, u64) {
        (&self.name, self.overflow.load(Ordering::Relaxed))
    }
}

pub(crate) fn escape(v: &str) -> String {
    v.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n")
}

/// All the metrics of one process.
#[derive(Clone, Default)]
pub struct Registry {
    families: Arc<Mutex<Vec<Arc<dyn Render>>>>,
}

impl fmt::Debug for Registry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registry").field("families", &self.lock().len()).finish()
    }
}

impl Registry {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<Arc<dyn Render>>> {
        self.families.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Registers a family. Names and labels are fixed at startup, so a bad one is a bug and
    /// panics.
    ///
    /// # Panics
    ///
    /// If `name` is not a valid metric name, a label is not in [`ALLOWED_LABELS`] or appears
    /// twice, or `max_series` is zero.
    pub fn register<S: Series>(
        &self,
        name: &str,
        help: &str,
        labels: &[&'static str],
        opts: S::Opts,
        max_series: usize,
    ) -> Family<S> {
        assert!(is_metric_name(name), "{name:?} is not a metric name");
        assert!(max_series > 0, "{name} needs room for at least one series");
        for (i, l) in labels.iter().enumerate() {
            assert!(ALLOWED_LABELS.contains(l), "{name} uses label {l:?}, which is not allowed");
            assert!(!labels[..i].contains(l), "{name} uses label {l:?} twice");
        }
        let inner = Arc::new(FamilyInner {
            name: name.to_string(),
            help: help.to_string(),
            labels: labels.to_vec(),
            opts,
            max_series,
            series: RwLock::new(HashMap::new()),
            overflow: AtomicU64::new(0),
        });
        self.lock().push(inner.clone());
        Family { inner }
    }

    /// Registers a counter family.
    ///
    /// # Panics
    ///
    /// As [`Registry::register`].
    pub fn counter(&self, name: &str, help: &str, labels: &[&'static str]) -> CounterVec {
        self.register(name, help, labels, (), DEFAULT_MAX_SERIES)
    }

    /// Registers a gauge family.
    ///
    /// # Panics
    ///
    /// As [`Registry::register`].
    pub fn gauge(&self, name: &str, help: &str, labels: &[&'static str]) -> GaugeVec {
        self.register(name, help, labels, (), DEFAULT_MAX_SERIES)
    }

    /// Registers a histogram family of durations in seconds, from a microsecond to about an
    /// hour.
    ///
    /// # Panics
    ///
    /// As [`Registry::register`].
    pub fn histogram(&self, name: &str, help: &str, labels: &[&'static str]) -> HistogramVec {
        self.register(name, help, labels, Buckets::SECONDS, DEFAULT_MAX_SERIES)
    }

    /// Everything in the Prometheus text format, version 0.0.4.
    #[must_use]
    pub fn render(&self) -> String {
        let families = self.lock().clone();
        let mut out = String::with_capacity(4096);
        let mut overflowed = Vec::new();
        for f in &families {
            f.render(&mut out);
            let (name, n) = f.overflow();
            if n > 0 {
                overflowed.push((name.to_string(), n));
            }
        }
        if !overflowed.is_empty() {
            out.push_str("# HELP hive_telemetry_overflow_total Lookups folded into the other series because a metric hit its series cap.\n");
            out.push_str("# TYPE hive_telemetry_overflow_total counter\n");
            for (name, n) in overflowed {
                let _ = writeln!(out, "hive_telemetry_overflow_total{{metric=\"{name}\"}} {n}");
            }
        }
        out
    }
}

fn is_metric_name(s: &str) -> bool {
    let mut chars = s.chars();
    chars.next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_' || c == ':')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
}

/// A number that only goes up.
#[derive(Clone, Debug, Default)]
pub struct Counter(Arc<AtomicU64>);

impl Counter {
    /// Adds one.
    pub fn inc(&self) {
        self.add(1);
    }

    /// Adds `n`.
    pub fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }

    /// The current value.
    #[must_use]
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

impl Series for Counter {
    const TYPE: &'static str = "counter";
    type Opts = ();

    fn new((): &()) -> Self {
        Self::default()
    }

    fn render(&self, name: &str, labels: &str, out: &mut String) {
        let _ = writeln!(out, "{name}{} {}", braces(labels), self.get());
    }
}

/// A number that goes up and down.
#[derive(Clone, Debug, Default)]
pub struct Gauge(Arc<AtomicI64>);

impl Gauge {
    /// Sets the value.
    pub fn set(&self, v: i64) {
        self.0.store(v, Ordering::Relaxed);
    }

    /// Adds `n`, which may be negative.
    pub fn add(&self, n: i64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }

    /// The current value.
    #[must_use]
    pub fn get(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

impl Series for Gauge {
    const TYPE: &'static str = "gauge";
    type Opts = ();

    fn new((): &()) -> Self {
        Self::default()
    }

    fn render(&self, name: &str, labels: &str, out: &mut String) {
        let _ = writeln!(out, "{name}{} {}", braces(labels), self.get());
    }
}

pub(crate) fn braces(labels: &str) -> String {
    if labels.is_empty() { String::new() } else { format!("{{{labels}}}") }
}

/// A family of counters.
pub type CounterVec = Family<Counter>;
/// A family of gauges.
pub type GaugeVec = Family<Gauge>;
/// A family of histograms.
pub type HistogramVec = Family<Histogram>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_and_gauges_render_in_label_order() {
        let r = Registry::new();
        let c = r.counter("hive_create_total", "Creates.", &["backend", "result"]);
        c.with(&["oci", "ok"]).add(3);
        c.with(&["fc", "ok"]).inc();
        c.with(&["oci", "ok"]).inc();
        let g = r.gauge("hive_cells", "Cells.", &[]);
        g.with(&[]).set(-2);
        assert_eq!(
            r.render(),
            "# HELP hive_create_total Creates.\n# TYPE hive_create_total counter\n\
             hive_create_total{backend=\"fc\",result=\"ok\"} 1\n\
             hive_create_total{backend=\"oci\",result=\"ok\"} 4\n\
             # HELP hive_cells Cells.\n# TYPE hive_cells gauge\nhive_cells -2\n"
        );
    }

    #[test]
    #[should_panic(expected = "not allowed")]
    fn a_cell_id_label_is_refused() {
        Registry::new().counter("hive_exec_total", "Execs.", &["cell_id"]);
    }

    #[test]
    #[should_panic(expected = "not a metric name")]
    fn a_bad_name_is_refused() {
        Registry::new().counter("hive-exec", "Execs.", &[]);
    }

    #[test]
    fn series_past_the_cap_fold_into_other() {
        let r = Registry::new();
        let c: CounterVec = r.register("hive_x_total", "X.", &["project"], (), 2);
        for p in ["a", "b", "c", "d", "a"] {
            c.with(&[p]).inc();
        }
        let out = r.render();
        assert!(out.contains("hive_x_total{project=\"a\"} 2\n"));
        assert!(out.contains("hive_x_total{project=\"other\"} 2\n"));
        assert!(out.contains("hive_telemetry_overflow_total{metric=\"hive_x_total\"} 2\n"));
    }

    #[test]
    fn label_values_are_escaped() {
        let r = Registry::new();
        r.counter("hive_y_total", "Y.", &["reason"]).with(&["a \"b\"\\\nc"]).inc();
        assert!(r.render().contains(r#"hive_y_total{reason="a \"b\"\\\nc"} 1"#));
    }

    #[test]
    fn counting_from_many_threads_loses_nothing() {
        let c = Registry::new().counter("hive_z_total", "Z.", &["op"]).with(&["run"]);
        std::thread::scope(|s| {
            for _ in 0..8 {
                s.spawn(|| (0..100_000).for_each(|_| c.inc()));
            }
        });
        assert_eq!(c.get(), 800_000);
    }
}
