//! Histograms with exponential buckets.

use crate::metrics::{Series, braces};
use std::fmt::{self, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Where the bucket bounds sit. Bucket 0 holds everything up to `min`, bucket `i` holds values up
/// to `min * 2^(i / per_octave)`, and one more bucket past the last holds the rest.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Buckets {
    /// The upper bound of the first bucket.
    pub min: f64,
    /// Buckets per doubling.
    pub per_octave: u32,
    /// Doublings covered.
    pub octaves: u32,
}

impl Buckets {
    /// Durations in seconds, one microsecond to about 72 minutes, eight buckets per doubling.
    pub const SECONDS: Buckets = Buckets { min: 1e-6, per_octave: 8, octaves: 32 };

    fn finite(&self) -> usize {
        (self.per_octave * self.octaves) as usize + 1
    }

    fn index(&self, v: f64) -> usize {
        if v.is_nan() || v <= self.min {
            return 0;
        }
        let i = ((v / self.min).log2() * f64::from(self.per_octave)).ceil();
        if i >= self.finite() as f64 { self.finite() } else { i as usize }
    }

    /// The upper bound of bucket `i`.
    #[must_use]
    pub fn upper(&self, i: usize) -> f64 {
        if i >= self.finite() {
            f64::INFINITY
        } else {
            self.min * (i as f64 / f64::from(self.per_octave)).exp2()
        }
    }
}

struct Cell {
    buckets: Buckets,
    counts: Box<[AtomicU64]>,
    sum: AtomicU64,
}

/// A distribution of observations. The count is the sum of the buckets rather than a counter of
/// its own, which saves a contended atomic on every observation.
#[derive(Clone)]
pub struct Histogram(Arc<Cell>);

impl fmt::Debug for Histogram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Histogram").field("count", &self.snapshot().count()).finish()
    }
}

impl Histogram {
    /// A histogram with the given buckets, outside any registry.
    #[must_use]
    pub fn new(buckets: Buckets) -> Self {
        let counts = (0..=buckets.finite()).map(|_| AtomicU64::new(0)).collect();
        Self(Arc::new(Cell { buckets, counts, sum: AtomicU64::new(0f64.to_bits()) }))
    }

    /// Records one value.
    pub fn observe(&self, v: f64) {
        let c = &*self.0;
        c.counts[c.buckets.index(v)].fetch_add(1, Ordering::Relaxed);
        let _ = c.sum.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |bits| {
            Some((f64::from_bits(bits) + v).to_bits())
        });
    }

    /// Records a duration in seconds.
    pub fn observe_duration(&self, d: std::time::Duration) {
        self.observe(d.as_secs_f64());
    }

    /// Starts a timer that records the time until it is stopped or dropped.
    #[must_use]
    pub fn start(&self) -> Timer {
        Timer { h: Some(self.clone()), start: std::time::Instant::now() }
    }

    /// A copy of the counts as they are now.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        let c = &*self.0;
        Snapshot {
            buckets: c.buckets,
            counts: c.counts.iter().map(|n| n.load(Ordering::Relaxed)).collect(),
            sum: f64::from_bits(c.sum.load(Ordering::Relaxed)),
        }
    }
}

impl Series for Histogram {
    const TYPE: &'static str = "histogram";
    type Opts = Buckets;

    fn new(opts: &Buckets) -> Self {
        Histogram::new(*opts)
    }

    fn render(&self, name: &str, labels: &str, out: &mut String) {
        let snap = self.snapshot();
        let sep = if labels.is_empty() { "" } else { "," };
        let mut cumulative = 0;
        // Only buckets that hold something are written. The cumulative counts stay correct, and
        // a latency series that lives in a few buckets costs a few lines rather than 257.
        for (i, &n) in snap.counts.iter().enumerate().take(snap.counts.len() - 1) {
            if n == 0 {
                continue;
            }
            cumulative += n;
            let _ = writeln!(
                out,
                "{name}_bucket{{{labels}{sep}le=\"{}\"}} {cumulative}",
                snap.buckets.upper(i)
            );
        }
        let total = snap.count();
        let _ = writeln!(out, "{name}_bucket{{{labels}{sep}le=\"+Inf\"}} {total}");
        let _ = writeln!(out, "{name}_sum{} {}", braces(labels), snap.sum);
        let _ = writeln!(out, "{name}_count{} {total}", braces(labels));
    }
}

/// Records the time since it was started into a histogram.
#[derive(Debug)]
pub struct Timer {
    h: Option<Histogram>,
    start: std::time::Instant,
}

impl Timer {
    /// Records now instead of at drop, and returns the time recorded.
    pub fn stop(mut self) -> std::time::Duration {
        let d = self.start.elapsed();
        if let Some(h) = self.h.take() {
            h.observe_duration(d);
        }
        d
    }

    /// Drops the timer without recording, for a path whose time should not count.
    pub fn discard(mut self) {
        self.h = None;
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        if let Some(h) = self.h.take() {
            h.observe_duration(self.start.elapsed());
        }
    }
}

/// A frozen copy of a histogram.
#[derive(Clone, Debug, PartialEq)]
pub struct Snapshot {
    buckets: Buckets,
    counts: Vec<u64>,
    sum: f64,
}

impl Snapshot {
    /// How many values were recorded.
    #[must_use]
    pub fn count(&self) -> u64 {
        self.counts.iter().sum()
    }

    /// The sum of the values.
    #[must_use]
    pub fn sum(&self) -> f64 {
        self.sum
    }

    /// The upper bound of the bucket holding quantile `q`, so never below the true value and at
    /// most one bucket width above it. `None` when empty.
    #[must_use]
    pub fn quantile(&self, q: f64) -> Option<f64> {
        let total = self.count();
        if total == 0 {
            return None;
        }
        let rank = ((q.clamp(0.0, 1.0) * total as f64).ceil() as u64).max(1);
        let mut seen = 0;
        for (i, &n) in self.counts.iter().enumerate() {
            seen += n;
            if seen >= rank {
                return Some(self.buckets.upper(i));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_land_in_the_right_bucket() {
        let b = Buckets::SECONDS;
        assert_eq!(b.index(0.0), 0);
        assert_eq!(b.index(1e-6), 0);
        assert_eq!(b.index(1.01e-6), 1);
        assert_eq!(b.index(2e-6 * 1.0001), 9);
        assert_eq!(b.index(f64::NAN), 0);
        assert_eq!(b.index(1e9), b.finite());
        for v in [3.3e-6, 0.0123, 0.5, 7.0, 100.0] {
            let i = b.index(v);
            assert!(b.upper(i) >= v && b.upper(i - 1) < v, "{v} in {i}");
        }
    }

    #[test]
    fn quantiles_are_within_one_bucket() {
        let h = Histogram::new(Buckets::SECONDS);
        for i in 1..=1000 {
            h.observe(f64::from(i) / 1000.0);
        }
        let s = h.snapshot();
        assert_eq!(s.count(), 1000);
        assert!((s.sum() - 500.5).abs() < 1e-9);
        for (q, want) in [(0.5, 0.5), (0.99, 0.99), (1.0, 1.0)] {
            let got = s.quantile(q).unwrap();
            assert!(got >= want && got <= want * 1.0906, "p{q}: {got}");
        }
        assert_eq!(Histogram::new(Buckets::SECONDS).snapshot().quantile(0.5), None);
    }

    #[test]
    fn the_text_format_is_cumulative_and_sparse() {
        let r = crate::Registry::new();
        let h = r.histogram("hive_exec_seconds", "Exec latency.", &["op"]).with(&["run"]);
        h.observe(0.001);
        h.observe(0.001);
        h.observe(0.1);
        let out = r.render();
        let buckets: Vec<&str> =
            out.lines().filter(|l| l.starts_with("hive_exec_seconds_bucket")).collect();
        assert_eq!(buckets.len(), 3, "{out}");
        assert!(buckets[0].ends_with(" 2"));
        assert!(buckets[1].ends_with(" 3"));
        assert_eq!(buckets[2], "hive_exec_seconds_bucket{op=\"run\",le=\"+Inf\"} 3");
        assert!(out.contains("hive_exec_seconds_count{op=\"run\"} 3\n"));
    }

    #[test]
    fn a_timer_records_once() {
        let h = Histogram::new(Buckets::SECONDS);
        let t = h.start();
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(t.stop() >= std::time::Duration::from_millis(2));
        drop(h.start());
        h.start().discard();
        assert_eq!(h.snapshot().count(), 2);
    }

    /// Prints what one observation costs with eight threads on one histogram. Run it in release:
    /// `cargo test -p hive-telemetry --release -- --ignored observe_cost --nocapture`.
    #[test]
    #[ignore = "a measurement, not a check"]
    fn observe_cost() {
        for threads in [1, 8] {
            let h = Histogram::new(Buckets::SECONDS);
            let per = 2_000_000;
            let start = std::time::Instant::now();
            std::thread::scope(|s| {
                for t in 0..threads {
                    let h = h.clone();
                    s.spawn(move || {
                        for i in 0..per {
                            h.observe(1e-5 * f64::from((i + t) % 1000 + 1));
                        }
                    });
                }
            });
            let ns = start.elapsed().as_nanos() as f64 / f64::from(per * threads);
            println!("{threads} threads: {ns:.1} ns per observe, amortised");
        }
    }
}
