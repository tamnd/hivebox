//! Time, from the point of view of a service.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::ops::{Add, Sub};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::Duration;
use tokio::sync::oneshot;

/// A timer future.
pub type Sleep = Pin<Box<dyn Future<Output = ()> + Send>>;

/// A point on a clock's monotonic timeline, in nanoseconds from an origin the clock picks. Only
/// differences between two instants from the same clock mean anything.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Instant(u64);

impl Instant {
    /// The instant `nanos` after the clock's origin.
    #[must_use]
    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    /// Nanoseconds since the clock's origin.
    #[must_use]
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    /// How long after `earlier` this is, or zero if it is not after it.
    #[must_use]
    pub fn saturating_duration_since(self, earlier: Instant) -> Duration {
        Duration::from_nanos(self.0.saturating_sub(earlier.0))
    }
}

impl Add<Duration> for Instant {
    type Output = Instant;

    fn add(self, d: Duration) -> Instant {
        Instant(self.0.saturating_add(nanos(d)))
    }
}

impl Sub for Instant {
    type Output = Duration;

    fn sub(self, earlier: Instant) -> Duration {
        self.saturating_duration_since(earlier)
    }
}

fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

/// A clock.
pub trait Clock: Send + Sync + fmt::Debug {
    /// Monotonic time, for measuring intervals and setting deadlines.
    fn now(&self) -> Instant;

    /// Wall time in nanoseconds since the Unix epoch, for timestamps that leave the process.
    fn unix_nanos(&self) -> u64;

    /// A future that completes once `d` has passed on this clock.
    fn sleep(&self, d: Duration) -> Sleep;

    /// A future that completes once this clock reaches `deadline`.
    fn sleep_until(&self, deadline: Instant) -> Sleep {
        self.sleep(deadline.saturating_duration_since(self.now()))
    }
}

/// The error from [`timeout`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Elapsed;

impl fmt::Display for Elapsed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("deadline elapsed")
    }
}

impl std::error::Error for Elapsed {}

/// Runs `fut` until it finishes or `d` passes on `clock`, whichever is first. The future is
/// polled first, so one that is already ready wins even with a zero timeout.
pub async fn timeout<F: Future>(
    clock: &dyn Clock,
    d: Duration,
    fut: F,
) -> Result<F::Output, Elapsed> {
    let fut = std::pin::pin!(fut);
    match futures::future::select(fut, clock.sleep(d)).await {
        futures::future::Either::Left((out, _)) => Ok(out),
        futures::future::Either::Right(((), _)) => Err(Elapsed),
    }
}

/// The real clock, through tokio so that tokio's paused time works in tests.
#[derive(Clone, Copy, Debug, Default)]
pub struct TokioClock;

impl Clock for TokioClock {
    fn now(&self) -> Instant {
        static ORIGIN: OnceLock<tokio::time::Instant> = OnceLock::new();
        let origin = *ORIGIN.get_or_init(tokio::time::Instant::now);
        Instant(nanos(origin.elapsed()))
    }

    fn unix_nanos(&self) -> u64 {
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, nanos)
    }

    fn sleep(&self, d: Duration) -> Sleep {
        Box::pin(tokio::time::sleep(d))
    }
}

/// A clock that moves only when told to. Clones share the same time.
#[derive(Clone, Default)]
pub struct SimClock {
    inner: Arc<Mutex<SimInner>>,
}

#[derive(Default)]
struct SimInner {
    now: u64,
    // Nanoseconds since the Unix epoch at the origin.
    wall_origin: u64,
    seq: u64,
    // Keyed by deadline and then by creation order, so timers with the same deadline fire in the
    // order they were set and a run is reproducible.
    timers: BTreeMap<(u64, u64), oneshot::Sender<()>>,
}

impl fmt::Debug for SimClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self.lock();
        f.debug_struct("SimClock")
            .field("now", &inner.now)
            .field("timers", &inner.timers.len())
            .finish()
    }
}

/// 2030-01-01T00:00:00Z, so simulated timestamps are recognisable in logs.
const SIM_WALL_ORIGIN: u64 = 1_893_456_000 * 1_000_000_000;

impl SimClock {
    /// A clock at its origin, with the wall clock at 2030-01-01.
    #[must_use]
    pub fn new() -> Self {
        let clock = Self::default();
        clock.lock().wall_origin = SIM_WALL_ORIGIN;
        clock
    }

    fn lock(&self) -> MutexGuard<'_, SimInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Moves time forward by `d` and fires every timer that falls due, in deadline order.
    pub fn advance(&self, d: Duration) {
        let fired = {
            let mut inner = self.lock();
            inner.now = inner.now.saturating_add(nanos(d));
            let first_later = (inner.now.saturating_add(1), 0);
            let later = inner.timers.split_off(&first_later);
            std::mem::replace(&mut inner.timers, later)
        };
        for (_, tx) in fired {
            let _ = tx.send(());
        }
    }

    /// The deadline of the next pending timer, if any.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Instant> {
        self.lock().timers.keys().next().map(|&(t, _)| Instant(t))
    }

    /// Jumps to the next pending timer and fires it along with any others due at the same
    /// time. Returns false if nothing was pending.
    pub fn advance_to_next(&self) -> bool {
        let Some(next) = self.next_deadline() else { return false };
        let d = next.saturating_duration_since(self.now());
        self.advance(d);
        true
    }

    /// Steps the wall clock by `d` in either direction without moving monotonic time, the way
    /// NTP or a snapshot restore would.
    pub fn skew_wall(&self, forward: bool, d: Duration) {
        let mut inner = self.lock();
        inner.wall_origin = if forward {
            inner.wall_origin.saturating_add(nanos(d))
        } else {
            inner.wall_origin.saturating_sub(nanos(d))
        };
    }
}

impl Clock for SimClock {
    fn now(&self) -> Instant {
        Instant(self.lock().now)
    }

    fn unix_nanos(&self) -> u64 {
        let inner = self.lock();
        inner.wall_origin.saturating_add(inner.now)
    }

    fn sleep(&self, d: Duration) -> Sleep {
        if d.is_zero() {
            return Box::pin(std::future::ready(()));
        }
        let (tx, rx) = oneshot::channel();
        {
            let mut inner = self.lock();
            let key = (inner.now.saturating_add(nanos(d)), inner.seq);
            inner.seq += 1;
            inner.timers.insert(key, tx);
        }
        // If the clock goes away the timer fires rather than hanging forever.
        Box::pin(async move {
            let _ = rx.await;
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test]
    async fn sim_timers_fire_in_deadline_order_and_only_when_due() {
        let clock = SimClock::new();
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut tasks = Vec::new();
        for (name, ms) in [("c", 30), ("a", 10), ("b", 20), ("a2", 10)] {
            let (clock, order) = (clock.clone(), order.clone());
            let sleep = clock.sleep(Duration::from_millis(ms));
            tasks.push(tokio::spawn(async move {
                sleep.await;
                order.lock().unwrap().push((name, clock.now().as_nanos() / 1_000_000));
            }));
        }
        clock.advance(Duration::from_millis(9));
        tokio::task::yield_now().await;
        assert!(order.lock().unwrap().is_empty());
        while clock.advance_to_next() {
            tokio::task::yield_now().await;
        }
        for t in tasks {
            t.await.unwrap();
        }
        let mut got = order.lock().unwrap().clone();
        got.sort_by_key(|&(_, t)| t);
        assert_eq!(got.iter().map(|&(_, t)| t).collect::<Vec<_>>(), [10, 10, 20, 30]);
        assert_eq!(clock.now(), Instant::from_nanos(30_000_000));
    }

    #[tokio::test]
    async fn timeout_under_sim_depends_only_on_the_clock() {
        let clock = SimClock::new();
        let slow = clock.sleep(Duration::from_secs(10));
        let c2 = clock.clone();
        let t = tokio::spawn(async move { timeout(&c2, Duration::from_secs(5), slow).await });
        tokio::task::yield_now().await;
        clock.advance(Duration::from_secs(5));
        assert_eq!(t.await.unwrap(), Err(Elapsed));
        assert_eq!(timeout(&clock, Duration::ZERO, async { 7 }).await, Ok(7));
    }

    #[tokio::test]
    async fn wall_skew_leaves_monotonic_time_alone() {
        let clock = SimClock::new();
        let (t0, w0) = (clock.now(), clock.unix_nanos());
        clock.skew_wall(false, Duration::from_secs(3600));
        assert_eq!(clock.now(), t0);
        assert_eq!(w0 - clock.unix_nanos(), 3_600_000_000_000);
    }

    #[tokio::test(start_paused = true)]
    async fn the_tokio_clock_follows_paused_time() {
        let fired = Arc::new(AtomicU32::new(0));
        let f = fired.clone();
        let t0 = TokioClock.now();
        tokio::spawn(async move {
            TokioClock.sleep(Duration::from_secs(60)).await;
            f.fetch_add(1, Ordering::SeqCst);
        });
        tokio::time::sleep(Duration::from_secs(61)).await;
        assert_eq!(fired.load(Ordering::SeqCst), 1);
        assert!(TokioClock.now() - t0 >= Duration::from_secs(61));
        assert!(TokioClock.unix_nanos() > SIM_WALL_ORIGIN / 2);
    }
}
