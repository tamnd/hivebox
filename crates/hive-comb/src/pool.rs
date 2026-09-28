//! The loop that keeps a pool of spare cgroups or network namespaces topped up.

use std::io;
use std::time::Duration;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// The wait after a batch fails, doubled after every failure in a row up to [`RETRY_MAX`].
const RETRY: Duration = Duration::from_millis(10);
const RETRY_MAX: Duration = Duration::from_secs(5);

/// Calls `fill` on a blocking thread until it says the pool is full, then sleeps until `wake` fires,
/// and returns once `stop` does. `fill` makes one batch and returns whether the pool is full.
///
/// A failed batch is tried again after a backoff rather than on the next take, so a pool that had
/// one bad moment on a quiet node does not stay short until the next cell comes along.
pub(crate) async fn refill<F>(what: &str, fill: F, wake: &Notify, stop: &CancellationToken)
where
    F: Fn() -> io::Result<bool> + Clone + Send + 'static,
{
    let mut retry = RETRY;
    loop {
        let f = fill.clone();
        let filled =
            tokio::task::spawn_blocking(f).await.unwrap_or_else(|e| Err(io::Error::other(e)));
        match filled {
            Ok(true) => {
                retry = RETRY;
                tokio::select! {
                    () = wake.notified() => {}
                    () = stop.cancelled() => return,
                }
            }
            Ok(false) => {
                retry = RETRY;
                if stop.is_cancelled() {
                    return;
                }
            }
            Err(e) => {
                eprintln!("hive-comb: making a spare {what}, trying again in {retry:?}: {e}");
                tokio::select! {
                    () = tokio::time::sleep(retry) => {}
                    () = stop.cancelled() => return,
                }
                retry = (retry * 2).min(RETRY_MAX);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn a_failed_batch_is_tried_again_without_a_take() {
        // Fails twice, then fills the pool in two batches.
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let fill = move || match c.fetch_add(1, Ordering::Relaxed) {
            0 | 1 => Err(io::Error::other("no room")),
            2 => Ok(false),
            _ => Ok(true),
        };
        let wake = Arc::new(Notify::new());
        let stop = CancellationToken::new();
        let task = tokio::spawn({
            let (wake, stop) = (wake.clone(), stop.clone());
            async move { refill("thing", fill, &wake, &stop).await }
        });
        let until = std::time::Instant::now() + Duration::from_secs(5);
        while calls.load(Ordering::Relaxed) < 4 {
            assert!(std::time::Instant::now() < until, "stuck after {calls:?} calls");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        // Full now, so it waits for a take.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(calls.load(Ordering::Relaxed), 4);
        wake.notify_one();
        while calls.load(Ordering::Relaxed) < 5 {
            assert!(std::time::Instant::now() < until, "a take did not wake it");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        stop.cancel();
        task.await.unwrap();
    }
}
