//! Reports to scout: what the node has and what it has given out, once a second, and at once
//! when that moves a lot.
//!
//! The comb keeps one stream open to scout and opens a new one when it breaks, waiting a little
//! longer each time up to [`MAX_BACKOFF`]. The first report on a stream carries the layer filter,
//! and later ones only when the mounted layers changed.

use std::collections::HashMap;
use std::sync::PoisonError;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use hive_proto::internal as pb;
use hive_proto::internal::scout_client::ScoutClient;
use hive_scout::NodeReport;
pub use hive_scout::project_id;
use hive_types::{Backend, Qos};
use hive_waggle::{BackendSet, LayerBloom};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{Comb, config::ScoutLink};

/// How often a report goes out when nothing much changes.
pub const EVERY: Duration = Duration::from_secs(1);

/// How often the node looks for a change big enough to report at once.
const LOOK: Duration = Duration::from_millis(100);

/// The longest wait between tries to reach scout.
pub const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// A change in cells or memory bigger than this share of the node's room is reported at once.
const URGENT_SHARE: f64 = 0.05;

/// Projects with the most cells on the node that a report names.
const TOP_PROJECTS: usize = 8;

/// Pool depth a node without pools reports, which placement reads as a full pool.
const NO_POOL: u32 = 64;

/// Sends reports to `link` until `stop`.
pub async fn run(comb: Comb, link: ScoutLink, stop: CancellationToken) {
    const FIRST: Duration = Duration::from_millis(200);
    let mut reporter = Reporter::new(&comb, &link);
    let (mut backoff, mut quiet) = (FIRST, false);
    loop {
        let started = Instant::now();
        let Err(e) = tokio::select! {
            () = stop.cancelled() => return,
            r = reporter.stream(&comb, &link.endpoint) => r,
        };
        // A stream that lasted starts the backoff over. Only the first failure in a row is
        // logged, so a scout that is down for an hour does not fill the log.
        if started.elapsed() > MAX_BACKOFF {
            (backoff, quiet) = (FIRST, false);
        }
        if !quiet {
            eprintln!("hive-comb: reporting to scout at {}: {e}", link.endpoint);
            quiet = true;
        }
        tokio::select! {
            () = stop.cancelled() => return,
            () = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// What the last report said, to tell when the next one has to go at once.
struct Reporter {
    addr: std::sync::Arc<str>,
    seq: u64,
    cpu_milli: u64,
    backends: BackendSet,
    burst_cap: u32,
    rate: Rate,
    last: Option<Sent>,
    layers: Vec<[u8; 32]>,
}

#[derive(Clone, Copy)]
struct Sent {
    at: Instant,
    healthy: bool,
    cells: u32,
    mem_mib: u64,
}

impl Reporter {
    fn new(comb: &Comb, link: &ScoutLink) -> Self {
        let inner = &comb.inner;
        let backends: Vec<Backend> = inner.drivers.iter().map(|d| d.backend()).collect();
        let burst_cap = backends
            .iter()
            .filter_map(|b| inner.cfg.create_limit.get(b))
            .max()
            .map_or(0, |&n| u32::try_from(n).unwrap_or(u32::MAX));
        let cpus = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
        Self {
            addr: link.advertise.as_str().into(),
            seq: 0,
            cpu_milli: cpus as u64 * 1000,
            backends: BackendSet::of(&backends),
            burst_cap,
            rate: Rate::default(),
            last: None,
            layers: Vec::new(),
        }
    }

    /// One stream: connects, then reports until it breaks.
    async fn stream(
        &mut self,
        comb: &Comb,
        endpoint: &str,
    ) -> Result<std::convert::Infallible, String> {
        let channel = tonic::transport::Endpoint::from_shared(endpoint.to_owned())
            .map_err(|e| e.to_string())?
            .connect_timeout(MAX_BACKOFF)
            .tcp_nodelay(true)
            .connect()
            .await
            .map_err(|e| e.to_string())?;
        let mut client = ScoutClient::new(channel);
        let (tx, rx) = mpsc::channel::<pb::NodeReport>(4);
        let out =
            futures::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|r| (r, rx)) });
        // The first report goes before the call, so scout has something to answer.
        let first = self.report(comb, true);
        tx.send(pb::NodeReport::from(&first)).await.map_err(|e| e.to_string())?;
        let mut acks = client.report(out).await.map_err(|e| e.message().to_owned())?.into_inner();
        let mut look = tokio::time::interval(LOOK);
        look.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                ack = acks.next() => match ack {
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Err(e.message().to_owned()),
                    None => return Err("scout closed the stream".into()),
                },
                _ = look.tick() => {
                    if self.due(comb) {
                        let r = self.report(comb, false);
                        tx.send(pb::NodeReport::from(&r)).await.map_err(|e| e.to_string())?;
                    }
                }
            }
        }
    }

    /// Whether a report should go now: a second passed, health changed, or cells or memory
    /// moved by more than 5% of the node's room.
    fn due(&self, comb: &Comb) -> bool {
        let Some(last) = self.last else { return true };
        if last.at.elapsed() >= EVERY {
            return true;
        }
        let inner = &comb.inner;
        let usage = inner.admission.usage();
        let admit = inner.admission.ceiling_for(Qos::Standard) >> 20;
        let max = inner.admission.max_cells() as u64;
        let moved = |a: u64, b: u64, whole: u64| a.abs_diff(b) as f64 > whole as f64 * URGENT_SHARE;
        !inner.shutdown.is_cancelled() != last.healthy
            || moved(u64::from(last.cells), usage.cells as u64, max)
            || moved(last.mem_mib, usage.mem >> 20, admit)
    }

    /// The node as it is now. `whole` sends the layer filter whether or not it changed.
    fn report(&mut self, comb: &Comb, whole: bool) -> NodeReport {
        let inner = &comb.inner;
        let usage = inner.admission.usage();
        let now = Instant::now();
        // A comb that restarts in the same epoch has to go on counting upwards, so seq starts
        // from the clock.
        let clock = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_micros());
        self.seq = (self.seq + 1).max(u64::try_from(clock).unwrap_or(u64::MAX));
        let healthy = !inner.shutdown.is_cancelled();
        let cells = u32::try_from(usage.cells).unwrap_or(u32::MAX);
        let mem_committed_mib = usage.mem >> 20;
        let mut layers: Vec<[u8; 32]> = inner.mounted_layers();
        layers.sort_unstable();
        let changed = layers != self.layers;
        let bloom = (whole || changed).then(|| {
            let mut b = LayerBloom::default();
            for d in &layers {
                b.insert(d);
            }
            b
        });
        self.layers = layers;
        self.last = Some(Sent { at: now, healthy, cells, mem_mib: mem_committed_mib });
        NodeReport {
            node: inner.cfg.node,
            epoch: inner.cfg.epoch,
            seq: self.seq,
            addr: self.addr.clone(),
            healthy,
            backends: self.backends,
            cpu_milli: self.cpu_milli,
            cpu_committed_milli: usage.cpu_milli,
            mem_admit_mib: inner.admission.ceiling_for(Qos::Standard) >> 20,
            mem_committed_mib,
            cells,
            max_cells: u32::try_from(inner.admission.max_cells()).unwrap_or(u32::MAX),
            pool_depth: self.pool_depth(comb),
            create_rate: self.rate.update(usage.admitted, now),
            burst_cap: self.burst_cap,
            layers: bloom,
            top_projects: top_projects(comb),
        }
    }

    /// The shallower of the standard cgroup pool and the network namespace pool, since a create
    /// needs one of each.
    fn pool_depth(&self, comb: &Comb) -> u32 {
        let cgroups = comb.spare_cgroups().map(|d| d[1]);
        let netns = comb.spare_netns();
        let depth = match (cgroups, netns) {
            (Some(a), Some(b)) => a.min(b),
            (Some(a), None) | (None, Some(a)) => a,
            (None, None) => return NO_POOL,
        };
        u32::try_from(depth).unwrap_or(u32::MAX)
    }
}

/// The projects with the most live cells on the node.
fn top_projects(comb: &Comb) -> Vec<(u64, u32)> {
    let mut by: HashMap<&str, u32> = HashMap::new();
    let shards: Vec<_> = comb
        .inner
        .shards
        .iter()
        .map(|s| s.read().unwrap_or_else(PoisonError::into_inner))
        .collect();
    for cell in shards.iter().flat_map(|s| s.values()) {
        if !cell.state().is_terminal() {
            *by.entry(cell.project.as_str()).or_default() += 1;
        }
    }
    let mut top: Vec<(u64, u32)> = by.into_iter().map(|(p, n)| (project_id(p), n)).collect();
    top.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    top.truncate(TOP_PROJECTS);
    top
}

/// Creates a second, as a moving average over about the last 5 seconds.
#[derive(Default)]
struct Rate {
    last: Option<(u64, Instant)>,
    rate: f64,
}

impl Rate {
    fn update(&mut self, admitted: u64, now: Instant) -> f64 {
        if let Some((before, at)) = self.last {
            let secs = now.duration_since(at).as_secs_f64();
            if secs > 0.0 {
                let fresh = admitted.saturating_sub(before) as f64 / secs;
                // Weighted by how long the gap was, so reports sent early after a big change do
                // not swing the average more than a whole second would.
                let w = (secs / 5.0).min(1.0);
                self.rate += (fresh - self.rate) * w;
            }
        }
        self.last = Some((admitted, now));
        self.rate
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_ids_are_stable_and_differ() {
        assert_eq!(project_id("swe"), project_id("swe"));
        assert_ne!(project_id("swe"), project_id("bench"));
    }

    #[test]
    fn the_rate_follows_creates_and_decays() {
        let t0 = Instant::now();
        let mut r = Rate::default();
        assert_eq!(r.update(0, t0), 0.0);
        let mut at = t0;
        for i in 1..=30 {
            at += Duration::from_secs(1);
            r.update(i * 100, at);
        }
        let busy = r.update(3000, at + Duration::from_millis(1));
        assert!((95.0..=100.0).contains(&busy), "{busy}");
        for _ in 0..30 {
            at += Duration::from_secs(1);
            r.update(3000, at);
        }
        assert!(r.update(3000, at + Duration::from_secs(1)) < 1.0);
    }
}
