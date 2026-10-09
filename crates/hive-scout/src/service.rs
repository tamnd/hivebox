//! Scout as a service: combs stream reports in over `hivebox.internal.v1.Scout`, a timer
//! publishes snapshots, and watchers follow them as deltas. Another timer runs the rebalancer
//! over the snapshot, since scout is the one place that sees every node.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use futures::StreamExt;
use futures::stream::BoxStream;
use hive_proto::internal as pb;
use hive_proto::internal::scout_server::{Scout as ScoutRpc, ScoutServer};
use hive_telemetry::{CounterVec, GaugeVec, HistogramVec, Registry};
use hive_waggle::{Plan, Rebalancer};
use tokio::sync::{Notify, watch};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, Streaming};

use crate::{Applied, NodeReport, Scout, Snapshot, wire};

/// How often snapshots go out when no report asks for one sooner.
pub const TICK: Duration = Duration::from_millis(100);

/// Urgent reports publish at once, but no more often than this, so a flood of them costs a
/// bounded number of snapshots.
pub const MIN_GAP: Duration = Duration::from_millis(10);

/// Moves a rebalance line on stderr names before it gives the count of the rest.
const SAID_MOVES: usize = 8;

/// A scout shared between the report streams and the timer. Cloning it is cheap.
#[derive(Clone, Debug)]
pub struct Service {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    scout: Mutex<Scout>,
    /// Woken by an urgent report, so [`Service::run`] publishes before the next tick.
    urgent: Notify,
    /// Ends every report stream, since combs never end theirs.
    closing: CancellationToken,
    start: Instant,
    metrics: Metrics,
    /// The rebalancer's last plan.
    plan: Mutex<Option<Arc<Plan>>>,
}

#[derive(Debug)]
struct Metrics {
    registry: Registry,
    reports: CounterVec,
    nodes: GaugeVec,
    cells: GaugeVec,
    mem_admit: GaugeVec,
    mem_committed: GaugeVec,
    version: GaugeVec,
    watchers: GaugeVec,
    plans: CounterVec,
    plan_seconds: HistogramVec,
    hot: GaugeVec,
    move_cells: GaugeVec,
    move_mib: GaugeVec,
    burst_over: GaugeVec,
    burst_cloud_room: GaugeVec,
}

impl Default for Service {
    fn default() -> Self {
        Self::new()
    }
}

impl Service {
    /// A service that knows no nodes yet.
    #[must_use]
    pub fn new() -> Self {
        let registry = Registry::new();
        let metrics = Metrics {
            reports: registry.counter(
                "hive_scout_reports_total",
                "Node reports received, by what became of them.",
                &["result"],
            ),
            nodes: registry.gauge("hive_scout_nodes", "Nodes scout knows, by state.", &["state"]),
            cells: registry.gauge("hive_scout_cells", "Cells on the healthy nodes.", &[]),
            mem_admit: registry.gauge(
                "hive_scout_mem_admit_mib",
                "Memory the healthy nodes admit cells up to, in MiB.",
                &[],
            ),
            mem_committed: registry.gauge(
                "hive_scout_mem_committed_mib",
                "Memory the healthy nodes have given to cells, in MiB.",
                &[],
            ),
            version: registry.gauge(
                "hive_scout_snapshot_version",
                "The last snapshot's version.",
                &[],
            ),
            watchers: registry.gauge(
                "hive_scout_watchers",
                "Streams following the cluster through Watch.",
                &[],
            ),
            plans: registry.counter(
                "hive_scout_rebalance_plans_total",
                "Rounds the rebalancer has planned.",
                &[],
            ),
            plan_seconds: registry.histogram(
                "hive_scout_rebalance_seconds",
                "Time to plan one rebalance round.",
                &[],
            ),
            hot: registry.gauge(
                "hive_scout_hot_nodes",
                "On-prem nodes past the hot share in the last rebalance round.",
                &[],
            ),
            move_cells: registry.gauge(
                "hive_scout_rebalance_cells",
                "Idle cells the last rebalance round said to move off hot nodes.",
                &[],
            ),
            move_mib: registry.gauge(
                "hive_scout_rebalance_mib",
                "Memory those cells hold, in MiB.",
                &[],
            ),
            burst_over: registry.gauge(
                "hive_scout_burst_over_mib",
                "Memory given out past the burst line, in MiB, once the on-prem nodes have stayed past it for 5 rounds, and 0 before.",
                &[],
            ),
            burst_cloud_room: registry.gauge(
                "hive_scout_burst_cloud_room_mib",
                "Memory free on the healthy cloud nodes, in MiB, in the last rebalance round.",
                &[],
            ),
            registry,
        };
        Self {
            inner: Arc::new(Inner {
                scout: Mutex::new(Scout::new()),
                urgent: Notify::new(),
                closing: CancellationToken::new(),
                start: Instant::now(),
                metrics,
                plan: Mutex::new(None),
            }),
        }
    }

    /// The metrics to serve on `/metrics`.
    #[must_use]
    pub fn registry(&self) -> &Registry {
        &self.inner.metrics.registry
    }

    /// The last snapshot published.
    #[must_use]
    pub fn snapshot(&self) -> Arc<Snapshot> {
        self.inner.scout().snapshot()
    }

    /// The rebalancer's last plan, or `None` before its first round.
    #[must_use]
    pub fn plan(&self) -> Option<Arc<Plan>> {
        self.inner.plan.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    /// A receiver that always holds the last snapshot.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<Arc<Snapshot>> {
        self.inner.scout().subscribe()
    }

    /// The gRPC service, to add to a tonic server.
    #[must_use]
    pub fn server(&self) -> ScoutServer<Self> {
        ScoutServer::new(self.clone())
    }

    /// Ends every report stream, now and as they open, so a server shutting down gracefully
    /// does not wait on combs that would report forever. They reconnect, to another scout or to
    /// this one when it is back.
    pub fn close(&self) {
        self.inner.closing.cancel();
    }

    /// Takes one report. An urgent one wakes [`Service::run`] to publish now rather than at
    /// the next tick. The snapshot is never built here, so a report holds the lock for about a
    /// microsecond however many nodes there are.
    pub fn apply(&self, report: NodeReport) -> Applied {
        let now = self.inner.start.elapsed();
        let applied = self.inner.scout().apply(report, now);
        let result = match applied {
            Applied::Taken => "taken",
            Applied::Urgent => {
                self.inner.urgent.notify_one();
                "urgent"
            }
            Applied::Stale => "stale",
        };
        self.inner.metrics.reports.with(&[result]).inc();
        applied
    }

    /// Publishes a snapshot every [`TICK`] while anything changed, and after an urgent report
    /// at most every [`MIN_GAP`], until `stop`.
    pub async fn run(self, stop: CancellationToken) {
        let mut every = tokio::time::interval(TICK);
        every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = stop.cancelled() => return,
                _ = every.tick() => {}
                () = self.inner.urgent.notified() => {}
            }
            self.inner.tick();
            // Urgent reports that came in meanwhile wait out the gap and go in the next one.
            tokio::select! {
                () = stop.cancelled() => return,
                () = tokio::time::sleep(MIN_GAP) => {}
            }
        }
    }

    /// Plans a rebalance over the last snapshot every `every`, starting one `every` from now so
    /// the combs have reported, until `stop`. The plan goes in the metrics and in
    /// [`Service::plan`], and on stderr when it has moves or a burst in it.
    pub async fn rebalance(
        self,
        mut rebalancer: Rebalancer,
        every: Duration,
        stop: CancellationToken,
    ) {
        let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = stop.cancelled() => return,
                _ = tick.tick() => {}
            }
            let snap = self.snapshot();
            let took = Instant::now();
            let plan = rebalancer.plan(&snap.view);
            let m = &self.inner.metrics;
            m.plan_seconds.with(&[]).observe_duration(took.elapsed());
            m.plans.with(&[]).inc();
            m.hot.with(&[]).set(i64::from(plan.hot));
            m.move_cells.with(&[]).set(i64::from(plan.cells()));
            m.move_mib.with(&[]).set(gauge(plan.mem_mib()));
            m.burst_over.with(&[]).set(gauge(plan.burst.map_or(0, |b| b.over_mib)));
            m.burst_cloud_room.with(&[]).set(gauge(plan.burst.map_or(0, |b| b.cloud_room_mib)));
            if let Some(line) = said(&plan) {
                eprintln!("hive-scout: rebalance: {line}");
            }
            *self.inner.plan.lock().unwrap_or_else(PoisonError::into_inner) = Some(Arc::new(plan));
        }
    }
}

/// The plan in one line, or `None` when there is nothing to do.
fn said(plan: &Plan) -> Option<String> {
    if plan.moves.is_empty() && plan.burst.is_none() {
        return None;
    }
    let mut line = format!("on-prem {:.1}% used, {} hot", plan.share * 100.0, plan.hot);
    for m in plan.moves.iter().take(SAID_MOVES) {
        line += &format!(
            "; move {} idle cells ({} MiB) from node {} to node {}",
            m.cells, m.mem_mib, m.from, m.to
        );
    }
    if plan.moves.len() > SAID_MOVES {
        line += &format!("; {} more moves", plan.moves.len() - SAID_MOVES);
    }
    if let Some(b) = plan.burst {
        line += &format!(
            "; past the burst line {} rounds, {} MiB over it, {} MiB idle on-prem, {} MiB free on cloud nodes",
            b.rounds, b.over_mib, b.idle_mib, b.cloud_room_mib
        );
    }
    Some(line)
}

impl Inner {
    fn scout(&self) -> std::sync::MutexGuard<'_, Scout> {
        self.scout.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn tick(&self) {
        let now = self.start.elapsed();
        let Some(snap) = self.scout().tick(now) else { return };
        let (m, t) = (&self.metrics, snap.totals);
        m.nodes.with(&["healthy"]).set(i64::from(t.healthy));
        m.nodes.with(&["down"]).set(i64::from(t.nodes - t.healthy));
        m.cells.with(&[]).set(gauge(t.cells));
        m.mem_admit.with(&[]).set(gauge(t.mem_admit_mib));
        m.mem_committed.with(&[]).set(gauge(t.mem_committed_mib));
        m.version.with(&[]).set(gauge(snap.version));
    }
}

fn gauge(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

#[tonic::async_trait]
impl ScoutRpc for Service {
    type ReportStream = BoxStream<'static, Result<pb::ReportAck, Status>>;

    async fn report(
        &self,
        req: Request<Streaming<pb::NodeReport>>,
    ) -> Result<Response<Self::ReportStream>, Status> {
        let this = self.clone();
        let acks = req.into_inner().map(move |r| {
            let r = r?;
            let seq = r.seq;
            let report =
                NodeReport::try_from(r).map_err(|e| Status::invalid_argument(e.to_string()))?;
            let stale = this.apply(report) == Applied::Stale;
            Ok(pb::ReportAck { seq, stale })
        });
        let closing = self.inner.closing.clone().cancelled_owned();
        Ok(Response::new(acks.take_until(closing).boxed()))
    }

    type WatchStream = BoxStream<'static, Result<pb::ClusterDelta, Status>>;

    /// The whole cluster first, then after each snapshot only the nodes that changed. A watcher
    /// that falls behind skips the snapshots it missed, and the next delta covers them.
    async fn watch(
        &self,
        _req: Request<pb::WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let rx = self.subscribe();
        let watchers = self.inner.metrics.watchers.with(&[]);
        watchers.add(1);
        let guard = Watching(watchers);
        let deltas = futures::stream::unfold(
            (rx, None::<Arc<Snapshot>>, guard),
            |(mut rx, prev, guard)| async move {
                if prev.is_some() && rx.changed().await.is_err() {
                    return None;
                }
                let snap = rx.borrow_and_update().clone();
                let delta = wire::delta(prev.as_deref(), &snap);
                Some((Ok(delta), (rx, Some(snap), guard)))
            },
        );
        let closing = self.inner.closing.clone().cancelled_owned();
        Ok(Response::new(deltas.take_until(closing).boxed()))
    }
}

/// Counts a watcher until its stream is dropped.
struct Watching(hive_telemetry::Gauge);

impl Drop for Watching {
    fn drop(&mut self) {
        self.0.add(-1);
    }
}
