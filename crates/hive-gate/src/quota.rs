//! Project quotas, spent from shares the keeper hands out.
//!
//! The keeper splits a project's quota between the gates, and each gate spends its share on its
//! own, so a create costs no call to the keeper. A gate asks again when half its share is spent
//! or half its life is gone, for about one and a half times what the project asked of it
//! lately. A create the share is too small for asks at once, at most every [`RETRY`], and is
//! refused if the new share is still too small.
//!
//! While no keeper member answers, the gate keeps spending the share it had, and a project it
//! never got a share for is not held to its quota, so the gate keeps working without the keeper.
//! The design is in `spec/04_control_plane.md`, section 2.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use hive_proto::internal as pb;
use hive_proto::internal::keeper_client::KeeperClient;
use hive_scout::project_id;
use hive_telemetry::CounterVec;
use hive_types::{Error, Reason};
use tonic::Code;
use tonic::transport::Channel;

use crate::nodes::Nodes;

/// The soonest a project whose share ran short asks the keeper again.
const RETRY: Duration = Duration::from_millis(500);
/// The fewest cells and creates a second asked for, so a quiet project can still start a
/// burst without waiting for a second share.
const MIN_CELLS: u64 = 64;
const MIN_RATE: u32 = 20;
/// How soon a gate asks again when the keeper said some gate got less than it asked for.
const CONTENDED: Duration = Duration::from_secs(1);
/// How long a project the keeper does not know is let through before the gate asks again.
const UNKNOWN: Duration = Duration::from_secs(30);

/// The quota shares this gate holds, one per project. Cloning it is cheap.
#[derive(Clone, Debug)]
pub struct Quotas {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    gate: String,
    nodes: Nodes,
    members: Vec<(String, KeeperClient<Channel>)>,
    at: AtomicUsize,
    projects: Mutex<HashMap<Arc<str>, Share>>,
    refused: CounterVec,
    asks: CounterVec,
}

impl Quotas {
    /// Shares asked of the keeper at `members`, as `host:port`, for the gate named `gate`, with
    /// live counts from `nodes` and metrics in `registry`.
    ///
    /// # Errors
    ///
    /// A member address does not parse, or there are none.
    pub fn new(
        gate: String,
        members: &[String],
        nodes: Nodes,
        registry: &hive_telemetry::Registry,
    ) -> Result<Self, String> {
        let members = crate::keys::clients(members)?;
        Ok(Self {
            inner: Arc::new(Inner {
                gate,
                nodes,
                members,
                at: AtomicUsize::new(0),
                projects: Mutex::default(),
                refused: registry.counter(
                    "hive_gate_quota_refused_total",
                    "Creates the gate refused for the project's quota, by which limit.",
                    &["reason"],
                ),
                asks: registry.counter(
                    "hive_gate_quota_asks_total",
                    "Shares of quota the gate asked the keeper for, by what came back.",
                    &["result"],
                ),
            }),
        })
    }

    /// Takes `n` cells out of `project`'s share.
    ///
    /// # Errors
    ///
    /// `QUOTA_EXCEEDED` when the project is at its quota of cells or creates a second.
    pub async fn charge(&self, project: &str, n: u32) -> Result<(), Error> {
        let n = u64::from(n);
        let now = Instant::now();
        {
            let mut projects = self.lock();
            if let Some(s) = projects.get_mut(project) {
                match s.charge(n, now) {
                    Ok(()) => {
                        if s.wants_more(now) {
                            s.asking = true;
                            s.asked = now;
                            let this = self.clone();
                            let project = project.to_owned();
                            tokio::spawn(async move { this.ask(&project, 0).await });
                        }
                        return Ok(());
                    }
                    Err(e) if s.asking || now < s.asked + RETRY => {
                        return Err(self.refuse(e));
                    }
                    Err(_) => {
                        s.asking = true;
                        s.asked = now;
                    }
                }
            }
        }
        self.ask(project, n).await;
        let mut projects = self.lock();
        match projects.get_mut(project) {
            Some(s) => s.charge(n, Instant::now()).map_err(|e| self.refuse(e)),
            None => Ok(()),
        }
    }

    /// Gives every share back, so other gates can have it before it runs out. For a gate that
    /// is stopping.
    pub async fn give_back(&self) {
        let projects: Vec<Arc<str>> = self.lock().keys().cloned().collect();
        for project in projects {
            let req = pb::TakeQuotaRequest {
                project: project.to_string(),
                gate: self.inner.gate.clone(),
                ..Default::default()
            };
            let _ = self.call(req).await;
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<Arc<str>, Share>> {
        self.inner.projects.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn refuse(&self, short: Short) -> Error {
        let (limit, message) = match short {
            Short::Cells(q) => ("cells", format!("the project is at its quota of {q} live cells")),
            Short::Rate(q) => {
                ("creates", format!("the project is past its quota of {q} creates a second"))
            }
        };
        self.inner.refused.with(&[limit]).inc();
        Error::new(Reason::QuotaExceeded, message)
    }

    /// Asks the keeper for a new share of `project`'s quota big enough for `need` more cells
    /// on top of what it spent lately, and keeps it.
    async fn ask(&self, project: &str, need: u64) {
        let now = Instant::now();
        let (cells, rate, recent) = match self.lock().get_mut(project) {
            Some(s) => {
                let (c, r) = s.want(now);
                (c.max(need.saturating_mul(2)), r, s.recent(now))
            }
            None => (MIN_CELLS.max(need.saturating_mul(2)), MIN_RATE, 0),
        };
        let snap = self.inner.nodes.snapshot();
        let live = snap.projects.get(&project_id(project)).copied().unwrap_or(0) + recent;
        let req = pb::TakeQuotaRequest {
            project: project.to_owned(),
            gate: self.inner.gate.clone(),
            cells,
            creates_per_s: rate,
            live,
        };
        let got = self.call(req).await;
        let now = Instant::now();
        let mut projects = self.lock();
        match got {
            Ok(slice) => {
                self.inner.asks.with(&["ok"]).inc();
                let q = slice.quota.unwrap_or_default();
                let ttl = Duration::from_millis(slice.ttl_ms);
                let s = projects.entry(Arc::from(project)).or_insert_with(|| Share::new(now));
                s.renew(q.cells, q.creates_per_s, slice.cells, slice.creates_per_s, ttl, now);
                if slice.contended {
                    s.next = CONTENDED;
                }
            }
            Err(e) if e.code() == Code::NotFound => {
                // A project only the config's keys know. It has no quota to hold it to.
                self.inner.asks.with(&["unknown"]).inc();
                let s = projects.entry(Arc::from(project)).or_insert_with(|| Share::new(now));
                s.renew(0, 0, 0, 0, UNKNOWN, now);
            }
            Err(e) => {
                self.inner.asks.with(&["failed"]).inc();
                eprintln!(
                    "hive-gate: asking the keeper for quota for {project}: {}, keeping the share it had",
                    e.message()
                );
                match projects.get_mut(project) {
                    Some(s) => s.asking = false,
                    // No share yet: let the project through for a while, and ask again then
                    // rather than on every create.
                    None => {
                        let mut s = Share::new(now);
                        s.renew(0, 0, 0, 0, RETRY * 4, now);
                        projects.insert(Arc::from(project), s);
                    }
                }
            }
        }
    }

    /// Calls `TakeQuota` on one member after another until one answers, starting at the one
    /// that answered last.
    async fn call(&self, req: pb::TakeQuotaRequest) -> Result<pb::QuotaSlice, tonic::Status> {
        let members = &self.inner.members;
        let start = self.inner.at.load(Ordering::Relaxed);
        let mut last = None;
        for i in 0..members.len() {
            let at = (start + i) % members.len();
            let mut client = members[at].1.clone();
            match client.take_quota(req.clone()).await {
                Ok(r) => {
                    self.inner.at.store(at, Ordering::Relaxed);
                    return Ok(r.into_inner());
                }
                Err(e) if matches!(e.code(), Code::NotFound | Code::InvalidArgument) => {
                    return Err(e);
                }
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap_or_else(|| tonic::Status::unavailable("no keeper member")))
    }
}

/// Which limit a create ran into, with the project's quota for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Short {
    Cells(u64),
    Rate(u32),
}

/// What is left of one project's share, and how much the project asked for lately.
#[derive(Debug)]
struct Share {
    /// The project's whole quota, where 0 is no limit.
    max_cells: u64,
    max_rate: u32,
    /// The cells the last share gave, and how many of them are left.
    granted: u64,
    cells: u64,
    /// The creates a second the share gives, and a bucket of them that holds one second.
    rate: u32,
    tokens: f64,
    filled: Instant,
    /// When the share came, how long it lasts, and how soon to ask for the next.
    got: Instant,
    ttl: Duration,
    next: Duration,
    /// Whether a call to the keeper is on its way, and when the last one started.
    asking: bool,
    asked: Instant,
    /// Cells asked for since the share came, and creates a second asked for lately.
    demand: u64,
    per_s: f64,
    /// When the gate first asked for the project, which the seconds below count from.
    born: Instant,
    /// Cells made in this second and the one before, which scout may not count yet, since a
    /// comb reports once a second and scout sends its snapshot after.
    second: u64,
    now_n: u64,
    last_n: u64,
}

impl Share {
    fn new(now: Instant) -> Self {
        Self {
            max_cells: 0,
            max_rate: 0,
            granted: 0,
            cells: 0,
            rate: 0,
            tokens: 0.0,
            filled: now,
            got: now,
            ttl: Duration::ZERO,
            next: Duration::ZERO,
            asking: true,
            asked: now,
            demand: 0,
            per_s: 0.0,
            born: now,
            second: 0,
            now_n: 0,
            last_n: 0,
        }
    }

    /// Takes in a new share.
    fn renew(
        &mut self,
        max_cells: u64,
        max_rate: u32,
        cells: u64,
        rate: u32,
        ttl: Duration,
        now: Instant,
    ) {
        let secs = now.duration_since(self.got).as_secs_f64();
        if secs > 0.0 {
            let lately = self.demand as f64 / secs;
            self.per_s = if self.per_s == 0.0 { lately } else { (self.per_s + lately) / 2.0 };
        }
        self.max_cells = max_cells;
        self.max_rate = max_rate;
        self.granted = cells;
        self.cells = cells;
        self.fill(now);
        // A new project starts with a full second of creates.
        self.tokens =
            if self.rate == 0 { f64::from(rate) } else { self.tokens.min(f64::from(rate)) };
        self.rate = rate;
        self.got = now;
        self.ttl = ttl;
        self.next = ttl / 2;
        self.asking = false;
        self.demand = 0;
    }

    /// What to ask for: one and a half times what the project asked for lately, the cells for
    /// a whole share's life since the gate asks again halfway.
    fn want(&self, now: Instant) -> (u64, u32) {
        let secs = now.duration_since(self.got).as_secs_f64().max(1.0);
        let per_s = self.per_s.max(self.demand as f64 / secs) * 1.5;
        let cells = (per_s * self.ttl.max(Duration::from_secs(1)).as_secs_f64()) as u64;
        (cells.max(MIN_CELLS), (per_s.ceil() as u32).max(MIN_RATE))
    }

    fn fill(&mut self, now: Instant) {
        let secs = now.duration_since(self.filled).as_secs_f64();
        self.tokens = (self.tokens + secs * f64::from(self.rate)).min(f64::from(self.rate));
        self.filled = now;
    }

    fn charge(&mut self, n: u64, now: Instant) -> Result<(), Short> {
        self.demand += n;
        if self.max_cells > 0 && self.cells < n {
            return Err(Short::Cells(self.max_cells));
        }
        if self.max_rate > 0 {
            self.fill(now);
            // A batch bigger than a second's worth goes through when the bucket is full, and
            // the creates after it wait until the bucket is paid back.
            if self.tokens < n.min(u64::from(self.rate)) as f64 || self.rate == 0 {
                return Err(Short::Rate(self.max_rate));
            }
            self.tokens -= n as f64;
        }
        if self.max_cells > 0 {
            self.cells -= n;
        }
        self.count(n, now);
        Ok(())
    }

    /// Whether to ask for the next share now: half of this one is spent or half its life is
    /// gone, or a second when the quota is short.
    fn wants_more(&self, now: Instant) -> bool {
        !self.asking
            && now >= self.asked + RETRY
            && (now >= self.got + self.next
                || (self.max_cells > 0 && self.cells * 2 < self.granted))
    }

    fn count(&mut self, n: u64, now: Instant) {
        let second = now.duration_since(self.born).as_secs();
        if second != self.second {
            self.last_n = if second == self.second + 1 { self.now_n } else { 0 };
            self.now_n = 0;
            self.second = second;
        }
        self.now_n += n;
    }

    /// The cells made this second and the one before.
    fn recent(&self, now: Instant) -> u64 {
        match now.duration_since(self.born).as_secs().saturating_sub(self.second) {
            0 => self.now_n + self.last_n,
            1 => self.now_n,
            _ => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn share(max_cells: u64, max_rate: u32, cells: u64, rate: u32, now: Instant) -> Share {
        let mut s = Share::new(now);
        s.renew(max_cells, max_rate, cells, rate, Duration::from_secs(30), now);
        s
    }

    #[test]
    fn cells_run_out_and_half_spent_asks_for_more() {
        let t = Instant::now();
        let mut s = share(1000, 0, 100, 0, t);
        s.asked = t - RETRY;
        assert_eq!(s.charge(40, t), Ok(()));
        assert!(!s.wants_more(t));
        assert_eq!(s.charge(20, t), Ok(()));
        assert!(s.wants_more(t), "60 of 100 spent");
        assert_eq!(s.charge(41, t), Err(Short::Cells(1000)));
        assert_eq!(s.charge(40, t), Ok(()));
        assert_eq!(s.cells, 0);
    }

    #[test]
    fn creates_are_held_to_the_rate_with_a_second_of_burst() {
        let t = Instant::now();
        let mut s = share(0, 100, 0, 50, t);
        // A full bucket lets a batch bigger than a second's worth through, then pays it back.
        assert_eq!(s.charge(80, t), Ok(()));
        assert_eq!(s.charge(1, t), Err(Short::Rate(100)));
        assert_eq!(s.charge(1, t + Duration::from_millis(700)), Ok(()));
        // A share of no creates refuses them all.
        let mut none = share(0, 100, 0, 0, t);
        assert_eq!(none.charge(1, t + Duration::from_secs(5)), Err(Short::Rate(100)));
    }

    #[test]
    fn no_limit_takes_anything_and_asks_again_halfway_through_its_life() {
        let t = Instant::now();
        let mut s = share(0, 0, 0, 0, t);
        s.asked = t - RETRY;
        assert_eq!(s.charge(1_000_000, t), Ok(()));
        assert!(!s.wants_more(t + Duration::from_secs(14)));
        assert!(s.wants_more(t + Duration::from_secs(15)));
        s.asking = true;
        assert!(!s.wants_more(t + Duration::from_secs(15)));
    }

    #[test]
    fn the_share_asked_for_follows_what_the_project_asked() {
        let t = Instant::now();
        let mut s = share(0, 0, 0, 0, t);
        assert_eq!(s.want(t), (MIN_CELLS, MIN_RATE));
        for i in 0..10 {
            let _ = s.charge(100, t + Duration::from_millis(i * 100));
        }
        // 1000 cells in 10 seconds is 100 a second, so 150 a second for 30 seconds.
        assert_eq!(s.want(t + Duration::from_secs(10)), (4500, 150));
    }

    #[test]
    fn recent_cells_cover_this_second_and_the_last() {
        let t = Instant::now();
        let mut s = share(0, 0, 0, 0, t);
        s.count(5, t);
        s.count(7, t + Duration::from_millis(1500));
        assert_eq!(s.recent(t + Duration::from_millis(1600)), 12);
        assert_eq!(s.recent(t + Duration::from_millis(2100)), 7);
        assert_eq!(s.recent(t + Duration::from_secs(4)), 0);
    }
}
