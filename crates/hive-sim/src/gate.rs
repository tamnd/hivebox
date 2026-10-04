//! The gates, modelled on `hive_gate`'s create path: a share of each project's quota spent
//! through the gate's own `Share`, then waggle's `Placer`, then the comb.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use hive_gate::quota::{MIN_CELLS, MIN_RATE, RETRY, Share, Take, UNKNOWN};
use hive_keeper::state::Refusal;
use hive_scout::project_id;
use hive_types::{Backend, CellId, Reason, Resources};
use hive_waggle::{ClusterView, NodeView, PlaceReq, Placement, Placer};

use crate::comb::ROOM;
use crate::keeper::SLICE_MS;
use crate::world::{Addr, Body, PROJECTS, Tick, View, World};

/// As `hive_gate::cells`: places a batch at most this many more times.
const RETRIES: usize = 2;
/// How long a gate waits on a keeper call.
const KEEPER_CALL: u64 = 5000;
/// How long a call to a comb that went quiet takes to fail: HTTP/2 keepalive pings every 20
/// seconds and gives up 20 seconds later, and the call then fails as unavailable.
const HUNG: u64 = 40_000;

/// What every cell asks for, and what every node has: room for `ROOM` of them.
const RES: Resources =
    Resources { vcpu_milli: 1000, mem_mib: 512, disk_gib: 1, pids: 0, open_files: 0 };

#[derive(Clone, Debug)]
struct Job {
    req: u64,
    attempt: u32,
    project: String,
    key: String,
    tries: usize,
    exclude: Vec<u16>,
    /// A keyed cell every node in its order turned away, asked for once more by nodes that turn
    /// away keys they turned away before.
    anyway: bool,
}

pub(crate) struct Gate {
    placer: Placer,
    base: Instant,
    view: View,
    shares: BTreeMap<String, Share>,
    /// Projects with a quota let through without a share, as the keeper could not be reached.
    open: BTreeSet<String>,
    asks: BTreeMap<u64, (String, Option<Job>)>,
    /// Creates sent to a comb: the job, the node, and whether the comb is known to have it.
    flights: BTreeMap<u64, (Job, u16, bool)>,
}

impl Gate {
    pub(crate) fn new(_g: usize, seed: u64) -> Self {
        Self {
            placer: Placer::new(seed),
            base: Instant::now(),
            view: View::default(),
            shares: BTreeMap::new(),
            open: BTreeSet::new(),
            asks: BTreeMap::new(),
            flights: BTreeMap::new(),
        }
    }
}

impl World {
    fn instant(&self, g: usize) -> Instant {
        self.gates[g].base + Duration::from_millis(self.now)
    }

    pub(crate) fn gate_got(&mut self, g: usize, from: Addr, body: Body) {
        match body {
            Body::View(v) => self.gates[g].view = v,
            Body::Ask { req, attempt, project, key } => {
                let job = Job {
                    req,
                    attempt,
                    project,
                    key,
                    tries: 0,
                    exclude: Vec::new(),
                    anyway: false,
                };
                self.gate_quota(g, job);
            }
            Body::Sliced { call, got } => {
                let Some((project, job)) = self.gates[g].asks.remove(&call) else { return };
                let now = self.instant(g);
                let gate = &mut self.gates[g];
                match got {
                    Ok((q, slice, contended)) => {
                        let s =
                            gate.shares.entry(project.clone()).or_insert_with(|| Share::new(now));
                        let ttl = Duration::from_millis(SLICE_MS);
                        s.renew(
                            q.cells,
                            q.creates_per_s,
                            slice.cells,
                            slice.creates_per_s,
                            ttl,
                            now,
                        );
                        if contended {
                            s.contended();
                        }
                        gate.open.remove(&project);
                    }
                    Err(Refusal::NotFound(_)) => {
                        let s =
                            gate.shares.entry(project.clone()).or_insert_with(|| Share::new(now));
                        s.renew(0, 0, 0, 0, UNKNOWN, now);
                    }
                    Err(_) => self.ask_failed(g, &project),
                }
                if let Some(job) = job {
                    self.charge_asked(g, job);
                }
            }
            Body::Accepted { call } => {
                if let Some(f) = self.gates[g].flights.get_mut(&call) {
                    f.2 = true;
                }
            }
            Body::Created { call, got } => {
                let Some((job, node, _)) = self.gates[g].flights.remove(&call) else { return };
                match got {
                    Ok(id) => self.gate_answer(g, &job, Ok(id)),
                    Err(Reason::CapacityUnavailable) if job.tries < RETRIES => {
                        self.place_again(g, job, node);
                    }
                    Err(Reason::CapacityUnavailable) if !job.key.is_empty() && !job.anyway => {
                        self.walk_again(g, job);
                    }
                    Err(e) => self.gate_answer(g, &job, Err(e)),
                }
            }
            Body::Refused { call } => {
                if let Some((job, node, seen)) = self.gates[g].flights.remove(&call) {
                    self.broken(g, job, node, seen);
                }
            }
            Body::End { id } => self.gate_stop(g, id),
            _ => {}
        }
        let _ = from;
    }

    pub(crate) fn gate_tick(&mut self, g: usize, t: Tick) {
        let Tick::Timeout(call) = t else { return };
        if let Some((project, job)) = self.gates[g].asks.remove(&call) {
            self.ask_failed(g, &project);
            if let Some(job) = job {
                self.charge_asked(g, job);
            }
        } else if let Some((job, node, seen)) = self.gates[g].flights.remove(&call) {
            self.broken(g, job, node, seen);
        }
    }

    /// As `Quotas::charge`: spends the share, asking the keeper first when it is spent.
    fn gate_quota(&mut self, g: usize, job: Job) {
        let now = self.instant(g);
        let Some(s) = self.gates[g].shares.get_mut(&job.project) else {
            self.ask(g, &job.project.clone(), 1, Some(job));
            return;
        };
        match s.take(1, now) {
            Take::Taken { ask } => {
                if ask {
                    self.ask(g, &job.project.clone(), 0, None);
                }
                self.charged(g, job);
            }
            Take::Short(_) => self.gate_answer(g, &job, Err(Reason::QuotaExceeded)),
            Take::Ask => self.ask(g, &job.project.clone(), 1, Some(job)),
        }
    }

    fn ask(&mut self, g: usize, project: &str, need: u64, job: Option<Job>) {
        let now = self.instant(g);
        let gate = &self.gates[g];
        let (cells, rate, recent) = match gate.shares.get(project) {
            Some(s) => {
                let (c, r) = s.want(now);
                (c.max(need.saturating_mul(2)), r, s.recent(now))
            }
            None => (MIN_CELLS.max(need.saturating_mul(2)), MIN_RATE, 0),
        };
        let live = gate.view.projects.get(project).copied().unwrap_or(0) + recent;
        let call = self.call_id();
        self.gates[g].asks.insert(call, (project.to_owned(), job));
        let body = Body::TakeQuota { call, project: project.to_owned(), cells, rate, live };
        self.send(Addr::Gate(g), Addr::Keeper, body);
        self.tick(KEEPER_CALL, Addr::Gate(g), Tick::Timeout(call));
    }

    fn ask_failed(&mut self, g: usize, project: &str) {
        let now = self.instant(g);
        let gate = &mut self.gates[g];
        if let Some(s) = gate.shares.get_mut(project) {
            s.failed();
        } else {
            let mut s = Share::new(now);
            s.renew(0, 0, 0, 0, RETRY * 4, now);
            gate.shares.insert(project.to_owned(), s);
            if PROJECTS.iter().any(|(p, q, _)| *p == project && q.cells > 0) {
                gate.open.insert(project.to_owned());
            }
        }
    }

    /// Charges the share the keeper just answered for.
    fn charge_asked(&mut self, g: usize, job: Job) {
        let now = self.instant(g);
        let got = match self.gates[g].shares.get_mut(&job.project) {
            Some(s) => s.charge(1, now),
            None => Ok(()),
        };
        match got {
            Ok(()) => self.charged(g, job),
            Err(_) => self.gate_answer(g, &job, Err(Reason::QuotaExceeded)),
        }
    }

    fn charged(&mut self, g: usize, job: Job) {
        if self.gates[g].open.contains(&job.project) {
            self.report.unchecked += 1;
            self.check.unchecked(&job.project);
        }
        self.gate_place(g, job);
    }

    fn gate_place(&mut self, g: usize, job: Job) {
        let last = job.tries == RETRIES;
        let now = Duration::from_millis(self.now);
        let gate = &mut self.gates[g];
        let mut view = ClusterView::default();
        for (&node, &(_, epoch, report, fresh, cells)) in &gate.view.nodes {
            let room = ROOM as u64;
            let mut n = NodeView::empty(
                node,
                room * u64::from(RES.vcpu_milli),
                room * u64::from(RES.mem_mib),
            );
            n.epoch = epoch;
            n.report = report;
            n.healthy = fresh;
            n.cells = cells;
            n.max_cells = ROOM as u32;
            n.cpu_committed_milli = u64::from(cells) * u64::from(RES.vcpu_milli);
            n.mem_committed_mib = u64::from(cells) * u64::from(RES.mem_mib);
            view.nodes.push(n);
        }
        let req = PlaceReq {
            backend: Backend::Container,
            resources: RES,
            n: 1,
            layers: &[],
            project: project_id(&job.project),
            affinity: None,
            exclude: &job.exclude,
        };
        // As `Batch::run`: a keyed cell goes to its key's home, or the next node in its order.
        let placed = if !job.key.is_empty() {
            let key = [job.project.as_bytes(), &[0], job.key.as_bytes()].concat();
            match gate.placer.home(&view, &req, &key, now) {
                Some(node) => Placement { nodes: vec![(node, 1)], unplaced: 0 },
                None => Placement::default(),
            }
        } else {
            gate.placer.place(&view, &req, now)
        };
        if !job.key.is_empty() {
            let key = [job.project.as_bytes(), &[0], job.key.as_bytes()].concat();
            let order = Placer::key_order(&view, Backend::Container, &key);
            self.check.placing(&job.project, &job.key, order);
        }
        let Some(&(node, _)) = placed.nodes.first() else {
            if !job.key.is_empty() && !job.anyway && !job.exclude.is_empty() {
                self.walk_again(g, job);
            } else if last || job.exclude.is_empty() {
                self.gate_answer(g, &job, Err(Reason::CapacityUnavailable));
            } else {
                let mut job = job;
                job.tries += 1;
                self.gate_place(g, job);
            }
            return;
        };
        let machine = self.gates[g].view.nodes[&node].0;
        let call = self.call_id();
        let body = Body::Create {
            call,
            project: job.project.clone(),
            key: job.key.clone(),
            anyway: job.anyway,
        };
        self.gates[g].flights.insert(call, (job, node, false));
        self.send(Addr::Gate(g), Addr::Comb(machine), body);
        self.tick(HUNG, Addr::Gate(g), Tick::Timeout(call));
    }

    /// The call to the comb broke. As `send_chunk`: before the comb's headers came back, the
    /// gate takes it that the comb may never have seen the call and an unkeyed cell may go
    /// elsewhere. A keyed cell, or one whose headers came, may be there, and the caller is told
    /// the node could not be reached.
    fn broken(&mut self, g: usize, job: Job, node: u16, seen: bool) {
        if !seen && job.tries < RETRIES && job.key.is_empty() {
            self.place_again(g, job, node);
        } else {
            self.gate_answer(g, &job, Err(Reason::DroneUnreachable));
        }
    }

    fn place_again(&mut self, g: usize, mut job: Job, node: u16) {
        self.gates[g].placer.refused(node, 1, &RES);
        job.exclude.push(node);
        job.tries += 1;
        self.gate_place(g, job);
    }

    /// As `Batch::run`: every node in a keyed cell's order turned it away, some maybe only
    /// because they had turned the key away before, so the gate asks them again in order and
    /// the first with room takes it.
    fn walk_again(&mut self, g: usize, mut job: Job) {
        job.anyway = true;
        job.tries = 0;
        job.exclude.clear();
        self.gate_place(g, job);
    }

    fn gate_answer(&mut self, g: usize, job: &Job, got: Result<CellId, Reason>) {
        let at = self.now;
        self.check.answer(job.req, job.attempt, g, at);
        let body = Body::Answer { req: job.req, attempt: job.attempt, got };
        self.send(Addr::Gate(g), Addr::Client, body);
    }

    /// As `Nodes::owner`: a cell from an older epoch of its node is lost, and one on a node
    /// scout does not know is not found.
    fn gate_stop(&mut self, g: usize, id: CellId) {
        let Some(&(machine, epoch, ..)) = self.gates[g].view.nodes.get(&id.node()) else { return };
        if id.epoch() < epoch {
            return;
        }
        let call = self.call_id();
        self.send(Addr::Gate(g), Addr::Comb(machine), Body::Stop { call, id });
    }
}
