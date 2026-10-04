//! The simulated world: the clock, the queue of events, the network between the parts, and the
//! faults the seed picks.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};

use hive_keeper::state::{Node, Quota, Refusal, Slice};
use hive_rt::Rng as _;
use hive_rt::rng::SimRng;
use hive_types::{CellId, Reason};

use crate::check::{Checker, Violation};
use crate::client::Clients;
use crate::comb::Machine;
use crate::gate::Gate;
use crate::keeper::Keeper;

/// What one run looks like.
#[derive(Clone, Debug)]
pub struct Config {
    /// Picks everything: the faults, the load, the delays and the ids.
    pub seed: u64,
    /// Machines that run a comb. One more is kept as a spare, to replace one of them.
    pub nodes: u16,
    /// Gates, each with its own share of each project's quota.
    pub gates: u16,
    /// Seconds of load. The run then drains for two minutes with no new creates.
    pub secs: u64,
    /// Creates a second the clients send, on average.
    pub creates_per_s: u32,
    /// The faults to pick from.
    pub faults: Faults,
    /// Keep a line per message and fault, for replaying a seed.
    pub trace: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            seed: 1,
            nodes: 5,
            gates: 3,
            secs: 60,
            creates_per_s: 40,
            faults: Faults::default(),
            trace: false,
        }
    }
}

/// The faults a run may inject. Each is on by default.
#[derive(Clone, Debug)]
#[allow(clippy::struct_excessive_bools)]
pub struct Faults {
    /// Cut a node off from the keeper, from the gates and scout, or from both.
    pub partitions: bool,
    /// Lose messages for a while, and hold some back for up to three seconds.
    pub loss: bool,
    /// Deliver some requests twice.
    pub duplicates: bool,
    /// Kill a comb, which comes back with its cells still running.
    pub crashes: bool,
    /// Cut a node's power, which kills its cells too.
    pub power: bool,
    /// Bring the spare up under the name of a node that is cut off.
    pub replace: bool,
    /// Lose the keeper's leader for a while.
    pub keeper: bool,
    /// How far a keeper member's wall clock may be off, in milliseconds.
    pub skew_ms: u64,
    /// How fast or slow a node's clock may run, in millionths.
    pub drift_ppm: u64,
}

impl Default for Faults {
    fn default() -> Self {
        Self {
            partitions: true,
            loss: true,
            duplicates: true,
            crashes: true,
            power: true,
            replace: true,
            keeper: true,
            skew_ms: 2000,
            drift_ppm: 500,
        }
    }
}

impl Faults {
    /// No faults at all.
    #[must_use]
    pub fn none() -> Self {
        Self {
            partitions: false,
            loss: false,
            duplicates: false,
            crashes: false,
            power: false,
            replace: false,
            keeper: false,
            skew_ms: 0,
            drift_ppm: 0,
        }
    }
}

/// How a run went.
#[derive(Clone, Debug, Default)]
pub struct Report {
    /// The seed the run was.
    pub seed: u64,
    /// Events the run handled: messages, timers and faults.
    pub events: u64,
    /// Creates the clients asked for.
    pub requests: u64,
    /// Creates that got a cell.
    pub created: u64,
    /// Creates refused for quota or for room.
    pub refused: u64,
    /// Requests that could not tell whether a cell was made, and stopped trying.
    pub failed: u64,
    /// Cells made for a try the client had given up on, which run until their TTL.
    pub late: u64,
    /// Cells made, counting ones whose create answer was lost.
    pub cells: u64,
    /// Creates a gate let through without a share, while the keeper could not be reached.
    pub unchecked: u64,
    /// The most a project's live cells went past its quota.
    pub overshoot: i64,
    /// How far past its quota that project was allowed to go then.
    pub allowance: i64,
    /// Epochs the keeper handed out past each node's first.
    pub fenced_epochs: u64,
    /// Cells stopped because their node came back in a newer epoch.
    pub fenced_cells: u64,
    /// Keyed cells made a second time because the node with the first was out of the placing
    /// gate's view, or the first was from an older epoch of its node and so lost, which the
    /// design allows.
    pub moved: u64,
    /// Keyed cells made a second time on a node that turned the key away for room and then
    /// restarted, which forgets the keys it turned away.
    pub forgot: u64,
    /// The faults injected, with the time of each.
    pub faults: Vec<String>,
    /// The invariants that did not hold.
    pub violations: Vec<Violation>,
    /// The last lines of the trace, when the run kept one.
    pub trace: Vec<String>,
    /// A hash of everything that happened, so two runs of a seed can be compared.
    pub digest: u64,
}

/// Runs one seed.
#[must_use]
pub fn run(cfg: &Config) -> Report {
    let mut w = World::new(cfg.clone());
    w.start();
    while let Some(Reverse(q)) = w.queue.pop() {
        w.now = q.at;
        w.events += 1;
        w.dispatch(q.ev);
        if w.now > w.end {
            break;
        }
    }
    w.finish()
}

/// Who a message is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Addr {
    Keeper,
    Scout,
    Comb(usize),
    Gate(usize),
    Client,
}

/// The projects every run has, with their quotas. A limit of 0 is no limit.
pub(crate) const PROJECTS: [(&str, Quota, u64); 3] = [
    ("alpha", Quota { cells: 150, creates_per_s: 40 }, 50),
    ("beta", Quota { cells: 0, creates_per_s: 0 }, 30),
    ("gamma", Quota { cells: 40, creates_per_s: 0 }, 20),
];

/// What one node reports to scout each second.
#[derive(Clone, Debug)]
pub(crate) struct NodeReport {
    pub(crate) machine: usize,
    pub(crate) node: u16,
    pub(crate) epoch: u16,
    pub(crate) cells: u32,
    pub(crate) projects: BTreeMap<String, u64>,
}

/// The cluster as scout sends it to the gates.
#[derive(Clone, Debug, Default)]
pub(crate) struct View {
    /// By node: the machine, the epoch, the report count, whether it reported lately, and the
    /// cells it has.
    pub(crate) nodes: BTreeMap<u16, (usize, u16, u64, bool, u32)>,
    pub(crate) projects: BTreeMap<String, u64>,
}

#[derive(Clone, Debug)]
pub(crate) enum Body {
    Register {
        call: u64,
        name: String,
        epoch: u16,
    },
    Registered {
        call: u64,
        got: Result<Node, Refusal>,
    },
    Renew {
        call: u64,
        node: u16,
        epoch: u16,
    },
    Renewed {
        call: u64,
        got: Result<Node, Refusal>,
    },
    TakeQuota {
        call: u64,
        project: String,
        cells: u64,
        rate: u32,
        live: u64,
    },
    Sliced {
        call: u64,
        got: Result<(Quota, Slice, bool), Refusal>,
    },
    Report(NodeReport),
    View(View),
    Create {
        call: u64,
        project: String,
        key: String,
        anyway: bool,
    },
    /// The comb has the call: `hive_comb`'s create sends its response headers before it starts
    /// any cell, so from here on a broken call may have left a cell behind.
    Accepted {
        call: u64,
    },
    Created {
        call: u64,
        got: Result<CellId, Reason>,
    },
    Stop {
        call: u64,
        id: CellId,
    },
    Stopped {
        call: u64,
        got: Result<(), Reason>,
    },
    /// A client's create, and the attempt it is.
    Ask {
        req: u64,
        attempt: u32,
        project: String,
        key: String,
    },
    Answer {
        req: u64,
        attempt: u32,
        got: Result<CellId, Reason>,
    },
    /// A client's stop of a cell it made.
    End {
        id: CellId,
    },
    /// The process the call went to is not running, so the caller is told at once.
    Refused {
        call: u64,
    },
}

/// Timers, each for one part.
#[derive(Clone, Debug)]
pub(crate) enum Tick {
    /// A call got no answer in time.
    Timeout(u64),
    /// A comb's next lease renewal, for the comb's life `life`.
    Renew(u64),
    /// A comb tries to register again.
    Register(u64),
    /// A comb reports to scout.
    Report(u64),
    /// A cell's hard TTL runs out.
    CellEnd(CellId),
    /// A comb comes back after a crash or a power cut.
    Boot,
    /// Scout sends the view.
    Push,
    /// A client sends its next create.
    Load,
    /// A client tries a create again.
    Retry(u64),
    /// A client stops a cell.
    StopCell(CellId),
    /// A cell's drone is up.
    Started(CellId),
}

#[derive(Clone, Debug)]
pub(crate) enum Fault {
    /// Cut machine `m` off from the keeper, the front, or both.
    Cut {
        m: usize,
        keeper: bool,
        front: bool,
        ms: u64,
    },
    Heal {
        m: usize,
    },
    Loss {
        p: f64,
        ms: u64,
    },
    LossEnd,
    Crash {
        m: usize,
    },
    PowerOff {
        m: usize,
        ms: u64,
    },
    /// The spare comes up under machine `m`'s name, and `m` stays cut off until it is
    /// switched off for good.
    Replace {
        m: usize,
    },
    KeeperDown {
        ms: u64,
    },
    KeeperUp,
    /// The load stops, every fault heals, and the cluster drains.
    Drain,
}

#[derive(Clone, Debug)]
pub(crate) enum Event {
    Deliver {
        from: Addr,
        to: Addr,
        body: Body,
    },
    /// A message a part sends once it has done some work.
    Send {
        from: Addr,
        to: Addr,
        body: Body,
    },
    Tick(Addr, Tick),
    Fault(Fault),
}

#[derive(Debug)]
pub(crate) struct Queued {
    pub(crate) at: u64,
    pub(crate) seq: u64,
    pub(crate) ev: Event,
}

impl PartialEq for Queued {
    fn eq(&self, o: &Self) -> bool {
        (self.at, self.seq) == (o.at, o.seq)
    }
}
impl Eq for Queued {}
impl PartialOrd for Queued {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Queued {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        (self.at, self.seq).cmp(&(o.at, o.seq))
    }
}

/// How a machine is cut off.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Cut {
    pub(crate) keeper: bool,
    pub(crate) front: bool,
}

pub(crate) struct World {
    pub(crate) cfg: Config,
    /// True time, in milliseconds since the run began.
    pub(crate) now: u64,
    pub(crate) end: u64,
    /// When the load stops.
    pub(crate) drain_at: u64,
    pub(crate) draining: bool,
    pub(crate) rng: SimRng,
    pub(crate) queue: BinaryHeap<Reverse<Queued>>,
    seq: u64,
    pub(crate) events: u64,
    calls: u64,
    pub(crate) cuts: BTreeMap<usize, Cut>,
    pub(crate) loss: f64,
    pub(crate) keeper: Keeper,
    pub(crate) machines: Vec<Machine>,
    pub(crate) gates: Vec<Gate>,
    /// Scout: the last report of each node, and when it came.
    pub(crate) reports: BTreeMap<u16, (NodeReport, u64, u64)>,
    pub(crate) clients: Clients,
    pub(crate) check: Checker,
    pub(crate) report: Report,
    pub(crate) trace: Vec<String>,
    digest: u64,
}

impl World {
    fn new(cfg: Config) -> Self {
        let rng = SimRng::new(cfg.seed);
        let drain_at = cfg.secs * 1000;
        let keeper = Keeper::new(&cfg, &rng);
        let machines = (0..=usize::from(cfg.nodes))
            .map(|m| Machine::new(m, usize::from(cfg.nodes), &cfg, &rng))
            .collect();
        let gates = (0..usize::from(cfg.gates)).map(|g| Gate::new(g, rng.next_u64())).collect();
        Self {
            now: 0,
            end: drain_at + 120_000,
            drain_at,
            draining: false,
            rng,
            queue: BinaryHeap::new(),
            seq: 0,
            events: 0,
            calls: 0,
            cuts: BTreeMap::new(),
            loss: 0.0,
            keeper,
            machines,
            gates,
            reports: BTreeMap::new(),
            clients: Clients::default(),
            check: Checker::default(),
            report: Report { seed: cfg.seed, ..Report::default() },
            trace: Vec::new(),
            digest: 0xcbf2_9ce4_8422_2325,
            cfg,
        }
    }

    fn start(&mut self) {
        self.keeper.create_projects(0);
        for m in 0..usize::from(self.cfg.nodes) {
            let at = self.rng.below(500);
            self.at(at, Event::Tick(Addr::Comb(m), Tick::Boot));
        }
        self.at(1000, Event::Tick(Addr::Scout, Tick::Push));
        self.at(2000, Event::Tick(Addr::Client, Tick::Load));
        self.plan_faults();
        self.at(self.drain_at, Event::Fault(Fault::Drain));
    }

    /// Picks the faults for the run, in the middle of the load.
    fn plan_faults(&mut self) {
        let f = self.cfg.faults.clone();
        let span = self.drain_at.saturating_sub(10_000).max(1);
        let nodes = u64::from(self.cfg.nodes);
        let when = |w: &Self| 5000 + w.rng.below(span);
        let pick = |w: &Self| w.rng.below(nodes) as usize;
        if f.partitions {
            for _ in 0..1 + self.rng.below(3) {
                let (m, at) = (pick(self), when(self));
                let (keeper, front) = match self.rng.below(3) {
                    0 => (true, false),
                    1 => (false, true),
                    _ => (true, true),
                };
                let ms = 1000 + self.rng.below(20_000);
                self.at(at, Event::Fault(Fault::Cut { m, keeper, front, ms }));
            }
        }
        if f.loss {
            for _ in 0..1 + self.rng.below(2) {
                let at = when(self);
                let p = 0.05 + self.rng.below(30) as f64 / 100.0;
                let ms = 1000 + self.rng.below(10_000);
                self.at(at, Event::Fault(Fault::Loss { p, ms }));
            }
        }
        if f.crashes {
            for _ in 0..self.rng.below(4) {
                let (m, at) = (pick(self), when(self));
                self.at(at, Event::Fault(Fault::Crash { m }));
            }
        }
        if f.power && self.rng.below(2) == 0 {
            let (m, at) = (pick(self), when(self));
            let ms = 2000 + self.rng.below(15_000);
            self.at(at, Event::Fault(Fault::PowerOff { m, ms }));
        }
        if f.replace && self.rng.below(3) == 0 {
            let (m, at) = (pick(self), when(self));
            self.at(at, Event::Fault(Fault::Replace { m }));
        }
        if f.keeper {
            for _ in 0..self.rng.below(3) {
                let at = when(self);
                let ms = 500 + self.rng.below(6000);
                self.at(at, Event::Fault(Fault::KeeperDown { ms }));
            }
        }
    }

    pub(crate) fn at(&mut self, at: u64, ev: Event) {
        self.seq += 1;
        self.queue.push(Reverse(Queued { at, seq: self.seq, ev }));
    }

    pub(crate) fn after(&mut self, ms: u64, ev: Event) {
        self.at(self.now + ms, ev);
    }

    pub(crate) fn tick(&mut self, ms: u64, to: Addr, t: Tick) {
        self.after(ms, Event::Tick(to, t));
    }

    pub(crate) fn call_id(&mut self) -> u64 {
        self.calls += 1;
        self.calls
    }

    pub(crate) fn log(&mut self, line: impl FnOnce() -> String) {
        if self.cfg.trace {
            let l = line();
            self.trace.push(format!("{:>7} {l}", self.now));
        }
    }

    /// Mixes `what` into the run's digest.
    pub(crate) fn mix(&mut self, what: u64) {
        self.digest = (self.digest ^ what).wrapping_mul(0x0100_0000_01b3);
    }

    /// Whether a message from `a` to `b` is cut by a partition.
    fn cut(&self, a: Addr, b: Addr) -> bool {
        let side = |x: Addr, y: Addr| match x {
            Addr::Comb(m) => self.cuts.get(&m).is_some_and(|c| match y {
                Addr::Keeper => c.keeper,
                Addr::Scout | Addr::Gate(_) => c.front,
                _ => false,
            }),
            _ => false,
        };
        side(a, b) || side(b, a)
    }

    /// Sends a message, which may be lost, held back, or delivered twice.
    pub(crate) fn send(&mut self, from: Addr, to: Addr, body: Body) {
        if let Addr::Comb(m) = to {
            let mach = &self.machines[m];
            if !mach.power {
                // A machine that is off answers nothing, and its callers wait until they give up.
                return;
            }
            if matches!(body, Body::Create { .. } | Body::Stop { .. }) && !mach.listening() {
                // Nothing listens there: a caller with a call outstanding hears so at once.
                if let Some(call) = call_of(&body) {
                    self.after(
                        1,
                        Event::Deliver { from: to, to: from, body: Body::Refused { call } },
                    );
                }
                return;
            }
            if !mach.running() {
                // The comb that made the call is gone, and the answer with it.
                return;
            }
        }
        if let Addr::Comb(m) = from
            && !self.machines[m].power
        {
            return;
        }
        // Loss hits the links to the nodes and the keeper. Clients, gates and scout sit close.
        let far = |a: Addr| matches!(a, Addr::Comb(_) | Addr::Keeper);
        let lossy = self.loss > 0.0 && (far(from) || far(to));
        if self.cut(from, to) || (lossy && self.chance(self.loss)) {
            self.log(|| format!("lost {from:?} -> {to:?} {}", name(&body)));
            return;
        }
        let twice = self.cfg.faults.duplicates && is_request(&body) && self.chance(0.01);
        let delay = self.delay();
        self.log(|| format!("{from:?} -> {to:?} {} in {delay} ms", name(&body)));
        if twice {
            let again = self.delay();
            self.after(again, Event::Deliver { from, to, body: body.clone() });
        }
        self.after(delay, Event::Deliver { from, to, body });
    }

    pub(crate) fn send_after(&mut self, ms: u64, from: Addr, to: Addr, body: Body) {
        self.after(ms, Event::Send { from, to, body });
    }

    fn delay(&self) -> u64 {
        if self.cfg.faults.loss && self.chance(0.01) {
            200 + self.rng.below(2800)
        } else {
            1 + self.rng.below(10)
        }
    }

    pub(crate) fn chance(&self, p: f64) -> bool {
        (self.rng.below(1_000_000) as f64) < p * 1_000_000.0
    }

    fn dispatch(&mut self, ev: Event) {
        match ev {
            Event::Deliver { from, to, body } => {
                // A message to a cut off machine that was sent before the cut is lost with it.
                if self.cut(from, to) {
                    return;
                }
                self.mix(self.now ^ (name(&body).len() as u64) << 32);
                match to {
                    Addr::Keeper => self.keeper_got(from, body),
                    Addr::Scout => self.scout_got(body),
                    Addr::Comb(m) => self.comb_got(m, from, body),
                    Addr::Gate(g) => self.gate_got(g, from, body),
                    Addr::Client => self.client_got(body),
                }
            }
            Event::Send { from, to, body } => self.send(from, to, body),
            Event::Tick(to, t) => match to {
                Addr::Comb(m) => self.comb_tick(m, t),
                Addr::Gate(g) => self.gate_tick(g, t),
                Addr::Client => self.client_tick(t),
                Addr::Scout => self.push_view(),
                Addr::Keeper => {}
            },
            Event::Fault(f) => self.fault(f),
        }
    }

    fn scout_got(&mut self, body: Body) {
        let Body::Report(r) = body else { return };
        let node = r.node;
        let count = self.reports.get(&node).map_or(0, |(_, _, n)| *n) + 1;
        match self.reports.get(&node) {
            // A report from an older epoch is from a comb that lost the node.
            Some((old, _, _)) if old.epoch > r.epoch => {}
            _ => {
                self.reports.insert(node, (r, self.now, count));
            }
        }
    }

    /// The live cells of `project` scout does not count: on nodes it has not heard from lately,
    /// or from another epoch of the node than the one it heard from.
    pub(crate) fn unseen(&self, project: &str) -> u64 {
        let seen = |id: CellId| match self.reports.get(&id.node()) {
            Some((r, at, _)) => self.now.saturating_sub(*at) <= 3000 && r.epoch == id.epoch(),
            None => false,
        };
        let cells = self.check.cells.values();
        cells.filter(|l| l.ended.is_none() && l.project == project && !seen(l.id)).count() as u64
    }

    /// Scout sends every gate the cluster as it last heard of it.
    fn push_view(&mut self) {
        let mut view = View::default();
        for (&node, (r, at, count)) in &self.reports {
            let fresh = self.now.saturating_sub(*at) <= 3000;
            view.nodes.insert(node, (r.machine, r.epoch, *count, fresh, r.cells));
            if fresh {
                for (p, n) in &r.projects {
                    *view.projects.entry(p.clone()).or_default() += n;
                }
            }
        }
        for g in 0..self.gates.len() {
            self.send(Addr::Scout, Addr::Gate(g), Body::View(view.clone()));
        }
        self.tick(1000, Addr::Scout, Tick::Push);
    }

    fn fault(&mut self, f: Fault) {
        if self.draining && !matches!(f, Fault::Heal { .. } | Fault::LossEnd | Fault::KeeperUp) {
            return;
        }
        let line = format!("{:>7} {f:?}", self.now);
        self.log(|| format!("fault {f:?}"));
        match f {
            Fault::Cut { m, keeper, front, ms } => {
                // A replaced machine stays cut off from everything.
                if self.machines[m].retired || self.machines[m].zombie {
                    return;
                }
                self.report.faults.push(line);
                self.cuts.insert(m, Cut { keeper, front });
                self.after(ms, Event::Fault(Fault::Heal { m }));
            }
            Fault::Heal { m } => {
                if !self.machines[m].zombie {
                    self.cuts.remove(&m);
                }
            }
            Fault::Loss { p, ms } => {
                self.report.faults.push(line);
                self.loss = p;
                self.after(ms, Event::Fault(Fault::LossEnd));
            }
            Fault::LossEnd => self.loss = 0.0,
            Fault::Crash { m } => {
                if self.machines[m].power && !self.machines[m].retired {
                    self.report.faults.push(line);
                    self.comb_crash(m);
                    let back = 300 + self.rng.below(4000);
                    self.tick(back, Addr::Comb(m), Tick::Boot);
                }
            }
            Fault::PowerOff { m, ms } => {
                if self.machines[m].power && !self.machines[m].retired {
                    self.report.faults.push(line);
                    self.power_off(m);
                    self.tick(ms, Addr::Comb(m), Tick::Boot);
                }
            }
            Fault::Replace { m } => {
                let spare = usize::from(self.cfg.nodes);
                if self.machines[m].retired || self.machines[spare].power {
                    return;
                }
                self.report.faults.push(line);
                self.machines[m].zombie = true;
                self.cuts.insert(m, Cut { keeper: true, front: true });
                self.machines[spare].name = self.machines[m].name.clone();
                self.tick(1000 + self.rng.below(10_000), Addr::Comb(spare), Tick::Boot);
            }
            Fault::KeeperDown { ms } => {
                if self.keeper.up {
                    self.report.faults.push(line);
                    self.keeper.up = false;
                    self.after(ms, Event::Fault(Fault::KeeperUp));
                }
            }
            Fault::KeeperUp => self.keeper.elect(&self.rng),
            Fault::Drain => {
                self.draining = true;
                self.loss = 0.0;
                if !self.keeper.up {
                    self.keeper.elect(&self.rng);
                }
                for m in 0..self.machines.len() {
                    if self.machines[m].zombie {
                        // Switched off for good, now that its replacement runs.
                        self.power_off(m);
                        self.machines[m].retired = true;
                        continue;
                    }
                    self.cuts.remove(&m);
                    if !self.machines[m].power
                        && !self.machines[m].retired
                        && m < usize::from(self.cfg.nodes)
                    {
                        self.tick(10, Addr::Comb(m), Tick::Boot);
                    }
                }
            }
        }
    }

    fn finish(mut self) -> Report {
        let last = self.now;
        self.check_end(last);
        let mut r = std::mem::take(&mut self.report);
        r.events = self.events;
        r.violations = std::mem::take(&mut self.check.violations);
        r.cells = self.check.cells.len() as u64;
        r.fenced_epochs = self.keeper.bumps();
        if self.cfg.trace {
            let from = self.trace.len().saturating_sub(400);
            r.trace = self.trace.split_off(from);
        }
        self.mix(r.events ^ r.cells << 20 ^ r.created << 40);
        r.digest = self.digest;
        r
    }
}

/// The call a request carries, for one that wants an answer.
fn call_of(b: &Body) -> Option<u64> {
    match b {
        Body::Register { call, .. }
        | Body::Renew { call, .. }
        | Body::TakeQuota { call, .. }
        | Body::Create { call, .. }
        | Body::Stop { call, .. } => Some(*call),
        _ => None,
    }
}

/// Whether a message may arrive twice.
fn is_request(b: &Body) -> bool {
    // Only calls to the keeper: a client that loses the answer tries the next member, and both
    // may commit. Everything else rides one TCP connection.
    matches!(b, Body::Register { .. } | Body::Renew { .. } | Body::TakeQuota { .. })
}

fn name(b: &Body) -> String {
    match b {
        Body::Register { name, epoch, .. } => format!("Register {name} epoch {epoch}"),
        Body::Registered { got, .. } => match got {
            Ok(n) => format!("Registered node {} epoch {}", n.node, n.epoch),
            Err(e) => format!("Registered {e:?}"),
        },
        Body::Renew { node, epoch, .. } => format!("Renew node {node} epoch {epoch}"),
        Body::Renewed { got, .. } => {
            format!("Renewed {}", if got.is_ok() { "ok" } else { "refused" })
        }
        Body::TakeQuota { project, cells, live, .. } => {
            format!("TakeQuota {project} cells {cells} live {live}")
        }
        Body::Sliced { got, .. } => match got {
            Ok((_, s, c)) => format!("Sliced {} cells contended {c}", s.cells),
            Err(e) => format!("Sliced {e:?}"),
        },
        Body::Report(r) => format!("Report node {} epoch {} cells {}", r.node, r.epoch, r.cells),
        Body::View(v) => format!("View {} nodes", v.nodes.len()),
        Body::Create { project, key, .. } => format!("Create {project} {key:?}"),
        Body::Created { got, .. } => format!("Created {got:?}"),
        Body::Stop { id, .. } => format!("Stop {id}"),
        Body::Stopped { call, got } => format!("Stopped {call} {got:?}"),
        Body::Ask { req, attempt, project, key } => {
            format!("Ask {req}.{attempt} {project} {key:?}")
        }
        Body::Answer { req, attempt, got } => format!("Answer {req}.{attempt} {got:?}"),
        Body::End { id } => format!("End {id}"),
        Body::Refused { call } => format!("Refused {call}"),
        Body::Accepted { call } => format!("Accepted {call}"),
    }
}
