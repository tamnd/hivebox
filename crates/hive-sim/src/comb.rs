//! The machines and the combs on them, modelled on `hive_comb`: registration and the lease as
//! in `lease.rs`, and fencing, the idempotency keys and the id sequence as in `comb.rs`.

use std::collections::BTreeMap;

use hive_keeper::state::Refusal;
use hive_rt::Rng as _;
use hive_rt::rng::SimRng;
use hive_types::{CellId, Reason};

use crate::keeper::LEASE_MS;
use crate::world::{Addr, Body, Config, NodeReport, Tick, World};

/// As `hive_comb::lease`: a call to the keeper, the first wait after a failed one, and the
/// longest wait between registrations.
const CALL: u64 = 8000;
const RETRY: u64 = 500;
const MAX_BACKOFF: u64 = 5000;
/// Cell ids a comb reserves on disk at a time.
const SEQ_BLOCK: u64 = 4096;
/// The cells a node has room for.
pub(crate) const ROOM: usize = 64;
/// How long a comb turns a key away again after turning it away for room, as `TURNED_FOR`.
const TURNED_FOR: u64 = 600_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Run {
    Starting,
    Running,
}

/// A cell with a record that has not ended. Its drone runs while the power is on, whether a
/// comb runs or not.
#[derive(Debug)]
struct Cell {
    project: String,
    key: String,
    run: Run,
    /// The hard TTL, and when it runs out by the machine's clock once the cell runs.
    ttl: u64,
    ends_at: u64,
}

/// What a comb finds on disk when it starts.
#[derive(Debug, Default)]
struct Disk {
    lease: Option<(u16, u16)>,
    /// The cell sequence reserved so far.
    seq: u64,
}

#[derive(Debug)]
enum Phase {
    Registering {
        call: u64,
        backoff: u64,
    },
    Serving {
        node: u16,
        epoch: u16,
        /// When the lease runs out by this comb's reckoning, on the machine's clock.
        ends: u64,
        /// The renewal on its way, and when it was sent.
        renew: Option<(u64, u64)>,
    },
}

/// One comb process.
#[derive(Debug)]
struct Comb {
    life: u64,
    phase: Phase,
    idem: BTreeMap<(String, String), CellId>,
    next: u64,
    block: u64,
    /// Creates waiting for a cell to start.
    waiting: BTreeMap<CellId, Vec<(u64, Addr)>>,
    /// The keys turned away for room, and when.
    turned: BTreeMap<(String, String), u64>,
}

pub(crate) struct Machine {
    pub(crate) name: String,
    pub(crate) power: bool,
    /// Switched off for good.
    pub(crate) retired: bool,
    /// Replaced by the spare while it was cut off, and left running until the drain.
    pub(crate) zombie: bool,
    /// How fast the machine's clock runs, in millionths off.
    drift: i64,
    disk: Disk,
    cells: BTreeMap<CellId, Cell>,
    comb: Option<Comb>,
    life: u64,
}

impl Machine {
    pub(crate) fn new(m: usize, spare: usize, cfg: &Config, rng: &SimRng) -> Self {
        let d = cfg.faults.drift_ppm;
        let drift = if d == 0 { 0 } else { rng.below(2 * d + 1) as i64 - d as i64 };
        Self {
            name: if m == spare { "spare".into() } else { format!("node-{m}") },
            power: false,
            retired: false,
            zombie: false,
            drift,
            disk: Disk::default(),
            cells: BTreeMap::new(),
            comb: None,
            life: 0,
        }
    }

    /// Whether the comb takes calls.
    pub(crate) fn listening(&self) -> bool {
        self.power && self.comb.as_ref().is_some_and(|c| matches!(c.phase, Phase::Serving { .. }))
    }

    /// Whether a comb process is up, serving or not.
    pub(crate) fn running(&self) -> bool {
        self.power && self.comb.is_some()
    }

    fn serving(&self) -> Option<(u16, u16)> {
        match self.comb.as_ref()?.phase {
            Phase::Serving { node, epoch, .. } => Some((node, epoch)),
            Phase::Registering { .. } => None,
        }
    }
}

impl World {
    /// The time on machine `m`'s clock.
    fn local(&self, m: usize) -> u64 {
        let d = self.machines[m].drift;
        (i128::from(self.now) * i128::from(1_000_000 + d) / 1_000_000) as u64
    }

    /// Wakes machine `m` after `ms` by its own clock.
    fn local_tick(&mut self, m: usize, ms: u64, t: Tick) {
        let d = self.machines[m].drift;
        let real = (i128::from(ms) * 1_000_000 / i128::from(1_000_000 + d)) as u64;
        self.tick(real.max(1), Addr::Comb(m), t);
    }

    fn life(&self, m: usize) -> Option<u64> {
        self.machines[m].comb.as_ref().map(|c| c.life)
    }

    pub(crate) fn comb_tick(&mut self, m: usize, t: Tick) {
        let life = self.life(m);
        match t {
            Tick::Boot => {
                let mach = &mut self.machines[m];
                if mach.retired || mach.comb.is_some() {
                    return;
                }
                mach.power = true;
                mach.life += 1;
                let life = mach.life;
                mach.comb = Some(Comb {
                    life,
                    phase: Phase::Registering { call: 0, backoff: RETRY },
                    idem: BTreeMap::new(),
                    next: 0,
                    block: 0,
                    waiting: BTreeMap::new(),
                    turned: BTreeMap::new(),
                });
                self.log(|| format!("comb {m} starts, life {life}"));
                self.register(m);
            }
            Tick::Register(l) if life == Some(l) => self.register(m),
            Tick::Renew(l) if life == Some(l) => self.renew(m),
            Tick::Report(l) if life == Some(l) => self.node_report(m),
            Tick::Timeout(call) => self.comb_timeout(m, call),
            Tick::Started(id) => self.started(m, id),
            Tick::CellEnd(id) => {
                let now = self.local(m);
                let mach = &self.machines[m];
                let Some(cell) = mach.cells.get(&id).filter(|c| c.run == Run::Running) else {
                    return;
                };
                // With no comb, the next one looks at the cell's timer when it starts.
                if mach.comb.is_some() {
                    if now >= cell.ends_at {
                        self.end_cell(m, id);
                    } else {
                        // A drifting clock can bring the tick a little early.
                        let left = cell.ends_at - now;
                        self.local_tick(m, left, Tick::CellEnd(id));
                    }
                }
            }
            _ => {}
        }
    }

    fn register(&mut self, m: usize) {
        let call = self.call_id();
        let mach = &mut self.machines[m];
        let Some(Comb { phase: Phase::Registering { call: c, .. }, .. }) = &mut mach.comb else {
            return;
        };
        *c = call;
        let name = mach.name.clone();
        let epoch = mach.disk.lease.map_or(0, |(_, e)| e);
        self.send(Addr::Comb(m), Addr::Keeper, Body::Register { call, name, epoch });
        self.local_tick(m, CALL, Tick::Timeout(call));
    }

    fn comb_timeout(&mut self, m: usize, call: u64) {
        let Some(c) = &mut self.machines[m].comb else { return };
        let life = c.life;
        match &mut c.phase {
            Phase::Registering { call: c, backoff } if *c == call => {
                let wait = *backoff;
                *backoff = (*backoff * 2).min(MAX_BACKOFF);
                *c = 0;
                self.local_tick(m, wait, Tick::Register(life));
            }
            Phase::Serving { renew: Some((c, _)), .. } if *c == call => self.renew_failed(m),
            _ => {}
        }
    }

    fn renew(&mut self, m: usize) {
        let now = self.local(m);
        let call = self.call_id();
        let Some(Comb { phase: Phase::Serving { node, epoch, ends, renew }, .. }) =
            &mut self.machines[m].comb
        else {
            return;
        };
        if renew.is_some() {
            return;
        }
        if now >= *ends {
            self.comb_lose(m, "could not renew the lease before it ran out");
            return;
        }
        *renew = Some((call, now));
        let (node, epoch) = (*node, *epoch);
        self.send(Addr::Comb(m), Addr::Keeper, Body::Renew { call, node, epoch });
        self.local_tick(m, CALL, Tick::Timeout(call));
    }

    fn renew_failed(&mut self, m: usize) {
        let now = self.local(m);
        let Some(Comb { life, phase: Phase::Serving { ends, renew, .. }, .. }) =
            &mut self.machines[m].comb
        else {
            return;
        };
        *renew = None;
        let wait = RETRY.min(ends.saturating_sub(now));
        let life = *life;
        self.local_tick(m, wait, Tick::Renew(life));
    }

    pub(crate) fn comb_got(&mut self, m: usize, from: Addr, body: Body) {
        match body {
            Body::Registered { call, got } => {
                let Some(Comb { phase: Phase::Registering { call: c, .. }, .. }) =
                    &self.machines[m].comb
                else {
                    return;
                };
                if *c != call {
                    return;
                }
                match got {
                    Ok(n) => self.open(m, n.node, n.epoch),
                    // The keeper refused the node outright, so the comb stops, and its
                    // supervisor starts it again later.
                    Err(e) => self.comb_exit(m, &format!("the keeper refused it: {e:?}"), 5000),
                }
            }
            Body::Renewed { call, got } => {
                let now = self.local(m);
                let Some(Comb { life, phase: Phase::Serving { ends, renew, .. }, .. }) =
                    &mut self.machines[m].comb
                else {
                    return;
                };
                let Some((c, sent)) = *renew else { return };
                if c != call {
                    return;
                }
                let life = *life;
                match got {
                    Ok(_) => {
                        *ends = sent + LEASE_MS;
                        *renew = None;
                        let _ = now;
                        self.local_tick(m, LEASE_MS / 3, Tick::Renew(life));
                    }
                    Err(Refusal::LeaseLost(_) | Refusal::NotFound(_)) => {
                        self.comb_lose(m, "lost the lease");
                    }
                    Err(_) => self.renew_failed(m),
                }
            }
            Body::Create { call, project, key, anyway } => {
                self.create(m, call, from, project, key, anyway);
            }
            Body::Stop { call, id } => self.stop(m, call, from, id),
            _ => {}
        }
    }

    /// The comb has its lease: it takes in the cells it finds, fences off the ones from older
    /// epochs, and starts serving.
    fn open(&mut self, m: usize, node: u16, epoch: u16) {
        let now = self.local(m);
        let mach = &mut self.machines[m];
        mach.disk.lease = Some((node, epoch));
        let stale: Vec<CellId> =
            mach.cells.keys().copied().filter(|id| id.epoch() < epoch).collect();
        for id in &stale {
            self.report.fenced_cells += 1;
            self.end_cell(m, *id);
        }
        let mach = &mut self.machines[m];
        let mut idem = BTreeMap::new();
        let mut due = Vec::new();
        for (id, c) in &mach.cells {
            if !c.key.is_empty() {
                idem.insert((c.project.clone(), c.key.clone()), *id);
            }
            if c.run == Run::Running {
                due.push((*id, c.ends_at.saturating_sub(now)));
            }
        }
        let seq = mach.disk.seq;
        let Some(c) = &mut mach.comb else { return };
        c.idem = idem;
        c.next = seq;
        c.block = seq;
        c.phase = Phase::Serving { node, epoch, ends: now + LEASE_MS, renew: None };
        let life = c.life;
        self.log(|| format!("comb {m} serves node {node} epoch {epoch}"));
        let at = self.now;
        self.check.serve(m, node, epoch, at);
        let left: Vec<CellId> = self.machines[m].cells.keys().copied().collect();
        self.check.fenced(node, epoch, &left, at);
        for (id, wait) in due {
            self.local_tick(m, wait, Tick::CellEnd(id));
        }
        self.local_tick(m, LEASE_MS / 3, Tick::Renew(life));
        self.local_tick(m, 10, Tick::Report(life));
    }

    /// The comb process ends. Its cells keep running. Calls it had not answered see the
    /// connection reset.
    fn comb_exit(&mut self, m: usize, why: &str, restart: u64) {
        self.comb_down(m, why);
        if restart > 0 {
            let back = restart + self.rng.below(2000);
            self.tick(back, Addr::Comb(m), Tick::Boot);
        }
    }

    /// As `Comb::lose`: the comb lost its lease, so it stops every cell it has on its way out.
    fn comb_lose(&mut self, m: usize, why: &str) {
        self.comb_down(m, why);
        let ids: Vec<CellId> = self.machines[m].cells.keys().copied().collect();
        for id in ids {
            self.end_cell(m, id);
        }
        let back = 1000 + self.rng.below(2000);
        self.tick(back, Addr::Comb(m), Tick::Boot);
    }

    pub(crate) fn comb_crash(&mut self, m: usize) {
        self.comb_down(m, "crashed");
    }

    fn comb_down(&mut self, m: usize, why: &str) {
        let Some(c) = self.machines[m].comb.take() else { return };
        self.log(|| format!("comb {m} exits: {why}"));
        self.check.unserve(m);
        for (call, to) in c.waiting.into_values().flatten() {
            self.send(Addr::Comb(m), to, Body::Refused { call });
        }
        // A cell still starting lives on only if its drone was launched already.
        let starting: Vec<CellId> = self.machines[m]
            .cells
            .iter()
            .filter(|(_, c)| c.run == Run::Starting)
            .map(|(id, _)| *id)
            .collect();
        for id in starting {
            if self.rng.below(2) == 0 {
                self.end_cell(m, id);
            }
        }
    }

    pub(crate) fn power_off(&mut self, m: usize) {
        self.comb_down_silent(m);
        let mach = &mut self.machines[m];
        mach.power = false;
        let ids: Vec<CellId> = mach.cells.keys().copied().collect();
        for id in ids {
            self.end_cell(m, id);
        }
    }

    /// The comb stops with the machine, and nothing it had open hears of it.
    fn comb_down_silent(&mut self, m: usize) {
        if self.machines[m].comb.take().is_some() {
            self.check.unserve(m);
        }
    }

    fn create(
        &mut self,
        m: usize,
        call: u64,
        from: Addr,
        project: String,
        key: String,
        anyway: bool,
    ) {
        let now = self.local(m);
        let Some((node, epoch)) = self.machines[m].serving() else {
            self.send(Addr::Comb(m), from, Body::Refused { call });
            return;
        };
        self.send(Addr::Comb(m), from, Body::Accepted { call });
        let mach = &mut self.machines[m];
        let Some(c) = &mut mach.comb else { return };
        if !key.is_empty()
            && let Some(&id) = c.idem.get(&(project.clone(), key.clone()))
        {
            match mach.cells.get(&id).map(|x| x.run) {
                Some(Run::Running) => {
                    self.send(Addr::Comb(m), from, Body::Created { call, got: Ok(id) });
                    return;
                }
                Some(Run::Starting) => {
                    c.waiting.entry(id).or_default().push((call, from));
                    return;
                }
                None => {
                    c.idem.remove(&(project.clone(), key.clone()));
                }
            }
        }
        // As `Comb::create`: a key turned away for room is turned away again for a while, so a
        // retry passes on to the node that took it.
        let pk = (project.clone(), key.clone());
        if !key.is_empty() && !anyway && c.turned.get(&pk).is_some_and(|&t| now < t + TURNED_FOR) {
            let got = Err(Reason::CapacityUnavailable);
            self.send(Addr::Comb(m), from, Body::Created { call, got });
            return;
        }
        if mach.cells.len() >= ROOM {
            if !key.is_empty() {
                c.turned.insert(pk, now);
                let at = self.now;
                self.check.turned(&project, &key, node, at);
            }
            let got = Err(Reason::CapacityUnavailable);
            self.send(Addr::Comb(m), from, Body::Created { call, got });
            return;
        }
        if c.next == c.block {
            c.block += SEQ_BLOCK;
            mach.disk.seq = c.block;
        }
        let seq = c.next;
        c.next += 1;
        let tag = self.rng.below(1 << 48);
        let Some(id) = CellId::new(1, node, epoch, seq, tag) else { return };
        let ttl = 5000 + self.rng.below(35_000);
        let mach = &mut self.machines[m];
        let Some(c) = &mut mach.comb else { return };
        if !key.is_empty() {
            c.idem.insert((project.clone(), key.clone()), id);
            c.turned.remove(&(project.clone(), key.clone()));
        }
        c.waiting.entry(id).or_default().push((call, from));
        let at = self.now;
        self.check.created(id, &project, &key, at);
        let unseen = self.unseen(&project);
        let gates = self.gates.len() as u64;
        self.check.bound(&project, unseen, gates, at);
        self.machines[m]
            .cells
            .insert(id, Cell { project, key, run: Run::Starting, ttl, ends_at: 0 });
        let up = 50 + self.rng.below(250);
        self.local_tick(m, up, Tick::Started(id));
    }

    fn started(&mut self, m: usize, id: CellId) {
        let now = self.local(m);
        let mach = &mut self.machines[m];
        if !mach.power {
            return;
        }
        let Some(cell) = mach.cells.get_mut(&id) else { return };
        if cell.run != Run::Starting {
            return;
        }
        cell.run = Run::Running;
        cell.ends_at = now + cell.ttl;
        let ttl = cell.ttl;
        let Some(c) = &mut mach.comb else { return };
        let waiting = c.waiting.remove(&id).unwrap_or_default();
        for (call, to) in waiting {
            self.send(Addr::Comb(m), to, Body::Created { call, got: Ok(id) });
        }
        self.local_tick(m, ttl, Tick::CellEnd(id));
    }

    fn end_cell(&mut self, m: usize, id: CellId) {
        let mach = &mut self.machines[m];
        let Some(cell) = mach.cells.remove(&id) else { return };
        if let Some(c) = &mut mach.comb {
            let k = (cell.project, cell.key);
            if c.idem.get(&k) == Some(&id) {
                c.idem.remove(&k);
            }
            let waiting = c.waiting.remove(&id).unwrap_or_default();
            for (call, to) in waiting {
                let got = Err(Reason::Internal);
                self.send(Addr::Comb(m), to, Body::Created { call, got });
            }
        }
        let at = self.now;
        self.check.ended(id, at);
    }

    fn stop(&mut self, m: usize, call: u64, from: Addr, id: CellId) {
        let Some((_, epoch)) = self.machines[m].serving() else { return };
        let got = if self.machines[m].cells.contains_key(&id) {
            self.end_cell(m, id);
            Ok(())
        } else if id.epoch() < epoch {
            Err(Reason::CellLost)
        } else {
            Err(Reason::CellNotFound)
        };
        self.send(Addr::Comb(m), from, Body::Stopped { call, got });
    }

    fn node_report(&mut self, m: usize) {
        let Some((node, epoch)) = self.machines[m].serving() else { return };
        let mach = &self.machines[m];
        let mut projects = BTreeMap::new();
        for c in mach.cells.values() {
            *projects.entry(c.project.clone()).or_default() += 1;
        }
        let cells = u32::try_from(mach.cells.len()).unwrap_or(u32::MAX);
        let life = self.life(m).unwrap_or(0);
        let r = NodeReport { machine: m, node, epoch, cells, projects };
        self.send(Addr::Comb(m), Addr::Scout, Body::Report(r));
        self.local_tick(m, 1000, Tick::Report(life));
    }
}
