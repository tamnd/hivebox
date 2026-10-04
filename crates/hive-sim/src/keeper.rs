//! The keeper: the real state machine, behind a leader that can be lost and whose wall clock
//! can be off.

use std::collections::BTreeMap;

use hive_keeper::state::{Command, Refusal, Reply, State};
use hive_rt::Rng as _;
use hive_rt::rng::SimRng;

use crate::world::{Addr, Body, Config, PROJECTS, World};

/// As the keeper's defaults: a node lease, and a quota slice.
pub(crate) const LEASE_MS: u64 = 10_000;
pub(crate) const SLICE_MS: u64 = 30_000;

/// The wall clock the run starts at, in milliseconds since the Unix epoch.
const WALL: u64 = 1_790_000_000_000;

pub(crate) struct Keeper {
    pub(crate) state: State,
    /// Whether a leader is up. Without one, calls get no answer.
    pub(crate) up: bool,
    /// How far the leader's wall clock is off.
    offset: i64,
    skew: u64,
    /// The first epoch each node got, to count the ones after.
    first: BTreeMap<u16, u16>,
}

impl Keeper {
    pub(crate) fn new(cfg: &Config, rng: &SimRng) -> Self {
        let skew = cfg.faults.skew_ms;
        let mut k =
            Self { state: State::default(), up: true, offset: 0, skew, first: BTreeMap::new() };
        k.offset = k.pick_offset(rng);
        k
    }

    fn pick_offset(&self, rng: &SimRng) -> i64 {
        if self.skew == 0 {
            return 0;
        }
        let s = i64::try_from(self.skew).unwrap_or(i64::MAX / 4);
        i64::try_from(rng.below(self.skew * 2 + 1)).unwrap_or(0) - s
    }

    /// A new leader takes over, on its own clock.
    pub(crate) fn elect(&mut self, rng: &SimRng) {
        self.up = true;
        self.offset = self.pick_offset(rng);
    }

    fn now_ms(&self, now: u64) -> u64 {
        (WALL + now).saturating_add_signed(self.offset)
    }

    pub(crate) fn create_projects(&mut self, now: u64) {
        for (name, quota, _) in PROJECTS {
            let now_ms = self.now_ms(now);
            self.state.apply(Command::CreateProject { name: name.into(), quota, now_ms });
        }
    }

    /// Epochs handed out past each node's first.
    pub(crate) fn bumps(&self) -> u64 {
        self.state
            .nodes
            .values()
            .map(|n| u64::from(n.epoch - self.first.get(&n.node).copied().unwrap_or(n.epoch)))
            .sum()
    }
}

impl World {
    pub(crate) fn keeper_got(&mut self, from: Addr, body: Body) {
        if !self.keeper.up {
            return;
        }
        let now_ms = self.keeper.now_ms(self.now);
        let reply = match body {
            Body::Register { call, name, epoch } => {
                let addr = format!("{from:?}");
                let cmd =
                    Command::Register { name, addr, epoch, now_ms, ttl_ms: LEASE_MS, wait: true };
                let got = match self.keeper.state.apply(cmd).0 {
                    Reply::Node(n) => {
                        self.keeper.first.entry(n.node).or_insert(n.epoch);
                        Ok(n)
                    }
                    r => Err(refusal(r)),
                };
                Body::Registered { call, got }
            }
            Body::Renew { call, node, epoch } => {
                let cmd = Command::Renew { node, epoch, now_ms, ttl_ms: LEASE_MS };
                let got = match self.keeper.state.apply(cmd).0 {
                    Reply::Node(n) => Ok(n),
                    r => Err(refusal(r)),
                };
                Body::Renewed { call, got }
            }
            Body::TakeQuota { call, project, cells, rate, live } => {
                let Addr::Gate(g) = from else { return };
                let cmd = Command::TakeQuota {
                    project,
                    gate: format!("gate-{g}"),
                    cells,
                    creates_per_s: rate,
                    live,
                    now_ms,
                    ttl_ms: SLICE_MS,
                };
                let got = match self.keeper.state.apply(cmd).0 {
                    Reply::Slice(q, s, contended) => Ok((q, s, contended)),
                    r => Err(refusal(r)),
                };
                Body::Sliced { call, got }
            }
            _ => return,
        };
        // A write commits on a majority before the leader answers.
        let commit = 1 + self.rng.below(4);
        self.send_after(commit, Addr::Keeper, from, reply);
    }
}

fn refusal(r: Reply) -> Refusal {
    match r {
        Reply::Refused(r) => r,
        r => Refusal::Invalid(format!("unexpected reply {r:?}")),
    }
}
