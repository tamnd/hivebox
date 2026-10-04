//! The invariants, checked as the run goes and once it ends.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use hive_types::CellId;

use crate::world::{PROJECTS, World};

/// An invariant that did not hold, with the simulated time it was seen at in milliseconds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Violation {
    /// Two combs served one node in one epoch at once.
    DoubleOwner {
        /// The node and the epoch.
        node: u16,
        /// The epoch both served.
        epoch: u16,
        /// The two machines.
        machines: (usize, usize),
        /// When the second started serving.
        at: u64,
    },
    /// A cell id was handed out twice.
    IdReused {
        /// The cell id.
        id: String,
        /// When it was handed out the second time.
        at: u64,
    },
    /// A comb started serving with a cell of its node from an older epoch still running.
    StaleCell {
        /// The node.
        node: u16,
        /// The epoch the comb started in.
        epoch: u16,
        /// The cell left running.
        id: String,
        /// When the comb started.
        at: u64,
    },
    /// A gate answered one try of a create twice.
    TwoAnswers {
        /// The create.
        req: u64,
        /// The try.
        attempt: u32,
        /// When the second answer went out.
        at: u64,
    },
    /// A create ended twice.
    TwoOutcomes {
        /// The create.
        req: u64,
        /// When it ended the second time.
        at: u64,
    },
    /// A create that never ended.
    NoOutcome {
        /// The create.
        req: u64,
    },
    /// A cell ended twice.
    EndedTwice {
        /// The cell.
        id: String,
        /// When it ended the second time.
        at: u64,
    },
    /// A cell that never ended.
    NeverEnded {
        /// The cell.
        id: String,
    },
    /// A project's live cells passed its quota by more than the slices allow.
    Overshoot {
        /// The project.
        project: String,
        /// Its live cells.
        live: u64,
        /// Its quota of live cells.
        quota: u64,
        /// How far past the quota the slices allowed it then.
        allowed: u64,
        /// When.
        at: u64,
    },
    /// Two live cells shared a project and an idempotency key.
    KeyOverlap {
        /// The project.
        project: String,
        /// The key.
        key: String,
        /// The cell made first.
        first: String,
        /// The cell made while the first was live.
        second: String,
        /// The second cell's node had turned the key away for room before the first was made.
        spilled: bool,
        /// When the second was made.
        at: u64,
    },
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DoubleOwner { node, epoch, machines, at } => write!(
                f,
                "at {at} ms machines {} and {} both serve node {node} in epoch {epoch}",
                machines.0, machines.1
            ),
            Self::IdReused { id, at } => write!(f, "at {at} ms cell id {id} was handed out again"),
            Self::StaleCell { node, epoch, id, at } => write!(
                f,
                "at {at} ms node {node} serves epoch {epoch} with cell {id} from an older one running"
            ),
            Self::TwoAnswers { req, attempt, at } => {
                write!(f, "at {at} ms try {attempt} of create {req} was answered twice")
            }
            Self::TwoOutcomes { req, at } => write!(f, "at {at} ms create {req} ended twice"),
            Self::NoOutcome { req } => write!(f, "create {req} never ended"),
            Self::EndedTwice { id, at } => write!(f, "at {at} ms cell {id} ended twice"),
            Self::NeverEnded { id } => write!(f, "cell {id} never ended"),
            Self::Overshoot { project, live, quota, allowed, at } => write!(
                f,
                "at {at} ms {project} had {live} live cells, past its quota of {quota} by more than the {allowed} allowed"
            ),
            Self::KeyOverlap { project, key, first, second, spilled, at } => {
                let node = |id: &str| id.parse::<CellId>().map_or(0, |id| id.node());
                write!(
                    f,
                    "at {at} ms cells {first} and {second} of {project} both run with key {key}, on nodes {} and {}",
                    node(first),
                    node(second)
                )?;
                if *spilled {
                    write!(f, ", after node {} turned it away", node(second))?;
                }
                Ok(())
            }
        }
    }
}

/// One cell, from the comb handing out its id until its drone stops.
#[derive(Debug)]
pub(crate) struct Life {
    pub(crate) id: CellId,
    pub(crate) project: String,
    pub(crate) ended: Option<u64>,
}

#[derive(Default)]
pub(crate) struct Checker {
    pub(crate) violations: Vec<Violation>,
    /// Every cell, by node, epoch and sequence, which together make the id unique.
    pub(crate) cells: BTreeMap<(u16, u16, u64), Life>,
    serving: BTreeMap<usize, (u16, u16)>,
    /// The live cell of each key, and the key's order of nodes as the gate that placed it saw.
    keys: BTreeMap<(String, String), (CellId, Vec<u16>)>,
    /// The key's order of nodes as the last gate to place it saw.
    seen: BTreeMap<(String, String), Vec<u16>>,
    /// The nodes that turned each key away for room, and when each last did.
    turned: BTreeMap<(String, String), BTreeMap<u16, u64>>,
    pub(crate) moved: u64,
    pub(crate) forgot: u64,
    /// When a comb last began serving each node.
    served: BTreeMap<u16, u64>,
    /// Live cells of each project, and the most over its quota each got.
    pub(crate) live: BTreeMap<String, u64>,
    pub(crate) over: BTreeMap<String, (i64, u64, u64)>,
    /// Creates of each project a gate let through without a share.
    unchecked: BTreeMap<String, u64>,
    /// Projects already found past what they were allowed, so each is told once.
    overshot: BTreeSet<String>,
    answers: BTreeSet<(u64, u32)>,
    outcomes: BTreeMap<u64, bool>,
}

fn key(id: CellId) -> (u16, u16, u64) {
    (id.node(), id.epoch(), id.seq())
}

impl Checker {
    pub(crate) fn serve(&mut self, m: usize, node: u16, epoch: u16, at: u64) {
        if let Some((&other, _)) =
            self.serving.iter().find(|(o, s)| **o != m && **s == (node, epoch))
        {
            self.violations.push(Violation::DoubleOwner { node, epoch, machines: (other, m), at });
        }
        self.serving.insert(m, (node, epoch));
        self.served.insert(node, at);
    }

    pub(crate) fn unserve(&mut self, m: usize) {
        self.serving.remove(&m);
    }

    /// Once a comb serves `epoch`, none of the cells left on its machine are from an older one.
    pub(crate) fn fenced(&mut self, node: u16, epoch: u16, left: &[CellId], at: u64) {
        for id in left {
            if id.node() == node && id.epoch() < epoch {
                let id = id.to_string();
                self.violations.push(Violation::StaleCell { node, epoch, id, at });
            }
        }
    }

    pub(crate) fn created(&mut self, id: CellId, project: &str, k: &str, at: u64) {
        if self.cells.contains_key(&key(id)) {
            self.violations.push(Violation::IdReused { id: id.to_string(), at });
            return;
        }
        let life = Life { id, project: project.to_owned(), ended: None };
        self.cells.insert(key(id), life);
        *self.live.entry(project.to_owned()).or_default() += 1;
        if !k.is_empty() {
            let pk = (project.to_owned(), k.to_owned());
            let now = self.seen.get(&pk).cloned().unwrap_or_default();
            // The gates saw the key's order differently up to the first cell's node, so the
            // second walked other nodes, or the first was from an older epoch of its node and
            // so lost.
            let above = |order: &[u16], node: u16| {
                order.iter().position(|&n| n == node).map(|i| order[..i].to_vec())
            };
            let away = |(first, then): &(CellId, Vec<u16>)| {
                let before = above(then, first.node());
                before.is_none()
                    || before != above(&now, first.node())
                    || (first.node() == id.node() && first.epoch() < id.epoch())
            };
            let turned = self.turned.get(&pk).and_then(|t| t.get(&id.node())).copied();
            let forgot =
                turned.is_some_and(|t| self.served.get(&id.node()).is_some_and(|&s| s > t));
            if self.keys.get(&pk).is_some_and(away) {
                self.moved += 1;
            } else if self.keys.contains_key(&pk) && forgot {
                self.forgot += 1;
            } else if let Some((first, _)) = self.keys.get(&pk) {
                let spilled = turned.is_some();
                self.violations.push(Violation::KeyOverlap {
                    project: pk.0.clone(),
                    key: pk.1.clone(),
                    first: first.to_string(),
                    second: id.to_string(),
                    spilled,
                    at,
                });
            }
            self.keys.insert(pk, (id, now));
        }
    }

    /// A gate places a keyed cell, seeing `order` as the key's order of nodes.
    pub(crate) fn placing(&mut self, project: &str, key: &str, order: Vec<u16>) {
        self.seen.insert((project.to_owned(), key.to_owned()), order);
    }

    /// A gate lets a create through without a share, as the keeper could not be reached.
    pub(crate) fn unchecked(&mut self, project: &str) {
        *self.unchecked.entry(project.to_owned()).or_default() += 1;
    }

    /// Checks a project's live cells against its quota once a cell is made. The keeper gives
    /// each of `gates` what is free of the quota, or an even split of it when the others hold
    /// more, so the shares can pass the quota by its even split for every gate but one. Past
    /// that come the creates let through without a share and the `unseen` cells, which scout
    /// did not count when the gates asked for their shares.
    pub(crate) fn bound(&mut self, project: &str, unseen: u64, gates: u64, at: u64) {
        let Some((_, q, _)) = PROJECTS.iter().find(|(p, ..)| *p == project) else { return };
        if q.cells == 0 {
            return;
        }
        let live = self.live.get(project).copied().unwrap_or(0);
        let unchecked = self.unchecked.get(project).copied().unwrap_or(0);
        let allowed = q.cells * (gates - 1) / gates + unseen + unchecked;
        let over = live as i64 - q.cells as i64;
        let e = self.over.entry(project.to_owned()).or_insert((i64::MIN, 0, 0));
        if over > e.0 {
            *e = (over, at, allowed);
        }
        if over > allowed as i64 && self.overshot.insert(project.to_owned()) {
            let project = project.to_owned();
            self.violations.push(Violation::Overshoot {
                project,
                live,
                quota: q.cells,
                allowed,
                at,
            });
        }
    }

    /// A node turns a keyed cell away for room.
    pub(crate) fn turned(&mut self, project: &str, key: &str, node: u16, at: u64) {
        self.turned.entry((project.to_owned(), key.to_owned())).or_default().insert(node, at);
    }

    pub(crate) fn ended(&mut self, id: CellId, at: u64) {
        let Some(life) = self.cells.get_mut(&key(id)) else { return };
        if life.ended.is_some() {
            self.violations.push(Violation::EndedTwice { id: id.to_string(), at });
            return;
        }
        life.ended = Some(at);
        if let Some(n) = self.live.get_mut(&life.project) {
            *n -= 1;
        }
        self.keys.retain(|_, v| v.0 != id);
    }

    pub(crate) fn answer(&mut self, req: u64, attempt: u32, _gate: usize, at: u64) {
        if !self.answers.insert((req, attempt)) {
            self.violations.push(Violation::TwoAnswers { req, attempt, at });
        }
    }

    pub(crate) fn request(&mut self, req: u64) {
        self.outcomes.insert(req, false);
    }

    pub(crate) fn outcome(&mut self, req: u64, at: u64) {
        let seen = self.outcomes.entry(req).or_default();
        if *seen {
            self.violations.push(Violation::TwoOutcomes { req, at });
        }
        *seen = true;
    }
}

impl World {
    /// What must hold once the cluster has drained: every create ended, and so did every cell.
    pub(crate) fn check_end(&mut self, _last: u64) {
        let c = &mut self.check;
        for (&req, &seen) in &c.outcomes {
            if !seen {
                c.violations.push(Violation::NoOutcome { req });
            }
        }
        for life in c.cells.values() {
            if life.ended.is_none() {
                c.violations.push(Violation::NeverEnded { id: life.id.to_string() });
            }
        }
        let (mut over, mut allowance) = (i64::MIN, 0);
        for &(o, _, allowed) in c.over.values() {
            if o > over {
                over = o;
                allowance = allowed as i64;
            }
        }
        self.report.moved = c.moved;
        self.report.forgot = c.forgot;
        self.report.overshoot = over.max(0);
        self.report.allowance = allowance;
    }
}
