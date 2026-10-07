//! What the keeper agrees on, and how each command changes it.
//!
//! Every change goes through [`State::apply`], which is deterministic: the time and anything
//! random come in the command, chosen by the member that took the call, so every member that
//! applies the same log ends up with the same state.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// What a project may use.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Quota {
    /// Most live cells, or 0 for no limit.
    pub cells: u64,
    /// Most creates a second, or 0 for no limit.
    pub creates_per_s: u32,
}

/// A project: the owner of cells, keys and quota.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Project {
    /// Its name, which is how everything refers to it.
    pub name: String,
    /// What it may use.
    pub quota: Quota,
    /// When it was made, in milliseconds since the Unix epoch.
    pub created_ms: u64,
    /// The shares of its quota the gates hold, by gate.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub slices: BTreeMap<String, Slice>,
}

/// A share of a project's quota that one gate spends on its own until it runs out.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Slice {
    /// How many more cells the gate may make.
    pub cells: u64,
    /// How many creates a second it may make.
    pub creates_per_s: u32,
    /// When the share runs out and goes back to the project.
    pub expires_ms: u64,
    /// Whether the gate got less than it asked for, so the others should give back what they
    /// do not need soon rather than when their shares run out.
    #[serde(default)]
    pub short: bool,
}

/// An API key, by the BLAKE3 hash of the key. The key itself is never stored.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Key {
    /// The first characters of the key, so people can tell keys apart.
    pub prefix: String,
    /// The project the key opens.
    pub project: String,
    /// When it was made.
    pub created_ms: u64,
    /// When it was revoked, or 0 while it works.
    pub revoked_ms: u64,
}

/// A comb's registration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    /// Its index, the `node` in every cell id it makes.
    pub node: u16,
    /// The name it registered under, stable across restarts.
    pub name: String,
    /// Where gates reach it.
    pub addr: String,
    /// Goes up each time the comb registers after losing its lease, which fences the cells it
    /// made before.
    pub epoch: u16,
    /// When its lease runs out.
    pub expires_ms: u64,
}

/// How many sealed hours of each node's audit chain the keeper holds. Older ones are dropped as
/// new ones come, 30 days of them.
pub const AUDIT_HOURS: usize = 24 * 30;

/// The most sealed hours one command may bring.
pub const AUDIT_BATCH: usize = 64;

/// A sealed hour of a node's audit chain, from its seal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditHour {
    /// The sequence number of its first event.
    pub first_seq: u64,
    /// How many events it holds.
    pub count: u64,
    /// The hash its first line points back at.
    pub prev: [u8; 32],
    /// The hash of its last line.
    pub root: [u8; 32],
}

impl AuditHour {
    /// The sequence number after its last event.
    #[must_use]
    pub fn end(&self) -> u64 {
        self.first_seq.saturating_add(self.count)
    }
}

/// How far a node said its audit chain had got.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditTip {
    /// The hour its last event fell in.
    pub hour: String,
    /// How many events the chain held.
    pub seq: u64,
    /// The hash of the last event's line.
    pub root: [u8; 32],
    /// When the node said so.
    pub at_ms: u64,
}

/// What the keeper holds of a node's audit chain: the roots of its last sealed hours, each one
/// following on from the one before, and the furthest point it said the chain had reached.
/// Neither may change once held, so a node that rewrites or cuts back its chain is refused.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditChain {
    /// The sealed hours by hour, as the files are named, such as `2026-10-07T06`.
    pub hours: BTreeMap<String, AuditHour>,
    /// The furthest point the node said its chain had reached.
    pub tip: Option<AuditTip>,
}

/// A change to the state. The member that takes the call fills in `now_ms` and anything
/// random, so applying it is the same everywhere.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    /// Makes a project.
    CreateProject {
        /// Its name.
        name: String,
        /// What it may use.
        quota: Quota,
        /// The time of the call.
        now_ms: u64,
    },
    /// Adds a key to a project.
    AddKey {
        /// The BLAKE3 hash of the key.
        hash: [u8; 32],
        /// The start of the key.
        prefix: String,
        /// The project it opens.
        project: String,
        /// The time of the call.
        now_ms: u64,
    },
    /// Revokes a key, named by its hash or its prefix.
    RevokeKey {
        /// The hash, if the caller has it.
        hash: Option<[u8; 32]>,
        /// The prefix, if the caller has that instead.
        prefix: String,
        /// The time of the call.
        now_ms: u64,
    },
    /// A comb starting, or coming back after it lost its lease.
    Register {
        /// The comb's stable name.
        name: String,
        /// Where gates reach it.
        addr: String,
        /// The epoch it last ran in, or 0.
        epoch: u16,
        /// The time of the call.
        now_ms: u64,
        /// How long the lease lasts.
        ttl_ms: u64,
        /// A comb asking with an older epoch than the node's waits while the node's lease is
        /// live. Commands logged before this was added read as false, and take a new epoch at
        /// once as they did then.
        #[serde(default)]
        wait: bool,
    },
    /// A comb renewing its lease.
    Renew {
        /// Its index.
        node: u16,
        /// The epoch it runs in.
        epoch: u16,
        /// The time of the call.
        now_ms: u64,
        /// How long the lease lasts from now.
        ttl_ms: u64,
    },
    /// A gate taking a share of a project's quota, in place of the one it had.
    TakeQuota {
        /// The project.
        project: String,
        /// The gate.
        gate: String,
        /// The cells it wants to be able to make.
        cells: u64,
        /// The creates a second it wants.
        creates_per_s: u32,
        /// The project's live cells as the gate sees them.
        live: u64,
        /// The time of the call.
        now_ms: u64,
        /// How long the share lasts.
        ttl_ms: u64,
    },
    /// Sets the key the keeper signs tokens with, if it has none. The first one wins, so two
    /// members that each made a key on their first token end up with the same one.
    SetRoot {
        /// The Ed25519 private key.
        private: [u8; 32],
    },
    /// A comb publishing its audit chain: the hours it sealed since the last one the keeper
    /// holds, oldest first, and how far the chain has got. Only the comb holding the node's
    /// lease may.
    Audit {
        /// Its index.
        node: u16,
        /// The epoch it runs in.
        epoch: u16,
        /// The time of the call.
        now_ms: u64,
        /// Sealed hours, by hour.
        hours: Vec<(String, AuditHour)>,
        /// How far the chain has got, as the tip's hour, sequence number and root.
        tip: Option<(String, u64, [u8; 32])>,
    },
    /// Several commands in one log entry, applied in order. Calls that arrive together are
    /// sent this way so they cost one round of disk writes instead of one each.
    Batch(Vec<Command>),
}

/// What applying a command gave.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Reply {
    /// Nothing to say, for log entries that are not commands.
    None,
    /// The project made.
    Project(Project),
    /// The key added or revoked, with its hash.
    Key([u8; 32], Key),
    /// The node registered or renewed.
    Node(Node),
    /// The key the keeper signs tokens with.
    Root([u8; 32]),
    /// The share a gate got, with the project's whole quota and whether any gate got less than
    /// it asked for.
    Slice(Quota, Slice, bool),
    /// The command was refused, and why.
    Refused(Refusal),
    /// The replies to a batch, in its order.
    Batch(Vec<Reply>),
    /// The audit roots were taken, and this is the last sealed hour now held, or empty.
    Audit(String),
}

/// Why a command was refused.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Refusal {
    /// A name is not a name, or a value is out of range.
    Invalid(String),
    /// The project, key or node is not there.
    NotFound(String),
    /// A project of that name, or a key with that hash, is there already.
    Exists(String),
    /// The lease ran out, or the epoch is not the node's, so the comb has to register again.
    LeaseLost(String),
    /// Every node index is taken, or a node used up its epochs.
    Exhausted(String),
    /// The node's lease is live in a newer epoch than the comb asked with, so the comb waits for
    /// it to run out.
    Held(String),
    /// Audit roots that do not follow on from the ones held, so the node's chain was rewritten,
    /// cut back or lost.
    Conflict(String),
}

/// Everything the keeper agrees on.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct State {
    /// Projects by name.
    pub projects: BTreeMap<String, Project>,
    /// Keys by hash.
    pub keys: BTreeMap<[u8; 32], Key>,
    /// Nodes by index.
    pub nodes: BTreeMap<u16, Node>,
    /// The Ed25519 private key tokens are signed with, made on the first token.
    #[serde(default)]
    pub root: Option<[u8; 32]>,
    /// The audit chains of the nodes, by node name, which stays the same when the index does
    /// not.
    #[serde(default)]
    pub audit: BTreeMap<String, AuditChain>,
}

impl State {
    /// Applies `cmd`, a batch too, and adds the rows it changed to `changed`.
    pub fn apply_all(&mut self, cmd: Command, changed: &mut Vec<Changed>) -> Reply {
        if let Command::Batch(cmds) = cmd {
            return Reply::Batch(cmds.into_iter().map(|c| self.apply_all(c, changed)).collect());
        }
        let (reply, row) = self.apply(cmd);
        changed.extend(row);
        reply
    }

    /// Applies `cmd` and says what it gave and which row it changed. A batch is refused here,
    /// it goes through [`State::apply_all`].
    pub fn apply(&mut self, cmd: Command) -> (Reply, Option<Changed>) {
        match cmd {
            Command::Batch(_) => {
                refused(Refusal::Invalid("a batch is applied with apply_all".into()))
            }
            Command::CreateProject { name, quota, now_ms } => {
                if !hive_types::is_name(&name) {
                    return refused(Refusal::Invalid(format!("{name:?} is not a project name")));
                }
                if self.projects.contains_key(&name) {
                    return refused(Refusal::Exists(format!("project {name} exists")));
                }
                let p = Project {
                    name: name.clone(),
                    quota,
                    created_ms: now_ms,
                    slices: BTreeMap::new(),
                };
                self.projects.insert(name.clone(), p.clone());
                (Reply::Project(p), Some(Changed::Project(name)))
            }
            Command::AddKey { hash, prefix, project, now_ms } => {
                if !self.projects.contains_key(&project) {
                    return refused(Refusal::NotFound(format!("no project {project}")));
                }
                if self.keys.contains_key(&hash) {
                    return refused(Refusal::Exists("that key exists".into()));
                }
                let k = Key { prefix, project, created_ms: now_ms, revoked_ms: 0 };
                self.keys.insert(hash, k.clone());
                (Reply::Key(hash, k), Some(Changed::Key(hash)))
            }
            Command::RevokeKey { hash, prefix, now_ms } => {
                let found = match hash {
                    Some(h) => self.keys.get_mut(&h).map(|k| (h, k)),
                    None if prefix.is_empty() => None,
                    None => {
                        let mut it = self.keys.iter_mut().filter(|(_, k)| k.prefix == prefix);
                        match (it.next(), it.next()) {
                            (Some((h, k)), None) => Some((*h, k)),
                            (Some(_), Some(_)) => {
                                return refused(Refusal::Invalid(format!(
                                    "more than one key starts {prefix}, so name it by hash"
                                )));
                            }
                            _ => None,
                        }
                    }
                };
                let Some((h, k)) = found else {
                    return refused(Refusal::NotFound("no such key".into()));
                };
                if k.revoked_ms == 0 {
                    k.revoked_ms = now_ms;
                }
                (Reply::Key(h, k.clone()), Some(Changed::Key(h)))
            }
            Command::SetRoot { private } => {
                if let Some(root) = self.root {
                    return (Reply::Root(root), None);
                }
                self.root = Some(private);
                (Reply::Root(private), Some(Changed::Root))
            }
            Command::Register { name, addr, epoch, now_ms, ttl_ms, wait } => {
                if !hive_types::is_name(&name) {
                    return refused(Refusal::Invalid(format!("{name:?} is not a node name")));
                }
                let expires_ms = now_ms.saturating_add(ttl_ms);
                if let Some(n) = self.nodes.values_mut().find(|n| n.name == name) {
                    let live = n.expires_ms > now_ms;
                    // A comb back within its lease keeps its epoch, and with it its cells.
                    if !(epoch == n.epoch && live) {
                        // One with an older epoch is another machine under the node's name, or
                        // this one after it lost its data, and whichever comb holds the lease
                        // may still run cells. Once the lease runs out that comb has stopped
                        // them, so only then does the node start again in a new epoch.
                        if wait && live && epoch < n.epoch {
                            return refused(Refusal::Held(format!(
                                "node {} is held in epoch {} for {} ms more",
                                n.node,
                                n.epoch,
                                n.expires_ms - now_ms
                            )));
                        }
                        let Some(next) = n.epoch.max(epoch).checked_add(1) else {
                            return refused(Refusal::Exhausted(format!(
                                "node {} used up its epochs",
                                n.node
                            )));
                        };
                        n.epoch = next;
                    }
                    n.addr = addr;
                    n.expires_ms = expires_ms;
                    return (Reply::Node(n.clone()), Some(Changed::Node(n.node)));
                }
                let Some(node) = self.free_node() else {
                    return refused(Refusal::Exhausted("every node index is taken".into()));
                };
                // A comb that ran in an epoch this keeper never gave, as when its data was
                // lost, starts past it, so the cells it has from then are fenced off.
                let Some(epoch) = epoch.checked_add(1) else {
                    return refused(Refusal::Exhausted(format!("node {name} used up its epochs")));
                };
                let n = Node { node, name, addr, epoch, expires_ms };
                self.nodes.insert(node, n.clone());
                (Reply::Node(n), Some(Changed::Node(node)))
            }
            Command::Renew { node, epoch, now_ms, ttl_ms } => {
                let Some(n) = self.nodes.get_mut(&node) else {
                    return refused(Refusal::NotFound(format!("no node {node}")));
                };
                if n.epoch != epoch {
                    return refused(Refusal::LeaseLost(format!(
                        "node {node} is in epoch {}, not {epoch}",
                        n.epoch
                    )));
                }
                if n.expires_ms <= now_ms {
                    return refused(Refusal::LeaseLost(format!(
                        "the lease of node {node} ran out {} ms ago",
                        now_ms - n.expires_ms
                    )));
                }
                n.expires_ms = now_ms.saturating_add(ttl_ms);
                (Reply::Node(n.clone()), Some(Changed::Node(node)))
            }
            Command::Audit { node, epoch, now_ms, hours, tip } => {
                let name = match self.nodes.get(&node) {
                    None => return refused(Refusal::NotFound(format!("no node {node}"))),
                    Some(n) if n.epoch != epoch || n.expires_ms <= now_ms => {
                        return refused(Refusal::LeaseLost(format!(
                            "node {node} in epoch {epoch} holds no lease"
                        )));
                    }
                    Some(n) => n.name.clone(),
                };
                let tip = tip.map(|(hour, seq, root)| AuditTip { hour, seq, root, at_ms: now_ms });
                match self.audit(&name, hours, tip) {
                    Ok((last, changed)) => (Reply::Audit(last), changed),
                    Err(r) => refused(r),
                }
            }
            Command::TakeQuota { project, gate, cells, creates_per_s, live, now_ms, ttl_ms } => {
                if !hive_types::is_name(&gate) {
                    return refused(Refusal::Invalid(format!("{gate:?} is not a gate name")));
                }
                let Some(p) = self.projects.get_mut(&project) else {
                    return refused(Refusal::NotFound(format!("no project {project}")));
                };
                // Shares that ran out go back to the project, and so does the gate's own, which
                // the new one replaces.
                p.slices.retain(|g, s| s.expires_ms > now_ms && *g != gate);
                let (held_cells, held_rate) = p.slices.values().fold((0u64, 0u64), |(c, r), s| {
                    (c.saturating_add(s.cells), r + u64::from(s.creates_per_s))
                });
                let gates = p.slices.len() as u64 + 1;
                let q = p.quota;
                // A gate gets what is free, and at least an even split when the others hold
                // more than theirs, which they give back the next time they ask. A limit of 0
                // is no limit, and then the share is all the gate asked for.
                let cells_got = if q.cells == 0 {
                    cells
                } else {
                    let room = q.cells.saturating_sub(live);
                    cells.min(room.saturating_sub(held_cells).max(room / gates))
                };
                let rate_got = if q.creates_per_s == 0 {
                    creates_per_s
                } else {
                    let all = u64::from(q.creates_per_s);
                    let free = all.saturating_sub(held_rate).max(all / gates);
                    creates_per_s.min(u32::try_from(free).unwrap_or(u32::MAX))
                };
                let s = Slice {
                    cells: cells_got,
                    creates_per_s: rate_got,
                    expires_ms: now_ms.saturating_add(ttl_ms),
                    short: cells_got < cells || rate_got < creates_per_s,
                };
                let contended = s.short || p.slices.values().any(|o| o.short);
                if cells_got > 0 || rate_got > 0 {
                    p.slices.insert(gate, s);
                }
                (Reply::Slice(q, s, contended), Some(Changed::Project(project)))
            }
        }
    }

    /// Takes the hours and the tip node `name` sent, if they follow on from what is held, and
    /// says the last hour held after and what changed. Nothing changes when any of it is
    /// refused.
    fn audit(
        &mut self,
        name: &str,
        hours: Vec<(String, AuditHour)>,
        tip: Option<AuditTip>,
    ) -> Result<(String, Option<Changed>), Refusal> {
        if hours.len() > AUDIT_BATCH {
            return Err(Refusal::Invalid(format!("more than {AUDIT_BATCH} audit hours at once")));
        }
        let empty = AuditChain::default();
        let chain = self.audit.get(name).unwrap_or(&empty);
        let mut last = chain.hours.iter().next_back();
        let first = chain.hours.keys().next();
        let mut new = Vec::new();
        for (hour, h) in &hours {
            if !is_hour(hour) || h.count == 0 {
                return Err(Refusal::Invalid(format!("{hour:?} is not a sealed audit hour")));
            }
            if let Some(held) = chain.hours.get(hour) {
                if held != h {
                    return Err(conflict(name, format!("hour {hour} has another root held")));
                }
                continue;
            }
            // An hour from before the oldest held was dropped, or came before the first one
            // published, so there is nothing to check it against.
            if first.is_some_and(|f| hour < f) {
                continue;
            }
            if let Some((lh, l)) = last {
                if hour <= lh {
                    return Err(conflict(name, format!("hour {hour} is missing before {lh}")));
                }
                if h.first_seq != l.end() || h.prev != l.root {
                    return Err(conflict(
                        name,
                        format!("hour {hour} does not follow on from {lh}"),
                    ));
                }
            }
            for t in chain.tip.iter().chain(&tip) {
                agrees(name, hour, h, t)?;
            }
            new.push((hour, h));
            last = Some((hour, h));
        }
        let mut keep_tip = None;
        if let Some(t) = tip {
            if !is_hour(&t.hour) {
                return Err(Refusal::Invalid(format!("{:?} is not an audit hour", t.hour)));
            }
            for (hour, h) in chain.hours.get_key_value(&t.hour).into_iter().chain(last) {
                agrees(name, hour, h, &t)?;
            }
            match &chain.tip {
                Some(o) if t.seq < o.seq => {
                    return Err(conflict(
                        name,
                        format!("its chain went back from {} to {} events", o.seq, t.seq),
                    ));
                }
                Some(o) if t.seq == o.seq && t.root != o.root => {
                    return Err(conflict(
                        name,
                        format!("the first {} events have another root held", t.seq),
                    ));
                }
                Some(o) if t.seq > o.seq && t.hour < o.hour => {
                    return Err(conflict(name, format!("hour {} came after {}", t.hour, o.hour)));
                }
                Some(o) if t.seq == o.seq => {}
                _ => keep_tip = Some(t),
            }
        }
        // Everything checks out, so it all goes in.
        let added: Vec<String> = new.iter().map(|(h, _)| (*h).clone()).collect();
        let chain = self.audit.entry(name.to_string()).or_default();
        for (hour, h) in hours.into_iter().filter(|(hour, _)| added.contains(hour)) {
            chain.hours.insert(hour, h);
        }
        let mut gone = Vec::new();
        while chain.hours.len() > AUDIT_HOURS {
            gone.extend(chain.hours.pop_first().map(|(h, _)| h));
        }
        let tip = keep_tip.is_some();
        if keep_tip.is_some() {
            chain.tip = keep_tip;
        }
        let last = chain.hours.keys().next_back().cloned().unwrap_or_default();
        let changed = (tip || !added.is_empty()).then(|| Changed::Audit {
            node: name.to_string(),
            added,
            gone,
            tip,
        });
        Ok((last, changed))
    }

    /// The lowest node index not handed out. Index 0 never is, so a zero in a message means
    /// none.
    fn free_node(&self) -> Option<u16> {
        let mut want = 1u16;
        for &n in self.nodes.keys() {
            if n != want {
                break;
            }
            want = want.checked_add(1)?;
        }
        Some(want)
    }

    /// The project a key opens, if the key works.
    #[must_use]
    pub fn project_of(&self, hash: &[u8; 32]) -> Option<&str> {
        self.keys.get(hash).filter(|k| k.revoked_ms == 0).map(|k| k.project.as_str())
    }
}

/// A row [`State::apply`] changed, so the store writes just that row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Changed {
    /// The project of this name.
    Project(String),
    /// The key with this hash.
    Key([u8; 32]),
    /// The node with this index.
    Node(u16),
    /// The signing key.
    Root,
    /// A node's audit chain: the hours added and dropped, and whether the tip moved.
    Audit {
        /// The node's name.
        node: String,
        /// The hours added.
        added: Vec<String>,
        /// The hours dropped.
        gone: Vec<String>,
        /// Whether the tip moved.
        tip: bool,
    },
}

/// Whether `s` names an hour the way audit files are named, such as `2026-10-07T06`.
fn is_hour(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 13
        && b.iter().enumerate().all(|(i, &c)| match i {
            4 | 7 => c == b'-',
            10 => c == b'T',
            _ => c.is_ascii_digit(),
        })
}

fn conflict(node: &str, why: String) -> Refusal {
    Refusal::Conflict(format!("the audit chain of node {node} does not match the keeper's: {why}"))
}

/// Whether sealed hour `hour` and tip `t` can both be of one chain. A tip in an hour before the
/// sealed one is at or before its start, one in the same hour is within it, and one in an hour
/// after is at or past its end. A tip right at the start or the end has the root there.
fn agrees(node: &str, hour: &str, h: &AuditHour, t: &AuditTip) -> Result<(), Refusal> {
    let (a, b) = (h.first_seq, h.end());
    let within = match t.hour.as_str().cmp(hour) {
        std::cmp::Ordering::Less => t.seq <= a,
        std::cmp::Ordering::Equal => a <= t.seq && t.seq <= b,
        std::cmp::Ordering::Greater => t.seq >= b,
    };
    let root = (t.seq != a || t.root == h.prev) && (t.seq != b || t.root == h.root);
    if within && root {
        Ok(())
    } else {
        Err(conflict(
            node,
            format!(
                "hour {hour} does not agree with the tip at sequence {} in hour {}",
                t.seq, t.hour
            ),
        ))
    }
}

fn refused(r: Refusal) -> (Reply, Option<Changed>) {
    (Reply::Refused(r), None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn project(s: &mut State, name: &str) -> Reply {
        s.apply(Command::CreateProject { name: name.into(), quota: Quota::default(), now_ms: 1 }).0
    }

    fn register(s: &mut State, name: &str, epoch: u16, now_ms: u64) -> Reply {
        let addr = format!("http://{name}:7420");
        let cmd = Command::Register {
            name: name.into(),
            addr,
            epoch,
            now_ms,
            ttl_ms: 30_000,
            wait: true,
        };
        s.apply(cmd).0
    }

    fn node(r: Reply) -> Node {
        match r {
            Reply::Node(n) => n,
            other => panic!("not a node: {other:?}"),
        }
    }

    #[test]
    fn projects_and_keys() {
        let mut s = State::default();
        assert!(matches!(project(&mut s, "swe"), Reply::Project(_)));
        assert!(matches!(project(&mut s, "swe"), Reply::Refused(Refusal::Exists(_))));
        assert!(matches!(project(&mut s, "no spaces"), Reply::Refused(Refusal::Invalid(_))));

        let add = |s: &mut State, hash: [u8; 32], prefix: &str, project: &str| {
            s.apply(Command::AddKey {
                hash,
                prefix: prefix.into(),
                project: project.into(),
                now_ms: 2,
            })
            .0
        };
        assert!(matches!(add(&mut s, [1; 32], "hb_aaaa", "swe"), Reply::Key(..)));
        assert!(matches!(add(&mut s, [1; 32], "hb_aaaa", "swe"), Reply::Refused(_)));
        assert!(matches!(add(&mut s, [2; 32], "hb_bbbb", "nope"), Reply::Refused(_)));
        assert!(matches!(add(&mut s, [3; 32], "hb_aaaa", "swe"), Reply::Key(..)));
        assert_eq!(s.project_of(&[1; 32]), Some("swe"));

        // Two keys share the prefix, so it does not name one.
        let by_prefix = |s: &mut State, p: &str| {
            s.apply(Command::RevokeKey { hash: None, prefix: p.into(), now_ms: 9 }).0
        };
        assert!(matches!(by_prefix(&mut s, "hb_aaaa"), Reply::Refused(Refusal::Invalid(_))));
        let r =
            s.apply(Command::RevokeKey { hash: Some([3; 32]), prefix: String::new(), now_ms: 9 });
        assert!(matches!(r.0, Reply::Key(_, Key { revoked_ms: 9, .. })));
        assert_eq!(s.project_of(&[3; 32]), None);
        assert!(matches!(by_prefix(&mut s, "hb_zzzz"), Reply::Refused(Refusal::NotFound(_))));
    }

    #[test]
    fn a_comb_keeps_its_epoch_within_its_lease_and_gets_a_new_one_after() {
        let mut s = State::default();
        let a = node(register(&mut s, "a", 0, 1_000));
        assert_eq!((a.node, a.epoch, a.expires_ms), (1, 1, 31_000));
        let b = node(register(&mut s, "b", 0, 1_000));
        assert_eq!((b.node, b.epoch), (2, 1));

        // Restarted within the lease: same index, same epoch.
        let a = node(register(&mut s, "a", 1, 20_000));
        assert_eq!((a.node, a.epoch, a.expires_ms), (1, 1, 50_000));

        let renew = |s: &mut State, node, epoch, now_ms| {
            s.apply(Command::Renew { node, epoch, now_ms, ttl_ms: 30_000 }).0
        };
        assert_eq!(node(renew(&mut s, 1, 1, 40_000)).expires_ms, 70_000);
        assert!(matches!(renew(&mut s, 1, 2, 40_000), Reply::Refused(Refusal::LeaseLost(_))));
        assert!(matches!(renew(&mut s, 9, 1, 40_000), Reply::Refused(Refusal::NotFound(_))));

        // The lease ran out: renewing fails and registering again bumps the epoch.
        assert!(matches!(renew(&mut s, 1, 1, 70_000), Reply::Refused(Refusal::LeaseLost(_))));
        let a = node(register(&mut s, "a", 1, 70_001));
        assert_eq!((a.node, a.epoch), (1, 2));
        // A comb that forgot its epoch waits for the lease to run out, and gets a new one then.
        assert!(matches!(register(&mut s, "a", 0, 70_002), Reply::Refused(Refusal::Held(_))));
        assert!(matches!(register(&mut s, "a", 1, 100_000), Reply::Refused(Refusal::Held(_))));
        let a = node(register(&mut s, "a", 0, 100_001));
        assert_eq!((a.node, a.epoch), (1, 3));
        // As one logged before combs waited did, it takes a new epoch at once.
        let old = Command::Register {
            name: "a".into(),
            addr: "http://a:7420".into(),
            epoch: 0,
            now_ms: 100_002,
            ttl_ms: 30_000,
            wait: false,
        };
        assert_eq!(node(s.apply(old).0).epoch, 4);
    }

    #[test]
    fn node_indexes_fill_the_lowest_gap() {
        let mut s = State::default();
        for name in ["a", "b", "c"] {
            node(register(&mut s, name, 0, 0));
        }
        s.nodes.remove(&2);
        assert_eq!(node(register(&mut s, "d", 0, 0)).node, 2);
        assert_eq!(node(register(&mut s, "e", 0, 0)).node, 4);
        s.nodes.get_mut(&1).unwrap().epoch = u16::MAX;
        assert!(matches!(register(&mut s, "a", 0, 30_000), Reply::Refused(Refusal::Exhausted(_))));
    }

    #[test]
    fn a_batch_applies_in_order_and_answers_each_command() {
        let mut s = State::default();
        let mut changed = Vec::new();
        let quota = Quota::default();
        let r = s.apply_all(
            Command::Batch(vec![
                Command::CreateProject { name: "a".into(), quota, now_ms: 1 },
                Command::CreateProject { name: "a".into(), quota, now_ms: 2 },
                Command::CreateProject { name: "b".into(), quota, now_ms: 3 },
            ]),
            &mut changed,
        );
        let Reply::Batch(r) = r else { panic!("{r:?}") };
        assert!(matches!(r[0], Reply::Project(_)));
        assert!(matches!(r[1], Reply::Refused(Refusal::Exists(_))));
        assert!(matches!(r[2], Reply::Project(_)));
        assert_eq!(changed, [Changed::Project("a".into()), Changed::Project("b".into())]);
    }

    #[test]
    fn a_comb_asking_for_an_epoch_the_keeper_never_gave_starts_past_it() {
        let mut s = State::default();
        assert_eq!(node(register(&mut s, "a", 3, 0)).epoch, 4);
        assert_eq!(node(register(&mut s, "a", 9, 1)).epoch, 10);
        assert_eq!(node(register(&mut s, "a", 10, 2)).epoch, 10);
    }

    #[test]
    fn gates_share_a_quota_and_get_it_back_when_a_share_runs_out() {
        let mut s = State::default();
        let quota = Quota { cells: 100, creates_per_s: 50 };
        s.apply(Command::CreateProject { name: "swe".into(), quota, now_ms: 1 });
        let take = |s: &mut State, gate: &str, cells: u64, rate: u32, live: u64, now_ms: u64| {
            let cmd = Command::TakeQuota {
                project: "swe".into(),
                gate: gate.into(),
                cells,
                creates_per_s: rate,
                live,
                now_ms,
                ttl_ms: 30_000,
            };
            match s.apply(cmd).0 {
                Reply::Slice(q, sl, contended) => {
                    assert_eq!(q, quota);
                    (sl.cells, sl.creates_per_s, contended)
                }
                other => panic!("{other:?}"),
            }
        };
        // Ten cells are live, so the first gate gets 60 of the 90 left and 30 creates a second.
        assert_eq!(take(&mut s, "a", 60, 30, 10, 1), (60, 30, false));
        // The second gets an even split, more than the first left free, and both hear that
        // the quota is short.
        assert_eq!(take(&mut s, "b", 60, 30, 10, 2), (45, 25, true));
        // A gate asking again replaces its own share rather than adding to it, and now gets
        // only what the other left.
        assert_eq!(take(&mut s, "a", 60, 30, 10, 3), (45, 25, true));
        // Giving a share back frees it for the others.
        assert_eq!(take(&mut s, "a", 0, 0, 10, 4), (0, 0, true));
        assert_eq!(take(&mut s, "b", 90, 50, 10, 5), (90, 50, false));
        // A share that ran out is free again too.
        assert_eq!(take(&mut s, "a", 90, 50, 10, 30_006), (90, 50, false));
        assert!(!s.projects["swe"].slices.contains_key("b"));

        // No limit gives whatever was asked.
        s.apply(Command::CreateProject { name: "free".into(), quota: Quota::default(), now_ms: 1 });
        let cmd = Command::TakeQuota {
            project: "free".into(),
            gate: "a".into(),
            cells: 7,
            creates_per_s: 9,
            live: 1_000_000,
            now_ms: 2,
            ttl_ms: 30_000,
        };
        assert!(matches!(
            s.apply(cmd).0,
            Reply::Slice(_, Slice { cells: 7, creates_per_s: 9, .. }, false)
        ));
    }

    #[test]
    fn the_first_signing_key_set_is_the_one_kept() {
        let mut s = State::default();
        let (r, row) = s.apply(Command::SetRoot { private: [1; 32] });
        assert_eq!((r, row), (Reply::Root([1; 32]), Some(Changed::Root)));
        let (r, row) = s.apply(Command::SetRoot { private: [2; 32] });
        assert_eq!((r, row), (Reply::Root([1; 32]), None));
        assert_eq!(s.root, Some([1; 32]));
    }

    const T: [&str; 4] = ["2026-10-07T00", "2026-10-07T01", "2026-10-07T02", "2026-10-07T03"];

    /// The root after hour `i` of a chain of ten events an hour.
    fn at(i: u64) -> [u8; 32] {
        [(i % 251 + 1) as u8; 32]
    }

    /// Hour `i` of a chain of ten events an hour, from 2026-10-07T00 on.
    fn hour(i: u64) -> (String, AuditHour) {
        let prev = if i == 0 { [0; 32] } else { at(i - 1) };
        let name = format!("2026-10-{:02}T{:02}", 7 + i / 24, i % 24);
        (name, AuditHour { first_seq: i * 10, count: 10, prev, root: at(i) })
    }

    fn audit(
        s: &mut State,
        hours: Vec<(String, AuditHour)>,
        tip: Option<(&str, u64, [u8; 32])>,
    ) -> (Reply, Option<Changed>) {
        let tip = tip.map(|(h, seq, root)| (h.to_string(), seq, root));
        s.apply(Command::Audit { node: 1, epoch: 1, now_ms: 2, hours, tip })
    }

    fn conflict((r, _): (Reply, Option<Changed>)) -> String {
        match r {
            Reply::Refused(Refusal::Conflict(m)) => m,
            other => panic!("not a conflict: {other:?}"),
        }
    }

    #[test]
    fn audit_roots_have_to_follow_on_from_the_ones_held() {
        let mut s = State::default();
        // Only the comb holding the node's lease may publish its roots.
        let r = audit(&mut s, vec![hour(0)], None).0;
        assert!(matches!(r, Reply::Refused(Refusal::NotFound(_))), "{r:?}");
        register(&mut s, "node-a", 0, 1);
        let cmd =
            |epoch, now_ms| Command::Audit { node: 1, epoch, now_ms, hours: vec![], tip: None };
        for c in [cmd(2, 2), cmd(1, 40_000)] {
            let r = s.apply(c).0;
            assert!(matches!(r, Reply::Refused(Refusal::LeaseLost(_))), "{r:?}");
        }

        let (r, row) = audit(&mut s, vec![hour(0), hour(1)], Some((T[2], 25, [9; 32])));
        assert_eq!(r, Reply::Audit(T[1].into()));
        let added = vec![T[0].to_string(), T[1].to_string()];
        assert_eq!(
            row,
            Some(Changed::Audit { node: "node-a".into(), added, gone: vec![], tip: true })
        );
        // The same again changes nothing.
        let again = audit(&mut s, vec![hour(0), hour(1)], Some((T[2], 25, [9; 32])));
        assert_eq!(again, (Reply::Audit(T[1].into()), None));

        // Another root for an hour held, an hour that does not point back at the last one or
        // leaves one out, a seal that ends before the tip said its hour had got to, and a tip
        // that goes back are all refused, and change nothing.
        let before = s.clone();
        let mut forged = hour(1);
        forged.1.root = [7; 32];
        assert!(conflict(audit(&mut s, vec![forged], None)).contains("another root"));
        let mut off = hour(2);
        off.1.prev = [7; 32];
        assert!(conflict(audit(&mut s, vec![off], None)).contains("follow on"));
        assert!(conflict(audit(&mut s, vec![hour(3)], None)).contains("follow on"));
        let mut cut = hour(2);
        cut.1.count = 4;
        assert!(conflict(audit(&mut s, vec![cut], None)).contains("does not agree"));
        assert!(conflict(audit(&mut s, vec![], Some((T[2], 24, [9; 32])))).contains("went back"));
        assert!(
            conflict(audit(&mut s, vec![], Some((T[2], 25, [8; 32])))).contains("another root")
        );
        // A tip right at the end of a sealed hour has that hour's root.
        let r = audit(&mut s, vec![], Some((T[1], 20, [8; 32])));
        assert!(conflict(r).contains("does not agree"));
        assert_eq!(s, before);

        // Hour 2 sealed with the tip inside it goes in, and so does a tip at the end of hour 3.
        let (r, _) = audit(&mut s, vec![hour(2)], Some((T[3], 31, [5; 32])));
        assert_eq!(r, Reply::Audit(T[2].into()));
        let (r, _) = audit(&mut s, vec![hour(3)], Some((T[3], 40, at(3))));
        assert_eq!(r, Reply::Audit(T[3].into()));
        let chain = &s.audit["node-a"];
        assert_eq!(chain.hours.len(), 4);
        assert_eq!(chain.tip.as_ref().map(|t| (t.seq, t.root)), Some((40, at(3))));
    }

    #[test]
    fn the_keeper_holds_the_last_30_days_of_audit_hours() {
        let mut s = State::default();
        register(&mut s, "node-a", 0, 1);
        let all = AUDIT_HOURS as u64 + 10;
        let mut gone = Vec::new();
        for from in (0..all).step_by(AUDIT_BATCH) {
            let hours = (from..all.min(from + AUDIT_BATCH as u64)).map(hour).collect();
            match audit(&mut s, hours, None) {
                (Reply::Audit(_), Some(Changed::Audit { gone: g, .. })) => gone.extend(g),
                other => panic!("{other:?}"),
            }
        }
        let chain = &s.audit["node-a"];
        assert_eq!(chain.hours.len(), AUDIT_HOURS);
        assert_eq!(chain.hours.keys().next(), Some(&hour(10).0));
        assert_eq!(gone, (0..10).map(|i| hour(i).0).collect::<Vec<_>>());
        // An hour from before the oldest held has nothing to be checked against, and is left out.
        let r = audit(&mut s, vec![hour(3)], None);
        assert_eq!(r, (Reply::Audit(hour(all - 1).0), None));
        let many = (all..all + AUDIT_BATCH as u64 + 1).map(hour).collect();
        let r = audit(&mut s, many, None).0;
        assert!(matches!(r, Reply::Refused(Refusal::Invalid(_))), "{r:?}");
    }
}
