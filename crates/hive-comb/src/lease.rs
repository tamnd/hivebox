//! The comb's lease from the keeper.
//!
//! A comb in a cluster registers with the keeper before it opens, and gets its node index and
//! its epoch back. It keeps the epoch while it renews its lease in time, so a restart within the
//! lease keeps its cells. A comb whose lease ran out, or that the keeper gave a new epoch, has
//! lost its node: gates fail its cells as lost, so it stops, and the next start registers again
//! and fails the cells it still has from the old epoch.
//!
//! The last node index and epoch are kept in the `lease` file in the data directory, so the comb
//! can ask for the same epoch back after a restart, along with when the lease runs out by this
//! machine's clock. The cells keep running while the comb is down, and nothing else stops them
//! once the lease is gone and the keeper gives the node to another comb, so a comb that cannot
//! register again before then stops them itself.
//!
//! Each renewal also carries the roots of the comb's audit chain: the hours it sealed since the
//! last one the keeper holds, and how far the chain has got. The keeper refuses roots that do
//! not follow on from the ones it holds, which the comb reports, and the renewal goes through
//! either way.

use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use hive_telemetry::audit::{self, AuditLog};

use hive_proto::internal as pb;
use hive_proto::internal::keeper_client::KeeperClient;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tonic::Code;
use tonic::transport::{Channel, Endpoint};

use crate::config::KeeperLink;

/// The file in the data directory that holds the last lease.
pub const FILE: &str = "lease";

/// How long one call to the keeper may take. A write waits up to 5 s for a new leader.
const CALL: Duration = Duration::from_secs(8);

/// How long the comb waits before it tries again after a call failed.
const RETRY: Duration = Duration::from_millis(500);

/// The longest wait between tries to register.
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// The most sealed hours one renewal carries, as many as the keeper takes at once.
const AUDIT_HOURS: usize = 64;

/// The audit log whose roots go with each renewal, and the directory it is in. It is set once
/// the comb has opened the log, and holds it weakly, so the comb closing the log is not held up.
pub type AuditLink = Arc<OnceLock<(Weak<AuditLog>, PathBuf)>>;

/// The node this comb is, as the keeper gave it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Lease {
    /// The node index, which goes into every cell id.
    pub node: u16,
    /// The epoch, which goes into every cell id too.
    pub epoch: u16,
    /// How long the lease lasts after each renewal.
    pub ttl: Duration,
}

/// The keeper group, called through one member at a time and the next one when it fails.
#[derive(Debug)]
pub struct Keeper {
    members: Vec<(String, KeeperClient<Channel>)>,
    at: usize,
}

impl Keeper {
    /// Clients for every member. Nothing is dialled until the first call.
    ///
    /// # Errors
    ///
    /// A member address does not parse.
    pub fn new(members: &[String]) -> Result<Self, String> {
        let members = members
            .iter()
            .map(|m| {
                let channel = Endpoint::from_shared(format!("http://{m}"))
                    .map_err(|e| format!("keeper member {m}: {e}"))?
                    .connect_timeout(Duration::from_secs(1))
                    .timeout(CALL)
                    .tcp_nodelay(true)
                    .connect_lazy();
                Ok((m.clone(), KeeperClient::new(channel)))
            })
            .collect::<Result<Vec<_>, String>>()?;
        if members.is_empty() {
            return Err("the keeper has no members".into());
        }
        Ok(Self { members, at: 0 })
    }

    /// Registers once through the current member, and moves on to the next member if that one
    /// could not be reached.
    async fn register(&mut self, req: pb::RegisterRequest) -> Result<Lease, tonic::Status> {
        let r = self.members[self.at].1.register(req).await;
        self.next_on_failure(r).map(|l| lease(&l))
    }

    /// Renews once, the same way.
    async fn renew(
        &mut self,
        l: Lease,
        (audit_hours, audit_tip): Roots,
    ) -> Result<pb::Lease, tonic::Status> {
        let req = pb::RenewRequest {
            node: u32::from(l.node),
            epoch: u32::from(l.epoch),
            audit_hours,
            audit_tip,
        };
        let r = self.members[self.at].1.renew(req).await;
        self.next_on_failure(r)
    }

    fn next_on_failure<T>(
        &mut self,
        r: Result<tonic::Response<T>, tonic::Status>,
    ) -> Result<T, tonic::Status> {
        match r {
            Ok(r) => Ok(r.into_inner()),
            Err(s) => {
                if retry(&s) {
                    self.at = (self.at + 1) % self.members.len();
                }
                Err(s)
            }
        }
    }

    /// The member calls go to now.
    fn member(&self) -> &str {
        &self.members[self.at].0
    }
}

/// How registering ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Start {
    /// The keeper gave the comb its node. The lease counts from the instant, when the call that
    /// got it went out.
    Leased(Lease, Instant),
    /// The last lease ran out before the keeper gave the node back, so the cells the comb has
    /// from it have to stop. These are its node index and epoch.
    Lapsed(u16, u16),
}

/// Registers `link` and returns the lease, trying until the keeper answers. The epoch asked for
/// is the one in the data directory's `lease` file, and the lease is written there before this
/// returns. If the file says when that lease runs out and the keeper has not answered by then,
/// this gives up and says so.
///
/// # Errors
///
/// The keeper refused the node, for a bad name or because it ran out of node indexes or
/// epochs, or the `lease` file could not be written.
pub async fn register(
    keeper: &mut Keeper,
    link: &KeeperLink,
    data_dir: &Path,
) -> Result<Start, String> {
    let path = data_dir.join(FILE);
    let last = last(&path);
    let lapse = last
        .and_then(|(.., until)| until)
        .map(|until| Instant::now() + Duration::from_millis(until.saturating_sub(now_ms())));
    let last = last.map(|(node, epoch, _)| (node, epoch));
    let req = pb::RegisterRequest {
        name: link.name.clone(),
        addr: link.advertise.clone(),
        epoch: last.map_or(0, |(_, e)| u32::from(e)),
    };
    let mut backoff = RETRY;
    let mut said = false;
    let (l, sent, wall) = loop {
        let (sent, wall) = (Instant::now(), now_ms());
        match keeper.register(req.clone()).await {
            Ok(l) => break (l, sent, wall),
            Err(s) if !retry(&s) => {
                return Err(format!("the keeper refused node {}: {}", link.name, s.message()));
            }
            Err(s) => {
                if let (Some((node, epoch)), Some(at)) = (last, lapse)
                    && Instant::now() >= at
                {
                    return Ok(Start::Lapsed(node, epoch));
                }
                if !said {
                    eprintln!(
                        "hive-comb: registering with the keeper at {}: {}",
                        keeper.member(),
                        s.message()
                    );
                    said = true;
                }
                let left = lapse.map_or(backoff, |at| at.saturating_duration_since(Instant::now()));
                tokio::time::sleep(backoff.min(left)).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    };
    if let Some((node, _)) = last
        && node != l.node
    {
        eprintln!("hive-comb: the keeper moved this node from index {node} to {}", l.node);
    }
    write(&path, l, Some(until(wall, l)))
        .map_err(|e| format!("writing {}: {e}", path.display()))?;
    Ok(Start::Leased(l, sent))
}

/// Renews `lease` a third of the way through each lease until `stop`. The lease is counted from
/// when the comb sent the call that got it, `since` for the first, which is no later than the
/// keeper counts it from. Each renewal writes when the lease now runs out to `file`.
///
/// # Errors
///
/// The keeper says the lease is gone: it ran out, or the node registered again in a newer
/// epoch. Or the keeper could not be reached until the lease ran out. Either way the comb has
/// lost its node and has to stop.
pub async fn keep(
    mut keeper: Keeper,
    mut l: Lease,
    since: Instant,
    file: PathBuf,
    audit: AuditLink,
    stop: CancellationToken,
) -> Result<(), String> {
    let mut wait = l.ttl / 3;
    let mut ends = since + l.ttl;
    let mut said = false;
    let mut unwritten = false;
    let mut roots = Publisher::default();
    loop {
        tokio::select! {
            () = stop.cancelled() => return Ok(()),
            () = tokio::time::sleep(wait) => {}
        }
        let (sent, wall) = (Instant::now(), now_ms());
        if sent >= ends {
            return Err(format!(
                "could not renew the lease of node {} in epoch {} before it ran out",
                l.node, l.epoch
            ));
        }
        let sending = roots.next(&audit).await;
        match keeper.renew(l, sending).await {
            Ok(got) => {
                roots.got(got.audit_hour.clone(), &got.audit_refused);
                let renewed = lease(&got);
                ends = sent + renewed.ttl;
                match write(&file, renewed, Some(until(wall, renewed))) {
                    Ok(()) => unwritten = false,
                    Err(e) if !unwritten => {
                        eprintln!("hive-comb: writing {}: {e}", file.display());
                        unwritten = true;
                    }
                    Err(_) => {}
                }
                if said {
                    eprintln!("hive-comb: renewed the lease through {}", keeper.member());
                    said = false;
                }
                l = renewed;
                wait = l.ttl / 3;
            }
            Err(s) if matches!(s.code(), Code::FailedPrecondition | Code::NotFound) => {
                return Err(format!(
                    "lost the lease of node {} in epoch {}: {}",
                    l.node,
                    l.epoch,
                    s.message()
                ));
            }
            Err(s) => {
                if !said {
                    eprintln!("hive-comb: renewing the lease: {}", s.message());
                    said = true;
                }
                wait = RETRY.min(ends.saturating_duration_since(Instant::now()));
            }
        }
    }
}

/// What a renewal carries of the audit chain: sealed hours and the tip.
type Roots = (Vec<pb::AuditHour>, Option<pb::AuditTip>);

/// Works out which audit roots go with the next renewal.
#[derive(Debug, Default)]
struct Publisher {
    /// The last sealed hour the keeper holds, once a renewal has said.
    held: Option<String>,
    /// The hours found after `held` when the log had sealed this many, which go again until the
    /// keeper takes them.
    found: Option<(String, u64, Vec<pb::AuditHour>)>,
    /// The last refusal reported, so it is said once and not on every renewal.
    refused: String,
    /// The tip that went with the last renewal, and the one the keeper took, so a tip that has
    /// not moved is not sent again and a quiet node's renewals stay small.
    sent: Option<(u64, Vec<u8>)>,
    taken: Option<(u64, Vec<u8>)>,
}

impl Publisher {
    async fn next(&mut self, link: &AuditLink) -> Roots {
        let Some((log, dir)) =
            link.get().and_then(|(log, dir)| Some((log.upgrade()?, dir.clone())))
        else {
            return Roots::default();
        };
        let (tip, sealed) = (log.tip(), log.stats().sealed);
        drop(log);
        let mut tip = (tip.seq > 0).then(|| pb::AuditTip {
            hour: tip.hour,
            seq: tip.seq,
            root: tip.root.to_vec(),
            at_ms: 0,
        });
        self.sent = tip.as_ref().map(|t| (t.seq, t.root.clone()));
        if self.sent.is_some() && self.sent == self.taken {
            tip = None;
        }
        // The first renewal finds out from the keeper where to start.
        let Some(held) = self.held.clone() else { return (Vec::new(), tip) };
        if let Some((h, n, hours)) = &self.found
            && *h == held
            && *n == sealed
        {
            return (hours.clone(), tip);
        }
        let after = held.clone();
        let read = tokio::task::spawn_blocking(move || audit::seals(&dir, &after, AUDIT_HOURS));
        let hours: Vec<_> = match read.await {
            Ok(Ok(seals)) => seals.into_iter().filter_map(|s| hour(&s)).collect(),
            Ok(Err(e)) => {
                eprintln!("hive-comb: reading the audit seals: {e}");
                Vec::new()
            }
            Err(_) => Vec::new(),
        };
        self.found = Some((held, sealed, hours.clone()));
        (hours, tip)
    }

    fn got(&mut self, held: String, refused: &str) {
        if refused != self.refused {
            if !refused.is_empty() {
                eprintln!("hive-comb: the keeper refused the audit roots: {refused}");
            }
            self.refused = refused.to_owned();
        }
        if refused.is_empty() {
            self.taken = self.sent.take();
        }
        self.held = Some(held);
    }
}

fn hour(s: &audit::Seal) -> Option<pb::AuditHour> {
    Some(pb::AuditHour {
        hour: s.hour.clone(),
        first_seq: s.first_seq,
        count: s.count,
        prev: audit::unhex(&s.prev)?.to_vec(),
        root: audit::unhex(&s.root)?.to_vec(),
    })
}

/// Whether a failed call is worth trying again: the keeper or its leader was out of reach, or
/// the call ran out of time.
fn retry(s: &tonic::Status) -> bool {
    matches!(
        s.code(),
        Code::Unavailable
            | Code::DeadlineExceeded
            | Code::Unknown
            | Code::Cancelled
            | Code::Internal
    )
}

fn lease(l: &pb::Lease) -> Lease {
    Lease {
        node: u16::try_from(l.node).unwrap_or(0),
        epoch: u16::try_from(l.epoch).unwrap_or(0),
        ttl: Duration::from_millis(l.ttl_ms),
    }
}

/// The node index and epoch in the `lease` file, if there is one that reads.
pub fn read(path: &Path) -> Option<(u16, u16)> {
    last(path).map(|(node, epoch, _)| (node, epoch))
}

/// Notes in the `lease` file at `path` that the cells from its lease were stopped, so the next
/// start waits on the keeper for as long as it takes.
///
/// # Errors
///
/// The file could not be written.
pub fn lapsed(path: &Path) -> std::io::Result<()> {
    match last(path) {
        Some((node, epoch, _)) => write(path, Lease { node, epoch, ttl: Duration::ZERO }, None),
        None => Ok(()),
    }
}

/// The node index, the epoch and, from a comb that wrote it, the Unix time in milliseconds the
/// lease runs out at.
fn last(path: &Path) -> Option<(u16, u16, Option<u64>)> {
    let text = std::fs::read_to_string(path).ok()?;
    let mut words = text.split_whitespace();
    let node = words.next()?.parse().ok()?;
    let epoch = words.next()?.parse().ok()?;
    Some((node, epoch, words.next().and_then(|w| w.parse().ok())))
}

/// Writes the lease to `path`, through a file next to it so a crash leaves the old one or the
/// new one.
fn write(path: &Path, l: Lease, until: Option<u64>) -> std::io::Result<()> {
    let tmp = PathBuf::from(format!("{}.tmp", path.display()));
    let text = match until {
        Some(u) => format!("{} {} {u}\n", l.node, l.epoch),
        None => format!("{} {}\n", l.node, l.epoch),
    };
    std::fs::write(&tmp, text)?;
    std::fs::File::open(&tmp)?.sync_all()?;
    std::fs::rename(&tmp, path)
}

/// When a lease got by a call sent at Unix time `wall` runs out.
fn until(wall: u64, l: Lease) -> u64 {
    wall.saturating_add(u64::try_from(l.ttl.as_millis()).unwrap_or(u64::MAX))
}

fn now_ms() -> u64 {
    let d = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hive_telemetry::AuditEvent;

    #[tokio::test]
    async fn a_tip_the_keeper_has_taken_is_not_sent_again() {
        let dir = std::env::temp_dir().join(format!("hive-comb-tip-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let log = Arc::new(AuditLog::open(&dir, "node-a").unwrap());
        let link = AuditLink::default();
        link.set((Arc::downgrade(&log), dir.clone())).unwrap();
        let mut p = Publisher::default();
        let seq = |r: &Roots| r.1.as_ref().map(|t| t.seq);

        // Nothing recorded yet, so there is no tip.
        assert_eq!(seq(&p.next(&link).await), None);
        log.record(AuditEvent { op: "exec.run".into(), ..AuditEvent::default() });
        log.flush().unwrap();
        assert_eq!(seq(&p.next(&link).await), Some(1));
        // A renewal that failed took nothing, so the tip goes again, and once it is taken it
        // stops going until it moves.
        assert_eq!(seq(&p.next(&link).await), Some(1));
        p.got(String::new(), "");
        assert_eq!(seq(&p.next(&link).await), None);
        p.got(String::new(), "");
        assert_eq!(seq(&p.next(&link).await), None);
        log.record(AuditEvent { op: "exec.run".into(), ..AuditEvent::default() });
        log.flush().unwrap();
        assert_eq!(seq(&p.next(&link).await), Some(2));
        // A refused tip is sent again.
        p.got(String::new(), "refused");
        assert_eq!(seq(&p.next(&link).await), Some(2));

        drop(log);
        assert_eq!(p.next(&link).await, Roots::default());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
