//! What the comb tells a SIEM, from `spec/10_security.md`, section 10: the packets the guard
//! dropped and the names the DNS proxy refused, read four times a second and put down to their
//! cells, and from the API, the calls refused for want of a right and every quarantine. The
//! [`Siem`] counts the repeats and sends them on.

use crate::comb::Inner;
use crate::net::{Drained, Net};
use hive_guard::{Deny, Reason};
use hive_telemetry::siem::{self, SecurityEvent, Severity, Siem};
use hive_types::CellId;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

/// How often the guard's ring and the proxy's refusals are read. The ring holds about 44,000
/// drops, so it takes more than 170,000 dropped packets a second for any to go unreported, and
/// those are still counted, as `net.unreported`.
const POLL: Duration = Duration::from_millis(250);

/// How many different destinations, or names, of one cell a window tells apart. Past that, the
/// rest are counted together as `other`, so a cell scanning ports makes a few lines and not one
/// for each port.
const SPREAD: usize = 16;

/// Reads the guard and the DNS proxy until `stop`, and hands what it finds to `siem`.
pub(crate) async fn watch(
    inner: Arc<Inner>,
    siem: Arc<Siem>,
    net: Arc<Net>,
    window: Duration,
    stop: CancellationToken,
) {
    let mut tick = tokio::time::interval(POLL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut spread = Spread::new(window);
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            () = stop.cancelled() => return,
        }
        // The lock it takes is the one wiring holds while netlink works, so not on this thread.
        let n = net.clone();
        let Ok(drained) = tokio::task::spawn_blocking(move || n.drain()).await else { continue };
        let project = |id: CellId| inner.find(id).map(|c| c.project.clone()).unwrap_or_default();
        for e in spread.events(drained, project) {
            siem.emit(e);
        }
    }
}

/// The destinations and names each cell has been told apart by in this window.
struct Spread {
    window: Duration,
    ends: Instant,
    seen: HashMap<(Option<CellId>, &'static str), HashSet<String>>,
    /// The drops the ring had no room for, all told, when last read.
    lost: Option<u64>,
}

impl Spread {
    fn new(window: Duration) -> Self {
        Self { window, ends: Instant::now() + window, seen: HashMap::new(), lost: None }
    }

    /// The events in `d`, one for each cell and thing it did, with how many times it did it.
    fn events(&mut self, d: Drained, project: impl Fn(CellId) -> String) -> Vec<SecurityEvent> {
        let now = Instant::now();
        if now >= self.ends {
            self.seen.clear();
            self.ends = now + self.window;
        }
        let mut counts: HashMap<(Option<CellId>, &'static str, Severity, String), u64> =
            HashMap::new();
        for (id, deny) in d.denies {
            let (kind, severity, detail) = describe(&deny);
            let detail = match kind {
                "net.denied" => self.limit(id, kind, detail, "dst=other"),
                _ => detail,
            };
            *counts.entry((id, kind, severity, detail)).or_default() += 1;
        }
        // The count goes back to the pins, so the first one read is where it starts from.
        if let Some(total) = d.lost
            && let Some(before) = self.lost.replace(total)
            && total > before
        {
            let key = (None, "net.unreported", Severity::Warning, "reason=ring_full".to_string());
            *counts.entry(key).or_default() += total - before;
        }
        for (id, name, why, n) in d.refused {
            let (kind, severity) = match why {
                "rate" => ("dns.limited", Severity::Notice),
                _ => ("dns.denied", Severity::Warning),
            };
            let name = if name.is_empty() { "other".to_string() } else { name };
            let detail = self.limit(Some(id), kind, format!("name={name}"), "name=other");
            *counts.entry((Some(id), kind, severity, detail)).or_default() += n;
        }
        let ts = siem::now();
        let mut events: Vec<SecurityEvent> = counts
            .into_iter()
            .map(|((id, kind, severity, detail), n)| {
                let mut e = SecurityEvent::new(kind, severity).detail(detail).count(n);
                e.ts = ts;
                match id {
                    Some(id) => e.cell(id.to_string()).project(project(id)),
                    None => e,
                }
            })
            .collect();
        events.sort_unstable_by(|a, b| {
            (&a.cell, a.kind, &a.detail).cmp(&(&b.cell, b.kind, &b.detail))
        });
        events
    }

    /// `detail`, or `other` once the cell has had [`SPREAD`] different ones of this kind.
    fn limit(
        &mut self,
        id: Option<CellId>,
        kind: &'static str,
        detail: String,
        other: &str,
    ) -> String {
        let seen = self.seen.entry((id, kind)).or_default();
        if seen.contains(&detail) {
            return detail;
        }
        if seen.len() >= SPREAD {
            return other.to_string();
        }
        seen.insert(detail.clone());
        detail
    }
}

/// The kind, severity and detail of a drop.
fn describe(d: &Deny) -> (&'static str, Severity, String) {
    match d.reason {
        Reason::Policy => ("net.denied", Severity::Warning, format!("dst={}", dst(d))),
        Reason::Spoof => ("net.spoofed", Severity::Error, format!("src={}", d.ip)),
        Reason::Ipv6 => ("net.dropped", Severity::Notice, "reason=ipv6".into()),
        Reason::Proto => ("net.dropped", Severity::Notice, "reason=protocol".into()),
        Reason::Malformed => ("net.dropped", Severity::Notice, "reason=malformed".into()),
        Reason::NoCell | Reason::PassRule | Reason::PassDns | Reason::PassArp => {
            ("net.dropped", Severity::Notice, "reason=no_cell".into())
        }
    }
}

/// Where a dropped packet was going, as `192.0.2.1:443/tcp`.
fn dst(d: &Deny) -> String {
    match d.proto {
        6 => format!("{}:{}/tcp", d.ip, d.port),
        17 => format!("{}:{}/udp", d.ip, d.port),
        1 => format!("{}/icmp", d.ip),
        p => format!("{}/{p}", d.ip),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn id(seq: u64) -> CellId {
        CellId::new(1, 1, 1, seq, 0xabc).unwrap()
    }

    fn deny(reason: Reason, last: u8, port: u16) -> Deny {
        Deny { cell: 1, reason, ip: Ipv4Addr::new(192, 0, 2, last), port, proto: 6 }
    }

    #[test]
    fn drops_are_counted_by_cell_and_destination() {
        let mut s = Spread::new(Duration::from_secs(10));
        let (a, b) = (id(1), id(2));
        let mut d = Drained::default();
        for _ in 0..500 {
            d.denies.push((Some(a), deny(Reason::Policy, 1, 443)));
        }
        d.denies.push((Some(b), deny(Reason::Policy, 1, 443)));
        d.denies.push((Some(b), deny(Reason::Spoof, 9, 0)));
        d.denies.push((None, deny(Reason::Ipv6, 0, 0)));
        d.refused.push((a, "evil.example".into(), "policy", 3));
        d.refused.push((a, "pypi.org".into(), "rate", 40));
        let got = s.events(d, |c| if c == a { "pa".into() } else { "pb".into() });
        let lines: Vec<_> = got
            .iter()
            .map(|e| (e.cell.as_str(), e.project.as_str(), e.kind, e.detail.as_str(), e.count))
            .collect();
        let (sa, sb) = (a.to_string(), b.to_string());
        assert_eq!(
            lines,
            [
                ("", "", "net.dropped", "reason=ipv6", 1),
                (&sa, "pa", "dns.denied", "name=evil.example", 3),
                (&sa, "pa", "dns.limited", "name=pypi.org", 40),
                (&sa, "pa", "net.denied", "dst=192.0.2.1:443/tcp", 500),
                (&sb, "pb", "net.denied", "dst=192.0.2.1:443/tcp", 1),
                (&sb, "pb", "net.spoofed", "src=192.0.2.9", 1),
            ]
        );
        assert_eq!(got[5].severity, Severity::Error);
    }

    #[test]
    fn a_cell_scanning_ports_is_told_apart_by_its_first_few() {
        let mut s = Spread::new(Duration::from_secs(10));
        let a = id(1);
        let mut d = Drained::default();
        for port in 1..=1000 {
            d.denies.push((Some(a), deny(Reason::Policy, 1, port)));
        }
        let got = s.events(d, |_| String::new());
        assert_eq!(got.len(), SPREAD + 1);
        let other = got.iter().find(|e| e.detail == "dst=other").unwrap();
        assert_eq!(other.count, 1000 - SPREAD as u64);
        // In the same window, a port it was told apart by still is, and a new one is not.
        let mut d = Drained::default();
        d.denies.push((Some(a), deny(Reason::Policy, 1, 1)));
        d.denies.push((Some(a), deny(Reason::Policy, 1, 5000)));
        let details: Vec<_> =
            s.events(d, |_| String::new()).into_iter().map(|e| e.detail).collect();
        assert_eq!(details, ["dst=192.0.2.1:1/tcp", "dst=other"]);
    }

    #[test]
    fn drops_the_ring_had_no_room_for_are_counted_from_the_first_read_on() {
        let mut s = Spread::new(Duration::from_secs(10));
        let lost = |n| Drained { lost: n, ..Default::default() };
        assert!(s.events(lost(Some(700)), |_| String::new()).is_empty());
        assert!(s.events(lost(None), |_| String::new()).is_empty());
        let got = s.events(lost(Some(950)), |_| String::new());
        assert_eq!(got.len(), 1);
        assert_eq!(
            (got[0].kind, got[0].detail.as_str(), got[0].count),
            ("net.unreported", "reason=ring_full", 250)
        );
        assert!(s.events(lost(Some(950)), |_| String::new()).is_empty());
    }

    #[test]
    fn destinations_read_by_protocol() {
        let mut d = deny(Reason::Policy, 1, 53);
        d.proto = 17;
        assert_eq!(dst(&d), "192.0.2.1:53/udp");
        d.proto = 1;
        assert_eq!(dst(&d), "192.0.2.1/icmp");
        d.proto = 47;
        assert_eq!(dst(&d), "192.0.2.1/47");
    }
}
