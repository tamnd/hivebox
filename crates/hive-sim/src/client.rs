//! The clients: creates spread over the projects, most with an idempotency key, retried
//! through any gate when the answer does not say whether a cell was made.

use std::collections::BTreeMap;

use hive_rt::Rng as _;
use hive_types::{CellId, Reason};

use crate::world::{Addr, Body, PROJECTS, Tick, World};

/// Tries a keyed create gets in all, and how long each waits for an answer.
const TRIES: u32 = 4;
const DEADLINE: u64 = 20_000;

#[derive(Debug)]
enum Wait {
    /// For the answer to the current try, until the deadline.
    Answer(u64),
    /// Before the next try.
    Backoff(u64),
    Done,
}

#[derive(Debug)]
struct Req {
    project: String,
    key: String,
    attempt: u32,
    wait: Wait,
}

#[derive(Default)]
pub(crate) struct Clients {
    reqs: BTreeMap<u64, Req>,
    next: u64,
}

impl World {
    pub(crate) fn client_tick(&mut self, t: Tick) {
        match t {
            Tick::Load => {
                if self.draining {
                    return;
                }
                self.new_request();
                // The gaps between creates of a Poisson stream.
                let u = (self.rng.below(1_000_000) + 1) as f64 / 1_000_001.0;
                let gap = -u.ln() * 1000.0 / f64::from(self.cfg.creates_per_s.max(1));
                self.tick((gap as u64).max(1), Addr::Client, Tick::Load);
            }
            Tick::Retry(req) => {
                let now = self.now;
                let Some(r) = self.clients.reqs.get(&req) else { return };
                match r.wait {
                    Wait::Backoff(until) if now >= until => self.try_send(req),
                    Wait::Answer(deadline) if now >= deadline => self.unclear(req),
                    _ => {}
                }
            }
            Tick::StopCell(id) => {
                let g = self.rng.below(self.gates.len() as u64) as usize;
                self.send(Addr::Client, Addr::Gate(g), Body::End { id });
            }
            _ => {}
        }
    }

    fn new_request(&mut self) {
        let total: u64 = PROJECTS.iter().map(|(_, _, w)| w).sum();
        let mut pick = self.rng.below(total);
        let mut project = PROJECTS[0].0;
        for (p, _, w) in PROJECTS {
            if pick < w {
                project = p;
                break;
            }
            pick -= w;
        }
        self.clients.next += 1;
        let req = self.clients.next;
        let key = if self.rng.below(10) < 8 { format!("k{req}") } else { String::new() };
        let r = Req { project: project.into(), key, attempt: 0, wait: Wait::Done };
        self.clients.reqs.insert(req, r);
        self.report.requests += 1;
        self.check.request(req);
        self.try_send(req);
    }

    fn try_send(&mut self, req: u64) {
        let deadline = self.now + DEADLINE;
        let Some(r) = self.clients.reqs.get_mut(&req) else { return };
        r.wait = Wait::Answer(deadline);
        let body =
            Body::Ask { req, attempt: r.attempt, project: r.project.clone(), key: r.key.clone() };
        let g = self.rng.below(self.gates.len() as u64) as usize;
        self.send(Addr::Client, Addr::Gate(g), body);
        self.tick(DEADLINE, Addr::Client, Tick::Retry(req));
    }

    pub(crate) fn client_got(&mut self, body: Body) {
        let Body::Answer { req, attempt, got } = body else { return };
        let Some(r) = self.clients.reqs.get(&req) else { return };
        if r.attempt != attempt || !matches!(r.wait, Wait::Answer(_)) {
            // An answer to a try the client gave up on. A cell in it runs until its TTL.
            if got.is_ok() {
                self.report.late += 1;
            }
            return;
        }
        match got {
            Ok(id) => {
                self.done(req);
                self.report.created += 1;
                self.stop_later(id);
            }
            Err(Reason::QuotaExceeded | Reason::CapacityUnavailable | Reason::InvalidArgument) => {
                self.done(req);
                self.report.refused += 1;
            }
            Err(_) => self.unclear(req),
        }
    }

    /// The try ended without saying whether a cell was made. A keyed create tries again.
    fn unclear(&mut self, req: u64) {
        let now = self.now;
        let Some(r) = self.clients.reqs.get_mut(&req) else { return };
        if r.key.is_empty() || r.attempt + 1 >= TRIES {
            self.done(req);
            self.report.failed += 1;
            return;
        }
        r.attempt += 1;
        let wait = 200 << r.attempt;
        r.wait = Wait::Backoff(now + wait);
        self.tick(wait, Addr::Client, Tick::Retry(req));
    }

    fn done(&mut self, req: u64) {
        if let Some(r) = self.clients.reqs.get_mut(&req) {
            r.wait = Wait::Done;
        }
        let at = self.now;
        self.check.outcome(req, at);
    }

    /// Most clients stop their cell when done with it. The rest leave it to its TTL.
    fn stop_later(&mut self, id: CellId) {
        if self.rng.below(10) < 7 {
            let after = 1000 + self.rng.below(20_000);
            self.tick(after, Addr::Client, Tick::StopCell(id));
        }
    }
}
