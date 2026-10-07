//! The DNS proxy on [`crate::DNS_VIP`], from `spec/12_networking.md`, section 4.
//!
//! A cell may only look up names its profile lists. Anything else gets NXDOMAIN, which with the
//! built-in profiles is every name. For a name it may look up, the proxy asks the node's own
//! resolver, and before the answer goes back it lets the cell reach each address in it for the
//! answer's TTL, held between 30 s and 10 min, so a connect right after the lookup is allowed.
//!
//! The proxy only answers A queries with addresses. AAAA, HTTPS and SVCB get an empty answer,
//! since cells have no IPv6, and TXT and every other type are refused, as are queries from an
//! address that is not a cell's and queries past a cell's rate. Addresses in private, shared,
//! loopback, link local and other special ranges are taken out of answers, so a public name cannot
//! open the way to the node's own networks.
//!
//! Names of the node's own services, like [`crate::LLM_HOST`], are answered by the proxy itself
//! for the profiles that may reach them, and never go upstream. They do not count against the
//! cell's rate either, as an agent that opens a new connection for each call to the LLM gateway
//! looks it up each time, twice with the AAAA query, and the answer costs no more than a refusal.

use std::collections::HashMap;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use hickory_proto::op::{Message, MessageType, OpCode, ResponseCode};
use hickory_proto::rr::{RData, RecordType};
use tokio::net::UdpSocket;
use tokio::sync::{Semaphore, oneshot};

use crate::Profile;

/// Shortest time an answer allows its addresses for, so a cell that connects a little after the
/// lookup is not dropped.
pub const MIN_TTL: Duration = Duration::from_secs(30);
/// Longest time an answer allows its addresses for. Longer TTLs in answers are cut to this.
pub const MAX_TTL: Duration = Duration::from_secs(600);
/// Queries answered at once. Past this, new ones are dropped and the cell's resolver retries.
const IN_FLIGHT: usize = 1024;
/// A label longer than this under a wildcard pattern looks like data, not a host name.
const LONG_LABEL: usize = 40;

/// What the proxy needs from the node: who is asking, and a way to let a cell reach an address.
pub trait Cells: Send + Sync {
    /// The cell with address `ip`, as its `idx` in the guard's maps and its profile.
    fn cell(&self, ip: Ipv4Addr) -> Option<(u32, Profile)>;

    /// Lets cell `idx`, which asked from `from`, reach `ips` for `ttl`.
    ///
    /// # Errors
    ///
    /// The addresses could not be allowed, or `from` is no longer that cell's, and the answer
    /// must not go out.
    fn allow(&self, from: Ipv4Addr, idx: u32, ips: &[Ipv4Addr], ttl: Duration) -> io::Result<()>;

    /// Hears that the cell at `from` was refused `name`, `why` being `policy` for a name its
    /// profile may not look up and `rate` for a query past its rate. It must not block, since
    /// the answer waits for it.
    fn refused(&self, from: Ipv4Addr, name: &str, why: &'static str) {
        let _ = (from, name, why);
    }
}

/// The names a profile may look up. `example.com` is that name only, and `*.example.com` is any
/// name under it but not `example.com` itself.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Policy {
    exact: Vec<String>,
    under: Vec<String>,
}

impl Policy {
    /// A policy from patterns like `pypi.org` and `*.pythonhosted.org`.
    ///
    /// # Errors
    ///
    /// A pattern is not a host name, or has `*` anywhere but as the whole first label.
    pub fn new<S: AsRef<str>>(patterns: &[S]) -> Result<Self, String> {
        let mut p = Self::default();
        for pattern in patterns {
            let pattern = pattern.as_ref().trim_end_matches('.').to_ascii_lowercase();
            let (wild, name) = match pattern.strip_prefix("*.") {
                Some(rest) => (true, rest),
                None => (false, pattern.as_str()),
            };
            let label = |l: &str| {
                !l.is_empty()
                    && l.len() <= 63
                    && l.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            };
            if name.len() > 253 || !name.split('.').all(label) {
                return Err(format!("{pattern:?} is not a name like example.com or *.example.com"));
            }
            if wild {
                p.under.push(format!(".{name}"));
            } else {
                p.exact.push(name.to_string());
            }
        }
        Ok(p)
    }

    /// Whether `name`, lower case and with no trailing dot, may be looked up.
    #[must_use]
    pub fn allows(&self, name: &str) -> bool {
        if self.exact.iter().any(|e| e == name) {
            return true;
        }
        // Under a wildcard the first labels are the cell's to choose, which is where a tunnel
        // would put its data.
        self.under.iter().any(|u| name.ends_with(u.as_str())) && !looks_like_data(name)
    }
}

/// Long labels, or labels with about as many different letters as a random string has.
fn looks_like_data(name: &str) -> bool {
    name.split('.').any(|l| l.len() > LONG_LABEL || (l.len() >= 20 && entropy(l) > 4.0))
}

/// Bits per character of `s`, from how often each byte appears.
fn entropy(s: &str) -> f64 {
    let mut counts = [0u32; 256];
    for b in s.bytes() {
        counts[usize::from(b)] += 1;
    }
    let n = s.len() as f64;
    counts.iter().filter(|&&c| c > 0).map(|&c| f64::from(c) / n).map(|p| -p * p.log2()).sum()
}

/// Whether an answer may hand `ip` to a cell: not the node's own networks, nor anyone's private
/// ones, nor addresses with a special meaning.
#[must_use]
pub fn public(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    !(ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_unspecified()
        || ip.is_broadcast()
        || ip.is_multicast()
        || a == 0
        || a >= 240
        || (a == 100 && (64..128).contains(&b))
        || (a == 192 && b == 0 && c == 0)
        || (a == 198 && (b == 18 || b == 19)))
}

/// The resolvers in a `resolv.conf`, on port 53.
#[must_use]
pub fn upstreams(resolv_conf: &str) -> Vec<SocketAddr> {
    resolv_conf
        .lines()
        .filter_map(|l| l.trim().strip_prefix("nameserver"))
        .filter_map(|ip| ip.trim().parse().ok())
        .map(|ip| SocketAddr::new(ip, 53))
        .collect()
}

/// How the proxy runs.
#[derive(Clone, Debug)]
pub struct Settings {
    /// Resolvers to ask, in order. The next is tried when one does not answer in `timeout`.
    pub upstream: Vec<SocketAddr>,
    /// What each profile may look up. A profile with no entry may look up nothing.
    pub policies: HashMap<Profile, Policy>,
    /// The node's own names, answered here for the profiles each lists.
    pub hosts: Vec<Host>,
    /// Queries a cell may make each second, on average.
    pub rate: u32,
    /// Queries a cell may make at once after a quiet spell.
    pub burst: u32,
    /// How long to wait for each resolver.
    pub timeout: Duration,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            upstream: Vec::new(),
            policies: HashMap::new(),
            hosts: Vec::new(),
            rate: 50,
            burst: 100,
            timeout: Duration::from_secs(2),
        }
    }
}

/// A name of the node's own, with its address and the profiles that may look it up. Its address
/// must be one those profiles reach by a rule, since answering it allows nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Host {
    /// The name, in lower case and with no trailing dot.
    pub name: String,
    /// Its address.
    pub ip: Ipv4Addr,
    /// The profiles it is answered for. Others get NXDOMAIN, as for any name they may not look up.
    pub profiles: Vec<Profile>,
}

/// The TTL of an answer for a [`Host`].
const HOST_TTL: u32 = 300;

/// The proxy. One per node, shared by every cell.
pub struct Proxy {
    settings: Settings,
    cells: Arc<dyn Cells>,
    buckets: Mutex<HashMap<u32, (f64, Instant)>>,
    ids: AtomicU64,
    seed: std::hash::RandomState,
    links: tokio::sync::OnceCell<Vec<Option<Link>>>,
}

/// Sockets kept open to each resolver. Queries take them in turn, so each has its own source
/// port and a forged answer has to guess the port as well as the id.
const SOCKETS: usize = 8;

/// One socket to a resolver, and the queries waiting on it by id with the question each asked.
struct Link {
    sock: Arc<UdpSocket>,
    waiting: Arc<Mutex<Waiting>>,
    reader: tokio::task::AbortHandle,
}

type Waiting = HashMap<u16, (Vec<hickory_proto::op::Query>, oneshot::Sender<Message>)>;

impl Drop for Link {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

impl Link {
    async fn open(to: SocketAddr) -> io::Result<Self> {
        let any: SocketAddr = if to.is_ipv4() {
            (Ipv4Addr::UNSPECIFIED, 0).into()
        } else {
            "[::]:0".parse().unwrap_or(to)
        };
        let sock = Arc::new(UdpSocket::bind(any).await?);
        sock.connect(to).await?;
        let waiting: Arc<Mutex<Waiting>> = Arc::default();
        let (rx, map) = (sock.clone(), waiting.clone());
        let reader = tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            loop {
                // A resolver that is not there shows up as ECONNREFUSED here, from the ICMP error.
                let Ok(n) = rx.recv(&mut buf).await else { continue };
                let Ok(r) = Message::from_vec(&buf[..n]) else { continue };
                if r.metadata.message_type != MessageType::Response {
                    continue;
                }
                let mut map = map.lock().unwrap_or_else(PoisonError::into_inner);
                // An answer to a question that was not asked is ignored, and the query goes on
                // waiting for the real one.
                if map.get(&r.metadata.id).is_some_and(|(q, _)| *q == r.queries)
                    && let Some((_, tx)) = map.remove(&r.metadata.id)
                {
                    let _ = tx.send(r);
                }
            }
        })
        .abort_handle();
        Ok(Self { sock, waiting, reader })
    }
}

impl std::fmt::Debug for Proxy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Proxy").field("upstream", &self.settings.upstream).finish_non_exhaustive()
    }
}

impl Proxy {
    /// A proxy for `cells`.
    #[must_use]
    pub fn new(settings: Settings, cells: Arc<dyn Cells>) -> Self {
        Self {
            settings,
            cells,
            buckets: Mutex::default(),
            ids: AtomicU64::new(0),
            seed: std::hash::RandomState::new(),
            links: tokio::sync::OnceCell::new(),
        }
    }

    /// Answers every query that arrives on `sock`, each in a task of its own, until the task
    /// running this is dropped.
    pub async fn serve(self: Arc<Self>, sock: UdpSocket) {
        let sock = Arc::new(sock);
        let slots = Arc::new(Semaphore::new(IN_FLIGHT));
        let mut buf = vec![0u8; 4096];
        loop {
            let (n, from) = match sock.recv_from(&mut buf).await {
                Ok(got) => got,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
            };
            let SocketAddr::V4(from) = from else { continue };
            let Ok(slot) = slots.clone().try_acquire_owned() else { continue };
            let (this, sock, query) = (self.clone(), sock.clone(), buf[..n].to_vec());
            tokio::spawn(async move {
                if let Some(reply) = this.answer(*from.ip(), &query).await {
                    let _ = sock.send_to(&reply, from).await;
                }
                drop(slot);
            });
        }
    }

    /// The reply to `query` from the cell at `from`, or `None` for something that is not a query
    /// at all.
    pub async fn answer(&self, from: Ipv4Addr, query: &[u8]) -> Option<Vec<u8>> {
        let msg = Message::from_vec(query).ok()?;
        if msg.metadata.message_type != MessageType::Query {
            return None;
        }
        let reply = |code| {
            let mut r = Message::error_msg(msg.metadata.id, msg.metadata.op_code, code);
            r.queries.clone_from(&msg.queries);
            r.metadata.recursion_desired = msg.metadata.recursion_desired;
            r.metadata.recursion_available = true;
            r.to_vec().ok()
        };
        if msg.metadata.op_code != OpCode::Query {
            return reply(ResponseCode::NotImp);
        }
        let [q] = msg.queries.as_slice() else { return reply(ResponseCode::FormErr) };
        let Some((cell, profile)) = self.cells.cell(from) else {
            return reply(ResponseCode::Refused);
        };
        let name = q.name.to_ascii().trim_end_matches('.').to_ascii_lowercase();
        if let Some(host) = self.settings.hosts.iter().find(|h| h.name == name) {
            if !host.profiles.contains(&profile) {
                self.cells.refused(from, &name, "policy");
                return reply(ResponseCode::NXDomain);
            }
            return match q.query_type {
                RecordType::A => {
                    let mut r = Message::response(msg.metadata.id, OpCode::Query);
                    r.queries.clone_from(&msg.queries);
                    r.metadata.recursion_desired = msg.metadata.recursion_desired;
                    r.metadata.recursion_available = true;
                    r.metadata.authoritative = true;
                    r.answers.push(hickory_proto::rr::Record::from_rdata(
                        q.name.clone(),
                        HOST_TTL,
                        RData::A(hickory_proto::rr::rdata::A(host.ip)),
                    ));
                    r.to_vec().ok()
                }
                RecordType::AAAA | RecordType::HTTPS | RecordType::SVCB => {
                    reply(ResponseCode::NoError)
                }
                _ => reply(ResponseCode::Refused),
            };
        }
        if !self.take(cell) {
            self.cells.refused(from, &name, "rate");
            return reply(ResponseCode::Refused);
        }
        let allowed = self.settings.policies.get(&profile).is_some_and(|p| p.allows(&name));
        if !allowed {
            self.cells.refused(from, &name, "policy");
            return reply(ResponseCode::NXDomain);
        }
        match q.query_type {
            RecordType::A => {}
            RecordType::AAAA | RecordType::HTTPS | RecordType::SVCB => {
                return reply(ResponseCode::NoError);
            }
            _ => return reply(ResponseCode::Refused),
        }
        let Some(mut resp) = self.forward(&msg).await else {
            return reply(ResponseCode::ServFail);
        };
        resp.metadata.id = msg.metadata.id;
        resp.authorities.clear();
        resp.additionals.clear();
        resp.signature = None;
        resp.answers.retain(|r| match &r.data {
            RData::A(a) => public(a.0),
            RData::CNAME(_) => true,
            _ => false,
        });
        let ips: Vec<Ipv4Addr> = resp
            .answers
            .iter()
            .filter_map(|r| if let RData::A(a) = &r.data { Some(a.0) } else { None })
            .collect();
        if !ips.is_empty() {
            let shortest = resp.answers.iter().map(|r| r.ttl).min().unwrap_or(0);
            let ttl = u64::from(shortest).clamp(MIN_TTL.as_secs(), MAX_TTL.as_secs());
            for r in &mut resp.answers {
                r.ttl = r.ttl.min(u32::try_from(ttl).unwrap_or(u32::MAX));
            }
            if self.cells.allow(from, cell, &ips, Duration::from_secs(ttl)).is_err() {
                return reply(ResponseCode::ServFail);
            }
        }
        resp.to_vec().ok()
    }

    /// Takes a token from the cell's bucket, if it has one.
    fn take(&self, cell: u32) -> bool {
        let now = Instant::now();
        let (rate, burst) = (f64::from(self.settings.rate), f64::from(self.settings.burst));
        let mut buckets = self.buckets.lock().unwrap_or_else(PoisonError::into_inner);
        if buckets.len() > 65536 {
            // Cells come and go, so full buckets from a while ago are only taking up room.
            buckets.retain(|_, (_, at)| now.duration_since(*at) < Duration::from_secs(10));
        }
        let (tokens, at) = buckets.entry(cell).or_insert((burst, now));
        *tokens = (*tokens + now.duration_since(*at).as_secs_f64() * rate).min(burst);
        *at = now;
        if *tokens < 1.0 {
            return false;
        }
        *tokens -= 1.0;
        true
    }

    /// Asks the resolvers in turn with the question alone and an id of the proxy's own, and
    /// returns the first answer to it.
    async fn forward(&self, msg: &Message) -> Option<Message> {
        let links = self
            .links
            .get_or_init(|| async {
                let mut links = Vec::new();
                for &up in &self.settings.upstream {
                    for _ in 0..SOCKETS {
                        links.push(Link::open(up).await.ok());
                    }
                }
                links
            })
            .await;
        let mut out = Message::new(0, MessageType::Query, OpCode::Query);
        out.queries.clone_from(&msg.queries);
        out.metadata.recursion_desired = true;
        out.edns.clone_from(&msg.edns);
        for up in links.chunks(SOCKETS) {
            let turn = self.random() as usize % SOCKETS;
            let Some(link) = &up[turn] else { continue };
            let (tx, rx) = oneshot::channel();
            let id = {
                let mut waiting = link.waiting.lock().unwrap_or_else(PoisonError::into_inner);
                if waiting.len() > u16::MAX as usize / 2 {
                    continue;
                }
                let id = std::iter::repeat_with(|| self.random() as u16)
                    .find(|id| !waiting.contains_key(id))?;
                waiting.insert(id, (out.queries.clone(), tx));
                id
            };
            out.metadata.id = id;
            let answered = async {
                link.sock.send(&out.to_vec().ok()?).await.ok()?;
                tokio::time::timeout(self.settings.timeout, rx).await.ok()?.ok()
            };
            if let Some(r) = answered.await {
                return Some(r);
            }
            link.waiting.lock().unwrap_or_else(PoisonError::into_inner).remove(&id);
        }
        None
    }

    fn random(&self) -> u64 {
        use std::hash::BuildHasher;
        self.seed.hash_one(self.ids.fetch_add(1, Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::Query;
    use hickory_proto::rr::rdata::{A, CNAME};
    use hickory_proto::rr::{Name, Record};

    const CELL: Ipv4Addr = Ipv4Addr::new(100, 64, 0, 2);
    const PYPI: Ipv4Addr = Ipv4Addr::new(151, 101, 0, 223);
    /// A cell with a profile that may look up nothing.
    const OTHER: Ipv4Addr = Ipv4Addr::new(100, 64, 0, 4);

    #[derive(Default)]
    struct Fake {
        allowed: Mutex<Vec<(u32, Vec<Ipv4Addr>, Duration)>>,
        refused: Mutex<Vec<(Ipv4Addr, String, &'static str)>>,
    }

    impl Cells for Fake {
        fn cell(&self, ip: Ipv4Addr) -> Option<(u32, Profile)> {
            match ip {
                CELL => Some((7, Profile(16))),
                OTHER => Some((8, Profile(17))),
                _ => None,
            }
        }

        fn allow(&self, _: Ipv4Addr, idx: u32, ips: &[Ipv4Addr], ttl: Duration) -> io::Result<()> {
            self.allowed.lock().unwrap().push((idx, ips.to_vec(), ttl));
            Ok(())
        }

        fn refused(&self, from: Ipv4Addr, name: &str, why: &'static str) {
            self.refused.lock().unwrap().push((from, name.to_string(), why));
        }
    }

    /// A resolver that answers every A query with a CNAME, the address of pypi.org, a private
    /// address, and TTLs of 5 and 86400 seconds, and returns how many queries it got.
    async fn upstream() -> (SocketAddr, Arc<AtomicU64>) {
        let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = sock.local_addr().unwrap();
        let seen = Arc::new(AtomicU64::new(0));
        let count = seen.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            loop {
                let (n, from) = sock.recv_from(&mut buf).await.unwrap();
                count.fetch_add(1, Ordering::Relaxed);
                let q = Message::from_vec(&buf[..n]).unwrap();
                let name = q.queries[0].name.clone();
                let mut r = Message::response(q.metadata.id, OpCode::Query);
                r.queries.clone_from(&q.queries);
                let target = Name::from_ascii("dualstack.python.map.fastly.net.").unwrap();
                r.answers.push(Record::from_rdata(
                    name,
                    86400,
                    RData::CNAME(CNAME(target.clone())),
                ));
                r.answers.push(Record::from_rdata(target.clone(), 5, RData::A(A(PYPI))));
                r.answers.push(Record::from_rdata(
                    target,
                    5,
                    RData::A(A(Ipv4Addr::new(10, 0, 0, 1))),
                ));
                sock.send_to(&r.to_vec().unwrap(), from).await.unwrap();
            }
        });
        (addr, seen)
    }

    fn query(name: &str, kind: RecordType) -> Vec<u8> {
        let mut m = Message::new(4242, MessageType::Query, OpCode::Query);
        m.queries.push(Query::query(Name::from_ascii(name).unwrap(), kind));
        m.metadata.recursion_desired = true;
        m.to_vec().unwrap()
    }

    async fn proxy(rate: u32) -> (Proxy, Arc<Fake>, Arc<AtomicU64>) {
        let (addr, seen) = upstream().await;
        let fake = Arc::new(Fake::default());
        let policies = HashMap::from([(
            Profile(16),
            Policy::new(&["pypi.org", "*.pythonhosted.org"]).unwrap(),
        )]);
        // The first resolver never answers, so every lookup also shows the fallback working.
        let dead = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let hosts = vec![Host {
            name: "svc.hive.internal".into(),
            ip: Ipv4Addr::new(169, 254, 77, 81),
            profiles: vec![Profile(16)],
        }];
        let settings = Settings {
            upstream: vec![dead.local_addr().unwrap(), addr],
            policies,
            hosts,
            rate,
            burst: rate,
            timeout: Duration::from_millis(200),
        };
        std::mem::forget(dead);
        (Proxy::new(settings, fake.clone()), fake, seen)
    }

    async fn ask(p: &Proxy, from: Ipv4Addr, name: &str, kind: RecordType) -> Message {
        Message::from_vec(&p.answer(from, &query(name, kind)).await.unwrap()).unwrap()
    }

    #[tokio::test]
    async fn an_allowed_name_is_resolved_and_allowed_before_the_answer() {
        let (p, fake, seen) = proxy(100).await;
        let r = ask(&p, CELL, "PyPI.org.", RecordType::A).await;
        assert_eq!(r.metadata.id, 4242);
        assert_eq!(r.metadata.response_code, ResponseCode::NoError);
        let ips: Vec<Ipv4Addr> = r
            .answers
            .iter()
            .filter_map(|r| if let RData::A(a) = &r.data { Some(a.0) } else { None })
            .collect();
        assert_eq!(ips, [PYPI], "the private address is taken out");
        assert!(r.answers.iter().all(|r| r.ttl <= 30));
        assert_eq!(*fake.allowed.lock().unwrap(), [(7, vec![PYPI], MIN_TTL)]);
        assert_eq!(seen.load(Ordering::Relaxed), 1);

        let r = ask(&p, CELL, "files.pythonhosted.org", RecordType::A).await;
        assert_eq!(r.metadata.response_code, ResponseCode::NoError);
    }

    #[tokio::test]
    async fn everything_else_is_answered_without_asking_anyone() {
        let (p, fake, seen) = proxy(100).await;
        let code = |r: Message| r.metadata.response_code;
        assert_eq!(code(ask(&p, CELL, "example.com", RecordType::A).await), ResponseCode::NXDomain);
        assert_eq!(
            code(ask(&p, CELL, "pythonhosted.org", RecordType::A).await),
            ResponseCode::NXDomain
        );
        assert_eq!(
            code(ask(&p, CELL, "evilpypi.org", RecordType::A).await),
            ResponseCode::NXDomain
        );
        let tunnel = "mzxw6ytboi2dsnrtgq3tqojqgeztimbvgy3tqoi.pythonhosted.org";
        assert_eq!(code(ask(&p, CELL, tunnel, RecordType::A).await), ResponseCode::NXDomain);
        let long = format!("{}.pythonhosted.org", "a".repeat(41));
        assert_eq!(code(ask(&p, CELL, &long, RecordType::A).await), ResponseCode::NXDomain);
        assert_eq!(code(ask(&p, CELL, "pypi.org", RecordType::TXT).await), ResponseCode::Refused);
        let r = ask(&p, CELL, "pypi.org", RecordType::AAAA).await;
        assert_eq!((r.metadata.response_code, r.answers.len()), (ResponseCode::NoError, 0));
        let stranger = Ipv4Addr::new(100, 64, 0, 3);
        assert_eq!(code(ask(&p, stranger, "pypi.org", RecordType::A).await), ResponseCode::Refused);
        assert_eq!(seen.load(Ordering::Relaxed), 0);
        assert!(fake.allowed.lock().unwrap().is_empty());
        assert_eq!(p.answer(CELL, b"not dns").await, None);
        // The names the policy refused are heard of, and the record types it does not take and
        // the stranger are not.
        let refused = fake.refused.lock().unwrap();
        let names: Vec<_> = refused.iter().map(|(_, n, why)| (n.as_str(), *why)).collect();
        assert_eq!(
            names[..3],
            [("example.com", "policy"), ("pythonhosted.org", "policy"), ("evilpypi.org", "policy")]
        );
        assert_eq!(names.len(), 5);
        assert!(refused.iter().all(|(from, _, _)| *from == CELL));
    }

    #[tokio::test]
    async fn the_nodes_own_names_are_answered_here_for_their_profiles() {
        let (p, fake, seen) = proxy(100).await;
        let r = ask(&p, CELL, "SVC.hive.internal.", RecordType::A).await;
        assert_eq!(r.metadata.response_code, ResponseCode::NoError);
        let ips: Vec<_> = r
            .answers
            .iter()
            .map(|r| (r.ttl, if let RData::A(a) = &r.data { Some(a.0) } else { None }))
            .collect();
        assert_eq!(ips, [(300, Some(Ipv4Addr::new(169, 254, 77, 81)))]);
        let r = ask(&p, CELL, "svc.hive.internal", RecordType::AAAA).await;
        assert_eq!((r.metadata.response_code, r.answers.len()), (ResponseCode::NoError, 0));
        let code = |r: Message| r.metadata.response_code;
        let txt = ask(&p, CELL, "svc.hive.internal", RecordType::TXT).await;
        assert_eq!(code(txt), ResponseCode::Refused);
        let other = ask(&p, OTHER, "svc.hive.internal", RecordType::A).await;
        assert_eq!(code(other), ResponseCode::NXDomain);
        let refused = fake.refused.lock().unwrap().clone();
        assert_eq!(refused, [(OTHER, "svc.hive.internal".to_string(), "policy")]);
        assert_eq!(seen.load(Ordering::Relaxed), 0);
        assert!(fake.allowed.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_cell_past_its_rate_is_refused() {
        let (p, fake, _) = proxy(5).await;
        let mut codes = Vec::new();
        for _ in 0..7 {
            codes.push(ask(&p, CELL, "example.com", RecordType::A).await.metadata.response_code);
        }
        assert_eq!(codes[..5], [ResponseCode::NXDomain; 5]);
        assert_eq!(codes[5..], [ResponseCode::Refused; 2]);
        let why: Vec<_> = fake.refused.lock().unwrap().iter().map(|r| r.2).collect();
        assert_eq!(why, ["policy", "policy", "policy", "policy", "policy", "rate", "rate"]);
        for _ in 0..20 {
            let own = ask(&p, CELL, "svc.hive.internal", RecordType::A).await;
            assert_eq!(own.metadata.response_code, ResponseCode::NoError);
        }
    }

    #[test]
    fn patterns_and_addresses() {
        assert!(Policy::new(&["*.a.com", "b.org."]).is_ok());
        assert!(Policy::new(&["a.*.com"]).is_err());
        assert!(Policy::new(&["*"]).is_err());
        assert!(Policy::new(&["a..com"]).is_err());
        for ip in [
            "10.1.2.3",
            "172.16.0.1",
            "192.168.1.1",
            "100.64.0.9",
            "169.254.169.254",
            "127.0.0.1",
            "0.1.2.3",
            "224.0.0.1",
            "240.0.0.1",
            "198.18.0.1",
        ] {
            assert!(!public(ip.parse().unwrap()), "{ip}");
        }
        for ip in ["151.101.0.223", "1.1.1.1", "100.128.0.1", "198.51.100.7"] {
            assert!(public(ip.parse().unwrap()), "{ip}");
        }
        assert_eq!(
            upstreams(
                "# x\nnameserver 127.0.0.53\nnameserver ::1\noptions edns0\nnameserver bad\n"
            ),
            ["127.0.0.53:53".parse().unwrap(), "[::1]:53".parse().unwrap()]
        );
        assert!(entropy("aaaa") < 0.1);
        assert!(entropy("mzxw6ytboi2dsnrtgq3tqojqge") > 4.0);
        assert!(entropy("dualstack-python-map") < 4.0);
    }
}
