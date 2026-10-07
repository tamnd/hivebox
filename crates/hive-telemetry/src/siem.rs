//! Security events for a SIEM, from `spec/10_security.md`, section 10. A daemon hands each one to
//! [`Siem::emit`], which never blocks, and a thread of the [`Siem`]'s own sends them on to one
//! sink: a file of JSON lines, syslog over UDP or TCP, or an HTTP collector that takes NDJSON.
//!
//! A cell that sprays packets at an address its policy does not allow makes thousands of drops a
//! second, and a SIEM wants to hear that it happened, not each packet. So the first event of a
//! kind goes out at once, and the same event again, from the same cell with the same detail, is
//! only counted until the window ends, when one more line says how many more there were and when
//! the first and last of them came. A line costs the same however many events it stands for, so
//! what a node sends is bounded by how many different things happen in a window and not by how
//! often they do.
//!
//! Nothing here waits for the sink. Events queue for the thread, and when the queue is full they
//! are dropped and counted. A sink that is down is tried again after a pause that doubles up to
//! half a minute, the lines for it wait in a bounded backlog that drops its oldest first, and once
//! it takes lines again a `siem.dropped` line says how many events were lost on the way.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs, UdpSocket};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// The window repeats are counted over, unless the [`Options`] say otherwise.
pub const WINDOW: Duration = Duration::from_secs(10);

/// How many events may wait for the thread before [`Siem::emit`] drops them.
const QUEUE: usize = 1 << 14;

/// How many lines may wait for a sink that is down before the oldest are dropped.
const BACKLOG: usize = 1 << 14;

/// How many different events a window keeps apart. Past that, events new to the window are only
/// counted together, in one `siem.overflow` line when it ends.
const KEYS: usize = 4096;

/// The least time between two sends, so a steady trickle of events shares them. The first event
/// after a quiet spell goes at once.
const GAP: Duration = Duration::from_millis(200);

/// The first pause before a sink that failed is tried again, and the longest.
const RETRY: (Duration, Duration) = (Duration::from_secs(1), Duration::from_secs(30));

/// How long a connect, a write or the wait for an HTTP reply may take.
const TIMEOUT: Duration = Duration::from_secs(5);

/// The longest a text field of an event may be, in bytes. Longer ones are cut, so every line
/// fits in one syslog datagram.
const FIELD: usize = 512;

/// The biggest body one HTTP request carries.
const BODY: usize = 1 << 20;

/// The syslog facility the lines go out with: 4, security and authorization messages.
const FACILITY: u8 = 4;

/// How bad an event is, as syslog grades it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    /// Something the node did about an attack, such as a quarantine.
    Critical,
    /// Something only an attack does, such as a cell forging its source address.
    Error,
    /// Something the policy refused, which a mistake can do as well as an attack.
    Warning,
    /// Traffic the guard drops that is more often noise than intent.
    Notice,
    /// For the record.
    Info,
}

impl Severity {
    /// The syslog severity, from 2 for critical to 6 for informational.
    #[must_use]
    pub const fn syslog(self) -> u8 {
        match self {
            Self::Critical => 2,
            Self::Error => 3,
            Self::Warning => 4,
            Self::Notice => 5,
            Self::Info => 6,
        }
    }
}

/// One thing a SIEM should hear about.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecurityEvent {
    /// When, in nanoseconds since the Unix epoch.
    pub ts: u64,
    /// What happened, such as `net.denied` or `cell.quarantined`.
    pub kind: &'static str,
    /// How bad it is.
    pub severity: Severity,
    /// The project, or empty.
    pub project: String,
    /// The cell, or empty.
    pub cell: String,
    /// Who did it, an API key's id or a service's SPIFFE id, or empty.
    pub principal: String,
    /// The rest, as `key=value` pairs such as `reason=policy dst=192.0.2.1:443/tcp`.
    pub detail: String,
    /// How many times it happened, which is more than one when the daemon counted some already.
    pub count: u64,
}

impl SecurityEvent {
    /// An event of `kind` that happened once, now.
    #[must_use]
    pub fn new(kind: &'static str, severity: Severity) -> Self {
        Self {
            ts: now(),
            kind,
            severity,
            project: String::new(),
            cell: String::new(),
            principal: String::new(),
            detail: String::new(),
            count: 1,
        }
    }

    /// With the project.
    #[must_use]
    pub fn project(mut self, p: impl Into<String>) -> Self {
        self.project = p.into();
        self
    }

    /// With the cell.
    #[must_use]
    pub fn cell(mut self, c: impl Into<String>) -> Self {
        self.cell = c.into();
        self
    }

    /// With who did it.
    #[must_use]
    pub fn principal(mut self, p: impl Into<String>) -> Self {
        self.principal = p.into();
        self
    }

    /// With the detail.
    #[must_use]
    pub fn detail(mut self, d: impl Into<String>) -> Self {
        self.detail = d.into();
        self
    }

    /// As having happened `n` times.
    #[must_use]
    pub const fn count(mut self, n: u64) -> Self {
        self.count = n;
        self
    }
}

/// Where the events go.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Sink {
    /// Appended to a file as JSON lines, for a shipper to read. A file that was moved away, the
    /// way logrotate does, is opened anew.
    File(PathBuf),
    /// Syslog datagrams, in the format of RFC 5424, to `host:port`.
    Udp(String),
    /// Syslog over TCP to `host:port`, each message with its length in front, as RFC 6587 has it.
    Tcp(String),
    /// Posted in batches as NDJSON to `path` on `authority`, which is `host:port`.
    Http {
        /// `host:port`.
        authority: String,
        /// The path, from its first `/`.
        path: String,
    },
}

impl Sink {
    /// A sink from `/path` or `file:///path`, `udp://host:port`, `tcp://host:port` or
    /// `http://host:port/path`.
    ///
    /// # Errors
    ///
    /// It is none of those, or it is `https://`, which this does not speak: point it at a local
    /// collector that does, or at syslog over TCP.
    pub fn parse(s: &str) -> Result<Self, String> {
        let hostport = |hp: &str| match hp.rsplit_once(':') {
            Some((h, p)) if !h.is_empty() && p.parse::<u16>().is_ok_and(|p| p > 0) => {
                Ok(hp.to_string())
            }
            _ => Err(format!("{s:?} needs a host and a port, as in udp://127.0.0.1:514")),
        };
        if let Some(rest) = s.strip_prefix("udp://") {
            return Ok(Self::Udp(hostport(rest)?));
        }
        if let Some(rest) = s.strip_prefix("tcp://") {
            return Ok(Self::Tcp(hostport(rest)?));
        }
        if let Some(rest) = s.strip_prefix("http://") {
            let (authority, path) = match rest.find('/') {
                Some(i) => rest.split_at(i),
                None => (rest, "/"),
            };
            return Ok(Self::Http { authority: hostport(authority)?, path: path.to_string() });
        }
        if s.starts_with("https://") {
            return Err(format!(
                "{s:?}: https is not spoken here, so send to a local collector over http or to \
                 syslog over tcp"
            ));
        }
        let path = s.strip_prefix("file://").unwrap_or(s);
        if path.starts_with('/') {
            return Ok(Self::File(PathBuf::from(path)));
        }
        Err(format!("{s:?} is not a sink: give a /path, udp://, tcp:// or http://"))
    }
}

/// How a [`Siem`] sends.
#[derive(Clone, Debug)]
pub struct Options {
    /// Where to.
    pub sink: Sink,
    /// The node the events are from, which every line names.
    pub node: String,
    /// The daemon they are from, such as `hive-comb`.
    pub source: String,
    /// The window repeats are counted over.
    pub window: Duration,
    /// A header every HTTP request carries, such as `Authorization: Bearer ...`.
    pub header: Option<String>,
}

impl Options {
    /// Options for `sink`, with the default window and no header.
    #[must_use]
    pub fn new(sink: Sink, node: &str, source: &str) -> Self {
        Self { sink, node: node.into(), source: source.into(), window: WINDOW, header: None }
    }
}

/// A daemon's `[siem]` table, as its config file has it:
///
/// ```toml
/// [siem]
/// sink = "udp://10.0.0.9:514"
/// window = "10s"
/// header_file = "/etc/hivebox/siem.header"
/// ```
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Table {
    /// The sink, as [`Sink::parse`] takes it. Missing or empty sends nothing.
    pub sink: Option<String>,
    /// The window, like `10s` or `1m`.
    pub window: Option<String>,
    /// A file holding the one header each HTTP request carries.
    pub header_file: Option<PathBuf>,
}

/// Where a daemon sends security events, from its `[siem]` table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Link {
    /// The sink.
    pub sink: Sink,
    /// The window repeats of an event are counted over.
    pub window: Duration,
    /// A file holding the one header each HTTP request carries, such as an `Authorization`.
    pub header_file: Option<PathBuf>,
}

impl Table {
    /// Where the table says to send, or `None` for nowhere.
    ///
    /// # Errors
    ///
    /// The sink or the window does not parse, or there is a window or a header file and no sink.
    pub fn link(self) -> Result<Option<Link>, String> {
        let Some(sink) = self.sink.filter(|s| !s.is_empty()) else {
            if self.window.is_some() || self.header_file.is_some() {
                return Err("siem.window and siem.header_file need siem.sink".into());
            }
            return Ok(None);
        };
        let sink = Sink::parse(&sink).map_err(|e| format!("siem.sink: {e}"))?;
        let window = match self.window {
            Some(v) => duration(&v)
                .filter(|d| !d.is_zero())
                .ok_or_else(|| format!("siem.window = {v:?} is not a duration like 10s or 1m"))?,
            None => WINDOW,
        };
        Ok(Some(Link { sink, window, header_file: self.header_file }))
    }
}

impl Link {
    /// The options for daemon `source` on `node`, with the header read from its file.
    ///
    /// # Errors
    ///
    /// The header file cannot be read.
    pub fn options(&self, node: &str, source: &str) -> io::Result<Options> {
        let mut opts = Options::new(self.sink.clone(), node, source);
        opts.window = self.window;
        if let Some(path) = &self.header_file {
            let header = std::fs::read_to_string(path).map_err(|e| {
                io::Error::new(e.kind(), format!("reading {}: {e}", path.display()))
            })?;
            opts.header = Some(header.trim().to_string());
        }
        Ok(opts)
    }
}

/// A duration like `500ms`, `10s`, `1m` or `1h`.
fn duration(s: &str) -> Option<Duration> {
    let split = s.find(|c: char| !c.is_ascii_digit())?;
    let (n, unit) = s.split_at(split);
    let n: u64 = n.parse().ok()?;
    match unit {
        "ms" => Some(Duration::from_millis(n)),
        "s" => Some(Duration::from_secs(n)),
        "m" => n.checked_mul(60).map(Duration::from_secs),
        "h" => n.checked_mul(3600).map(Duration::from_secs),
        _ => None,
    }
}

/// How much a [`Siem`] has done.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SiemStats {
    /// Events handed to it, each counted as many times as it happened.
    pub events: u64,
    /// Lines made from them.
    pub lines: u64,
    /// Lines the sink took.
    pub sent: u64,
    /// Events lost to a full queue or backlog.
    pub dropped: u64,
    /// Sends that failed.
    pub failures: u64,
}

#[derive(Default)]
struct Counters {
    events: AtomicU64,
    lines: AtomicU64,
    sent: AtomicU64,
    dropped: AtomicU64,
    failures: AtomicU64,
}

enum Msg {
    Event(SecurityEvent),
    Flush(SyncSender<()>),
}

/// The sender. Dropping it ends the window, sends what is left once and stops the thread.
pub struct Siem {
    tx: Option<SyncSender<Msg>>,
    thread: Option<JoinHandle<()>>,
    counters: Arc<Counters>,
}

impl std::fmt::Debug for Siem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Siem").field("stats", &self.stats()).finish_non_exhaustive()
    }
}

impl Siem {
    /// Starts sending to `opts.sink`. A file sink is opened now, so a path that cannot be written
    /// shows up here; a network sink is only reached when there is something to send.
    ///
    /// # Errors
    ///
    /// The file cannot be opened, the header is not one header, or the thread did not start.
    pub fn open(opts: Options) -> io::Result<Self> {
        if let Some(h) = &opts.header
            && (!h.contains(':') || h.contains(['\r', '\n']))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the SIEM header is one line like Authorization: Bearer ...",
            ));
        }
        let mut conn = Conn::None;
        if let Sink::File(path) = &opts.sink {
            conn = Conn::File(open_file(path)?);
        }
        let counters = Arc::new(Counters::default());
        let (tx, rx) = mpsc::sync_channel(QUEUE);
        let w = Writer {
            opts,
            conn,
            window: HashMap::new(),
            overflow: None,
            out: VecDeque::new(),
            counters: counters.clone(),
            reported: 0,
        };
        let thread =
            std::thread::Builder::new().name("hive-siem".into()).spawn(move || run(w, &rx))?;
        Ok(Self { tx: Some(tx), thread: Some(thread), counters })
    }

    /// Queues `event`. When the queue is full it is dropped and counted.
    pub fn emit(&self, event: SecurityEvent) {
        let n = event.count;
        self.counters.events.fetch_add(n, Ordering::Relaxed);
        let Some(tx) = &self.tx else { return };
        if let Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) =
            tx.try_send(Msg::Event(event))
        {
            self.counters.dropped.fetch_add(n, Ordering::Relaxed);
        }
    }

    /// Ends the window and tries the sink with everything queued so far, then returns, whether the
    /// sink took it or not.
    pub fn flush(&self) {
        let Some(tx) = &self.tx else { return };
        let (done, wait) = mpsc::sync_channel(1);
        if tx.send(Msg::Flush(done)).is_ok() {
            let _ = wait.recv();
        }
    }

    /// How much it has done.
    pub fn stats(&self) -> SiemStats {
        let c = &self.counters;
        SiemStats {
            events: c.events.load(Ordering::Relaxed),
            lines: c.lines.load(Ordering::Relaxed),
            sent: c.sent.load(Ordering::Relaxed),
            dropped: c.dropped.load(Ordering::Relaxed),
            failures: c.failures.load(Ordering::Relaxed),
        }
    }
}

impl Drop for Siem {
    fn drop(&mut self) {
        self.tx = None;
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// The nanoseconds since the Unix epoch.
#[must_use]
pub fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_nanos() as u64)
}

/// `ts` in RFC 3339 to the millisecond, in UTC: `2026-10-07T06:12:01.123Z`.
#[must_use]
pub fn rfc3339(ts: u64) -> String {
    let (secs, ms) = (ts / 1_000_000_000, ts % 1_000_000_000 / 1_000_000);
    let (year, month, day) = crate::audit::civil(secs / 86_400);
    let rest = secs % 86_400;
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{ms:03}Z",
        rest / 3600,
        rest % 3600 / 60,
        rest % 60
    )
}

/// The events a window counts as the same.
#[derive(Clone, PartialEq, Eq, Hash)]
struct Key {
    kind: &'static str,
    project: String,
    cell: String,
    principal: String,
    detail: String,
}

/// The repeats of one event in this window.
struct Repeats {
    severity: Severity,
    count: u64,
    first: u64,
    last: u64,
}

/// A line waiting for the sink.
struct Line {
    json: String,
    ts: u64,
    kind: &'static str,
    severity: Severity,
    count: u64,
}

#[derive(Serialize)]
struct Wire<'a> {
    time: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    until: Option<String>,
    node: &'a str,
    source: &'a str,
    kind: &'a str,
    severity: Severity,
    project: &'a str,
    cell: &'a str,
    principal: &'a str,
    detail: &'a str,
    count: u64,
}

enum Conn {
    None,
    File(File),
    Udp(UdpSocket),
    Tcp(TcpStream),
}

struct Writer {
    opts: Options,
    conn: Conn,
    window: HashMap<Key, Repeats>,
    /// The repeats past [`KEYS`] in this window, with the first and last time.
    overflow: Option<Repeats>,
    out: VecDeque<Line>,
    counters: Arc<Counters>,
    /// How many dropped events a `siem.dropped` line has told of so far.
    reported: u64,
}

fn run(mut w: Writer, rx: &Receiver<Msg>) {
    let mut ends = Instant::now() + w.opts.window;
    let mut next_send = Instant::now();
    let mut backoff = RETRY.0;
    let mut open = true;
    while open {
        let due = if w.out.is_empty() { ends } else { ends.min(next_send) };
        let mut flushes = Vec::new();
        match rx.recv_timeout(due.saturating_duration_since(Instant::now())) {
            Ok(m) => {
                w.take(m, &mut flushes);
                while w.out.len() < BACKLOG
                    && let Ok(m) = rx.try_recv()
                {
                    w.take(m, &mut flushes);
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => open = false,
        }
        let now = Instant::now();
        let forced = !flushes.is_empty() || !open;
        if now >= ends || forced {
            w.end_window();
            ends = now + w.opts.window;
        }
        if !w.out.is_empty() && (forced || now >= next_send) {
            if w.send() {
                backoff = RETRY.0;
                next_send = now + GAP;
                // The sink is taking lines, so it can hear what was lost.
                w.report_drops();
            } else {
                w.counters.failures.fetch_add(1, Ordering::Relaxed);
                w.conn = Conn::None;
                next_send = now + backoff;
                backoff = (backoff * 2).min(RETRY.1);
            }
        }
        w.trim();
        for f in flushes {
            let _ = f.send(());
        }
    }
}

impl Writer {
    fn take(&mut self, m: Msg, flushes: &mut Vec<SyncSender<()>>) {
        let e = match m {
            Msg::Event(e) => e,
            Msg::Flush(done) => return flushes.push(done),
        };
        let key = Key {
            kind: e.kind,
            project: cut(e.project),
            cell: cut(e.cell),
            principal: cut(e.principal),
            detail: cut(e.detail),
        };
        if let Some(r) = self.window.get_mut(&key) {
            if r.count == 0 {
                r.first = e.ts;
            }
            r.count += e.count;
            r.last = r.last.max(e.ts);
            return;
        }
        if self.window.len() >= KEYS {
            // Too many different events to keep apart, so this one is only counted.
            let o = self.overflow.get_or_insert(Repeats {
                severity: Severity::Warning,
                count: 0,
                first: e.ts,
                last: e.ts,
            });
            o.count += e.count;
            o.last = o.last.max(e.ts);
            return;
        }
        self.push(&key, e.severity, e.ts, None, e.count);
        self.window.insert(key, Repeats { severity: e.severity, count: 0, first: 0, last: 0 });
    }

    /// One line for each event that repeated in the window, in the order the repeats began.
    fn end_window(&mut self) {
        let mut repeated: Vec<_> = self.window.drain().filter(|(_, r)| r.count > 0).collect();
        repeated.sort_unstable_by_key(|(_, r)| r.first);
        for (key, r) in repeated {
            self.push(&key, r.severity, r.first, Some(r.last), r.count);
        }
        if let Some(o) = self.overflow.take()
            && o.count > 0
        {
            let key = Key {
                kind: "siem.overflow",
                project: String::new(),
                cell: String::new(),
                principal: String::new(),
                detail: format!("keys={KEYS}"),
            };
            self.push(&key, o.severity, o.first, Some(o.last), o.count);
        }
    }

    fn push(&mut self, key: &Key, severity: Severity, ts: u64, until: Option<u64>, count: u64) {
        let wire = Wire {
            time: rfc3339(ts),
            until: until.map(rfc3339),
            node: &self.opts.node,
            source: &self.opts.source,
            kind: key.kind,
            severity,
            project: &key.project,
            cell: &key.cell,
            principal: &key.principal,
            detail: &key.detail,
            count,
        };
        let Ok(json) = serde_json::to_string(&wire) else { return };
        self.counters.lines.fetch_add(1, Ordering::Relaxed);
        self.out.push_back(Line { json, ts, kind: key.kind, severity, count });
    }

    /// A line for the events dropped since the last one said.
    fn report_drops(&mut self) {
        let dropped = self.counters.dropped.load(Ordering::Relaxed);
        if dropped > self.reported {
            let key = Key {
                kind: "siem.dropped",
                project: String::new(),
                cell: String::new(),
                principal: String::new(),
                detail: String::new(),
            };
            self.push(&key, Severity::Warning, now(), None, dropped - self.reported);
            self.reported = dropped;
        }
    }

    /// Drops the oldest lines past the backlog.
    fn trim(&mut self) {
        while self.out.len() > BACKLOG {
            if let Some(l) = self.out.pop_front() {
                self.counters.dropped.fetch_add(l.count, Ordering::Relaxed);
            }
        }
    }

    /// Sends what is waiting, and says whether the sink took all of it. Lines it took are gone
    /// from the backlog either way.
    fn send(&mut self) -> bool {
        let r = match self.opts.sink.clone() {
            Sink::File(path) => self.send_file(&path),
            Sink::Udp(to) => self.send_udp(&to),
            Sink::Tcp(to) => self.send_tcp(&to),
            Sink::Http { authority, path } => self.send_http(&authority, &path),
        };
        r.is_ok()
    }

    fn sent(&mut self, n: usize) {
        self.out.drain(..n);
        self.counters.sent.fetch_add(n as u64, Ordering::Relaxed);
    }

    fn send_file(&mut self, path: &std::path::Path) -> io::Result<()> {
        // A file moved away or deleted, as rotation does, is opened anew at the path.
        let moved = match (&self.conn, std::fs::metadata(path)) {
            (Conn::File(f), Ok(at)) => !same_file(f, &at),
            _ => true,
        };
        if moved {
            self.conn = Conn::File(open_file(path)?);
        }
        let Conn::File(f) = &mut self.conn else { return Err(io::Error::other("no file")) };
        let mut buf = Vec::new();
        for l in &self.out {
            buf.extend_from_slice(l.json.as_bytes());
            buf.push(b'\n');
        }
        f.write_all(&buf)?;
        let n = self.out.len();
        self.sent(n);
        Ok(())
    }

    fn send_udp(&mut self, to: &str) -> io::Result<()> {
        if !matches!(self.conn, Conn::Udp(_)) {
            let addr = resolve(to)?;
            let any: SocketAddr =
                if addr.is_ipv4() { ([0, 0, 0, 0], 0).into() } else { ([0u16; 8], 0).into() };
            let sock = UdpSocket::bind(any)?;
            sock.connect(addr)?;
            self.conn = Conn::Udp(sock);
        }
        let Conn::Udp(sock) = &self.conn else { return Err(io::Error::other("no socket")) };
        let mut n = 0;
        let mut failed = None;
        for l in &self.out {
            if let Err(e) = sock.send(syslog(&self.opts, l).as_bytes()) {
                failed = Some(e);
                break;
            }
            n += 1;
        }
        self.sent(n);
        failed.map_or(Ok(()), Err)
    }

    fn send_tcp(&mut self, to: &str) -> io::Result<()> {
        if !matches!(self.conn, Conn::Tcp(_)) {
            self.conn = Conn::Tcp(connect(to)?);
        }
        let Conn::Tcp(stream) = &mut self.conn else { return Err(io::Error::other("no stream")) };
        let mut buf = Vec::new();
        for l in &self.out {
            let msg = syslog(&self.opts, l);
            buf.extend_from_slice(format!("{} {msg}", msg.len()).as_bytes());
        }
        stream.write_all(&buf)?;
        let n = self.out.len();
        self.sent(n);
        Ok(())
    }

    fn send_http(&mut self, authority: &str, path: &str) -> io::Result<()> {
        while !self.out.is_empty() {
            let mut body = Vec::new();
            let mut n = 0;
            for l in &self.out {
                if n > 0 && body.len() + l.json.len() >= BODY {
                    break;
                }
                body.extend_from_slice(l.json.as_bytes());
                body.push(b'\n');
                n += 1;
            }
            post(authority, path, self.opts.header.as_deref(), &body)?;
            self.sent(n);
        }
        Ok(())
    }
}

/// One POST, on a connection of its own, which the reply's status line decides.
fn post(authority: &str, path: &str, header: Option<&str>, body: &[u8]) -> io::Result<()> {
    let mut stream = connect(authority)?;
    let mut req = format!(
        "POST {path} HTTP/1.1\r\nHost: {authority}\r\nContent-Type: application/x-ndjson\r\n\
         Content-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    if let Some(h) = header {
        req.push_str(h);
        req.push_str("\r\n");
    }
    req.push_str("\r\n");
    stream.write_all(req.as_bytes())?;
    stream.write_all(body)?;
    let mut status = String::new();
    BufReader::new((&stream).take(1024)).read_line(&mut status)?;
    let code = status.split(' ').nth(1).and_then(|c| c.parse::<u16>().ok());
    match code {
        Some(200..=299) => Ok(()),
        _ => Err(io::Error::other(format!("the collector said {:?}", status.trim_end()))),
    }
}

fn resolve(to: &str) -> io::Result<SocketAddr> {
    to.to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("{to} has no address")))
}

fn connect(to: &str) -> io::Result<TcpStream> {
    let stream = TcpStream::connect_timeout(&resolve(to)?, TIMEOUT)?;
    stream.set_write_timeout(Some(TIMEOUT))?;
    stream.set_read_timeout(Some(TIMEOUT))?;
    stream.set_nodelay(true)?;
    Ok(stream)
}

fn open_file(path: &std::path::Path) -> io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path).map_err(|e| {
        io::Error::new(e.kind(), format!("opening the SIEM file {}: {e}", path.display()))
    })
}

#[cfg(unix)]
fn same_file(f: &File, at: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    f.metadata().is_ok_and(|m| m.dev() == at.dev() && m.ino() == at.ino())
}

#[cfg(not(unix))]
fn same_file(_: &File, _: &std::fs::Metadata) -> bool {
    true
}

/// `l` as an RFC 5424 message, with the line's JSON as the message.
fn syslog(opts: &Options, l: &Line) -> String {
    let pri = FACILITY * 8 + l.severity.syslog();
    format!(
        "<{pri}>1 {} {} {} {} {} - {}",
        rfc3339(l.ts),
        field(&opts.node, 255),
        field(&opts.source, 48),
        std::process::id(),
        field(l.kind, 32),
        l.json
    )
}

/// A syslog header field: printable ASCII with no spaces, at most `max` long, or `-`.
fn field(s: &str, max: usize) -> String {
    let f: String = s.chars().filter(|c| c.is_ascii_graphic()).take(max).collect();
    if f.is_empty() { "-".into() } else { f }
}

/// `s` cut to [`FIELD`] bytes, at a character boundary.
fn cut(mut s: String) -> String {
    if s.len() > FIELD {
        let mut at = FIELD;
        while !s.is_char_boundary(at) {
            at -= 1;
        }
        s.truncate(at);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    fn scratch(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!("hive-siem-{name}-{}", std::process::id()));
        let _ = std::fs::remove_file(&p);
        p
    }

    fn lines(path: &std::path::Path) -> Vec<serde_json::Value> {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    fn denied(cell: &str) -> SecurityEvent {
        SecurityEvent::new("net.denied", Severity::Warning)
            .project("p")
            .cell(cell)
            .detail("reason=policy dst=192.0.2.1:443/tcp")
    }

    #[test]
    fn sinks_parse_and_https_is_refused() {
        assert_eq!(Sink::parse("/var/log/x.jsonl"), Ok(Sink::File("/var/log/x.jsonl".into())));
        assert_eq!(Sink::parse("file:///x"), Ok(Sink::File("/x".into())));
        assert_eq!(Sink::parse("udp://10.0.0.1:514"), Ok(Sink::Udp("10.0.0.1:514".into())));
        assert_eq!(Sink::parse("tcp://siem.local:601"), Ok(Sink::Tcp("siem.local:601".into())));
        assert_eq!(
            Sink::parse("http://127.0.0.1:8088/services/collector"),
            Ok(Sink::Http {
                authority: "127.0.0.1:8088".into(),
                path: "/services/collector".into()
            })
        );
        assert_eq!(
            Sink::parse("http://c:80"),
            Ok(Sink::Http { authority: "c:80".into(), path: "/".into() })
        );
        for bad in ["https://c:443/", "udp://nohost", "tcp://:514", "udp://h:0", "x.log", ""] {
            assert!(Sink::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn times_read_as_rfc3339() {
        assert_eq!(rfc3339(0), "1970-01-01T00:00:00.000Z");
        // 2026-10-07T06:12:01.123Z
        assert_eq!(rfc3339(1_791_353_521_123_456_789), "2026-10-07T06:12:01.123Z");
    }

    #[test]
    fn the_first_event_goes_out_at_once_and_repeats_are_counted_into_one_line() {
        let path = scratch("repeat");
        let mut opts = Options::new(Sink::File(path.clone()), "node-1", "hive-comb");
        opts.window = Duration::from_secs(3600);
        let siem = Siem::open(opts).unwrap();
        siem.emit(denied("c1"));
        // Not yet flushed, but the first one does not wait for the window.
        let deadline = Instant::now() + Duration::from_secs(5);
        while siem.stats().sent == 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(lines(&path).len(), 1);
        for _ in 0..999 {
            siem.emit(denied("c1"));
        }
        siem.emit(denied("c2").count(5));
        siem.flush();
        let got = lines(&path);
        assert_eq!(got.len(), 3, "{got:?}");
        assert_eq!(got[0]["count"], 1);
        assert_eq!(got[0]["cell"], "c1");
        assert_eq!(got[0]["node"], "node-1");
        assert_eq!(got[0]["severity"], "warning");
        assert!(got[0].get("until").is_none());
        assert_eq!(got[1]["cell"], "c2");
        assert_eq!(got[1]["count"], 5);
        assert_eq!(got[2]["cell"], "c1");
        assert_eq!(got[2]["count"], 999);
        assert!(got[2]["until"].is_string());
        let s = siem.stats();
        assert_eq!((s.events, s.lines, s.sent, s.dropped), (1005, 3, 3, 0));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_moved_file_is_opened_anew() {
        let path = scratch("rotate");
        let siem = Siem::open(Options::new(Sink::File(path.clone()), "n", "s")).unwrap();
        siem.emit(denied("a"));
        siem.flush();
        let old = path.with_extension("1");
        std::fs::rename(&path, &old).unwrap();
        siem.emit(denied("b"));
        siem.flush();
        assert_eq!(lines(&old).len(), 1);
        assert_eq!(lines(&path)[0]["cell"], "b");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&old);
    }

    #[test]
    fn syslog_over_udp_is_rfc5424() {
        let rx = UdpSocket::bind("127.0.0.1:0").unwrap();
        rx.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let to = rx.local_addr().unwrap().to_string();
        let siem = Siem::open(Options::new(Sink::Udp(to), "node 1", "hive-comb")).unwrap();
        siem.emit(SecurityEvent::new("cell.quarantined", Severity::Critical).cell("c9"));
        siem.flush();
        let mut buf = [0u8; 4096];
        let n = rx.recv(&mut buf).unwrap();
        let msg = std::str::from_utf8(&buf[..n]).unwrap();
        // Facility 4, severity 2.
        assert!(msg.starts_with("<34>1 "), "{msg}");
        let parts: Vec<&str> = msg.splitn(8, ' ').collect();
        assert_eq!(
            &parts[2..7],
            ["node1", "hive-comb", &std::process::id().to_string(), "cell.quarantined", "-"]
        );
        let json: serde_json::Value = serde_json::from_str(parts[7]).unwrap();
        assert_eq!(json["cell"], "c9");
        assert_eq!(json["node"], "node 1");
    }

    #[test]
    fn syslog_over_tcp_is_octet_counted_and_a_sink_that_comes_back_gets_the_backlog() {
        // A port with nothing on it yet.
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        drop(l);
        let siem = Siem::open(Options::new(Sink::Tcp(addr.to_string()), "n", "s")).unwrap();
        siem.emit(denied("a"));
        siem.emit(denied("b"));
        siem.flush();
        let s = siem.stats();
        assert_eq!((s.sent, s.failures), (0, 1));
        let l = TcpListener::bind(addr).unwrap();
        siem.flush();
        assert_eq!(siem.stats().sent, 2);
        drop(siem);
        let (mut c, _) = l.accept().unwrap();
        let mut all = String::new();
        c.read_to_string(&mut all).unwrap();
        let mut rest = all.as_str();
        let mut cells = Vec::new();
        while !rest.is_empty() {
            let (len, after) = rest.split_once(' ').unwrap();
            let (msg, next) = after.split_at(len.parse().unwrap());
            let json = msg.splitn(8, ' ').nth(7).unwrap();
            let v: serde_json::Value = serde_json::from_str(json).unwrap();
            cells.push(v["cell"].as_str().unwrap().to_string());
            rest = next;
        }
        assert_eq!(cells, ["a", "b"]);
    }

    #[test]
    fn http_posts_ndjson_with_the_header() {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut c, _) = l.accept().unwrap();
            let mut req = Vec::new();
            let mut buf = [0u8; 4096];
            loop {
                let n = c.read(&mut buf).unwrap();
                req.extend_from_slice(&buf[..n]);
                let s = String::from_utf8_lossy(&req);
                if let Some((head, body)) = s.split_once("\r\n\r\n") {
                    let len: usize = head
                        .lines()
                        .find_map(|h| h.strip_prefix("Content-Length: "))
                        .unwrap()
                        .parse()
                        .unwrap();
                    if body.len() >= len {
                        break;
                    }
                }
            }
            c.write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n").unwrap();
            String::from_utf8(req).unwrap()
        });
        let sink = Sink::parse(&format!("http://{addr}/ingest")).unwrap();
        let mut opts = Options::new(sink, "n", "s");
        opts.header = Some("Authorization: Bearer abc".into());
        let siem = Siem::open(opts).unwrap();
        siem.emit(denied("a"));
        siem.emit(denied("b"));
        siem.flush();
        let req = server.join().unwrap();
        assert!(req.starts_with("POST /ingest HTTP/1.1\r\n"), "{req}");
        assert!(req.contains("\r\nAuthorization: Bearer abc\r\n"));
        assert!(req.contains("\r\nContent-Type: application/x-ndjson\r\n"));
        let body = req.split_once("\r\n\r\n").unwrap().1;
        assert_eq!(body.lines().count(), 2);
        assert_eq!(siem.stats().sent, 2);
    }

    #[test]
    fn a_table_reads_as_a_link() {
        let t = |sink: Option<&str>, window: Option<&str>| Table {
            sink: sink.map(Into::into),
            window: window.map(Into::into),
            header_file: None,
        };
        assert_eq!(t(None, None).link(), Ok(None));
        assert_eq!(t(Some(""), None).link(), Ok(None));
        let l = t(Some("tcp://h:601"), Some("1m")).link().unwrap().unwrap();
        assert_eq!((l.sink, l.window), (Sink::Tcp("h:601".into()), Duration::from_secs(60)));
        let l = t(Some("/x"), None).link().unwrap().unwrap();
        assert_eq!(l.window, WINDOW);
        for bad in [t(Some("x"), None), t(Some("/x"), Some("0s")), t(None, Some("10s"))] {
            assert!(bad.link().is_err());
        }
        assert_eq!(duration("250ms"), Some(Duration::from_millis(250)));
        assert_eq!(duration("10"), None);
    }

    #[test]
    fn a_header_must_be_one_header() {
        let mut opts = Options::new(Sink::Udp("127.0.0.1:9".into()), "n", "s");
        opts.header = Some("Authorization: x\r\nX-Evil: y".into());
        assert!(Siem::open(opts).is_err());
    }

    #[test]
    fn long_fields_are_cut_at_a_character_boundary() {
        let s = cut("é".repeat(FIELD));
        assert!(s.len() <= FIELD && s.len() >= FIELD - 1);
        assert_eq!(field("a b\tc", 32), "abc");
        assert_eq!(field("", 32), "-");
    }
}
