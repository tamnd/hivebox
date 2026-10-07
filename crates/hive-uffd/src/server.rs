//! One restored VM's memory, served from the snapshot's memory file as the VM touches it.

use crate::hand::{self, Hello, Report};
use crate::sys::{self, Event, Map, Uffd};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::io::{self, Read};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The biggest single copy a prefetch makes, so a fault that comes in meanwhile waits for at most
/// this much.
const PREFETCH_RUN: u64 = 2 << 20;
/// How long the server sleeps in `poll` before it looks at the socket again.
const POLL_MS: i32 = 1000;

/// One stretch of guest memory: where it is in the VMM, how big, where its pages are in the
/// memory file, and the page size it is mapped with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Region {
    /// Where the VMM mapped it.
    pub base: u64,
    /// How long it is, in bytes.
    pub size: u64,
    /// Where its first page is in the memory file.
    pub offset: u64,
    /// The page size it is mapped with, 4 KiB unless the guest has huge pages.
    pub page: u64,
}

/// A region the way Firecracker sends it. Releases before 1.7 give the page size in KiB.
#[derive(Serialize, Deserialize)]
struct Wire {
    base_host_virt_addr: u64,
    size: u64,
    offset: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    page_size: Option<u64>,
    #[serde(default, skip_serializing)]
    page_size_kib: Option<u64>,
}

impl Region {
    /// Reads the regions a VMM sends with its userfaultfd.
    ///
    /// # Errors
    ///
    /// The text is not the JSON list Firecracker sends, or a region is empty, not page aligned or
    /// has a page size that is not a power of two.
    pub fn parse(text: &[u8]) -> io::Result<Vec<Self>> {
        let wire: Vec<Wire> = serde_json::from_slice(text)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("the regions: {e}")))?;
        wire.into_iter()
            .map(|w| {
                let page = w.page_size.or(w.page_size_kib.map(|k| k << 10)).unwrap_or(4096);
                let r = Self { base: w.base_host_virt_addr, size: w.size, offset: w.offset, page };
                let aligned = |v: u64| v.is_multiple_of(page);
                if !page.is_power_of_two() || r.size == 0 || !aligned(r.base | r.size | r.offset) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("region {r:?} is empty or not aligned to its page"),
                    ));
                }
                Ok(r)
            })
            .collect()
    }

    fn to_wire(self) -> Wire {
        Wire {
            base_host_virt_addr: self.base,
            size: self.size,
            offset: self.offset,
            page_size: Some(self.page),
            page_size_kib: None,
        }
    }

    fn holds(&self, addr: u64) -> bool {
        addr >= self.base && addr - self.base < self.size
    }
}

/// A snapshot's memory file, mapped once and shared by every VM restored from it, so the pages
/// they all start from are in the page cache once.
///
/// When the file is in memory, on tmpfs or hugetlbfs or made by [`Memory::load`], a VMM can map
/// it privately and register its guest memory for minor faults. The server then maps each page
/// in from the page cache with no copy, every VM shares the clean pages, and a page is copied
/// only when its VM first writes to it.
#[derive(Debug)]
pub struct Memory {
    map: Map,
    file: File,
    in_memory: bool,
}

impl Memory {
    /// Maps the memory file at `path`.
    ///
    /// # Errors
    ///
    /// The file cannot be opened or mapped, or is empty.
    pub fn open(path: &Path) -> io::Result<Self> {
        Self::from_file(File::open(path)?)
    }

    /// Copies the memory file at `path` into a file in memory and maps that, so VMs can be served
    /// in minor fault mode from one copy of the snapshot's pages.
    ///
    /// # Errors
    ///
    /// The file cannot be read, is empty, or the copy does not fit in memory.
    pub fn load(path: &Path) -> io::Result<Self> {
        let mut file = sys::memfd("hive-uffd")?;
        io::copy(&mut File::open(path)?, &mut file)?;
        Self::from_file(file)
    }

    fn from_file(file: File) -> io::Result<Self> {
        Ok(Self { map: Map::file(&file)?, in_memory: sys::in_memory(&file)?, file })
    }

    /// The file the pages are in, for a VMM in the same process or one it is passed to.
    #[must_use]
    pub fn file(&self) -> &File {
        &self.file
    }

    /// Whether the file is in memory, so VMs that map it can take minor faults.
    #[must_use]
    pub fn in_memory(&self) -> bool {
        self.in_memory
    }

    /// The file's length in bytes.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.map.len() as u64
    }

    /// Whether the file is empty, which `open` never lets happen.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.len() == 0
    }

    fn bytes(&self, at: u64, len: u64) -> &[u8] {
        // Regions are checked against the file's length when a VM connects, so both fit.
        self.map.bytes(at as usize, len as usize)
    }
}

/// The memory file offsets of the pages one restore touched, in the order it first touched them.
/// The next restore of the same snapshot fills these in before the VM asks, as REAP does.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Trace(pub Vec<u64>);

impl Trace {
    /// Reads a trace saved by [`Trace::save`]: each offset as eight little endian bytes.
    ///
    /// # Errors
    ///
    /// The file cannot be read or its length is not a multiple of eight.
    pub fn load(path: &Path) -> io::Result<Self> {
        let bytes = std::fs::read(path)?;
        if !bytes.len().is_multiple_of(8) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "a trace is whole u64s"));
        }
        Ok(Self(bytes.as_chunks::<8>().0.iter().map(|c| u64::from_le_bytes(*c)).collect()))
    }

    /// Writes the trace to `path`, through a temporary file so a reader never sees half of it.
    ///
    /// # Errors
    ///
    /// The file cannot be written.
    pub fn save(&self, path: &Path) -> io::Result<()> {
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, self.0.iter().flat_map(|o| o.to_le_bytes()).collect::<Vec<u8>>())?;
        std::fs::rename(&tmp, path)
    }
}

/// A trace built up over many restores of the same snapshot. The first restore's trace is taken
/// whole. A restore after it faults only on the pages its prefetch missed, and a page joins the
/// trace once it has been missed in `at_least` restores, so a page that one restore happened to
/// touch is not prefetched for every VM after it.
#[derive(Debug, Clone, Default)]
pub struct Merged {
    trace: Option<Trace>,
    has: HashSet<u64>,
    missed: HashMap<u64, u32>,
    at_least: u32,
}

impl Merged {
    /// Starts from `trace`, if there is one already, and adds a page once `at_least` restores
    /// missed it.
    pub fn new(trace: Option<Trace>, at_least: u32) -> Self {
        let has = trace.iter().flat_map(|t| t.0.iter().copied()).collect();
        Self { trace, has, missed: HashMap::new(), at_least: at_least.max(1) }
    }

    /// The trace so far, or `None` before the first restore.
    pub fn trace(&self) -> Option<&Trace> {
        self.trace.as_ref()
    }

    /// Takes in the trace of one restore that ended, and says how many pages joined the merged
    /// trace.
    pub fn add(&mut self, restore: &Trace) -> usize {
        let Some(trace) = &mut self.trace else {
            self.has = restore.0.iter().copied().collect();
            self.trace = Some(restore.clone());
            return restore.0.len();
        };
        let before = trace.0.len();
        for &at in &restore.0 {
            if self.has.contains(&at) {
                continue;
            }
            let n = self.missed.entry(at).or_default();
            *n += 1;
            if *n >= self.at_least {
                self.missed.remove(&at);
                self.has.insert(at);
                trace.0.push(at);
            }
        }
        trace.0.len() - before
    }
}

/// What a session did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    /// Faults answered with a page from the memory file.
    pub faults: u64,
    /// Of those, the faults answered by mapping the page in from the page cache, with no copy.
    pub mapped: u64,
    /// Faults answered with a zero page, for memory the guest had given back.
    pub zero_faults: u64,
    /// Faults on a page that was already there by the time the server got to it.
    pub raced: u64,
    /// Bytes filled in ahead of the VM from a trace.
    pub prefetched: u64,
    /// Ranges the guest gave back.
    pub removes: u64,
    /// The longest a fault took to answer once the server read it.
    pub slowest: Duration,
    /// All the time spent answering faults.
    pub busy: Duration,
}

/// One VM's memory: its userfaultfd, its regions, and the file its pages come from.
#[derive(Debug)]
pub struct Session {
    uffd: Uffd,
    stream: UnixStream,
    regions: Vec<Region>,
    memory: Arc<Memory>,
    removed: Vec<(u64, u64)>,
    pending: VecDeque<Event>,
    trace: Vec<u64>,
    stats: Stats,
    /// Whether a prefetch maps pages in, which it does until the kernel says the VM's memory is
    /// not registered for minor faults.
    map_in: bool,
    /// Where to say what the guest gives back, and the VM's number there.
    report: Option<(Arc<UnixStream>, u64)>,
}

impl Session {
    /// Takes the userfaultfd and the regions the VMM sends as soon as it connects.
    ///
    /// # Errors
    ///
    /// The VMM sends no descriptor, regions that do not parse, or a region that runs past the end
    /// of the memory file.
    pub fn accept(stream: UnixStream, memory: Arc<Memory>) -> io::Result<Self> {
        Self::start(Hello::read(stream)?, memory)
    }

    /// Takes a VM handed over whole, and wakes every thread waiting on its memory. That costs
    /// nothing for a VM no one served yet. For a VM whose server died, it sends each thread whose
    /// fault the old server read and never answered back to fault again, so this one sees it.
    ///
    /// # Errors
    ///
    /// As for [`Session::accept`], or the wake fails.
    pub fn start(hello: Hello, memory: Arc<Memory>) -> io::Result<Self> {
        let Hello { text, uffd, stream, removed } = hello;
        let regions = Region::parse(&text)?;
        if let Some(r) = regions.iter().find(|r| r.offset.saturating_add(r.size) > memory.len()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("region {r:?} runs past the {} byte memory file", memory.len()),
            ));
        }
        let uffd = Uffd::from(uffd);
        for r in &regions {
            uffd.wake(r.base, r.size)?;
        }
        Ok(Self {
            uffd,
            stream,
            regions,
            removed,
            pending: VecDeque::new(),
            trace: Vec::new(),
            stats: Stats::default(),
            map_in: memory.in_memory(),
            memory,
            report: None,
        })
    }

    /// Sends a [`Report::Removed`] for VM `id` to `to` each time the guest gives memory back, so
    /// whoever hands the VM to the next server can tell it.
    pub fn report_to(&mut self, to: Arc<UnixStream>, id: u64) {
        self.report = Some((to, id));
    }

    /// The regions the VMM sent.
    #[must_use]
    pub fn regions(&self) -> &[Region] {
        &self.regions
    }

    /// What the session has done so far.
    #[must_use]
    pub fn stats(&self) -> Stats {
        self.stats
    }

    /// The pages faulted in so far, in the order the VM first touched them. Pages filled by a
    /// prefetch are not in it, since the VM never asked for them.
    #[must_use]
    pub fn trace(&self) -> Trace {
        Trace(self.trace.clone())
    }

    /// Fills in the pages in `trace` before the VM asks for them. Neighbouring pages go in one
    /// copy, or one mapping in minor fault mode, of up to 2 MiB, and faults that come in meanwhile are answered between copies. A VM
    /// that goes away before the prefetch is done ends it, since there is nothing left to fill.
    ///
    /// # Errors
    ///
    /// The kernel refuses a copy for a reason other than the page being there already or the VM's
    /// memory being gone.
    pub fn prefetch(&mut self, trace: &Trace) -> io::Result<()> {
        let mut offsets = trace.0.clone();
        offsets.sort_unstable();
        offsets.dedup();
        let mut i = 0;
        while i < offsets.len() {
            let Some(r) = self.region_at_offset(offsets[i]) else {
                i += 1;
                continue;
            };
            let start = offsets[i] - offsets[i] % r.page;
            let mut end = start + r.page;
            i += 1;
            while i < offsets.len()
                && offsets[i] >= start
                && offsets[i] < end + r.page
                && end + r.page - start <= PREFETCH_RUN
                && offsets[i] + r.page <= r.offset + r.size
            {
                end = (offsets[i] - offsets[i] % r.page + r.page).max(end);
                i += 1;
            }
            match self.fill(r, start, end) {
                Err(e) if gone(&e) => return Ok(()),
                r => r?,
            }
            self.answer_waiting()?;
        }
        Ok(())
    }

    /// Answers faults until the VMM hangs up.
    ///
    /// # Errors
    ///
    /// The userfaultfd or the socket fails, or the kernel refuses a copy.
    pub fn serve(&mut self) -> io::Result<()> {
        loop {
            self.answer_waiting()?;
            let (_, hung_up) = sys::poll2(self.uffd.as_raw_fd(), self.stream.as_raw_fd(), POLL_MS)?;
            if hung_up {
                let mut b = [0u8; 256];
                match self.stream.read(&mut b) {
                    Ok(0) => return Ok(()),
                    Ok(_) => {}
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    Err(e) if e.kind() == io::ErrorKind::ConnectionReset => return Ok(()),
                    Err(e) => return Err(e),
                }
            }
        }
    }

    /// Answers every event already waiting.
    fn answer_waiting(&mut self) -> io::Result<()> {
        while let Some(event) = self.next_event()? {
            match event {
                Event::Fault { addr, minor } => self.fault(addr, minor)?,
                Event::Remove { start, end } => self.remove(start, end),
                Event::Other(_) => {}
            }
        }
        Ok(())
    }

    fn next_event(&mut self) -> io::Result<Option<Event>> {
        match self.pending.pop_front() {
            Some(e) => Ok(Some(e)),
            None => self.uffd.read(),
        }
    }

    fn fault(&mut self, addr: u64, minor: bool) -> io::Result<()> {
        let began = Instant::now();
        let Some(r) = self.regions.iter().copied().find(|r| r.holds(addr)) else {
            // Not guest memory, so nothing the snapshot has. A zero page keeps the thread going.
            self.uffd.zero(addr - addr % 4096, 4096)?;
            self.stats.zero_faults += 1;
            return Ok(());
        };
        let page = addr - (addr - r.base) % r.page;
        if self.removed.iter().any(|&(s, e)| page >= s && page < e) {
            self.retry(|u| u.zero(page, r.page).map(|()| r.page))?;
            self.stats.zero_faults += 1;
        } else {
            let at = r.offset + (page - r.base);
            let memory = self.memory.clone();
            let done = if minor {
                self.retry(|u| u.map_in(page, r.page))?
            } else {
                self.retry(|u| u.copy(page, memory.bytes(at, r.page)))?
            };
            if done == 0 {
                // The page was filled since the fault, by a prefetch or a fault from another
                // thread, so the thread only needs waking.
                self.uffd.wake(page, r.page)?;
                self.stats.raced += 1;
            } else {
                self.stats.faults += 1;
                self.stats.mapped += u64::from(minor);
                self.trace.push(at);
            }
        }
        let took = began.elapsed();
        self.stats.busy += took;
        self.stats.slowest = self.stats.slowest.max(took);
        Ok(())
    }

    fn remove(&mut self, start: u64, end: u64) {
        self.stats.removes += 1;
        if let Some((to, id)) = &self.report
            && let Err(e) = (Report::Removed { id: *id, start, end }).send(to)
        {
            eprintln!("hive-uffd: vm {id}: reporting memory given back: {e}");
        }
        self.removed.push((start, end));
        // Neighbouring and overlapping ranges become one, so a fault checks as few as it can.
        self.removed = hand::merge(std::mem::take(&mut self.removed));
    }

    /// Fills file offsets `start..end` of region `r`, page by page past any that are there.
    fn fill(&mut self, r: Region, start: u64, end: u64) -> io::Result<()> {
        let memory = self.memory.clone();
        let mut at = start;
        while at < end {
            let dst = r.base + (at - r.offset);
            if self.removed.iter().any(|&(s, e)| dst >= s && dst < e) {
                at += r.page;
                continue;
            }
            // A copy stops short of memory the guest gave back, so none of it gets old pages.
            let room = self.removed.iter().filter(|&&(s, _)| s > dst).map(|&(s, _)| s - dst).min();
            let len = room.map_or(end - at, |room| room.min(end - at));
            let done = if self.map_in {
                match self.retry(|u| u.map_in(dst, len)) {
                    // The VM's memory is not registered for minor faults, so its pages are copied.
                    Err(e) if e.raw_os_error() == Some(libc::EINVAL) => {
                        self.map_in = false;
                        continue;
                    }
                    // A hole in the file has no page to map, and the kernel fills it with zeros
                    // when the VM gets there.
                    Err(e) if e.raw_os_error() == Some(libc::EFAULT) => 0,
                    r => r?,
                }
            } else {
                self.retry(|u| u.copy(dst, memory.bytes(at, len)))?
            };
            self.stats.prefetched += done;
            // A short copy stopped at a page that is there already, which is skipped.
            at += if done < len { done + r.page } else { done };
        }
        Ok(())
    }

    /// Runs `op`, and while the kernel says the memory map is changing under it, reads the events
    /// that explain why, keeps them for later, and tries again.
    fn retry(&mut self, mut op: impl FnMut(&Uffd) -> io::Result<u64>) -> io::Result<u64> {
        loop {
            match op(&self.uffd) {
                Err(e) if e.raw_os_error() == Some(libc::EAGAIN) => {
                    while let Some(event) = self.uffd.read()? {
                        match event {
                            Event::Remove { start, end } => self.remove(start, end),
                            other => self.pending.push_back(other),
                        }
                    }
                }
                r => return r,
            }
        }
    }

    fn region_at_offset(&self, offset: u64) -> Option<Region> {
        self.regions.iter().copied().find(|r| offset >= r.offset && offset - r.offset < r.size)
    }
}

/// The side a VMM plays: guest memory mapped with nothing in it, registered with a userfaultfd
/// that goes to the server. Tests and benchmarks use it to restore without a VM.
#[derive(Debug)]
pub struct Guest {
    map: Map,
    _uffd: Uffd,
    _stream: UnixStream,
}

impl Guest {
    /// Maps `len` bytes, registers them, and hands them to the server listening on `socket` as one
    /// region starting at offset 0 of the memory file.
    ///
    /// # Errors
    ///
    /// The kernel does not allow this process a userfaultfd, or the server is not listening.
    pub fn connect(socket: &Path, len: usize) -> io::Result<Self> {
        Self::hand_over(socket, Map::anon(len)?, false)
    }

    /// Maps all of `memory` privately, registers it for minor faults, and hands it to the server
    /// listening on `socket`, the way a VMM restores from a snapshot kept in memory.
    ///
    /// # Errors
    ///
    /// As for [`Guest::connect`], or the kernel has no minor faults for shared memory, which came
    /// in Linux 5.14.
    pub fn connect_mapped(socket: &Path, memory: &Memory) -> io::Result<Self> {
        Self::hand_over(socket, Map::private(memory.file())?, true)
    }

    fn hand_over(socket: &Path, map: Map, minor: bool) -> io::Result<Self> {
        let len = map.len();
        let uffd = Uffd::new()?;
        let features = if minor { sys::UFFD_FEATURE_MINOR_SHMEM } else { 0 };
        uffd.api(sys::UFFD_FEATURE_EVENT_REMOVE | features)?;
        uffd.register(map.addr(), len as u64, minor)?;
        let stream = UnixStream::connect(socket)?;
        let region = Region { base: map.addr(), size: len as u64, offset: 0, page: 4096 };
        let text = serde_json::to_vec(&[region.to_wire()]).map_err(io::Error::other)?;
        sys::send_fd(&stream, &text, uffd.as_raw_fd())?;
        Ok(Self { map, _uffd: uffd, _stream: stream })
    }

    /// The guest's bytes at `at..at + len`, faulting in what is not there yet.
    #[must_use]
    pub fn bytes(&self, at: usize, len: usize) -> &[u8] {
        self.map.bytes(at, len)
    }

    /// Touches the page holding `at` and returns its first byte.
    #[must_use]
    pub fn touch(&self, at: usize) -> u8 {
        self.map.touch(at)
    }

    /// Writes `b` at `at`, as the guest does.
    pub fn write(&mut self, at: usize, b: u8) {
        self.map.poke(at, b);
    }

    /// Gives `at..at + len` back to the kernel, as the balloon does.
    ///
    /// # Errors
    ///
    /// The kernel refuses the `madvise`.
    pub fn discard(&mut self, at: usize, len: usize) -> io::Result<()> {
        self.map.discard(at, len)
    }
}

/// Whether a copy failed because the VM's memory is gone: the kernel finds no registered mapping
/// once the VMM unmaps it, and no process once the VMM exits.
fn gone(e: &io::Error) -> bool {
    matches!(e.raw_os_error(), Some(libc::ENOENT | libc::ESRCH))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixListener;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::mpsc;

    const PAGE: usize = 4096;

    /// A scratch directory, removed at the end.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new() -> Self {
            static N: AtomicU64 = AtomicU64::new(0);
            let n = N.fetch_add(1, Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!("hive-uffd-{}-{n}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Whether this process may make a userfaultfd at all.
    fn allowed() -> bool {
        match Uffd::new() {
            Ok(_) => true,
            Err(e) => {
                eprintln!("skipped: no userfaultfd for this process: {e}");
                false
            }
        }
    }

    /// A memory file whose every page is filled with the low byte of its number, plus one.
    fn memory(dir: &Path, pages: usize) -> Arc<Memory> {
        let path = dir.join("mem");
        let data: Vec<u8> = (0..pages).flat_map(|p| [(p % 255 + 1) as u8; PAGE]).collect();
        std::fs::write(&path, data).unwrap();
        Arc::new(Memory::open(&path).unwrap())
    }

    /// The memory file of [`memory`] copied into memory, or `None` when the kernel has no minor
    /// faults for shared memory.
    fn in_memory(dir: &Path, pages: usize) -> Option<Arc<Memory>> {
        memory(dir, pages);
        let m = Memory::load(&dir.join("mem")).unwrap();
        assert!(m.in_memory());
        match Uffd::new().unwrap().api(sys::UFFD_FEATURE_MINOR_SHMEM) {
            Ok(()) => Some(Arc::new(m)),
            Err(e) => {
                eprintln!("skipped: no minor faults on shared memory: {e}");
                None
            }
        }
    }

    fn expect(p: usize) -> u8 {
        (p % 255 + 1) as u8
    }

    /// Serves one VM on a socket in `dir`, prefetching `trace` first. The channel says when the
    /// prefetch is done, and the thread returns the session once the guest hangs up.
    fn server(
        dir: &Path,
        memory: Arc<Memory>,
        trace: Option<Trace>,
    ) -> (PathBuf, mpsc::Receiver<()>, std::thread::JoinHandle<Session>) {
        let sock = dir.join("uffd.sock");
        let _ = std::fs::remove_file(&sock);
        let listener = UnixListener::bind(&sock).unwrap();
        let (tx, rx) = mpsc::channel();
        let handle = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut s = Session::accept(stream, memory).unwrap();
            if let Some(t) = &trace {
                s.prefetch(t).unwrap();
            }
            tx.send(()).unwrap();
            s.serve().unwrap();
            s
        });
        (sock, rx, handle)
    }

    #[test]
    fn regions_parse_in_both_firecracker_forms() {
        let new = br#"[{"base_host_virt_addr":4096,"size":8192,"offset":0,"page_size":4096}]"#;
        let old =
            br#"[{"base_host_virt_addr":0,"size":4194304,"offset":2097152,"page_size_kib":2048}]"#;
        assert_eq!(
            Region::parse(new).unwrap(),
            [Region { base: 4096, size: 8192, offset: 0, page: 4096 }]
        );
        assert_eq!(Region::parse(old).unwrap()[0].page, 2 << 20);
        let crooked = br#"[{"base_host_virt_addr":4095,"size":8192,"offset":0,"page_size":4096}]"#;
        assert!(Region::parse(crooked).is_err());
        assert!(Region::parse(b"{}").is_err());
    }

    #[test]
    fn a_trace_round_trips() {
        let dir = Scratch::new();
        let t = Trace(vec![0, 4096, 1 << 40, 12288]);
        t.save(&dir.0.join("t")).unwrap();
        assert_eq!(Trace::load(&dir.0.join("t")).unwrap(), t);
        std::fs::write(dir.0.join("bad"), [1, 2, 3]).unwrap();
        assert!(Trace::load(&dir.0.join("bad")).is_err());
    }

    #[test]
    fn faults_are_answered_from_the_memory_file() {
        if !allowed() {
            return;
        }
        let dir = Scratch::new();
        let pages = 2048;
        let (sock, ready, handle) = server(&dir.0, memory(&dir.0, pages), None);
        let guest = Guest::connect(&sock, pages * PAGE).unwrap();
        ready.recv().unwrap();
        let touched: Vec<usize> = (0..pages).step_by(7).collect();
        for &p in &touched {
            let b = guest.bytes(p * PAGE, PAGE);
            assert!(b.iter().all(|&x| x == expect(p)), "page {p}");
        }
        drop(guest);
        let s = handle.join().unwrap();
        assert_eq!(s.stats().faults, touched.len() as u64);
        let order: Vec<u64> = touched.iter().map(|&p| (p * PAGE) as u64).collect();
        assert_eq!(s.trace().0, order);
    }

    #[test]
    fn a_trace_fills_the_pages_before_the_vm_asks() {
        if !allowed() {
            return;
        }
        let dir = Scratch::new();
        let pages = 4096;
        let mem = memory(&dir.0, pages);
        // Runs of neighbours and lone pages, the way a guest's working set looks.
        let touched: Vec<usize> = (0..pages).filter(|p| p % 64 < 9 || p % 101 == 0).collect();
        let trace = Trace(touched.iter().rev().map(|&p| (p * PAGE) as u64).collect());
        let (sock, ready, handle) = server(&dir.0, mem, Some(trace));
        let guest = Guest::connect(&sock, pages * PAGE).unwrap();
        ready.recv().unwrap();
        for &p in &touched {
            assert_eq!(guest.touch(p * PAGE), expect(p), "page {p}");
        }
        // A page the trace left out still faults in.
        assert_eq!(guest.touch(30 * PAGE), expect(30));
        drop(guest);
        let s = handle.join().unwrap().stats();
        assert_eq!(s.prefetched, (touched.len() * PAGE) as u64);
        assert_eq!(s.faults, 1);
    }

    #[test]
    fn memory_the_guest_gave_back_comes_back_as_zeros() {
        if !allowed() {
            return;
        }
        let dir = Scratch::new();
        let pages = 64;
        let (sock, ready, handle) = server(&dir.0, memory(&dir.0, pages), None);
        let mut guest = Guest::connect(&sock, pages * PAGE).unwrap();
        ready.recv().unwrap();
        assert_eq!(guest.touch(10 * PAGE), expect(10));
        assert_eq!(guest.touch(11 * PAGE), expect(11));
        guest.discard(10 * PAGE, 2 * PAGE).unwrap();
        assert!(guest.bytes(10 * PAGE, 2 * PAGE).iter().all(|&b| b == 0));
        assert_eq!(guest.touch(12 * PAGE), expect(12));
        drop(guest);
        let s = handle.join().unwrap().stats();
        assert_eq!((s.faults, s.zero_faults, s.removes), (3, 2, 1));
    }

    #[test]
    fn minor_faults_map_the_pages_in_and_writes_stay_private() {
        if !allowed() {
            return;
        }
        let dir = Scratch::new();
        let pages = 2048;
        let Some(mem) = in_memory(&dir.0, pages) else { return };
        assert!(!Memory::open(&dir.0.join("mem")).unwrap().in_memory());
        let (sock, ready, handle) = server(&dir.0, mem.clone(), None);
        let mut guest = Guest::connect_mapped(&sock, &mem).unwrap();
        ready.recv().unwrap();
        let touched: Vec<usize> = (0..pages).step_by(5).collect();
        for &p in &touched {
            assert!(guest.bytes(p * PAGE, PAGE).iter().all(|&x| x == expect(p)), "page {p}");
        }
        // A write to a mapped page copies it for this VM alone, and a write to a page not mapped
        // yet maps it in first.
        guest.write(5 * PAGE + 1, 0);
        guest.write(6 * PAGE, 0);
        assert_eq!(guest.bytes(5 * PAGE, 2), [expect(5), 0]);
        assert_eq!(guest.bytes(6 * PAGE, 2), [0, expect(6)]);
        assert_eq!(mem.bytes(5 * PAGE as u64, 2), [expect(5); 2]);
        assert_eq!(mem.bytes(6 * PAGE as u64, 2), [expect(6); 2]);
        drop(guest);
        let s = handle.join().unwrap();
        let n = touched.len() as u64 + 1;
        assert_eq!((s.stats().faults, s.stats().mapped), (n, n));
        assert_eq!(s.trace().0.len() as u64, n);
    }

    #[test]
    fn a_trace_maps_the_pages_in_before_the_vm_asks() {
        if !allowed() {
            return;
        }
        let dir = Scratch::new();
        let pages = 4096;
        let Some(mem) = in_memory(&dir.0, pages) else { return };
        let touched: Vec<usize> = (0..pages).filter(|p| p % 64 < 9 || p % 101 == 0).collect();
        let trace = Trace(touched.iter().rev().map(|&p| (p * PAGE) as u64).collect());
        let (sock, ready, handle) = server(&dir.0, mem.clone(), Some(trace));
        let guest = Guest::connect_mapped(&sock, &mem).unwrap();
        ready.recv().unwrap();
        for &p in &touched {
            assert_eq!(guest.touch(p * PAGE), expect(p), "page {p}");
        }
        assert_eq!(guest.touch(30 * PAGE), expect(30));
        drop(guest);
        let s = handle.join().unwrap().stats();
        assert_eq!(s.prefetched, (touched.len() * PAGE) as u64);
        assert_eq!((s.faults, s.mapped), (1, 1));
    }

    #[test]
    fn mapped_memory_the_guest_gave_back_comes_back_as_zeros() {
        if !allowed() {
            return;
        }
        let dir = Scratch::new();
        let pages = 64;
        let Some(mem) = in_memory(&dir.0, pages) else { return };
        let (sock, ready, handle) = server(&dir.0, mem.clone(), None);
        let mut guest = Guest::connect_mapped(&sock, &mem).unwrap();
        ready.recv().unwrap();
        assert_eq!(guest.touch(10 * PAGE), expect(10));
        assert_eq!(guest.touch(11 * PAGE), expect(11));
        guest.write(12 * PAGE, 7);
        guest.discard(10 * PAGE, 3 * PAGE).unwrap();
        assert!(guest.bytes(10 * PAGE, 3 * PAGE).iter().all(|&b| b == 0));
        assert_eq!(guest.touch(13 * PAGE), expect(13));
        drop(guest);
        let s = handle.join().unwrap().stats();
        assert_eq!((s.faults, s.mapped, s.zero_faults, s.removes), (4, 4, 3, 1));
    }

    #[test]
    fn a_vm_with_anonymous_memory_gets_copies_of_memory_in_memory() {
        if !allowed() {
            return;
        }
        let dir = Scratch::new();
        let pages = 256;
        let Some(mem) = in_memory(&dir.0, pages) else { return };
        let trace = Trace((0..64).map(|p| p * PAGE as u64).collect());
        let (sock, ready, handle) = server(&dir.0, mem, Some(trace));
        let guest = Guest::connect(&sock, pages * PAGE).unwrap();
        ready.recv().unwrap();
        for p in 0..64 {
            assert_eq!(guest.touch(p * PAGE), expect(p), "page {p}");
        }
        assert_eq!(guest.touch(100 * PAGE), expect(100));
        drop(guest);
        let s = handle.join().unwrap().stats();
        assert_eq!(s.prefetched, 64 * PAGE as u64);
        assert_eq!((s.faults, s.mapped), (1, 0));
    }

    #[test]
    fn a_vm_handed_over_is_served_at_the_other_end() {
        if !allowed() {
            return;
        }
        let dir = Scratch::new();
        let mem = memory(&dir.0, 16);
        let sock = dir.0.join("uffd.sock");
        let listener = UnixListener::bind(&sock).unwrap();
        let guest = Guest::connect(&sock, 16 * PAGE).unwrap();
        let hello = Hello::read(listener.accept().unwrap().0).unwrap();
        let (a, b) = hand::pair().unwrap();
        hello.send(&a, 7).unwrap();
        let (id, got) = Hello::receive(&b).unwrap().unwrap();
        assert_eq!((id, &got.text), (7, &hello.text));
        // The sender's copies go, and the ones handed over keep the VM covered.
        drop(hello);
        let mut s = Session::start(got, mem).unwrap();
        let served = std::thread::spawn(move || s.serve().map(|()| s.stats()));
        assert_eq!(guest.touch(3 * PAGE), expect(3));
        drop(guest);
        assert_eq!(served.join().unwrap().unwrap().faults, 1);
        drop(a);
        assert!(Hello::receive(&b).unwrap().is_none());
    }

    #[test]
    fn a_fault_the_last_server_read_and_never_answered_comes_again() {
        if !allowed() {
            return;
        }
        let dir = Scratch::new();
        let mem = memory(&dir.0, 64);
        let sock = dir.0.join("uffd.sock");
        let listener = UnixListener::bind(&sock).unwrap();
        let guest = Arc::new(Guest::connect(&sock, 64 * PAGE).unwrap());
        let hello = Hello::read(listener.accept().unwrap().0).unwrap();
        let toucher = {
            let guest = guest.clone();
            std::thread::spawn(move || guest.touch(9 * PAGE))
        };
        // The first server reads the fault and dies before it answers.
        let first = Uffd::from(hello.uffd.try_clone().unwrap());
        let event = loop {
            match first.read().unwrap() {
                Some(e) => break e,
                None => std::thread::sleep(Duration::from_millis(1)),
            }
        };
        assert!(matches!(event, Event::Fault { minor: false, .. }), "{event:?}");
        drop(first);
        let mut s = Session::start(hello, mem).unwrap();
        let served = std::thread::spawn(move || s.serve().map(|()| s.stats()));
        assert_eq!(toucher.join().unwrap(), expect(9));
        drop(guest);
        assert_eq!(served.join().unwrap().unwrap().faults, 1);
    }

    #[test]
    fn memory_given_back_before_a_hand_over_still_reads_as_zeros() {
        if !allowed() {
            return;
        }
        let dir = Scratch::new();
        let mem = memory(&dir.0, 64);
        let sock = dir.0.join("uffd.sock");
        let listener = UnixListener::bind(&sock).unwrap();
        let mut guest = Guest::connect(&sock, 64 * PAGE).unwrap();
        let mut hello = Hello::read(listener.accept().unwrap().0).unwrap();
        let discard = std::thread::spawn(move || {
            guest.discard(10 * PAGE, 3 * PAGE).unwrap();
            guest
        });
        // The first server reads that the guest gave the pages back, says so, and dies.
        let first = Uffd::from(hello.uffd.try_clone().unwrap());
        let event = loop {
            match first.read().unwrap() {
                Some(e) => break e,
                None => std::thread::sleep(Duration::from_millis(1)),
            }
        };
        let Event::Remove { start, end } = event else { panic!("{event:?}") };
        let (a, b) = hand::pair().unwrap();
        Report::Removed { id: 7, start, end }.send(&a).unwrap();
        drop(first);
        let guest = discard.join().unwrap();
        let Some(Report::Removed { id: 7, start, end }) = Report::receive(&b).unwrap() else {
            panic!("no report")
        };
        hello.gave_back(start, end);
        hello.send(&a, 7).unwrap();
        let (_, got) = Hello::receive(&b).unwrap().unwrap();
        drop(hello);
        let mut s = Session::start(got, mem).unwrap();
        let served = std::thread::spawn(move || s.serve().map(|()| s.stats()));
        assert!(guest.bytes(10 * PAGE, 3 * PAGE).iter().all(|&b| b == 0));
        assert_eq!(guest.touch(13 * PAGE), expect(13));
        drop(guest);
        let st = served.join().unwrap().unwrap();
        assert_eq!((st.faults, st.zero_faults), (1, 3));
    }

    #[test]
    fn a_merged_trace_takes_a_page_once_two_restores_missed_it() {
        let mut m = Merged::new(None, 2);
        assert!(m.trace().is_none());
        assert_eq!(m.add(&Trace(vec![3, 1, 2])), 3);
        assert_eq!(m.add(&Trace(vec![9, 7])), 0);
        assert_eq!(m.add(&Trace(vec![7, 8, 2])), 1);
        assert_eq!(m.add(&Trace(vec![1, 7])), 0);
        assert_eq!(m.trace().unwrap().0, vec![3, 1, 2, 7]);
        let mut again = Merged::new(m.trace().cloned(), 2);
        assert_eq!(again.add(&Trace(vec![7, 8])), 0);
        assert_eq!(again.add(&Trace(vec![8])), 1);
    }

    #[test]
    fn a_region_past_the_file_is_refused() {
        if !allowed() {
            return;
        }
        let dir = Scratch::new();
        let mem = memory(&dir.0, 4);
        let sock = dir.0.join("uffd.sock");
        let listener = UnixListener::bind(&sock).unwrap();
        let handle = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            Session::accept(stream, mem).map(|_| ())
        });
        let _guest = Guest::connect(&sock, 8 * PAGE).unwrap();
        let e = handle.join().unwrap().unwrap_err();
        assert!(e.to_string().contains("runs past"), "{e}");
    }

    #[test]
    fn a_vm_gone_before_the_prefetch_ends_it_quietly() {
        if !allowed() {
            return;
        }
        let dir = Scratch::new();
        let pages = 256;
        let mem = memory(&dir.0, pages);
        let sock = dir.0.join("uffd.sock");
        let listener = UnixListener::bind(&sock).unwrap();
        // The guest says hello and is gone before the server looks, so every copy finds nothing.
        drop(Guest::connect(&sock, pages * PAGE).unwrap());
        let (stream, _) = listener.accept().unwrap();
        let mut s = Session::accept(stream, mem).unwrap();
        s.prefetch(&Trace((0..pages as u64).map(|p| p * PAGE as u64).collect())).unwrap();
        assert_eq!(s.stats().prefetched, 0);
        s.serve().unwrap();
    }
}
