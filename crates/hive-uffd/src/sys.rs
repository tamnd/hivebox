//! The userfaultfd calls, the mapping the pages come from, and passing a file descriptor over a
//! Unix socket. Every `unsafe` block in the crate is in this file.

use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;

const UFFD_API: u64 = 0xaa;
const UFFDIO_API: libc::c_ulong = 0xc018_aa3f;
const UFFDIO_REGISTER: libc::c_ulong = 0xc020_aa00;
const UFFDIO_WAKE: libc::c_ulong = 0x8010_aa02;
const UFFDIO_COPY: libc::c_ulong = 0xc028_aa03;
const UFFDIO_ZEROPAGE: libc::c_ulong = 0xc020_aa04;
const UFFDIO_CONTINUE: libc::c_ulong = 0xc020_aa07;
const UFFDIO_REGISTER_MODE_MISSING: u64 = 1;
const UFFDIO_REGISTER_MODE_MINOR: u64 = 1 << 2;
/// Asks the kernel to say when the process gives pages back with `madvise(MADV_DONTNEED)`, which
/// is how the balloon frees guest memory.
pub(crate) const UFFD_FEATURE_EVENT_REMOVE: u64 = 1 << 3;
/// Asks for minor faults on shared memory: a page that is in the page cache but not mapped yet.
pub(crate) const UFFD_FEATURE_MINOR_SHMEM: u64 = 1 << 10;
const UFFD_PAGEFAULT_FLAG_MINOR: u64 = 1 << 2;
const TMPFS_MAGIC: u64 = 0x0102_1994;
const HUGETLBFS_MAGIC: u64 = 0x9584_58f6;
const UFFD_EVENT_PAGEFAULT: u8 = 0x12;
const UFFD_EVENT_REMOVE: u8 = 0x15;
const MSG_SIZE: usize = 32;

#[repr(C)]
struct Api {
    api: u64,
    features: u64,
    ioctls: u64,
}

#[repr(C)]
struct Range {
    start: u64,
    len: u64,
}

#[repr(C)]
struct Register {
    range: Range,
    mode: u64,
    ioctls: u64,
}

#[repr(C)]
struct Copy {
    dst: u64,
    src: u64,
    len: u64,
    mode: u64,
    copy: i64,
}

#[repr(C)]
struct Continue {
    range: Range,
    mode: u64,
    mapped: i64,
}

#[repr(C)]
struct Zeropage {
    range: Range,
    mode: u64,
    zeropage: i64,
}

/// What a userfaultfd has to say.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Event {
    /// A thread touched a page that is not mapped yet and waits for it. A minor fault is on a
    /// page that is in the page cache already, so it only needs mapping.
    Fault { addr: u64, minor: bool },
    /// The process gave back `start..end`, so a later touch there wants a zero page.
    Remove { start: u64, end: u64 },
    /// An event this server does not ask for.
    Other(u8),
}

/// A userfaultfd, from the process whose memory it covers or made here for a test.
#[derive(Debug)]
pub(crate) struct Uffd(OwnedFd);

impl Uffd {
    /// Makes a userfaultfd for this process, non blocking and closed on exec, the way Firecracker
    /// makes its own.
    pub(crate) fn new() -> io::Result<Self> {
        // SAFETY: userfaultfd takes only flags, and a non-negative result is a new descriptor
        // nothing else owns.
        let fd =
            unsafe { libc::syscall(libc::SYS_userfaultfd, libc::O_CLOEXEC | libc::O_NONBLOCK) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let fd = RawFd::try_from(fd).map_err(io::Error::other)?;
        // SAFETY: the descriptor was just made and is owned by nothing else.
        Ok(Self(unsafe { OwnedFd::from_raw_fd(fd) }))
    }

    /// Settles the API with the kernel, asking for `features`.
    pub(crate) fn api(&self, features: u64) -> io::Result<()> {
        let mut api = Api { api: UFFD_API, features, ioctls: 0 };
        self.ioctl(UFFDIO_API, (&raw mut api).cast())
    }

    /// Has faults on pages in `start..start + len` come here: pages missing from anonymous
    /// memory, or with `minor`, pages of shared memory that are in the page cache and not mapped.
    pub(crate) fn register(&self, start: u64, len: u64, minor: bool) -> io::Result<()> {
        let mode = if minor { UFFDIO_REGISTER_MODE_MINOR } else { UFFDIO_REGISTER_MODE_MISSING };
        let mut reg = Register { range: Range { start, len }, mode, ioctls: 0 };
        self.ioctl(UFFDIO_REGISTER, (&raw mut reg).cast())
    }

    /// Fills `dst..dst + len` from `src`, which has to be `len` readable bytes in this process,
    /// and wakes whatever waits there. Returns the bytes filled, which is short of `len` when a
    /// page in the range was already there.
    pub(crate) fn copy(&self, dst: u64, src: &[u8]) -> io::Result<u64> {
        let len = src.len() as u64;
        let mut c = Copy { dst, src: src.as_ptr() as u64, len, mode: 0, copy: 0 };
        match self.ioctl(UFFDIO_COPY, (&raw mut c).cast()) {
            Ok(()) => Ok(len),
            Err(e) if e.raw_os_error() == Some(libc::EEXIST) => {
                Ok(u64::try_from(c.copy).unwrap_or(0))
            }
            Err(e) => Err(e),
        }
    }

    /// Maps the page cache pages behind `dst..dst + len` in, read only in a private mapping so a
    /// write copies the page, and wakes whatever waits there. Returns the bytes mapped, which is
    /// short of `len` when a page in the range was mapped already.
    pub(crate) fn map_in(&self, dst: u64, len: u64) -> io::Result<u64> {
        let mut c = Continue { range: Range { start: dst, len }, mode: 0, mapped: 0 };
        match self.ioctl(UFFDIO_CONTINUE, (&raw mut c).cast()) {
            Ok(()) => Ok(len),
            Err(e) if e.raw_os_error() == Some(libc::EEXIST) => {
                Ok(u64::try_from(c.mapped).unwrap_or(0))
            }
            Err(e) => Err(e),
        }
    }

    /// Maps zero pages over `dst..dst + len` and wakes whatever waits there.
    pub(crate) fn zero(&self, dst: u64, len: u64) -> io::Result<()> {
        let mut z = Zeropage { range: Range { start: dst, len }, mode: 0, zeropage: 0 };
        match self.ioctl(UFFDIO_ZEROPAGE, (&raw mut z).cast()) {
            Err(e) if e.raw_os_error() == Some(libc::EEXIST) => self.wake(dst, len),
            r => r,
        }
    }

    /// Wakes threads waiting on `start..start + len`, for a page that turned out to be there.
    pub(crate) fn wake(&self, start: u64, len: u64) -> io::Result<()> {
        let mut r = Range { start, len };
        self.ioctl(UFFDIO_WAKE, (&raw mut r).cast())
    }

    /// The next event, or `None` when there is none waiting.
    pub(crate) fn read(&self) -> io::Result<Option<Event>> {
        let mut msg = [0u8; MSG_SIZE];
        // SAFETY: `msg` is MSG_SIZE writable bytes, and the descriptor is open.
        let n = unsafe { libc::read(self.0.as_raw_fd(), msg.as_mut_ptr().cast(), MSG_SIZE) };
        if n < 0 {
            let e = io::Error::last_os_error();
            return if e.kind() == io::ErrorKind::WouldBlock { Ok(None) } else { Err(e) };
        }
        if n as usize != MSG_SIZE {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short userfaultfd message"));
        }
        let word = |at: usize| u64::from_ne_bytes(msg[at..at + 8].try_into().unwrap_or_default());
        Ok(Some(match msg[0] {
            // The pagefault arm is flags then address, and the remove arm is start then end,
            // both after the 8 byte header.
            UFFD_EVENT_PAGEFAULT => {
                Event::Fault { addr: word(16), minor: word(8) & UFFD_PAGEFAULT_FLAG_MINOR != 0 }
            }
            UFFD_EVENT_REMOVE => Event::Remove { start: word(8), end: word(16) },
            other => Event::Other(other),
        }))
    }

    fn ioctl(&self, cmd: libc::c_ulong, arg: *mut libc::c_void) -> io::Result<()> {
        // SAFETY: each caller passes the struct its command takes, and the descriptor is open.
        let r = unsafe { libc::ioctl(self.0.as_raw_fd(), cmd, arg) };
        if r < 0 { Err(io::Error::last_os_error()) } else { Ok(()) }
    }
}

impl AsRawFd for Uffd {
    fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }
}

impl From<OwnedFd> for Uffd {
    fn from(fd: OwnedFd) -> Self {
        Self(fd)
    }
}

/// A memory mapping this module made, unmapped on drop.
#[derive(Debug)]
pub(crate) struct Map {
    addr: *mut u8,
    len: usize,
}

// SAFETY: the mapping is plain memory with no thread affinity, and every access through it is
// either a read of a read only file mapping or goes through `&mut self`.
unsafe impl Send for Map {}
// SAFETY: as above, a shared `Map` only hands out reads.
unsafe impl Sync for Map {}

impl Map {
    /// Maps all of `file` read only, shared with the page cache.
    pub(crate) fn file(file: &File) -> io::Result<Self> {
        Self::mmap(len(file)?, libc::PROT_READ, libc::MAP_SHARED, file.as_raw_fd())
    }

    /// Maps all of `file` for reading and writing, private, the way a VMM maps guest memory from a
    /// template: reads see the file and the first write to a page copies it.
    pub(crate) fn private(file: &File) -> io::Result<Self> {
        Self::mmap(
            len(file)?,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_NORESERVE,
            file.as_raw_fd(),
        )
    }

    /// Maps `len` bytes of fresh anonymous memory, the way a VMM maps guest memory.
    pub(crate) fn anon(len: usize) -> io::Result<Self> {
        Self::mmap(
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
            -1,
        )
    }

    fn mmap(len: usize, prot: libc::c_int, flags: libc::c_int, fd: RawFd) -> io::Result<Self> {
        // SAFETY: a fresh mapping at an address the kernel picks overlaps nothing in use.
        let addr = unsafe { libc::mmap(std::ptr::null_mut(), len, prot, flags, fd, 0) };
        if addr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { addr: addr.cast(), len })
    }

    pub(crate) fn addr(&self) -> u64 {
        self.addr as u64
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// The bytes at `at..at + len`.
    ///
    /// # Panics
    ///
    /// The range runs past the end of the mapping.
    pub(crate) fn bytes(&self, at: usize, len: usize) -> &[u8] {
        assert!(at.checked_add(len).is_some_and(|end| end <= self.len), "past the mapping");
        // SAFETY: the range is inside the mapping, which lives as long as `self`.
        unsafe { std::slice::from_raw_parts(self.addr.add(at), len) }
    }

    /// Writes `b` at `at`, which faults its page in if it is not there and copies it if it is
    /// mapped from a file.
    pub(crate) fn poke(&mut self, at: usize, b: u8) {
        assert!(at < self.len, "past the mapping");
        // SAFETY: `at` is inside the mapping, which is writable whenever a `&mut` exists, since
        // only the read only file map is ever shared and it is never borrowed mutably.
        unsafe { std::ptr::write_volatile(self.addr.add(at), b) };
    }

    /// Reads the byte at `at`, which faults its page in if it is not there. The read is volatile,
    /// so the compiler cannot drop a touch whose value goes unused.
    pub(crate) fn touch(&self, at: usize) -> u8 {
        let b = self.bytes(at, 1);
        // SAFETY: `b` is one readable byte.
        unsafe { std::ptr::read_volatile(b.as_ptr()) }
    }

    /// Gives `at..at + len` back to the kernel, as the balloon does with guest pages.
    pub(crate) fn discard(&mut self, at: usize, len: usize) -> io::Result<()> {
        assert!(at.checked_add(len).is_some_and(|end| end <= self.len), "past the mapping");
        // SAFETY: the range is inside the mapping, and nothing borrows it while `self` is
        // borrowed mutably.
        let r = unsafe { libc::madvise(self.addr.add(at).cast(), len, libc::MADV_DONTNEED) };
        if r < 0 { Err(io::Error::last_os_error()) } else { Ok(()) }
    }
}

impl Drop for Map {
    fn drop(&mut self) {
        // SAFETY: the mapping was made by `map` with this length and is not used after this.
        unsafe { libc::munmap(self.addr.cast(), self.len) };
    }
}

fn len(file: &File) -> io::Result<usize> {
    let len = usize::try_from(file.metadata()?.len()).map_err(io::Error::other)?;
    if len == 0 {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "the memory file is empty"));
    }
    Ok(len)
}

/// Whether `file` lives in memory, on tmpfs or hugetlbfs, where its pages can be mapped into a
/// VM in minor fault mode.
pub(crate) fn in_memory(file: &File) -> io::Result<bool> {
    // SAFETY: an all zero statfs is valid, and the kernel fills it for an open descriptor.
    let mut st: libc::statfs = unsafe { std::mem::zeroed() };
    // SAFETY: `st` is a writable statfs, and the descriptor is open.
    if unsafe { libc::fstatfs(file.as_raw_fd(), &raw mut st) } < 0 {
        return Err(io::Error::last_os_error());
    }
    #[allow(clippy::unnecessary_cast)]
    let kind = st.f_type as u64 & 0xffff_ffff;
    Ok(kind == TMPFS_MAGIC || kind == HUGETLBFS_MAGIC)
}

/// Makes an anonymous file in memory, the kind `memfd_create` makes, closed on exec.
pub(crate) fn memfd(name: &str) -> io::Result<File> {
    let name = std::ffi::CString::new(name).map_err(io::Error::other)?;
    // SAFETY: `name` is a C string that outlives the call, and a non-negative result is a new
    // descriptor nothing else owns.
    let fd = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: as above.
    Ok(unsafe { File::from_raw_fd(fd) })
}

/// Sends `data` and the descriptor `fd` in one message, the way Firecracker hands over its
/// userfaultfd.
pub(crate) fn send_fd(stream: &UnixStream, data: &[u8], fd: RawFd) -> io::Result<()> {
    let mut space = [0u8; 64];
    let mut iov = libc::iovec { iov_base: data.as_ptr().cast_mut().cast(), iov_len: data.len() };
    // SAFETY: an all zero msghdr is valid, and every pointer set below outlives the call.
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &raw mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = space.as_mut_ptr().cast();
    // SAFETY: CMSG_SPACE only does arithmetic.
    msg.msg_controllen = unsafe { libc::CMSG_SPACE(size_of::<RawFd>() as u32) } as _;
    // SAFETY: the control buffer is big enough for one descriptor, so the first header is in it.
    unsafe {
        let c = libc::CMSG_FIRSTHDR(&raw const msg);
        (*c).cmsg_level = libc::SOL_SOCKET;
        (*c).cmsg_type = libc::SCM_RIGHTS;
        (*c).cmsg_len = libc::CMSG_LEN(size_of::<RawFd>() as u32) as _;
        std::ptr::write_unaligned(libc::CMSG_DATA(c).cast::<RawFd>(), fd);
    }
    // SAFETY: `msg` points at live buffers, and the socket is open.
    let n = unsafe { libc::sendmsg(stream.as_raw_fd(), &raw const msg, 0) };
    if n < 0 { Err(io::Error::last_os_error()) } else { Ok(()) }
}

/// Reads one message into `buf` and the descriptor sent with it. Returns the length read.
pub(crate) fn recv_fd(stream: &UnixStream, buf: &mut [u8]) -> io::Result<(usize, Option<OwnedFd>)> {
    let mut space = [0u8; 64];
    let mut iov = libc::iovec { iov_base: buf.as_mut_ptr().cast(), iov_len: buf.len() };
    // SAFETY: an all zero msghdr is valid, and every pointer set below outlives the call.
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &raw mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = space.as_mut_ptr().cast();
    msg.msg_controllen = space.len() as _;
    // SAFETY: `msg` points at live buffers, and the socket is open.
    let n = unsafe { libc::recvmsg(stream.as_raw_fd(), &raw mut msg, libc::MSG_CMSG_CLOEXEC) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut fd = None;
    // SAFETY: the kernel filled the control buffer and set its length, so walking it with the
    // CMSG macros stays inside it.
    unsafe {
        let mut c = libc::CMSG_FIRSTHDR(&raw const msg);
        while !c.is_null() {
            if (*c).cmsg_level == libc::SOL_SOCKET && (*c).cmsg_type == libc::SCM_RIGHTS {
                let raw = std::ptr::read_unaligned(libc::CMSG_DATA(c).cast::<RawFd>());
                fd = Some(OwnedFd::from_raw_fd(raw));
            }
            c = libc::CMSG_NXTHDR(&raw const msg, c);
        }
    }
    Ok((n as usize, fd))
}

/// Waits until `a` or `b` can be read or hung up, or `timeout_ms` passes. Returns which of the two
/// are ready.
pub(crate) fn poll2(a: RawFd, b: RawFd, timeout_ms: i32) -> io::Result<(bool, bool)> {
    let mut fds = [
        libc::pollfd { fd: a, events: libc::POLLIN, revents: 0 },
        libc::pollfd { fd: b, events: libc::POLLIN, revents: 0 },
    ];
    // SAFETY: `fds` is two valid pollfd entries.
    let r = unsafe { libc::poll(fds.as_mut_ptr(), 2, timeout_ms) };
    if r < 0 {
        let e = io::Error::last_os_error();
        return if e.kind() == io::ErrorKind::Interrupted { Ok((false, false)) } else { Err(e) };
    }
    Ok((fds[0].revents != 0, fds[1].revents != 0))
}
