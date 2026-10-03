//! A read only block device served from this process through the kernel's NBD driver.
//!
//! [`Device::attach`] takes a free `/dev/nbdN`, gives the kernel one end of a socket pair, and
//! answers the requests that come down the other end by reading from a [`Source`]. This is the
//! ioctl interface, so there is no handshake: the kernel sends requests straight away and every
//! read becomes one call to [`Source::read_at`], several in flight at once. No timeout is set, so
//! a slow read is waited for rather than failed, which is what a lazily filled layer needs.
//!
//! The `nbd` module must be loaded. Writes are refused, since the device is read only.

use std::collections::BTreeSet;
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;

use futures::future::BoxFuture;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

/// Bytes the device works in.
const BLOCK: u64 = 4096;

const NBD_SET_SOCK: libc::c_ulong = 0xab00;
const NBD_SET_BLKSIZE: libc::c_ulong = 0xab01;
const NBD_DO_IT: libc::c_ulong = 0xab03;
const NBD_CLEAR_SOCK: libc::c_ulong = 0xab04;
const NBD_SET_SIZE_BLOCKS: libc::c_ulong = 0xab07;
const NBD_DISCONNECT: libc::c_ulong = 0xab08;
const NBD_SET_FLAGS: libc::c_ulong = 0xab0a;

const NBD_FLAG_HAS_FLAGS: libc::c_ulong = 1;
const NBD_FLAG_READ_ONLY: libc::c_ulong = 2;

const REQUEST_MAGIC: u32 = 0x2560_9513;
const REPLY_MAGIC: u32 = 0x6744_6698;

const CMD_READ: u16 = 0;
const CMD_WRITE: u16 = 1;
const CMD_DISC: u16 = 2;

/// What a device reads from.
pub trait Source: Send + Sync + 'static {
    /// How many bytes there are. The device is rounded up to whole blocks, and reads past the end
    /// come back as zeros.
    fn size(&self) -> u64;

    /// `len` bytes from `offset`, all within [`Source::size`].
    fn read_at(&self, offset: u64, len: usize) -> BoxFuture<'_, io::Result<Vec<u8>>>;
}

/// A device serving a [`Source`], until it is dropped. Dropping it disconnects the device, and
/// anything still reading it then gets I/O errors, so drop it only after unmounting.
#[derive(Debug)]
pub struct Device {
    path: PathBuf,
    dev: Arc<File>,
    thread: Option<JoinHandle<()>>,
    _claim: Claim,
}

/// Devices this process is setting up or serving.
static CLAIMED: Mutex<BTreeSet<u32>> = Mutex::new(BTreeSet::new());

/// A device this process has taken, given back when it drops, after the device is torn down.
#[derive(Debug)]
struct Claim(u32);

impl Claim {
    fn take(n: u32) -> Option<Self> {
        CLAIMED.lock().unwrap_or_else(PoisonError::into_inner).insert(n).then(|| Self(n))
    }
}

impl Drop for Claim {
    fn drop(&mut self) {
        CLAIMED.lock().unwrap_or_else(PoisonError::into_inner).remove(&self.0);
    }
}

impl Device {
    /// Serves `source` on a free NBD device. It must be called within a tokio runtime, which
    /// answers the device's requests for as long as the device lives.
    ///
    /// # Errors
    ///
    /// The `nbd` module is not loaded, every device is taken, or the kernel refuses the setup.
    pub async fn attach(source: Arc<dyn Source>) -> io::Result<Self> {
        let blocks = source.size().div_ceil(BLOCK).max(1);
        for n in free_devices()? {
            // The kernel only refuses a second socket from another thread, so two devices set up
            // from one thread would land on the same one without this.
            let Some(claim) = Claim::take(n) else { continue };
            let path = PathBuf::from(format!("/dev/nbd{n}"));
            let Ok(dev) = File::options().read(true).write(true).open(&path) else { continue };
            let (ours, theirs) = UnixStream::pair()?;
            let set = |what: &str, cmd: libc::c_ulong, arg: libc::c_ulong| {
                // SAFETY: these NBD ioctls take an integer argument, and the file is open.
                let r = unsafe { libc::ioctl(dev.as_raw_fd(), cmd, arg) };
                if r < 0 {
                    let e = io::Error::last_os_error();
                    return Err(io::Error::new(
                        e.kind(),
                        format!("{}: {what}: {e}", path.display()),
                    ));
                }
                Ok(())
            };
            set("NBD_SET_BLKSIZE", NBD_SET_BLKSIZE, BLOCK as libc::c_ulong)?;
            set("NBD_SET_SIZE_BLOCKS", NBD_SET_SIZE_BLOCKS, blocks as libc::c_ulong)?;
            set("NBD_SET_FLAGS", NBD_SET_FLAGS, NBD_FLAG_HAS_FLAGS | NBD_FLAG_READ_ONLY)?;
            match set("NBD_SET_SOCK", NBD_SET_SOCK, theirs.as_raw_fd() as libc::c_ulong) {
                Ok(()) => {}
                // Another process took it between the look and the ioctl.
                Err(e) if e.raw_os_error() == Some(libc::EBUSY) => continue,
                Err(e) => return Err(e),
            }
            ours.set_nonblocking(true)?;
            let ours = tokio::net::UnixStream::from_std(ours)?;
            tokio::spawn(serve(ours, source));
            let dev = Arc::new(dev);
            let held = dev.clone();
            // NBD_DO_IT runs the device and only returns once it is disconnected.
            let thread = std::thread::Builder::new().name(format!("nbd{n}")).spawn(move || {
                // SAFETY: NBD_DO_IT takes no argument, and `held` keeps the file open.
                unsafe { libc::ioctl(held.as_raw_fd(), NBD_DO_IT) };
                drop(theirs);
            })?;
            let device = Self { path, dev, thread: Some(thread), _claim: claim };
            device.started(n).await?;
            return Ok(device);
        }
        Err(io::Error::other("no free NBD device; is the nbd module loaded?"))
    }

    /// The device, as in `/dev/nbd3`.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Waits for NBD_DO_IT to start the device, which is when the kernel gives it its size. Until
    /// then it reads as empty, and a mount on it would fail.
    async fn started(&self, n: u32) -> io::Result<()> {
        let sys = PathBuf::from(format!("/sys/block/nbd{n}"));
        let t = std::time::Instant::now();
        while t.elapsed() < STARTING {
            let size = std::fs::read_to_string(sys.join("size")).unwrap_or_default();
            if sys.join("pid").exists() && !matches!(size.trim(), "" | "0") {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!("{} did not start", self.path.display()),
        ))
    }
}

/// The longest a device may take to start.
const STARTING: std::time::Duration = std::time::Duration::from_secs(10);

impl Drop for Device {
    fn drop(&mut self) {
        // Clearing the socket shuts it down from the kernel's side, so NBD_DO_IT returns even if
        // the runtime serving it is gone, and the join cannot wait on this thread's own runtime.
        // SAFETY: these take no argument, and the file is open.
        unsafe {
            libc::ioctl(self.dev.as_raw_fd(), NBD_DISCONNECT);
            libc::ioctl(self.dev.as_raw_fd(), NBD_CLEAR_SOCK);
        }
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// NBD devices with no server on them, lowest first: no size and no server pid.
fn free_devices() -> io::Result<Vec<u32>> {
    let mut free: Vec<u32> = std::fs::read_dir("/sys/block")?
        .filter_map(|e| {
            let e = e.ok()?;
            let n = e.file_name().to_str()?.strip_prefix("nbd")?.parse().ok()?;
            let size = std::fs::read_to_string(e.path().join("size")).ok()?;
            (size.trim() == "0" && !e.path().join("pid").exists()).then_some(n)
        })
        .collect();
    free.sort_unstable();
    Ok(free)
}

/// Answers requests until the kernel disconnects or the socket fails.
async fn serve(sock: tokio::net::UnixStream, source: Arc<dyn Source>) {
    let (mut rx, mut tx) = sock.into_split();
    let (send, mut replies) = mpsc::unbounded_channel::<(u64, io::Result<Vec<u8>>)>();
    let writer = tokio::spawn(async move {
        while let Some((handle, got)) = replies.recv().await {
            let (error, data) = match got {
                Ok(data) => (0, data),
                Err(e) => (e.raw_os_error().unwrap_or(libc::EIO), Vec::new()),
            };
            let mut head = [0; 16];
            head[..4].copy_from_slice(&REPLY_MAGIC.to_be_bytes());
            head[4..8].copy_from_slice(&u32::try_from(error).unwrap_or(5).to_be_bytes());
            head[8..].copy_from_slice(&handle.to_ne_bytes());
            if tx.write_all(&head).await.is_err() || tx.write_all(&data).await.is_err() {
                return;
            }
        }
    });
    let mut req = [0; 28];
    while rx.read_exact(&mut req).await.is_ok() {
        let field = |at: usize| u32::from_be_bytes(req[at..at + 4].try_into().unwrap_or_default());
        if field(0) != REQUEST_MAGIC {
            break;
        }
        let kind = (field(4) & 0xffff) as u16;
        let handle = u64::from_ne_bytes(req[8..16].try_into().unwrap_or_default());
        let from = u64::from_be_bytes(req[16..24].try_into().unwrap_or_default());
        let len = field(24) as usize;
        match kind {
            CMD_READ => {
                let (source, send) = (source.clone(), send.clone());
                tokio::spawn(async move {
                    let _ = send.send((handle, read(&*source, from, len).await));
                });
            }
            CMD_WRITE => {
                let mut sink = vec![0; len];
                if rx.read_exact(&mut sink).await.is_err() {
                    break;
                }
                let _ = send.send((handle, Err(io::Error::from_raw_os_error(libc::EPERM))));
            }
            CMD_DISC => break,
            // Flush has nothing to do on a read only device, and trim and the rest are refused.
            3 => {
                let _ = send.send((handle, Ok(Vec::new())));
            }
            _ => {
                let _ = send.send((handle, Err(io::Error::from_raw_os_error(libc::EINVAL))));
            }
        }
    }
    drop(send);
    let _ = writer.await;
}

/// `len` bytes from `from`, with zeros past the end of the source.
async fn read(source: &dyn Source, from: u64, len: usize) -> io::Result<Vec<u8>> {
    let have = usize::try_from(source.size().saturating_sub(from)).unwrap_or(usize::MAX).min(len);
    let mut data = if have == 0 { Vec::new() } else { source.read_at(from, have).await? };
    data.resize(len, 0);
    Ok(data)
}
