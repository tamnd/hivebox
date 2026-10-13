//! vsock sockets, for a drone inside a virtual machine and for the node side that reaches it.
//!
//! A drone in a QEMU guest listens on a vsock port, and the comb connects to the guest's context
//! id and that port through the host's vhost-vsock device. tokio has no vsock type, so these keep
//! the socket in an [`AsyncFd`] and read and write it directly. Only binding and connecting take
//! a `sockaddr_vm`, which is the one unsafe part.

use rustix::net::{AddressFamily, Shutdown, SocketFlags, SocketType};
use std::io;
use std::os::fd::{AsRawFd, OwnedFd};
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// The context id a listener binds to so it takes connections to any of its own, the kernel's
/// `VMADDR_CID_ANY`.
const CID_ANY: u32 = u32::MAX;

/// How many connections may wait to be accepted.
const BACKLOG: i32 = 64;

/// A connected vsock stream.
#[derive(Debug)]
pub struct VsockStream {
    fd: AsyncFd<OwnedFd>,
}

/// A vsock socket that takes connections, made from [`bind`].
#[derive(Debug)]
pub struct VsockListener {
    fd: AsyncFd<OwnedFd>,
}

/// Binds a vsock socket to `port` on any of this machine's context ids and listens on it. This
/// needs no runtime, so it can run before one starts, and [`VsockListener::new`] takes the
/// result.
///
/// # Errors
///
/// When the kernel has no vsock, or the port is taken.
pub fn bind(port: u32) -> io::Result<OwnedFd> {
    let fd = socket()?;
    let addr = sockaddr(CID_ANY, port);
    // SAFETY: `addr` is a whole sockaddr_vm that lives across the call, the length is its size,
    // and `fd` is an open socket this function owns.
    let r = unsafe { libc::bind(fd.as_raw_fd(), (&raw const addr).cast(), ADDR_LEN) };
    if r != 0 {
        return Err(io::Error::last_os_error());
    }
    rustix::net::listen(&fd, BACKLOG)?;
    Ok(fd)
}

impl VsockListener {
    /// Takes a socket from [`bind`]. This has to run inside a tokio runtime.
    ///
    /// # Errors
    ///
    /// When the runtime cannot watch the socket.
    pub fn new(fd: OwnedFd) -> io::Result<Self> {
        Ok(Self { fd: AsyncFd::new(fd)? })
    }

    /// Waits for the next connection.
    ///
    /// # Errors
    ///
    /// When accepting fails for a reason other than no connection waiting yet.
    pub async fn accept(&self) -> io::Result<VsockStream> {
        loop {
            let mut ready = self.fd.readable().await?;
            match ready.try_io(|fd| Ok(rustix::net::accept_with(fd.get_ref(), FLAGS)?)) {
                Ok(conn) => return VsockStream::new(conn?),
                Err(_would_block) => {}
            }
        }
    }
}

impl VsockStream {
    /// Connects to `port` on the machine with context id `cid`.
    ///
    /// # Errors
    ///
    /// When the kernel has no vsock, nothing listens there, or the guest is gone.
    pub async fn connect(cid: u32, port: u32) -> io::Result<Self> {
        let fd = socket()?;
        let addr = sockaddr(cid, port);
        // SAFETY: as in `bind`, a whole sockaddr_vm, its size, and an open socket owned here.
        let r = unsafe { libc::connect(fd.as_raw_fd(), (&raw const addr).cast(), ADDR_LEN) };
        if r != 0 {
            let e = io::Error::last_os_error();
            if e.raw_os_error() != Some(libc::EINPROGRESS) {
                return Err(e);
            }
        }
        let fd = AsyncFd::new(fd)?;
        // The socket turns writable once the connect is through either way, and the socket's
        // error says which way.
        drop(fd.writable().await?);
        rustix::net::sockopt::socket_error(fd.get_ref())??;
        Ok(Self { fd })
    }

    fn new(fd: OwnedFd) -> io::Result<Self> {
        Ok(Self { fd: AsyncFd::new(fd)? })
    }
}

const FLAGS: SocketFlags = SocketFlags::NONBLOCK.union(SocketFlags::CLOEXEC);

#[allow(clippy::cast_possible_truncation)]
const ADDR_LEN: libc::socklen_t = size_of::<libc::sockaddr_vm>() as libc::socklen_t;

fn socket() -> io::Result<OwnedFd> {
    Ok(rustix::net::socket_with(AddressFamily::VSOCK, SocketType::STREAM, FLAGS, None)?)
}

fn sockaddr(cid: u32, port: u32) -> libc::sockaddr_vm {
    // SAFETY: sockaddr_vm is only integers and a byte array, for which all zeroes is valid, and
    // the reserved fields have to be zero.
    let mut addr: libc::sockaddr_vm = unsafe { std::mem::zeroed() };
    addr.svm_family = libc::AF_VSOCK as libc::sa_family_t;
    addr.svm_cid = cid;
    addr.svm_port = port;
    addr
}

impl AsyncRead for VsockStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            let mut ready = ready!(self.fd.poll_read_ready(cx))?;
            let unfilled = buf.initialize_unfilled();
            match ready.try_io(|fd| Ok(rustix::io::read(fd.get_ref(), &mut *unfilled)?)) {
                Ok(Ok(n)) => {
                    buf.advance(n);
                    return Poll::Ready(Ok(()));
                }
                Ok(Err(e)) => return Poll::Ready(Err(e)),
                Err(_would_block) => {}
            }
        }
    }
}

impl AsyncWrite for VsockStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            let mut ready = ready!(self.fd.poll_write_ready(cx))?;
            match ready.try_io(|fd| Ok(rustix::io::write(fd.get_ref(), buf)?)) {
                Ok(r) => return Poll::Ready(r),
                Err(_would_block) => {}
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(rustix::net::shutdown(self.fd.get_ref(), Shutdown::Write).map_err(Into::into))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// The loopback context id, which needs the `vsock_loopback` module.
    const CID_LOCAL: u32 = 1;

    #[tokio::test]
    async fn round_trip_over_loopback() {
        let port = 0x4869_0000 | (std::process::id() & 0xffff);
        let fd = match bind(port) {
            Ok(fd) => fd,
            Err(e) => {
                eprintln!("skipped, no vsock here: {e}");
                return;
            }
        };
        let listener = VsockListener::new(fd).unwrap();
        let server = tokio::spawn(async move {
            let mut conn = listener.accept().await.unwrap();
            let mut got = vec![0u8; 5];
            conn.read_exact(&mut got).await.unwrap();
            conn.write_all(&got).await.unwrap();
            conn.shutdown().await.unwrap();
        });
        let mut c = match VsockStream::connect(CID_LOCAL, port).await {
            Ok(c) => c,
            Err(e) if e.raw_os_error() == Some(libc::ENODEV) => {
                eprintln!("skipped, no vsock loopback here: {e}");
                server.abort();
                return;
            }
            Err(e) => panic!("connect: {e}"),
        };
        c.write_all(b"hello").await.unwrap();
        let mut back = Vec::new();
        c.read_to_end(&mut back).await.unwrap();
        assert_eq!(back, b"hello");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn nobody_listening_is_an_error() {
        if bind(0x4869_ffff).is_err() {
            return;
        }
        // A context id no machine has.
        let r = VsockStream::connect(0x7fff_fff0, 1).await;
        assert!(r.is_err());
    }
}
