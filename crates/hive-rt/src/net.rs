//! Connections.
//!
//! Addresses are strings. `host:port` is TCP and `unix:/path` is a Unix socket. The simulated
//! network accepts any string and keeps everything in memory.

use futures::future::BoxFuture;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

/// A byte stream.
pub trait Io: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T: AsyncRead + AsyncWrite + Send + Unpin> Io for T {}

/// An open connection.
pub type Conn = Box<dyn Io>;

/// A way to reach other services.
pub trait Net: Send + Sync + fmt::Debug {
    /// Connects to `addr`.
    fn connect<'a>(&'a self, addr: &'a str) -> BoxFuture<'a, io::Result<Conn>>;

    /// Listens on `addr`.
    fn bind<'a>(&'a self, addr: &'a str) -> BoxFuture<'a, io::Result<Box<dyn Listener>>>;
}

/// A bound address taking connections.
pub trait Listener: Send + fmt::Debug {
    /// The next connection and the peer's address.
    fn accept(&mut self) -> BoxFuture<'_, io::Result<(Conn, String)>>;

    /// The address it is bound to, with the real port if it was bound to port 0.
    fn local_addr(&self) -> String;
}

/// TCP and Unix sockets through tokio.
#[derive(Clone, Copy, Debug, Default)]
pub struct TokioNet;

impl Net for TokioNet {
    fn connect<'a>(&'a self, addr: &'a str) -> BoxFuture<'a, io::Result<Conn>> {
        Box::pin(async move {
            if let Some(path) = addr.strip_prefix("unix:") {
                return Ok(Box::new(tokio::net::UnixStream::connect(path).await?) as Conn);
            }
            let s = tokio::net::TcpStream::connect(addr).await?;
            // Every protocol on top is request and response, so waiting to fill a segment only
            // adds latency.
            s.set_nodelay(true)?;
            Ok(Box::new(s) as Conn)
        })
    }

    fn bind<'a>(&'a self, addr: &'a str) -> BoxFuture<'a, io::Result<Box<dyn Listener>>> {
        Box::pin(async move {
            if let Some(path) = addr.strip_prefix("unix:") {
                let l = tokio::net::UnixListener::bind(path)?;
                return Ok(Box::new(UnixListener { inner: l, addr: addr.to_string() })
                    as Box<dyn Listener>);
            }
            let l = tokio::net::TcpListener::bind(addr).await?;
            let addr = l.local_addr()?.to_string();
            Ok(Box::new(TcpListener { inner: l, addr }) as Box<dyn Listener>)
        })
    }
}

#[derive(Debug)]
struct TcpListener {
    inner: tokio::net::TcpListener,
    addr: String,
}

impl Listener for TcpListener {
    fn accept(&mut self) -> BoxFuture<'_, io::Result<(Conn, String)>> {
        Box::pin(async move {
            let (s, peer) = self.inner.accept().await?;
            s.set_nodelay(true)?;
            Ok((Box::new(s) as Conn, peer.to_string()))
        })
    }

    fn local_addr(&self) -> String {
        self.addr.clone()
    }
}

#[derive(Debug)]
struct UnixListener {
    inner: tokio::net::UnixListener,
    addr: String,
}

impl Listener for UnixListener {
    fn accept(&mut self) -> BoxFuture<'_, io::Result<(Conn, String)>> {
        Box::pin(async move {
            let (s, _) = self.inner.accept().await?;
            Ok((Box::new(s) as Conn, "unix:".to_string()))
        })
    }

    fn local_addr(&self) -> String {
        self.addr.clone()
    }
}

/// An in-memory network. Clones share it.
#[derive(Clone, Default)]
pub struct SimNet {
    inner: Arc<Mutex<SimInner>>,
    next_peer: Arc<AtomicU64>,
}

#[derive(Default)]
struct SimInner {
    listeners: HashMap<String, mpsc::UnboundedSender<(Conn, String)>>,
    down: HashSet<String>,
}

impl fmt::Debug for SimNet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self.lock();
        f.debug_struct("SimNet")
            .field("listeners", &inner.listeners.len())
            .field("down", &inner.down)
            .finish()
    }
}

/// Bytes buffered each way on a simulated connection.
const SIM_BUFFER: usize = 256 * 1024;

impl SimNet {
    /// An empty network.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, SimInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Takes `addr` down or brings it back. New connections to a down address are refused.
    /// Connections already open stay open, the way a crashed listener behaves.
    pub fn set_down(&self, addr: &str, down: bool) {
        let mut inner = self.lock();
        if down {
            inner.down.insert(addr.to_string());
        } else {
            inner.down.remove(addr);
        }
    }
}

impl Net for SimNet {
    fn connect<'a>(&'a self, addr: &'a str) -> BoxFuture<'a, io::Result<Conn>> {
        Box::pin(async move {
            let tx = {
                let inner = self.lock();
                if inner.down.contains(addr) {
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionRefused,
                        format!("{addr} is down"),
                    ));
                }
                inner.listeners.get(addr).filter(|tx| !tx.is_closed()).cloned()
            };
            let tx = tx.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    format!("nothing listens on {addr}"),
                )
            })?;
            let (ours, theirs) = tokio::io::duplex(SIM_BUFFER);
            let peer = format!("sim:{}", self.next_peer.fetch_add(1, Ordering::Relaxed));
            tx.send((Box::new(theirs), peer)).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    format!("{addr} stopped listening"),
                )
            })?;
            Ok(Box::new(ours) as Conn)
        })
    }

    fn bind<'a>(&'a self, addr: &'a str) -> BoxFuture<'a, io::Result<Box<dyn Listener>>> {
        Box::pin(async move {
            let mut inner = self.lock();
            if inner.listeners.get(addr).is_some_and(|tx| !tx.is_closed()) {
                return Err(io::Error::new(io::ErrorKind::AddrInUse, format!("{addr} is taken")));
            }
            let (tx, rx) = mpsc::unbounded_channel();
            inner.listeners.insert(addr.to_string(), tx);
            Ok(Box::new(SimListener { rx, addr: addr.to_string() }) as Box<dyn Listener>)
        })
    }
}

#[derive(Debug)]
struct SimListener {
    rx: mpsc::UnboundedReceiver<(Conn, String)>,
    addr: String,
}

impl fmt::Debug for dyn Io {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Conn")
    }
}

impl Listener for SimListener {
    fn accept(&mut self) -> BoxFuture<'_, io::Result<(Conn, String)>> {
        Box::pin(async move {
            self.rx
                .recv()
                .await
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "the network is gone"))
        })
    }

    fn local_addr(&self) -> String {
        self.addr.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn ping(net: &dyn Net, bind: &str) {
        let mut l = net.bind(bind).await.unwrap();
        let addr = l.local_addr();
        let server = tokio::spawn(async move {
            let (mut c, _) = l.accept().await.unwrap();
            let mut buf = [0; 4];
            c.read_exact(&mut buf).await.unwrap();
            c.write_all(&buf).await.unwrap();
        });
        let mut c = net.connect(&addr).await.unwrap();
        c.write_all(b"ping").await.unwrap();
        let mut buf = [0; 4];
        c.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn sim_connections_carry_bytes() {
        ping(&SimNet::new(), "comb-1:7000").await;
    }

    #[tokio::test]
    async fn tcp_and_unix_connections_carry_bytes() {
        ping(&TokioNet, "127.0.0.1:0").await;
        let dir = std::env::temp_dir().join(format!("hive-rt-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("s.sock");
        let _ = std::fs::remove_file(&path);
        ping(&TokioNet, &format!("unix:{}", path.display())).await;
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn a_down_address_refuses_and_comes_back() {
        let net = SimNet::new();
        let _l = net.bind("keeper:1").await.unwrap();
        net.set_down("keeper:1", true);
        let err = net.connect("keeper:1").await.err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::ConnectionRefused);
        net.set_down("keeper:1", false);
        assert!(net.connect("keeper:1").await.is_ok());
        assert!(net.connect("nobody:1").await.is_err());
    }

    #[tokio::test]
    async fn an_address_frees_up_when_its_listener_drops() {
        let net = SimNet::new();
        let l = net.bind("gate:443").await.unwrap();
        assert_eq!(net.bind("gate:443").await.err().unwrap().kind(), io::ErrorKind::AddrInUse);
        drop(l);
        assert!(net.connect("gate:443").await.is_err());
        assert!(net.bind("gate:443").await.is_ok());
    }
}
