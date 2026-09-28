use crate::session::Sessions;
use crate::{Config, health, process};
use hive_proto::drone::api::{
    self, Command, RunRequest, SessionCreate, SessionRef, SessionRun, SessionSend,
};
use hive_proto::drone::handshake::{self, Secret, VERSION};
use hive_proto::drone::msg::{Hello, Status};
use hive_proto::drone::{Channel, FrameCodec, Incoming, Side, Stream};
use hive_rt::{OsRng, Rng};
use hive_types::{Error, Reason};
use prost::Message;
use std::io;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

/// The most stdin `process.run` buffers. Bigger input should be streamed.
const MAX_STDIN: usize = 64 << 20;
/// How long a finished call waits for the node to end its side before the stream is dropped.
const LINGER: Duration = Duration::from_secs(5);

/// The guest agent. One per cell, serving any number of connections from the node agent.
#[derive(Debug)]
pub struct Drone {
    cfg: Config,
    // The current secret and the one before it. See `handshake`.
    secrets: Mutex<[Secret; 2]>,
    started: Instant,
    processes: AtomicU32,
    sessions: Sessions,
}

impl Drone {
    /// A drone that expects the node to know `secret`.
    #[must_use]
    pub fn new(cfg: Config, secret: Secret) -> Arc<Self> {
        Arc::new(Self {
            cfg,
            secrets: Mutex::new([secret, secret]),
            started: Instant::now(),
            processes: AtomicU32::new(0),
            sessions: Sessions::default(),
        })
    }

    /// The configuration it runs commands with.
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.cfg
    }

    /// The secret the node should use on its next connection.
    #[must_use]
    pub fn current_secret(&self) -> Secret {
        self.secrets.lock().unwrap_or_else(PoisonError::into_inner)[0]
    }

    /// Runs the handshake on `io` and then serves calls until the node hangs up. Each call runs
    /// on its own task.
    pub async fn serve<T>(self: Arc<Self>, io: T) -> io::Result<()>
    where
        T: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let offered = *self.secrets.lock().unwrap_or_else(PoisonError::into_inner);
        let mut framed = Framed::new(io, FrameCodec);
        let hello = Hello {
            versions: vec![VERSION],
            build: self.cfg.build.clone(),
            caps: Vec::new(),
            nonce: OsRng.secret().to_vec(),
        };
        let est = handshake::guest(&mut framed, &offered, hello).await?;
        *self.secrets.lock().unwrap_or_else(PoisonError::into_inner) =
            [est.next_secret, offered[est.used]];
        let parts = framed.into_parts();
        if !parts.read_buf.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "the node sent frames before the handshake finished",
            ));
        }
        let (_channel, mut incoming) = Channel::start(parts.io, Side::Guest);
        while let Some(call) = incoming.recv().await {
            let drone = self.clone();
            tokio::spawn(async move { drone.dispatch(call).await });
        }
        Ok(())
    }

    async fn dispatch(&self, call: Incoming) {
        let Incoming { open, mut stream } = call;
        let result = match open.method.as_str() {
            api::PROCESS_RUN => {
                let _busy = Busy::new(&self.processes);
                match RunRequest::decode(open.request) {
                    Ok(req) => match read_stdin(req, &mut stream).await {
                        Ok(req) => process::run(&self.cfg, req).await.map(|r| r.encode_to_vec()),
                        Err(e) => Err(e),
                    },
                    Err(e) => Err(bad_request(e)),
                }
            }
            api::PROCESS_START => {
                let _busy = Busy::new(&self.processes);
                let done = match Command::decode(open.request) {
                    Ok(cmd) => process::start(&self.cfg, cmd, &mut stream).await,
                    Err(e) => Err(bad_request(e)),
                };
                match done {
                    Ok(()) => return linger(stream).await,
                    Err(e) => return stream.reset(Status::from(e)).await,
                }
            }
            api::HEALTH => {
                let processes = self.processes.load(Ordering::Relaxed);
                let sessions = self.sessions.len() as u32;
                let uptime = self.started.elapsed();
                Ok(health::read(&self.cfg.build, uptime, processes, sessions).encode_to_vec())
            }
            api::SESSION_CREATE => match SessionCreate::decode(open.request) {
                Ok(spec) => self.sessions.create(&self.cfg, spec).map(|s| s.encode_to_vec()),
                Err(e) => Err(bad_request(e)),
            },
            api::SESSION_RUN => match SessionRun::decode(open.request) {
                Ok(req) => self.sessions.run(&self.cfg, req).await.map(|r| r.encode_to_vec()),
                Err(e) => Err(bad_request(e)),
            },
            api::SESSION_SEND => match SessionSend::decode(open.request) {
                Ok(req) => self.sessions.send(&self.cfg, req).await.map(|r| r.encode_to_vec()),
                Err(e) => Err(bad_request(e)),
            },
            api::SESSION_CLOSE => match SessionRef::decode(open.request) {
                Ok(r) => self.sessions.close(&r.id).map(|()| Vec::new()),
                Err(e) => Err(bad_request(e)),
            },
            other => Err(Error::new(Reason::InvalidArgument, format!("no method {other:?}"))),
        };
        match result {
            Ok(answer) => {
                if stream.send_last(answer.into()).await.is_ok() {
                    linger(stream).await;
                }
            }
            Err(e) => stream.reset(Status::from(e)).await,
        }
    }
}

// Appends the stdin that followed the request as data, up to the end of the stream.
async fn read_stdin(mut req: RunRequest, stream: &mut Stream) -> Result<RunRequest, Error> {
    let mut more = Vec::new();
    while let Some(chunk) = stream.recv().await? {
        if req.stdin.len() + more.len() + chunk.len() > MAX_STDIN {
            return Err(Error::new(
                Reason::InvalidArgument,
                format!("stdin over {} MiB, use process.start to stream it", MAX_STDIN >> 20),
            ));
        }
        more.extend_from_slice(&chunk);
    }
    if !more.is_empty() {
        let mut all = req.stdin.to_vec();
        all.extend_from_slice(&more);
        req.stdin = all.into();
    }
    Ok(req)
}

// Waits for the node to end its side, so the stream closes cleanly instead of with a reset.
async fn linger(mut stream: Stream) {
    let _ = tokio::time::timeout(LINGER, async { while let Ok(Some(_)) = stream.recv().await {} })
        .await;
}

fn bad_request(e: prost::DecodeError) -> Error {
    Error::new(Reason::InvalidArgument, format!("a request that does not decode: {e}"))
}

struct Busy<'a>(&'a AtomicU32);

impl<'a> Busy<'a> {
    fn new(n: &'a AtomicU32) -> Self {
        n.fetch_add(1, Ordering::Relaxed);
        Self(n)
    }
}

impl Drop for Busy<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}
