//! Many calls over one socket, from `spec/09_guest_agent.md`, section 2.
//!
//! A [`Channel`] owns a connection after the handshake. Either end can open a stream, which is
//! one call with a request, any number of data frames each way, and an end. The node opens odd
//! stream ids and the guest opens even ones, so the two never collide.
//!
//! Flow control is per stream and counted in bytes. Each side starts with [`WINDOW`] bytes of
//! credit toward the other and may not send past it. The receiver hands credit back as the
//! application reads, so a slow reader stops a fast writer instead of growing a buffer. A peer
//! that sends past its credit is broken or hostile, and the channel is closed.
//!
//! Two tasks run per channel: a reader that routes incoming frames to streams, and a writer that
//! batches outgoing frames and flushes when there is nothing more queued.

use super::frame::{FLAG_END, Frame, FrameCodec, Kind, MAX_PAYLOAD};
use super::msg::{Open, Status};
use bytes::{Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use hive_types::{Error, Reason};
use prost::Message;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Semaphore, mpsc};
use tokio_util::codec::{FramedRead, FramedWrite};

/// The credit each side starts with on a new stream.
pub const WINDOW: u32 = 256 * 1024;
/// Most streams open at once on one channel.
pub const MAX_STREAMS: usize = 1024;
// The most credit a stream may hold toward the peer.
const MAX_CREDIT: usize = 4 * WINDOW as usize;

/// Which end of the channel this is. It decides the parity of stream ids.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    /// The node agent. Opens odd ids.
    Node,
    /// The guest agent. Opens even ids.
    Guest,
}

/// One end of a multiplexed channel.
#[derive(Clone, Debug)]
pub struct Channel {
    shared: Arc<Shared>,
}

/// A stream the other side opened, waiting to be served.
#[derive(Debug)]
pub struct Incoming {
    /// The method and request.
    pub open: Open,
    /// The stream to answer on.
    pub stream: Stream,
}

#[derive(Debug)]
struct Shared {
    out: mpsc::Sender<Frame>,
    streams: Mutex<HashMap<u32, Slot>>,
    next_id: AtomicU32,
    closed: AtomicBool,
}

#[derive(Debug)]
struct Slot {
    events: mpsc::UnboundedSender<Event>,
    credit: Arc<Semaphore>,
    // Bytes the peer may still send before it needs more credit from us.
    peer_allowance: u32,
}

#[derive(Debug)]
enum Event {
    Data(Bytes),
    End,
    Reset(Error),
}

impl Shared {
    // Nothing panics while holding the lock, and a map is fine to use even if something did.
    fn table(&self) -> MutexGuard<'_, HashMap<u32, Slot>> {
        self.streams.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Channel {
    /// Starts the reader and writer tasks on `io` and returns the channel with the queue of
    /// streams the peer opens. `io` must already have completed the handshake.
    pub fn start<T>(io: T, side: Side) -> (Self, mpsc::Receiver<Incoming>)
    where
        T: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (read, write) = tokio::io::split(io);
        let (out_tx, out_rx) = mpsc::channel(256);
        let (in_tx, in_rx) = mpsc::channel(64);
        let shared = Arc::new(Shared {
            out: out_tx,
            streams: Mutex::new(HashMap::new()),
            next_id: AtomicU32::new(if side == Side::Node { 1 } else { 2 }),
            closed: AtomicBool::new(false),
        });
        tokio::spawn(write_loop(FramedWrite::new(write, FrameCodec), out_rx));
        tokio::spawn(read_loop(FramedRead::new(read, FrameCodec), shared.clone(), in_tx));
        (Self { shared }, in_rx)
    }

    /// Whether the connection underneath has gone.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.shared.closed.load(Ordering::Acquire)
    }

    /// Opens a stream calling `method` with `request`. The request must fit in one frame with
    /// the method name. Anything bigger goes as data on the stream.
    pub async fn open(&self, method: &str, request: Bytes) -> Result<Stream, Error> {
        let open = Open { method: method.to_string(), request };
        if open.encoded_len() > MAX_PAYLOAD {
            return Err(Error::new(
                Reason::InvalidArgument,
                format!("a request of {} bytes does not fit in one frame", open.encoded_len()),
            ));
        }
        let (id, stream) = {
            let mut streams = self.shared.table();
            if self.is_closed() {
                return Err(closed());
            }
            if streams.len() >= MAX_STREAMS {
                return Err(Error::new(Reason::CapacityUnavailable, "too many open streams"));
            }
            let mut id = self.shared.next_id.fetch_add(2, Ordering::Relaxed);
            while id == 0 || streams.contains_key(&id) {
                id = self.shared.next_id.fetch_add(2, Ordering::Relaxed);
            }
            (id, register(&self.shared, &mut streams, id))
        };
        send(&self.shared, Frame::new(id, Kind::Open, open.encode_to_vec())).await?;
        Ok(stream)
    }

    /// Opens a stream, sends `request`, and collects everything the peer sends back. For calls
    /// with small answers. Anything that streams should use [`Channel::open`].
    pub async fn call(&self, method: &str, request: Bytes, limit: usize) -> Result<Bytes, Error> {
        let mut s = self.open(method, request).await?;
        s.finish().await?;
        let mut out = BytesMut::new();
        while let Some(chunk) = s.recv().await? {
            if out.len() + chunk.len() > limit {
                return Err(Error::new(Reason::OutputLimit, "the answer is over the limit"));
            }
            out.extend_from_slice(&chunk);
        }
        Ok(out.freeze())
    }
}

fn register(shared: &Arc<Shared>, streams: &mut HashMap<u32, Slot>, id: u32) -> Stream {
    let (tx, rx) = mpsc::unbounded_channel();
    let credit = Arc::new(Semaphore::new(WINDOW as usize));
    streams.insert(id, Slot { events: tx, credit: credit.clone(), peer_allowance: WINDOW });
    Stream {
        id,
        shared: shared.clone(),
        events: rx,
        credit,
        unacked: 0,
        sent_end: false,
        got_end: false,
        reset: false,
    }
}

/// One call on a channel. Dropping a stream that has not ended both ways resets it.
#[derive(Debug)]
pub struct Stream {
    id: u32,
    shared: Arc<Shared>,
    events: mpsc::UnboundedReceiver<Event>,
    credit: Arc<Semaphore>,
    // Bytes read by the application that have not been handed back to the peer as credit.
    unacked: u32,
    sent_end: bool,
    got_end: bool,
    reset: bool,
}

impl Stream {
    /// The stream id.
    #[must_use]
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Sends `data`, waiting for credit as needed.
    pub async fn send(&mut self, mut data: Bytes) -> Result<(), Error> {
        if self.sent_end {
            return Err(Error::new(Reason::Internal, "send after finish"));
        }
        while !data.is_empty() {
            let n = data.len().min(MAX_PAYLOAD);
            let permits = self.credit.acquire_many(n as u32).await.map_err(|_| closed())?;
            permits.forget();
            send(&self.shared, Frame::new(self.id, Kind::Data, data.split_to(n))).await?;
        }
        Ok(())
    }

    /// Sends the end of this side of the stream. The peer can still send.
    pub async fn finish(&mut self) -> Result<(), Error> {
        if self.sent_end {
            return Ok(());
        }
        self.sent_end = true;
        let frame =
            Frame { stream: self.id, kind: Kind::Data, flags: FLAG_END, payload: Bytes::new() };
        send(&self.shared, frame).await
    }

    /// Sends `data` and the end in one frame where it fits.
    pub async fn send_last(&mut self, data: Bytes) -> Result<(), Error> {
        if data.len() > MAX_PAYLOAD {
            self.send(data).await?;
            return self.finish().await;
        }
        let permits = self.credit.acquire_many(data.len() as u32).await.map_err(|_| closed())?;
        permits.forget();
        self.sent_end = true;
        send(
            &self.shared,
            Frame { stream: self.id, kind: Kind::Data, flags: FLAG_END, payload: data },
        )
        .await
    }

    /// The next piece of data, `None` once the peer has finished, or the error it reset with.
    pub async fn recv(&mut self) -> Result<Option<Bytes>, Error> {
        if self.got_end {
            return Ok(None);
        }
        match self.events.recv().await {
            Some(Event::Data(b)) => {
                self.unacked += b.len() as u32;
                if self.unacked >= WINDOW / 2 {
                    let n = std::mem::take(&mut self.unacked);
                    self.grant(n).await?;
                }
                Ok(Some(b))
            }
            Some(Event::End) => {
                self.got_end = true;
                Ok(None)
            }
            Some(Event::Reset(e)) => {
                self.reset = true;
                Err(e)
            }
            None => {
                self.reset = true;
                Err(closed())
            }
        }
    }

    /// Ends the stream both ways with `status`.
    pub async fn reset(mut self, status: Status) {
        self.reset = true;
        forget(&self.shared, self.id);
        let _ = send(&self.shared, Frame::new(self.id, Kind::Reset, status.encode_to_vec())).await;
    }

    async fn grant(&self, n: u32) -> Result<(), Error> {
        {
            let mut streams = self.shared.table();
            if let Some(slot) = streams.get_mut(&self.id) {
                slot.peer_allowance += n;
            }
        }
        send(&self.shared, Frame::new(self.id, Kind::Credit, n.to_be_bytes().to_vec())).await
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        forget(&self.shared, self.id);
        if !self.reset && !(self.sent_end && self.got_end) {
            let status = Status { reason: String::new(), message: "dropped".into() };
            let _ =
                self.shared.out.try_send(Frame::new(self.id, Kind::Reset, status.encode_to_vec()));
        }
    }
}

fn forget(shared: &Shared, id: u32) {
    shared.table().remove(&id);
}

async fn send(shared: &Shared, frame: Frame) -> Result<(), Error> {
    shared.out.send(frame).await.map_err(|_| closed())
}

fn closed() -> Error {
    Error::new(Reason::DroneUnreachable, "the drone channel is closed")
}

async fn write_loop<W>(mut sink: FramedWrite<W, FrameCodec>, mut rx: mpsc::Receiver<Frame>)
where
    W: AsyncWrite + Unpin,
{
    while let Some(frame) = rx.recv().await {
        if sink.feed(frame).await.is_err() {
            return;
        }
        // Batch whatever else is already queued into the same flush.
        while let Ok(frame) = rx.try_recv() {
            if sink.feed(frame).await.is_err() {
                return;
            }
        }
        if sink.flush().await.is_err() {
            return;
        }
    }
}

async fn read_loop<R>(
    mut source: FramedRead<R, FrameCodec>,
    shared: Arc<Shared>,
    incoming: mpsc::Sender<Incoming>,
) where
    R: AsyncRead + Unpin,
{
    let why = loop {
        let frame = match source.next().await {
            Some(Ok(f)) => f,
            Some(Err(e)) => break format!("read failed: {e}"),
            None => break "the peer hung up".to_string(),
        };
        if let Err(why) = route(&shared, &incoming, frame).await {
            break why;
        }
    };
    shared.closed.store(true, Ordering::Release);
    let slots: Vec<Slot> = shared.table().drain().map(|(_, s)| s).collect();
    for slot in slots {
        let _ = slot.events.send(Event::Reset(Error::new(Reason::DroneUnreachable, why.clone())));
        slot.credit.close();
    }
}

async fn route(
    shared: &Arc<Shared>,
    incoming: &mpsc::Sender<Incoming>,
    frame: Frame,
) -> Result<(), String> {
    let id = frame.stream;
    match frame.kind {
        Kind::Ping if id == 0 => {
            let _ = shared.out.send(Frame::new(0, Kind::Pong, frame.payload)).await;
        }
        Kind::Pong if id == 0 => {}
        _ if id == 0 => return Err(format!("{:?} on stream 0", frame.kind)),
        Kind::Open => {
            let open = Open::decode(frame.payload).map_err(|e| format!("bad open: {e}"))?;
            let stream = {
                let mut streams = shared.table();
                if streams.contains_key(&id) {
                    return Err(format!("stream {id} opened twice"));
                }
                if streams.len() >= MAX_STREAMS {
                    None
                } else {
                    Some(register(shared, &mut streams, id))
                }
            };
            match stream {
                Some(stream) => {
                    if incoming.send(Incoming { open, stream }).await.is_err() {
                        // Nobody is accepting. The stream was dropped with the message, which
                        // resets it.
                    }
                }
                None => {
                    let status =
                        Status::error(Reason::CapacityUnavailable, "too many open streams");
                    let _ =
                        shared.out.send(Frame::new(id, Kind::Reset, status.encode_to_vec())).await;
                }
            }
        }
        Kind::Data => {
            let mut streams = shared.table();
            // Data for a stream we already forgot is a race with a reset, not an error.
            if let Some(slot) = streams.get_mut(&id) {
                let n = frame.payload.len() as u32;
                if n > slot.peer_allowance {
                    return Err(format!("stream {id} sent past its credit"));
                }
                slot.peer_allowance -= n;
                if !frame.payload.is_empty() {
                    let _ = slot.events.send(Event::Data(frame.payload.clone()));
                }
                if frame.is_end() {
                    let _ = slot.events.send(Event::End);
                }
            }
        }
        Kind::Credit => {
            let n: [u8; 4] = frame.payload[..]
                .try_into()
                .map_err(|_| "a credit frame that is not 4 bytes".to_string())?;
            let n = u32::from_be_bytes(n) as usize;
            let streams = shared.table();
            if let Some(slot) = streams.get(&id) {
                // An honest peer never grants more than it was sent, so this bound only stops a
                // hostile one from overflowing the semaphore.
                if slot.credit.available_permits() + n > MAX_CREDIT {
                    return Err(format!("stream {id} was granted more credit than it can use"));
                }
                slot.credit.add_permits(n);
            }
        }
        Kind::Reset => {
            let status = Status::decode(frame.payload).unwrap_or_default();
            let err = status.to_error().unwrap_or_else(|| {
                Error::new(Reason::Internal, format!("reset by peer: {}", status.message))
            });
            if let Some(slot) = shared.table().remove(&id) {
                let _ = slot.events.send(Event::Reset(err));
                slot.credit.close();
            }
        }
        Kind::Hello | Kind::Welcome | Kind::Proof | Kind::Ping | Kind::Pong => {
            return Err(format!("{:?} on stream {id}", frame.kind));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair() -> ((Channel, mpsc::Receiver<Incoming>), (Channel, mpsc::Receiver<Incoming>)) {
        let (a, b) = tokio::io::duplex(1 << 20);
        (Channel::start(a, Side::Node), Channel::start(b, Side::Guest))
    }

    /// A guest that answers `echo` with its request followed by everything sent on the stream,
    /// and `count` with the number of bytes sent on the stream.
    fn echo_server(mut incoming: mpsc::Receiver<Incoming>) {
        tokio::spawn(async move {
            while let Some(Incoming { open, mut stream }) = incoming.recv().await {
                tokio::spawn(async move {
                    match open.method.as_str() {
                        "echo" => {
                            stream.send(open.request).await.unwrap();
                            while let Some(b) = stream.recv().await.unwrap() {
                                stream.send(b).await.unwrap();
                            }
                            stream.finish().await.unwrap();
                        }
                        "count" => {
                            let mut n = 0;
                            while let Some(b) = stream.recv().await.unwrap() {
                                n += b.len();
                            }
                            stream.send_last(Bytes::from(n.to_string())).await.unwrap();
                        }
                        _ => {
                            stream
                                .reset(Status::error(Reason::InvalidArgument, "no such method"))
                                .await
                        }
                    }
                });
            }
        });
    }

    #[tokio::test]
    async fn a_call_gets_its_answer() {
        let ((node, _), (_, incoming)) = pair();
        echo_server(incoming);
        let out = node.call("echo", Bytes::from_static(b"hello"), 1 << 20).await.unwrap();
        assert_eq!(&out[..], b"hello");
    }

    #[tokio::test]
    async fn an_unknown_method_resets_with_the_reason() {
        let ((node, _), (_, incoming)) = pair();
        echo_server(incoming);
        let err = node.call("nope", Bytes::new(), 1 << 20).await.unwrap_err();
        assert_eq!(err.reason, Reason::InvalidArgument);
    }

    #[tokio::test]
    async fn many_megabytes_pass_through_flow_control() {
        let ((node, _), (_, incoming)) = pair();
        echo_server(incoming);
        let mut s = node.open("count", Bytes::new()).await.unwrap();
        let block = Bytes::from(vec![0xab; 100_000]);
        // Sixteen windows cannot get through unless the reader keeps granting credit, so this
        // hangs if flow control is wrong.
        for _ in 0..16 * WINDOW as usize / block.len() {
            s.send(block.clone()).await.unwrap();
        }
        s.finish().await.unwrap();
        let got = s.recv().await.unwrap().unwrap();
        let want = (16 * WINDOW as usize / block.len()) * block.len();
        assert_eq!(got, Bytes::from(want.to_string()));
        assert_eq!(s.recv().await.unwrap(), None);
    }

    #[tokio::test]
    async fn concurrent_calls_do_not_mix() {
        let ((node, _), (_, incoming)) = pair();
        echo_server(incoming);
        let calls = (0..200u32).map(|i| {
            let node = node.clone();
            tokio::spawn(async move {
                let out = node.call("echo", Bytes::from(i.to_string()), 1024).await.unwrap();
                assert_eq!(out, Bytes::from(i.to_string()));
            })
        });
        for c in calls {
            c.await.unwrap();
        }
    }

    #[tokio::test]
    async fn a_dead_peer_fails_open_streams_with_drone_unreachable() {
        let (a, b) = tokio::io::duplex(1 << 16);
        let (node, _in) = Channel::start(a, Side::Node);
        let mut s = node.open("echo", Bytes::new()).await.unwrap();
        drop(b);
        let err = s.recv().await.unwrap_err();
        assert_eq!(err.reason, Reason::DroneUnreachable);
        assert!(node.is_closed());
    }

    #[tokio::test]
    async fn a_peer_that_ignores_credit_is_cut_off() {
        use futures::SinkExt;
        let (a, b) = tokio::io::duplex(1 << 22);
        let (guest, mut incoming) = Channel::start(a, Side::Guest);
        let mut raw = tokio_util::codec::Framed::new(b, FrameCodec);
        let open = Open { method: "x".into(), request: Bytes::new() };
        raw.send(Frame::new(1, Kind::Open, open.encode_to_vec())).await.unwrap();
        let _held = incoming.recv().await.unwrap();
        for _ in 0..=(WINDOW as usize / MAX_PAYLOAD) {
            raw.send(Frame::new(1, Kind::Data, vec![0u8; MAX_PAYLOAD])).await.unwrap();
        }
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !guest.is_closed() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    /// Pushes a gigabyte through a real socket pair, one stream and then 64 at once, and prints
    /// the rate. Run it in release: `cargo test -p hive-proto --release -- --ignored throughput`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "a measurement, not a check"]
    async fn throughput() {
        for streams in [1usize, 64] {
            let (a, b) = tokio::net::UnixStream::pair().unwrap();
            let (node, _) = Channel::start(a, Side::Node);
            let (_, incoming) = Channel::start(b, Side::Guest);
            echo_server(incoming);
            let total = 1usize << 30;
            let block = Bytes::from(vec![0x5a; MAX_PAYLOAD]);
            let start = std::time::Instant::now();
            let tasks: Vec<_> = (0..streams)
                .map(|_| {
                    let (node, block) = (node.clone(), block.clone());
                    tokio::spawn(async move {
                        let mut s = node.open("count", Bytes::new()).await.unwrap();
                        for _ in 0..total / streams / block.len() {
                            s.send(block.clone()).await.unwrap();
                        }
                        s.finish().await.unwrap();
                        s.recv().await.unwrap().unwrap()
                    })
                })
                .collect();
            for t in tasks {
                t.await.unwrap();
            }
            let secs = start.elapsed().as_secs_f64();
            println!("{streams} streams: {:.0} MiB/s", total as f64 / secs / (1 << 20) as f64);
        }
    }
}
