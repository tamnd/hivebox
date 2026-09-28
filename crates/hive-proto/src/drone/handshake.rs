//! The handshake that opens a drone channel, from `spec/09_guest_agent.md`, section 2.
//!
//! Both ends hold a 32 byte secret that the workload cannot read. On first boot it is the nonce
//! the node agent handed the guest agent out of band. After a successful handshake both ends
//! derive a new secret from the old one and the transcript, and a reconnect after a node agent
//! restart uses that. The exchange is three messages on stream 0:
//!
//! 1. The guest sends `Hello` with its versions, its capabilities and a fresh random nonce.
//! 2. The node sends `Welcome` with the chosen version, its own nonce and a MAC over both.
//! 3. The guest checks that MAC and sends `Proof`, a MAC under a different label.
//!
//! A process in the cell that dials or answers the socket without the secret cannot produce
//! either MAC, so it can neither pose as the node to the guest agent nor as the guest agent to
//! the node. The MAC is keyed BLAKE3, and the labels keep a node MAC from ever being replayed as
//! a guest one.

use super::frame::{Frame, Kind};
use super::msg::{Hello, Proof, Welcome};
use futures::{SinkExt, StreamExt};
use prost::Message;
use std::io;

/// The protocol version this build speaks natively.
pub const VERSION: u32 = 1;
/// The versions the node accepts. Once older versions exist this holds the two before [`VERSION`]
/// as well, so a guest agent built two releases ago still connects.
pub const SUPPORTED: &[u32] = &[VERSION];

/// A 32 byte shared secret.
pub type Secret = [u8; 32];

/// What both sides know after a handshake.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Established {
    /// The protocol version in use.
    pub version: u32,
    /// Capabilities both sides have.
    pub caps: Vec<String>,
    /// The guest agent's build string, as it reported it.
    pub build: String,
    /// The node's clock at the time of the welcome, in nanoseconds since the Unix epoch.
    pub clock_unix_nanos: u64,
    /// The secret for the next handshake on this cell.
    pub next_secret: Secret,
}

/// Runs the guest side of the handshake.
pub async fn guest<S>(io: &mut S, secret: &Secret, hello: Hello) -> io::Result<Established>
where
    S: futures::Sink<Frame, Error = io::Error> + futures::Stream<Item = io::Result<Frame>> + Unpin,
{
    let hello_bytes = hello.encode_to_vec();
    io.send(Frame::new(0, Kind::Hello, hello_bytes.clone())).await?;
    let frame = next(io, Kind::Welcome).await?;
    let welcome = Welcome::decode(frame.payload).map_err(bad)?;
    let unsigned = Welcome { mac: Vec::new(), ..welcome.clone() }.encode_to_vec();
    let expect = mac(secret, NODE_LABEL, &hello_bytes, &unsigned);
    if welcome.mac.len() != 32 || blake3::Hash::from_bytes(expect) != to_hash(&welcome.mac) {
        return Err(denied("the node's proof does not match the secret"));
    }
    if !hello.versions.contains(&welcome.chosen) {
        return Err(bad(format!(
            "the node chose version {}, which was not offered",
            welcome.chosen
        )));
    }
    let proof = Proof { mac: mac(secret, GUEST_LABEL, &hello_bytes, &unsigned).to_vec() };
    io.send(Frame::new(0, Kind::Proof, proof.encode_to_vec())).await?;
    Ok(Established {
        version: welcome.chosen,
        caps: welcome.caps,
        build: hello.build,
        clock_unix_nanos: welcome.clock_unix_nanos,
        next_secret: next_secret(secret, &hello_bytes, &unsigned),
    })
}

/// Runs the node side of the handshake. `caps` are the capabilities the node offers, and the
/// result holds the ones the guest also has.
pub async fn node<S>(
    io: &mut S,
    secret: &Secret,
    caps: &[&str],
    clock_unix_nanos: u64,
    nonce: [u8; 32],
) -> io::Result<Established>
where
    S: futures::Sink<Frame, Error = io::Error> + futures::Stream<Item = io::Result<Frame>> + Unpin,
{
    let frame = next(io, Kind::Hello).await?;
    let hello_bytes = frame.payload.to_vec();
    let hello = Hello::decode(frame.payload).map_err(bad)?;
    let chosen = hello
        .versions
        .iter()
        .copied()
        .filter(|v| SUPPORTED.contains(v))
        .max()
        .ok_or_else(|| bad(format!("no common version in {:?}", hello.versions)))?;
    let shared: Vec<String> =
        hello.caps.iter().filter(|c| caps.contains(&c.as_str())).cloned().collect();
    let mut welcome = Welcome {
        chosen,
        caps: shared.clone(),
        clock_unix_nanos,
        nonce: nonce.to_vec(),
        mac: Vec::new(),
    };
    let unsigned = welcome.encode_to_vec();
    welcome.mac = mac(secret, NODE_LABEL, &hello_bytes, &unsigned).to_vec();
    io.send(Frame::new(0, Kind::Welcome, welcome.encode_to_vec())).await?;
    let frame = next(io, Kind::Proof).await?;
    let proof = Proof::decode(frame.payload).map_err(bad)?;
    let expect = mac(secret, GUEST_LABEL, &hello_bytes, &unsigned);
    if proof.mac.len() != 32 || blake3::Hash::from_bytes(expect) != to_hash(&proof.mac) {
        return Err(denied("the guest agent's proof does not match the secret"));
    }
    Ok(Established {
        version: chosen,
        caps: shared,
        build: hello.build,
        clock_unix_nanos,
        next_secret: next_secret(secret, &hello_bytes, &unsigned),
    })
}

const NODE_LABEL: &[u8] = b"hivebox drone handshake v1: node";
const GUEST_LABEL: &[u8] = b"hivebox drone handshake v1: guest";

fn mac(secret: &Secret, label: &[u8], hello: &[u8], welcome: &[u8]) -> [u8; 32] {
    let mut h = blake3::Hasher::new_keyed(secret);
    for part in [label, hello, welcome] {
        h.update(&(part.len() as u64).to_le_bytes());
        h.update(part);
    }
    *h.finalize().as_bytes()
}

fn next_secret(secret: &Secret, hello: &[u8], welcome: &[u8]) -> Secret {
    let mut h = blake3::Hasher::new_derive_key("hivebox drone channel session secret v1");
    h.update(secret);
    for part in [hello, welcome] {
        h.update(&(part.len() as u64).to_le_bytes());
        h.update(part);
    }
    *h.finalize().as_bytes()
}

// Comparing through blake3::Hash makes the comparison constant time.
fn to_hash(bytes: &[u8]) -> blake3::Hash {
    let mut out = [0u8; 32];
    out.copy_from_slice(bytes);
    blake3::Hash::from_bytes(out)
}

async fn next<S>(io: &mut S, want: Kind) -> io::Result<Frame>
where
    S: futures::Stream<Item = io::Result<Frame>> + Unpin,
{
    match io.next().await {
        Some(Ok(f)) if f.stream == 0 && f.kind == want => Ok(f),
        Some(Ok(f)) => {
            Err(bad(format!("expected {want:?} on stream 0, got {:?} on {}", f.kind, f.stream)))
        }
        Some(Err(e)) => Err(e),
        None => Err(io::Error::new(io::ErrorKind::UnexpectedEof, "the peer hung up mid handshake")),
    }
}

fn bad(e: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}

fn denied(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message.to_string())
}

#[cfg(test)]
mod tests {
    use super::super::frame::FrameCodec;
    use super::*;
    use tokio_util::codec::Framed;

    fn hello() -> Hello {
        Hello {
            versions: vec![3, 2, 1],
            build: "test".into(),
            caps: vec!["pty".into(), "watch".into()],
            nonce: vec![9; 32],
        }
    }

    async fn run(
        guest_secret: Secret,
        node_secret: Secret,
    ) -> (io::Result<Established>, io::Result<Established>) {
        let (a, b) = tokio::io::duplex(1 << 16);
        // Each side owns its end, so a side that gives up hangs up and the other one sees it.
        tokio::join!(
            async move { guest(&mut Framed::new(a, FrameCodec), &guest_secret, hello()).await },
            async move {
                let mut io = Framed::new(b, FrameCodec);
                node(&mut io, &node_secret, &["pty", "envd-compat"], 42, [1; 32]).await
            },
        )
    }

    #[tokio::test]
    async fn matching_secrets_agree_on_everything() {
        let (g, n) = run([7; 32], [7; 32]).await;
        let (g, n) = (g.unwrap(), n.unwrap());
        assert_eq!(g, n);
        assert_eq!(g.version, 1);
        assert_eq!(g.caps, ["pty"]);
        assert_eq!(g.clock_unix_nanos, 42);
        assert_ne!(g.next_secret, [7; 32]);
    }

    #[tokio::test]
    async fn a_wrong_secret_fails_on_both_sides() {
        let (g, n) = run([7; 32], [8; 32]).await;
        assert_eq!(g.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
        // The guest hangs up without a proof, so the node sees the connection end.
        assert!(n.is_err());
    }

    #[tokio::test]
    async fn a_forged_proof_is_refused() {
        let (a, b) = tokio::io::duplex(1 << 16);
        let mut ga = Framed::new(a, FrameCodec);
        let mut nb = Framed::new(b, FrameCodec);
        let forger = async {
            ga.send(Frame::new(0, Kind::Hello, hello().encode_to_vec())).await.unwrap();
            let _welcome = ga.next().await;
            let proof = Proof { mac: vec![0; 32] };
            ga.send(Frame::new(0, Kind::Proof, proof.encode_to_vec())).await.unwrap();
        };
        let (_, n) = tokio::join!(forger, node(&mut nb, &[7; 32], &[], 0, [1; 32]));
        assert_eq!(n.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
    }

    #[tokio::test]
    async fn no_common_version_is_an_error() {
        let (a, b) = tokio::io::duplex(1 << 16);
        let mut ga = Framed::new(a, FrameCodec);
        let mut nb = Framed::new(b, FrameCodec);
        let h = Hello { versions: vec![99], ..hello() };
        ga.send(Frame::new(0, Kind::Hello, h.encode_to_vec())).await.unwrap();
        let n = node(&mut nb, &[7; 32], &[], 0, [1; 32]).await;
        assert_eq!(n.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }
}
