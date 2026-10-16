//! The handshake that opens a guest channel. Without the secret it never succeeds on either side,
//! whatever bytes arrive, and with it any hello either fails on both sides or leaves both agreeing
//! on the version, the capabilities and the next secret.

#![no_main]

use arbitrary::Arbitrary;
use hive_proto::drone::FrameCodec;
use hive_proto::drone::frame::MAX_PAYLOAD;
use hive_proto::drone::handshake::{self, VERSION};
use hive_proto::drone::msg::Hello;
use libfuzzer_sys::fuzz_target;
use prost::Message;
use tokio::io::AsyncWriteExt;
use tokio_util::codec::Framed;

const SECRET: [u8; 32] = [7; 32];

#[derive(Arbitrary, Debug)]
enum Input {
    /// Both ends hold the secret, and the guest says what it likes.
    Honest {
        versions: Vec<u32>,
        build: String,
        caps: Vec<String>,
        nonce: Vec<u8>,
        node_caps: Vec<String>,
        clock: u64,
    },
    /// Whatever bytes, as the guest, to a node that holds the secret.
    ToNode(Vec<u8>),
    /// Whatever bytes, as the node, to a guest that holds the secret.
    ToGuest(Vec<u8>),
}

fn hello(versions: Vec<u32>, build: String, caps: Vec<String>, nonce: Vec<u8>) -> Hello {
    Hello { versions, build, caps, nonce }
}

fuzz_target!(|input: Input| {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    rt.block_on(async {
        let (a, mut b) = tokio::io::duplex(1 << 16);
        match input {
            Input::Honest { versions, build, caps, nonce, node_caps, clock } => {
                let hello = hello(versions, build, caps, nonce);
                let node_caps: Vec<&str> = node_caps.iter().map(String::as_str).collect();
                let guest = async {
                    let mut io = Framed::new(a, FrameCodec);
                    handshake::guest(&mut io, &[SECRET], hello.clone()).await
                };
                let node = async {
                    let mut io = Framed::new(b, FrameCodec);
                    handshake::node(&mut io, &SECRET, &node_caps, clock, [1; 32]).await
                };
                let (g, n) = tokio::join!(guest, node);
                assert_eq!(g.is_ok(), n.is_ok(), "{g:?} {n:?}");
                // Room to spare for the welcome, which is the shared caps and 80 bytes more.
                if hello.versions.contains(&VERSION) && hello.encoded_len() * 2 + 128 < MAX_PAYLOAD
                {
                    assert!(n.is_ok(), "{n:?}");
                }
                if let (Ok(g), Ok(n)) = (g, n) {
                    assert_eq!(g, n);
                    assert_eq!(n.version, VERSION);
                    let shared: Vec<String> = hello
                        .caps
                        .iter()
                        .filter(|c| node_caps.contains(&c.as_str()))
                        .cloned()
                        .collect();
                    assert_eq!(n.caps, shared);
                    assert_ne!(n.next_secret, SECRET);
                }
            }
            Input::ToNode(bytes) => {
                let node = async {
                    let mut io = Framed::new(a, FrameCodec);
                    handshake::node(&mut io, &SECRET, &["exec"], 0, [1; 32]).await
                };
                let feed = async {
                    let _ = b.write_all(&bytes).await;
                    let _ = b.shutdown().await;
                };
                let (n, ()) = tokio::join!(node, feed);
                assert!(n.is_err(), "a node took a guest without the secret: {n:?}");
            }
            Input::ToGuest(bytes) => {
                let guest = async {
                    let mut io = Framed::new(a, FrameCodec);
                    let hello =
                        hello(vec![VERSION], "fuzz".into(), vec!["exec".into()], vec![2; 32]);
                    handshake::guest(&mut io, &[SECRET, [9; 32]], hello).await
                };
                let feed = async {
                    let _ = b.write_all(&bytes).await;
                    let _ = b.shutdown().await;
                };
                let (g, ()) = tokio::join!(guest, feed);
                assert!(g.is_err(), "a guest took a node without the secret: {g:?}");
            }
        }
    });
});
