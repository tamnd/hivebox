//! The guest frame codec on any bytes. Every frame it decodes re-encodes to the bytes it came
//! from, and it never takes more than one frame's worth of input per frame.

#![no_main]

use bytes::BytesMut;
use hive_proto::drone::FrameCodec;
use hive_proto::drone::frame::{HEADER_LEN, MAX_PAYLOAD};
use libfuzzer_sys::fuzz_target;
use tokio_util::codec::{Decoder, Encoder};

fuzz_target!(|data: &[u8]| {
    let mut src = BytesMut::from(data);
    loop {
        let before = src.clone();
        let frame = match FrameCodec.decode(&mut src) {
            Ok(Some(f)) => f,
            Ok(None) => {
                assert_eq!(src, before, "waiting for more input took some");
                break;
            }
            Err(_) => break,
        };
        assert!(frame.payload.len() <= MAX_PAYLOAD);
        let used = before.len() - src.len();
        assert_eq!(used, HEADER_LEN + frame.payload.len());
        let mut out = BytesMut::new();
        FrameCodec.encode(frame.clone(), &mut out).unwrap();
        assert_eq!(&out[..], &before[..used], "{frame:?} encodes to other bytes");
        assert_eq!(FrameCodec.decode(&mut out).unwrap(), Some(frame));
        assert!(out.is_empty());
    }
});
