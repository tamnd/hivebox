//! WAL replay. A log of puts and deletes replays to the records they leave, a log cut anywhere
//! replays to what the whole frames before the cut leave, and replay never panics on any bytes.

#![no_main]

use arbitrary::Arbitrary;
use bytes::Bytes;
use hive_comb::fuzz::{wal_encode, wal_replay};
use libfuzzer_sys::fuzz_target;
use std::collections::HashMap;

#[derive(Arbitrary, Debug)]
enum Op {
    Put(u8, Vec<u8>),
    Delete(u8),
}

#[derive(Arbitrary, Debug)]
struct Input {
    ops: Vec<Op>,
    cut: u32,
    tail: Vec<u8>,
}

fn key(k: u8) -> u128 {
    // Spread the keys over all sixteen bytes, so a key read back from the wrong place shows.
    u128::from(k).wrapping_mul(0x9e37_79b9_7f4a_7c15_f39c_c060_5ced_c835)
}

fuzz_target!(|input: Input| {
    let mut log = Vec::new();
    let mut live = HashMap::new();
    // The records after each whole frame, from the empty log on.
    let mut states = vec![(0, live.clone())];
    for op in &input.ops {
        match op {
            Op::Put(k, v) => {
                wal_encode(&mut log, key(*k), Some(v));
                live.insert(key(*k), Bytes::copy_from_slice(v));
            }
            Op::Delete(k) => {
                wal_encode(&mut log, key(*k), None);
                live.remove(&key(*k));
            }
        }
        states.push((log.len(), live.clone()));
    }
    assert_eq!(wal_replay(&log), (live, log.len()));

    // A torn write: everything up to the last whole frame before the cut comes back.
    let cut = input.cut as usize % (log.len() + 1);
    let (end, want) = states.iter().rev().find(|(end, _)| *end <= cut).unwrap();
    assert_eq!(wal_replay(&log[..cut]), (want.clone(), *end));

    // Garbage after the cut stops replay at a frame boundary, and replaying what it kept gives the
    // same records.
    log.truncate(cut);
    log.extend_from_slice(&input.tail);
    let (got, good) = wal_replay(&log);
    assert!(good >= *end && good <= log.len());
    assert_eq!(wal_replay(&log[..good]), (got, good));
});
