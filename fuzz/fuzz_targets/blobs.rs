//! The small blobs nectar stores and reads back: chunk recipes, page traces, image manifests and
//! BLAKE3 leaves. Whatever parses writes back to bytes that parse to the same thing.

#![no_main]

use hive_nectar::{BlobId, Leaves, Manifest, cas::Recipe, trace};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Some((&which, data)) = data.split_first() else { return };
    match which % 4 {
        0 => {
            if let Ok(r) = Recipe::from_bytes(data) {
                let bytes = r.to_bytes();
                assert_eq!(bytes, data);
                assert_eq!(Recipe::from_bytes(&bytes).unwrap(), r);
            }
        }
        1 => {
            if let Ok(t) = trace::from_bytes(data) {
                assert_eq!(trace::to_bytes(&t), data);
            }
        }
        2 => {
            if let Ok(m) = Manifest::from_bytes(data) {
                assert_eq!(Manifest::from_bytes(&m.to_bytes()).unwrap(), m);
            }
        }
        _ => {
            // A blob name and a size, then the leaves.
            let Some((head, leaves)) = data.split_at_checked(40) else { return };
            let blob = BlobId::from_bytes(head[..32].try_into().unwrap());
            let size = u64::from_le_bytes(head[32..].try_into().unwrap());
            if let Ok(l) = Leaves::from_bytes(blob, size, leaves) {
                assert_eq!(l.to_bytes(), leaves);
                assert_eq!(l.blob(), blob);
                let chunks = l.chunks();
                for i in [0, chunks.saturating_sub(1), chunks, u64::MAX] {
                    let _ = l.check(i, leaves);
                }
            }
        }
    }
});
