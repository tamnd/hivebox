//! The tar to EROFS path every image layer takes. On any input the writer either refuses it or
//! builds a layer whose superblock reads back, and the tar rewriter `mkfs.erofs` reads from never
//! panics either.

#![no_main]

use hive_nectar::erofs::{self, DEFAULT_CHUNK_SIZE, Writer};
use libfuzzer_sys::fuzz_target;
use std::path::PathBuf;
use std::sync::LazyLock;

// One scratch directory per fuzzing process, written over by every input.
static DIR: LazyLock<PathBuf> = LazyLock::new(|| {
    let dir = std::env::temp_dir().join(format!("hive-fuzz-erofs-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
});

fuzz_target!(|data: &[u8]| {
    let writer = Writer::new(DEFAULT_CHUNK_SIZE).unwrap();
    if let Ok(built) = writer.build(data, &DIR) {
        let sb = erofs::superblock(&built.meta);
        assert!(sb.is_ok(), "a layer the writer built does not read back: {sb:?}");
    }
    let _ = erofs::prepare(data, std::io::sink());
});
