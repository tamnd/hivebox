//! Compiles the eBPF program with clang. The C has no includes, so clang is all it needs.
//!
//! Without clang the crate still builds, with an empty object in place of the program, and
//! `Guard::open` says why it cannot load. That keeps `cargo check` working on machines that never
//! run cells.

use std::path::PathBuf;
use std::process::Command;

const SOURCE: &str = "src/bpf/guard.bpf.c";

fn main() {
    println!("cargo:rerun-if-changed={SOURCE}");
    println!("cargo:rerun-if-env-changed=HIVE_GUARD_CLANG");
    let out = PathBuf::from(std::env::var_os("OUT_DIR").unwrap()).join("guard.o");
    std::fs::write(&out, []).unwrap();
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("linux") {
        return;
    }
    let target = match std::env::var("CARGO_CFG_TARGET_ENDIAN").as_deref() {
        Ok("big") => "bpfeb",
        _ => "bpfel",
    };
    let named = std::env::var("HIVE_GUARD_CLANG").ok();
    let candidates =
        named.iter().map(String::as_str).chain(["clang", "clang-20", "clang-19", "clang-18"]);
    for clang in candidates {
        let status = Command::new(clang)
            .args(["-O2", "-g", "-Wall", "-Werror", "-target", target, "-c", SOURCE, "-o"])
            .arg(&out)
            .status();
        match status {
            Ok(s) if s.success() => return,
            Ok(s) => panic!("{clang} failed to compile {SOURCE}: {s}"),
            Err(_) => {}
        }
    }
    println!(
        "cargo:warning=no clang found, so hive-guard is built without its eBPF program. Install clang or set HIVE_GUARD_CLANG."
    );
    std::fs::write(&out, []).unwrap();
}
