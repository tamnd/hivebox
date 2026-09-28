//! Build and check tasks for the hivebox workspace.
//!
//! Everything here is a check that has to run somewhere and does not belong in a unit test, either
//! because it reads the whole tree or because it shells out. Running them through cargo rather than
//! a shell script means they behave the same on a laptop and on a runner.

#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

mod layers;
mod style;

fn main() -> ExitCode {
    let task = std::env::args().nth(1);
    let result = match task.as_deref() {
        Some("layers") => layers::check(&root()),
        Some("style") => style::check(&root()),
        Some("msrv") => msrv(),
        Some("ci") => ci(),
        Some("help" | "--help" | "-h") | None => {
            usage();
            return ExitCode::SUCCESS;
        }
        Some(other) => Err(format!("unknown task {other}, try `cargo xtask help`")),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("error: {message}");
            ExitCode::FAILURE
        }
    }
}

fn usage() {
    println!("cargo xtask <task>");
    println!();
    println!("  layers   every crate depends only on crates of strictly lower rank");
    println!("  style    the prose rules for markdown in this repository");
    println!("  msrv     the workspace still builds on the oldest Rust the manifest claims");
    println!("  ci       everything the per-commit workflow runs, in the same order");
}

fn root() -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest.parent().expect("xtask is not at the workspace root").to_path_buf()
}

/// The per-commit gate, cheapest first, so a formatting mistake costs seconds.
fn ci() -> Result<(), String> {
    let root = root();
    cargo(&["fmt", "--all", "--check"])?;
    layers::check(&root)?;
    style::check(&root)?;
    cargo(&["clippy", "--workspace", "--all-targets", "--all-features", "--", "-D", "warnings"])?;
    if has("cargo-nextest") {
        cargo(&["nextest", "run", "--workspace", "--all-features"])?;
        // nextest does not run doctests, and a doc example that does not compile is a broken
        // promise on the first page anybody reads.
        cargo(&["test", "--workspace", "--all-features", "--doc"])?;
    } else {
        println!("cargo-nextest is not installed, falling back to cargo test");
        cargo(&["test", "--workspace", "--all-features"])?;
    }
    cargo(&["doc", "--workspace", "--all-features", "--no-deps"])?;
    msrv()
}

fn msrv() -> Result<(), String> {
    let root = root();
    let manifest = std::fs::read_to_string(root.join("Cargo.toml"))
        .map_err(|e| format!("could not read the workspace manifest: {e}"))?;
    let version = manifest
        .lines()
        .find_map(|line| line.strip_prefix("rust-version"))
        .and_then(|rest| rest.split('"').nth(1))
        .ok_or("no rust-version in the workspace manifest")?
        .to_string();

    let installed = Command::new("rustup")
        .args(["toolchain", "list"])
        .output()
        .map(|out| String::from_utf8_lossy(&out.stdout).contains(&version))
        .unwrap_or(false);
    if !installed {
        // Loud rather than silent. A skipped check that says nothing is the same as no check.
        println!(
            "skipping the {version} check, that toolchain is not installed\n  \
             install it with `rustup toolchain install {version} --profile minimal`\n  \
             CI runs it either way"
        );
        return Ok(());
    }

    // Through `rustup run` rather than `cargo +version`, because `cargo xtask` sets `CARGO` to a
    // real binary and the `+toolchain` syntax only works through the rustup shim.
    println!("rustup run {version} cargo check --workspace --all-features");
    let status = Command::new("rustup")
        .args(["run", &version, "cargo", "check", "--workspace", "--all-features"])
        // Its own target directory, or every run of this task throws away what the others built.
        .env("CARGO_TARGET_DIR", root.join("target").join("msrv"))
        .env_remove("CARGO")
        .env_remove("RUSTC")
        .current_dir(&root)
        .status()
        .map_err(|e| format!("could not run rustup: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("the workspace does not build on Rust {version}"))
    }
}

fn has(tool: &str) -> bool {
    Command::new(tool).arg("--version").output().is_ok_and(|out| out.status.success())
}

fn cargo(args: &[&str]) -> Result<(), String> {
    println!("cargo {}", args.join(" "));
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let status = Command::new(cargo)
        .args(args)
        .current_dir(root())
        .status()
        .map_err(|e| format!("could not run cargo: {e}"))?;
    if status.success() { Ok(()) } else { Err(format!("cargo {} failed", args.join(" "))) }
}
