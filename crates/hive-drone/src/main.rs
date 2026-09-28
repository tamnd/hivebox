//! The `hive-drone` binary.

#![forbid(unsafe_code)]

use std::process::ExitCode;

fn main() -> ExitCode {
    match std::env::args().nth(1).as_deref() {
        Some("--version" | "-V") => {
            println!("hive-drone {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        _ => {
            eprintln!("hive-drone {}: not implemented yet", env!("CARGO_PKG_VERSION"));
            ExitCode::FAILURE
        }
    }
}
