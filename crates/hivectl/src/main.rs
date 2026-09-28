//! The `hivectl` binary.

#![forbid(unsafe_code)]

use std::process::ExitCode;

fn main() -> ExitCode {
    match std::env::args().nth(1).as_deref() {
        Some("--version" | "-V") => {
            println!("hivectl {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        _ => {
            eprintln!("hivectl {}: not implemented yet", env!("CARGO_PKG_VERSION"));
            ExitCode::FAILURE
        }
    }
}
