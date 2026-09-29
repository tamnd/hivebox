//! The `hivectl` binary.

#![forbid(unsafe_code)]

use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if matches!(args.first().map(String::as_str), Some("--version" | "-V")) {
        println!("hivectl {}", env!("CARGO_PKG_VERSION"));
        return ExitCode::SUCCESS;
    }
    let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("hivectl: {e}");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(hivectl::cli::main(args)) {
        Ok(code) => ExitCode::from(u8::try_from(code).unwrap_or(1)),
        Err(e) => {
            eprintln!("hivectl: {e}");
            ExitCode::FAILURE
        }
    }
}
