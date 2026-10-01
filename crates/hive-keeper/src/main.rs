//! The `hive-keeper` binary.
//!
//! ```text
//! hive-keeper [--config PATH]
//! ```
//!
//! Runs one member of the keeper group as the config file says, `/etc/hivebox/keeper.toml` by
//! default, until it gets SIGTERM or SIGINT.

#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::process::ExitCode;

use hive_keeper::Config;
use tokio_util::sync::CancellationToken;

const USAGE: &str = "usage: hive-keeper [--config PATH]";

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let mut config = PathBuf::from("/etc/hivebox/keeper.toml");
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--version" | "-V" => {
                println!("hive-keeper {}", env!("CARGO_PKG_VERSION"));
                return ExitCode::SUCCESS;
            }
            "--config" => match args.next() {
                Some(p) => config = PathBuf::from(p),
                None => {
                    eprintln!("hive-keeper: --config needs a path\n{USAGE}");
                    return ExitCode::from(2);
                }
            },
            other => {
                eprintln!("hive-keeper: unknown argument {other}\n{USAGE}");
                return ExitCode::from(2);
            }
        }
    }
    let cfg = match std::fs::read_to_string(&config)
        .map_err(|e| e.to_string())
        .and_then(|t| Config::from_toml(&t))
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("hive-keeper: {}: {e}", config.display());
            return ExitCode::from(2);
        }
    };
    let rt = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("hive-keeper: starting the runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(run(cfg)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("hive-keeper: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cfg: Config) -> Result<(), String> {
    use tokio::signal::unix::{SignalKind, signal};

    let listener = tokio::net::TcpListener::bind(cfg.listen)
        .await
        .map_err(|e| format!("listening on {}: {e}", cfg.listen))?;
    let stop = CancellationToken::new();
    let mut term = signal(SignalKind::terminate()).map_err(|e| e.to_string())?;
    let mut int = signal(SignalKind::interrupt()).map_err(|e| e.to_string())?;
    tokio::spawn({
        let stop = stop.clone();
        async move {
            tokio::select! {
                _ = term.recv() => {}
                _ = int.recv() => {}
            }
            stop.cancel();
        }
    });
    eprintln!(
        "hive-keeper {}: member {} of {}, serving on {}",
        env!("CARGO_PKG_VERSION"),
        cfg.id,
        cfg.members.len(),
        cfg.listen
    );
    hive_keeper::run(cfg, listener, stop).await
}
