//! The `hive-comb` binary: the node agent in standalone mode.
//!
//! ```text
//! hive-comb [--config PATH]
//! ```
//!
//! It reads its config from `PATH`, or from `/etc/hivebox/comb.toml` when there is one, opens the
//! comb and serves the local API on the config's socket. On SIGTERM or SIGINT it stops taking
//! calls and leaves every cell running, and the next comb to start takes them over.

#![forbid(unsafe_code)]

#[cfg(target_os = "linux")]
use std::process::ExitCode;

#[cfg(target_os = "linux")]
const USAGE: &str = "usage: hive-comb [--config PATH]";
#[cfg(target_os = "linux")]
const DEFAULT_CONFIG: &str = "/etc/hivebox/comb.toml";

#[cfg(target_os = "linux")]
fn config() -> Result<hive_comb::Config, String> {
    let mut path = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => path = Some(args.next().ok_or("--config needs a value")?),
            other => return Err(format!("unknown argument {other}\n{USAGE}")),
        }
    }
    let text = match path {
        Some(p) => std::fs::read_to_string(&p).map_err(|e| format!("reading {p}: {e}"))?,
        None => match std::fs::read_to_string(DEFAULT_CONFIG) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(e) => return Err(format!("reading {DEFAULT_CONFIG}: {e}")),
        },
    };
    hive_comb::Config::from_toml(&text)
}

#[cfg(target_os = "linux")]
fn main() -> ExitCode {
    if matches!(std::env::args().nth(1).as_deref(), Some("--version" | "-V")) {
        println!("hive-comb {}", env!("CARGO_PKG_VERSION"));
        return ExitCode::SUCCESS;
    }
    let cfg = match config() {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("hive-comb: {e}");
            return ExitCode::from(2);
        }
    };
    let rt = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("hive-comb: starting the runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(run(cfg)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("hive-comb: {e}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(target_os = "linux")]
async fn run(cfg: hive_comb::Config) -> std::io::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};
    use tokio_util::sync::CancellationToken;

    let socket = cfg.api_socket.clone();
    let mut term = signal(SignalKind::terminate())?;
    let mut int = signal(SignalKind::interrupt())?;
    // No backend is built into the binary yet. Each one is added here as it lands.
    let drivers = hive_cell::DriverRegistry::new();
    if drivers.iter().next().is_none() {
        eprintln!("hive-comb: no backend is built in yet, so every create will be refused");
    }
    let comb = hive_comb::Comb::open(cfg, drivers).await?;
    let listener = hive_comb::api::bind(&socket)?;
    eprintln!(
        "hive-comb {}: {} cells, serving on {}",
        env!("CARGO_PKG_VERSION"),
        comb.list().len(),
        socket.display()
    );
    let stop = CancellationToken::new();
    let mut server = tokio::spawn(hive_comb::api::serve(comb.clone(), listener, stop.clone()));
    let served = tokio::select! {
        _ = term.recv() => None,
        _ = int.recv() => None,
        r = &mut server => Some(r),
    };
    stop.cancel();
    let result = match served {
        Some(r) => r.unwrap_or_else(|e| Err(std::io::Error::other(e))),
        // Calls in flight get a few seconds to finish before the comb goes.
        None => {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), server).await;
            Ok(())
        }
    };
    let _ = std::fs::remove_file(&socket);
    comb.shutdown().await;
    result
}

#[cfg(not(target_os = "linux"))]
fn main() -> std::process::ExitCode {
    eprintln!("hive-comb runs only on Linux");
    std::process::ExitCode::FAILURE
}
