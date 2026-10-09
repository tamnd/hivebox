//! The `hive-gate` binary.
//!
//! ```text
//! hive-gate [--config PATH]
//! hive-gate key PROJECT
//! ```
//!
//! The first form serves the API as the config file says, `/etc/hivebox/gate.toml` by default.
//! The second makes an API key for `PROJECT`, prints it, and prints the lines to add to the
//! config so the gate takes it. Only the key's hash goes in the config.

#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::process::ExitCode;

use hive_gate::{Config, Gate, Nodes};
use tokio_util::sync::CancellationToken;

const USAGE: &str = "usage: hive-gate [--config PATH] | hive-gate key PROJECT";

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let mut config = PathBuf::from("/etc/hivebox/gate.toml");
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--version" | "-V" => {
                println!("hive-gate {}", env!("CARGO_PKG_VERSION"));
                return ExitCode::SUCCESS;
            }
            "key" => {
                let Some(project) = args.next().filter(|p| hive_types::is_name(p)) else {
                    eprintln!("hive-gate: key needs a project name\n{USAGE}");
                    return ExitCode::from(2);
                };
                return match hive_gate::config::new_key(&project) {
                    Ok((key, lines)) => {
                        println!("key: {key}\n\nAdd this to the gate's config:\n\n{lines}");
                        ExitCode::SUCCESS
                    }
                    Err(e) => {
                        eprintln!("hive-gate: making a key: {e}");
                        ExitCode::FAILURE
                    }
                };
            }
            "--config" => match args.next() {
                Some(p) => config = PathBuf::from(p),
                None => {
                    eprintln!("hive-gate: --config needs a path\n{USAGE}");
                    return ExitCode::from(2);
                }
            },
            other => {
                eprintln!("hive-gate: unknown argument {other}\n{USAGE}");
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
            eprintln!("hive-gate: {}: {e}", config.display());
            return ExitCode::from(2);
        }
    };
    let rt = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("hive-gate: starting the runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(run(cfg)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("hive-gate: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cfg: Config) -> std::io::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let registry = hive_telemetry::Registry::new();
    if let Some(addr) = cfg.metrics {
        let scrape = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
            std::io::Error::new(e.kind(), format!("serving metrics on {addr}: {e}"))
        })?;
        let registry = registry.clone();
        tokio::spawn(async move {
            if let Err(e) = hive_telemetry::serve(scrape, registry).await {
                eprintln!("hive-gate: the metrics endpoint stopped: {e}");
            }
        });
    }
    let listener = tokio::net::TcpListener::bind(cfg.listen)
        .await
        .map_err(|e| std::io::Error::new(e.kind(), format!("listening on {}: {e}", cfg.listen)))?;
    let peers = match cfg.peer_listen {
        Some(addr) => Some(tokio::net::TcpListener::bind(addr).await.map_err(|e| {
            std::io::Error::new(e.kind(), format!("listening for peer gates on {addr}: {e}"))
        })?),
        None => None,
    };
    let stop = CancellationToken::new();
    let mut nodes = Nodes::new(hive_scout::follow(cfg.scout.clone(), stop.clone()));
    if let Some(unit) = cfg.unit {
        nodes = nodes.with_units(unit, &cfg.units).map_err(std::io::Error::other)?;
    }
    let keys = hive_gate::Keys::new(cfg.keys);
    if !cfg.keeper.is_empty() {
        hive_gate::keys::follow(keys.clone(), &cfg.keeper, stop.clone())
            .map_err(std::io::Error::other)?;
        hive_gate::nodes::follow(nodes.clone(), &cfg.keeper, stop.clone())
            .map_err(std::io::Error::other)?;
    }
    let quotas = if cfg.keeper.is_empty() {
        None
    } else {
        let q = hive_gate::Quotas::new(cfg.name.clone(), &cfg.keeper, nodes.clone(), &registry)
            .map_err(std::io::Error::other)?;
        Some(q)
    };
    let mut gate =
        Gate::new(keys, nodes, quotas.clone(), &registry).with_burst_above(cfg.burst_above);
    if !cfg.keeper.is_empty() {
        let api = hive_gate::tokens::Api::new(&cfg.keeper).map_err(std::io::Error::other)?;
        gate = gate.with_tokens(api);
    }
    if let Some(e2b) = cfg.e2b {
        gate = gate.with_e2b(e2b);
    }
    if let Some(link) = &cfg.siem {
        let siem = hive_telemetry::siem::Siem::open(link.options(&cfg.name, "hive-gate")?)?;
        gate = gate.with_siem(std::sync::Arc::new(siem));
    }
    let mut term = signal(SignalKind::terminate())?;
    let mut int = signal(SignalKind::interrupt())?;
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
        "hive-gate {}: serving on {}, following scout at {}",
        env!("CARGO_PKG_VERSION"),
        cfg.listen,
        cfg.scout
    );
    if let Some(unit) = cfg.unit {
        let others: Vec<String> = cfg.units.iter().map(|(u, a)| format!("{u} at {a}")).collect();
        let peer = cfg.peer_listen.map_or_else(String::new, |a| format!(", peer gates on {a}"));
        eprintln!("hive-gate: unit {unit}, other units: [{}]{peer}", others.join(", "));
    }
    let served = match peers {
        // Either failing stops the gate.
        Some(p) => tokio::try_join!(
            hive_gate::serve(gate.for_peers(), p, stop.clone()),
            hive_gate::serve(gate, listener, stop)
        )
        .map(drop),
        None => hive_gate::serve(gate, listener, stop).await,
    };
    if let Some(q) = quotas {
        // So the other gates can have this one's shares now rather than when they run out.
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), q.give_back()).await;
    }
    served
}
