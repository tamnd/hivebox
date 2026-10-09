//! The `hive-scout` binary.
//!
//! ```text
//! hive-scout [--listen ADDR] [--metrics ADDR] [--rebalance SECS] [--burst-above SHARE]
//! ```
//!
//! It takes node reports on `ADDR` (default `0.0.0.0:7410`) and serves its metrics, the node
//! counts and totals among them, on the `--metrics` address when there is one. Every `SECS`
//! (default 60, and 0 turns it off) it plans a rebalance and logs the plan when there is
//! something to move. `SHARE` is the burst line, which should be the gates' `burst_above`. It keeps nothing
//! on disk, so restarting it loses nothing: the combs reconnect and it has the cluster again
//! within a second.

#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::process::ExitCode;
use std::time::Duration;

use hive_waggle::{BURST_ABOVE, REBALANCE_EVERY, Rebalancer};
use tokio_util::sync::CancellationToken;

const USAGE: &str =
    "usage: hive-scout [--listen ADDR] [--metrics ADDR] [--rebalance SECS] [--burst-above SHARE]";

/// Connections waiting to be accepted. When scout restarts every comb connects again within a
/// few hundred milliseconds, and the default of 1024 drops the rest, which then wait out a SYN
/// retry of a second or more.
const BACKLOG: u32 = 8192;

struct Args {
    listen: SocketAddr,
    metrics: Option<SocketAddr>,
    rebalance: Duration,
    burst_above: f64,
}

fn args() -> Result<Args, String> {
    let mut a = Args {
        listen: SocketAddr::from(([0, 0, 0, 0], 7410)),
        metrics: None,
        rebalance: REBALANCE_EVERY,
        burst_above: BURST_ABOVE,
    };
    let mut it = std::env::args().skip(1);
    let addr = |v: Option<String>, flag: &str| -> Result<SocketAddr, String> {
        let v = v.ok_or(format!("{flag} needs a value"))?;
        v.parse().map_err(|e| format!("{flag} {v}: {e}"))
    };
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--listen" => a.listen = addr(it.next(), "--listen")?,
            "--metrics" => a.metrics = Some(addr(it.next(), "--metrics")?),
            "--rebalance" => {
                let v = it.next().ok_or("--rebalance needs a value")?;
                let secs: u64 = v.parse().map_err(|e| format!("--rebalance {v}: {e}"))?;
                a.rebalance = Duration::from_secs(secs);
            }
            "--burst-above" => {
                let v = it.next().ok_or("--burst-above needs a value")?;
                a.burst_above = match v.parse::<f64>() {
                    Ok(x) if x.is_finite() && x >= 0.0 => x,
                    _ => return Err(format!("--burst-above {v} is not a share, 0 or more")),
                };
            }
            "--version" | "-V" => {
                println!("hive-scout {}", env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument {other}\n{USAGE}")),
        }
    }
    Ok(a)
}

fn main() -> ExitCode {
    let args = match args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("hive-scout: {e}");
            return ExitCode::from(2);
        }
    };
    let rt = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("hive-scout: starting the runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(run(args)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("hive-scout: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> std::io::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let service = hive_scout::Service::new();
    if let Some(addr) = args.metrics {
        let scrape = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
            std::io::Error::new(e.kind(), format!("serving metrics on {addr}: {e}"))
        })?;
        let registry = service.registry().clone();
        tokio::spawn(async move {
            if let Err(e) = hive_telemetry::serve(scrape, registry).await {
                eprintln!("hive-scout: the metrics endpoint stopped: {e}");
            }
        });
    }
    let listener = listen(args.listen)
        .map_err(|e| std::io::Error::new(e.kind(), format!("listening on {}: {e}", args.listen)))?;
    let incoming = accept_all(listener);
    let stop = CancellationToken::new();
    tokio::spawn(service.clone().run(stop.clone()));
    if !args.rebalance.is_zero() {
        let rebalancer = Rebalancer::new().with_burst_above(args.burst_above);
        tokio::spawn(service.clone().rebalance(rebalancer, args.rebalance, stop.clone()));
    }
    let mut term = signal(SignalKind::terminate())?;
    let mut int = signal(SignalKind::interrupt())?;
    let shutdown = {
        let (stop, service) = (stop.clone(), service.clone());
        async move {
            tokio::select! {
                _ = term.recv() => {}
                _ = int.recv() => {}
            }
            stop.cancel();
            service.close();
        }
    };
    eprintln!("hive-scout {}: taking reports on {}", env!("CARGO_PKG_VERSION"), args.listen);
    tonic::transport::Server::builder()
        .add_service(service.server())
        .serve_with_incoming_shutdown(incoming, shutdown)
        .await
        .map_err(std::io::Error::other)
}

fn listen(addr: SocketAddr) -> std::io::Result<tokio::net::TcpListener> {
    let socket = match addr {
        SocketAddr::V4(_) => tokio::net::TcpSocket::new_v4()?,
        SocketAddr::V6(_) => tokio::net::TcpSocket::new_v6()?,
    };
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    socket.listen(BACKLOG)
}

/// The listener's connections as a stream, with Nagle off since reports are small.
fn accept_all(
    listener: tokio::net::TcpListener,
) -> impl futures::Stream<Item = std::io::Result<tokio::net::TcpStream>> {
    futures::stream::unfold(listener, |l| async move {
        let conn = l.accept().await.map(|(s, _)| {
            let _ = s.set_nodelay(true);
            s
        });
        Some((conn, l))
    })
}
