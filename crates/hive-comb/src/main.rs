//! The `hive-comb` binary: the node agent in standalone mode.
//!
//! ```text
//! hive-comb [--config PATH]
//! ```
//!
//! It reads its config from `PATH`, or from `/etc/hivebox/comb.toml` when there is one, opens the
//! comb and serves the local API on the config's socket. On SIGTERM or SIGINT it stops taking
//! calls and leaves every cell running, and the next comb to start takes them over.
//!
//! `hive-comb --oci-worker` is not for people: it is how the container backend starts its workers,
//! which have to be single threaded processes of their own.

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
    // Before anything else, the runtime's threads above all, since a worker has to have none.
    if std::env::args_os().nth(1).is_some_and(|a| a == "--oci-worker") {
        return hive_cell_oci::worker::main();
    }
    if matches!(std::env::args().nth(1).as_deref(), Some("--version" | "-V")) {
        println!("hive-comb {}", env!("CARGO_PKG_VERSION"));
        return ExitCode::SUCCESS;
    }
    raise_open_files();
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

/// Raises the soft limit on open files to the hard one. The comb holds a connection to every
/// running cell's guest agent, so the usual soft limit of 1024 stops a node short of 1000 cells.
#[cfg(target_os = "linux")]
fn raise_open_files() {
    use rustix::process::{Resource, getrlimit, setrlimit};
    let mut limit = getrlimit(Resource::Nofile);
    if limit.current < limit.maximum {
        limit.current = limit.maximum;
        if let Err(e) = setrlimit(Resource::Nofile, limit) {
            eprintln!("hive-comb: raising the open files limit: {e}");
        }
    }
}

#[cfg(target_os = "linux")]
async fn run(mut cfg: hive_comb::Config) -> std::io::Result<()> {
    use hive_comb::lease;
    use tokio::signal::unix::{SignalKind, signal};
    use tokio_util::sync::CancellationToken;

    // Registering comes before the signal handlers, so a comb waiting on a keeper that is down
    // still goes at once on SIGTERM.
    let keeper = match cfg.keeper.clone() {
        Some(link) => {
            std::fs::create_dir_all(&cfg.data_dir)?;
            let mut k = lease::Keeper::new(&link.members).map_err(std::io::Error::other)?;
            let start = lease::register(&mut k, &link, &cfg.data_dir)
                .await
                .map_err(std::io::Error::other)?;
            let (l, sent) = match start {
                lease::Start::Leased(l, sent) => (l, sent),
                lease::Start::Lapsed(node, epoch) => {
                    (cfg.node, cfg.epoch) = (node, epoch);
                    return lapse(cfg).await;
                }
            };
            eprintln!(
                "hive-comb: registered as {} with the keeper: node {}, epoch {}, lease {:?}",
                link.name, l.node, l.epoch, l.ttl
            );
            (cfg.node, cfg.epoch) = (l.node, l.epoch);
            Some((k, l, sent))
        }
        None => None,
    };
    // Renewing starts now, since opening the comb over many cells can take longer than a lease.
    let stop = CancellationToken::new();
    let mut held = match keeper {
        Some((k, l, sent)) => {
            let file = cfg.data_dir.join(lease::FILE);
            tokio::spawn(lease::keep(k, l, sent, file, stop.clone()))
        }
        None => tokio::spawn(std::future::pending()),
    };
    let (socket, listen, metrics, scout) =
        (cfg.api_socket.clone(), cfg.listen, cfg.metrics, cfg.scout.clone());
    let mut term = signal(SignalKind::terminate())?;
    let mut int = signal(SignalKind::interrupt())?;
    let drivers = drivers(&cfg).await;
    if drivers.iter().next().is_none() {
        eprintln!("hive-comb: no backend can run here, so every create will be refused");
    }
    let comb = hive_comb::Comb::open(cfg, drivers).await?;
    let listener = hive_comb::api::bind(&socket)?;
    let tcp = match listen {
        Some(addr) => Some(tokio::net::TcpListener::bind(addr).await.map_err(|e| {
            std::io::Error::new(e.kind(), format!("serving the API on {addr}: {e}"))
        })?),
        None => None,
    };
    if let Some(addr) = metrics {
        let scrape = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
            std::io::Error::new(e.kind(), format!("serving metrics on {addr}: {e}"))
        })?;
        let registry = comb.metrics().registry().clone();
        tokio::spawn(async move {
            if let Err(e) = hive_telemetry::serve(scrape, registry).await {
                eprintln!("hive-comb: the metrics endpoint stopped: {e}");
            }
        });
    }
    eprintln!(
        "hive-comb {}: {} cells, serving on {}{}",
        env!("CARGO_PKG_VERSION"),
        comb.list().len(),
        socket.display(),
        listen.map_or(String::new(), |a| format!(" and {a}")),
    );
    if let Some(link) = scout {
        eprintln!("hive-comb: reporting to scout at {}", link.endpoint);
        tokio::spawn(hive_comb::report::run(comb.clone(), link, stop.clone()));
    }
    let mut server = tokio::spawn(hive_comb::api::serve(comb.clone(), listener, tcp, stop.clone()));
    let mut lost = None;
    let served = tokio::select! {
        _ = term.recv() => None,
        _ = int.recv() => None,
        r = &mut server => Some(r),
        // A comb that lost its lease has lost its node. Keeper says so to anyone who asks, so
        // its cells stop with it rather than run on next to ones made again elsewhere.
        Ok(Err(e)) = &mut held => {
            lost = Some(e);
            None
        }
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
    if lost.is_some() {
        comb.lose().await;
    }
    comb.shutdown().await;
    match lost {
        Some(e) => Err(std::io::Error::other(e)),
        None => result,
    }
}

/// The comb could not get its node back from the keeper before its last lease ran out, so the
/// keeper may have given the node to another comb, which can make the same keyed cells again. It
/// stops every cell it has from that lease, notes that in the lease file, and goes, and the next
/// start waits on the keeper for as long as it takes.
#[cfg(target_os = "linux")]
async fn lapse(cfg: hive_comb::Config) -> std::io::Result<()> {
    let file = cfg.data_dir.join(hive_comb::lease::FILE);
    let (node, epoch) = (cfg.node, cfg.epoch);
    let drivers = drivers(&cfg).await;
    let comb = hive_comb::Comb::open(cfg, drivers).await?;
    comb.lose().await;
    comb.shutdown().await;
    hive_comb::lease::lapsed(&file)?;
    Err(std::io::Error::other(format!(
        "could not reach the keeper before the lease of node {node} in epoch {epoch} ran out"
    )))
}

/// Every backend this node can run. One that cannot is left out, and the log says why.
#[cfg(target_os = "linux")]
async fn drivers(cfg: &hive_comb::Config) -> hive_cell::DriverRegistry {
    use hive_cell::CellDriver;

    let mut drivers = hive_cell::DriverRegistry::new();
    let c = &cfg.container;
    if c.enabled {
        let oci = hive_cell_oci::Config {
            drone: c.drone.clone(),
            state_dir: c.state_dir.clone(),
            workers: c.workers,
            uid_base: c.uid_base,
            uid_count: c.uid_count,
            ..hive_cell_oci::Config::default()
        };
        match hive_cell_oci::OciDriver::new(oci) {
            Ok(d) => match d.probe().await {
                Ok(fit) if fit.ready => drivers.add(std::sync::Arc::new(d)),
                Ok(fit) => eprintln!("hive-comb: no container cells: {}", fit.notes.join(", ")),
                Err(e) => eprintln!("hive-comb: no container cells: {e}"),
            },
            Err(e) => eprintln!("hive-comb: no container cells: {e}"),
        }
    }
    drivers
}

#[cfg(not(target_os = "linux"))]
fn main() -> std::process::ExitCode {
    eprintln!("hive-comb runs only on Linux");
    std::process::ExitCode::FAILURE
}
