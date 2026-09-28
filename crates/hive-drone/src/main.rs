//! The `hive-drone` binary.
//!
//! ```text
//! hive-drone --listen unix:/run/hive/drone.sock --secret-stdin [--shell /bin/sh]
//!            [--session-shell /bin/bash] [--workdir /] [--uid N] [--gid N]
//!            [--root PATH]...
//! ```
//!
//! The first secret is read from stdin as 32 raw bytes, so it never shows up in the process list
//! or the environment of a command.

#![forbid(unsafe_code)]

use hive_drone::{Config, Drone};
use std::io::Read;
use std::process::ExitCode;

const USAGE: &str = "usage: hive-drone --listen unix:PATH --secret-stdin [--shell PATH] [--session-shell PATH] [--workdir PATH] [--uid N] [--gid N] [--root PATH]...";

struct Args {
    listen: String,
    cfg: Config,
}

fn parse() -> Result<Args, String> {
    let mut listen = None;
    let mut secret_stdin = false;
    let mut cfg = Config::default();
    let mut roots = Vec::new();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = |name: &str| args.next().ok_or_else(|| format!("{name} needs a value"));
        match arg.as_str() {
            "--listen" => listen = Some(value("--listen")?),
            "--secret-stdin" => secret_stdin = true,
            "--shell" => cfg.shell = value("--shell")?.into(),
            "--session-shell" => cfg.session_shell = value("--session-shell")?.into(),
            "--workdir" => cfg.workdir = value("--workdir")?.into(),
            "--uid" => cfg.uid = Some(value("--uid")?.parse().map_err(|e| format!("--uid: {e}"))?),
            "--gid" => cfg.gid = Some(value("--gid")?.parse().map_err(|e| format!("--gid: {e}"))?),
            "--root" => {
                let root: std::path::PathBuf = value("--root")?.into();
                if !root.is_absolute() {
                    return Err(format!("--root {} is not an absolute path", root.display()));
                }
                roots.push(root);
            }
            other => return Err(format!("unknown argument {other}")),
        }
    }
    if !roots.is_empty() {
        cfg.roots = roots;
    }
    if !secret_stdin {
        return Err("--secret-stdin is required".into());
    }
    let listen = listen.ok_or("--listen is required")?;
    Ok(Args { listen, cfg })
}

fn main() -> ExitCode {
    if matches!(std::env::args().nth(1).as_deref(), Some("--version" | "-V")) {
        println!("hive-drone {}", env!("CARGO_PKG_VERSION"));
        return ExitCode::SUCCESS;
    }
    let args = match parse() {
        Ok(args) => args,
        Err(e) => {
            eprintln!("hive-drone: {e}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    let mut secret = [0u8; 32];
    if let Err(e) = std::io::stdin().read_exact(&mut secret) {
        eprintln!("hive-drone: reading the secret from stdin: {e}");
        return ExitCode::FAILURE;
    }
    let rt = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("hive-drone: starting the runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(serve(args, secret)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("hive-drone: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn serve(args: Args, secret: [u8; 32]) -> std::io::Result<()> {
    let Some(path) = args.listen.strip_prefix("unix:") else {
        return Err(std::io::Error::other(format!(
            "cannot listen on {}, only unix:PATH is supported so far",
            args.listen
        )));
    };
    // A socket left by an earlier run would make bind fail.
    let _ = std::fs::remove_file(path);
    let listener = tokio::net::UnixListener::bind(path)?;
    let drone = Drone::new(args.cfg, secret);
    loop {
        let (conn, _) = listener.accept().await?;
        let drone = drone.clone();
        tokio::spawn(async move {
            if let Err(e) = drone.serve(conn).await {
                eprintln!("hive-drone: connection ended: {e}");
            }
        });
    }
}
