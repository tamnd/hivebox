//! The `hive-drone` binary.
//!
//! ```text
//! hive-drone [--init] --listen (unix:PATH | fd:N) (--secret-stdin | --secret-file PATH)
//!            [--harden] [--protect PATH]... [--env KEY=VALUE]... [--shell /bin/sh]
//!            [--session-shell /bin/bash] [--workdir /] [--uid N] [--gid N] [--root PATH]...
//! ```
//!
//! The first secret is 32 raw bytes, read from stdin or from a file that is removed once read, so
//! it never shows up in the process list or the environment of a command. `--init`, which has to
//! come first, runs the drone under a small PID 1 that waits on orphans. `--harden` applies
//! Landlock and seccomp before serving, and makes each `--protect` path read only. `--env` adds
//! to the environment every command starts with. `--listen fd:N` takes a socket that is already
//! listening from its parent, which is how a container gets one bound outside it.

// Unsafe is denied everywhere but the one call that takes the inherited socket in `bind`.
#![deny(unsafe_code)]

#[cfg(target_os = "linux")]
use hive_drone::{Config, Drone};
#[cfg(target_os = "linux")]
use std::io::Read;
#[cfg(target_os = "linux")]
use std::process::ExitCode;

#[cfg(target_os = "linux")]
const USAGE: &str = "usage: hive-drone [--init] --listen (unix:PATH | fd:N) (--secret-stdin | --secret-file PATH) [--harden] [--protect PATH]... [--env KEY=VALUE]... [--shell PATH] [--session-shell PATH] [--workdir PATH] [--uid N] [--gid N] [--root PATH]...";

#[cfg(target_os = "linux")]
struct Args {
    listen: String,
    secret: Option<std::path::PathBuf>,
    harden: bool,
    protect: Vec<std::path::PathBuf>,
    cfg: Config,
}

#[cfg(target_os = "linux")]
fn parse() -> Result<Args, String> {
    let mut listen = None;
    let mut secret_stdin = false;
    let mut secret = None;
    let mut harden = false;
    let mut protect = Vec::new();
    let mut cfg = Config::default();
    let mut roots = Vec::new();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = |name: &str| args.next().ok_or_else(|| format!("{name} needs a value"));
        match arg.as_str() {
            "--listen" => listen = Some(value("--listen")?),
            "--secret-stdin" => secret_stdin = true,
            "--secret-file" => secret = Some(value("--secret-file")?.into()),
            "--harden" => harden = true,
            "--protect" => {
                let path: std::path::PathBuf = value("--protect")?.into();
                if !path.is_absolute() {
                    return Err(format!("--protect {} is not an absolute path", path.display()));
                }
                protect.push(path);
            }
            "--env" => {
                let pair = value("--env")?;
                let Some((key, val)) = pair.split_once('=').filter(|(k, _)| !k.is_empty()) else {
                    return Err(format!("--env {pair} is not KEY=VALUE"));
                };
                // A later value for the same key wins, as in a shell.
                cfg.base_env.retain(|(k, _)| k != key);
                cfg.base_env.push((key.into(), val.into()));
            }
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
    if secret_stdin == secret.is_some() {
        return Err("give one of --secret-stdin and --secret-file".into());
    }
    if !protect.is_empty() && !harden {
        return Err("--protect needs --harden".into());
    }
    let listen = listen.ok_or("--listen is required")?;
    Ok(Args { listen, secret, harden, protect, cfg })
}

#[cfg(target_os = "linux")]
fn main() -> ExitCode {
    if matches!(std::env::args().nth(1).as_deref(), Some("--version" | "-V")) {
        println!("hive-drone {}", env!("CARGO_PKG_VERSION"));
        return ExitCode::SUCCESS;
    }
    if std::env::args_os().nth(1).is_some_and(|a| a == "--init") {
        return hive_drone::init::run(std::env::args_os().skip(2).collect());
    }
    let args = match parse() {
        Ok(args) => args,
        Err(e) => {
            eprintln!("hive-drone: {e}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    let secret = match read_secret(args.secret.as_deref()) {
        Ok(secret) => secret,
        Err(e) => {
            eprintln!("hive-drone: reading the secret: {e}");
            return ExitCode::FAILURE;
        }
    };
    let listener = match bind(&args.listen) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("hive-drone: {e}");
            return ExitCode::FAILURE;
        }
    };
    // Before the runtime starts, since Landlock and seccomp bind only the calling thread and the
    // runtime's threads have to start out bound too.
    if args.harden {
        match hive_drone::harden::apply(&args.protect) {
            Ok(h) => eprintln!(
                "hive-drone: hardened, landlock {}, {} syscalls allowed, {} refused",
                h.landlock, h.allowed, h.denied
            ),
            Err(e) => {
                eprintln!("hive-drone: hardening: {e}");
                return ExitCode::FAILURE;
            }
        }
    }
    let rt = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("hive-drone: starting the runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(serve(listener, args.cfg, secret)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("hive-drone: {e}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(target_os = "linux")]
fn read_secret(file: Option<&std::path::Path>) -> std::io::Result<[u8; 32]> {
    let mut secret = [0u8; 32];
    match file {
        None => std::io::stdin().read_exact(&mut secret)?,
        Some(path) => {
            let read = std::fs::File::open(path).and_then(|mut f| f.read_exact(&mut secret));
            // Gone whether or not it was good, so a second reader never finds it.
            let removed = std::fs::remove_file(path);
            read?;
            removed?;
        }
    }
    Ok(secret)
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
fn bind(listen: &str) -> std::io::Result<std::os::unix::net::UnixListener> {
    use std::os::fd::{FromRawFd, OwnedFd, RawFd};
    let listener = if let Some(fd) = listen.strip_prefix("fd:") {
        let fd: RawFd =
            fd.parse().ok().filter(|n| *n > 2).ok_or_else(|| {
                std::io::Error::other(format!("{listen} is not an inherited socket"))
            })?;
        rustix::io::fcntl_getfd(
            // SAFETY: only borrowed here, to check that the descriptor is open at all.
            unsafe { std::os::fd::BorrowedFd::borrow_raw(fd) },
        )
        .map_err(|e| std::io::Error::other(format!("{listen}: {e}")))?;
        // SAFETY: the parent passed this descriptor down for the drone alone, it is open as the
        // check above found, and nothing else in this process has taken it.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        // Commands the drone starts must not get the socket too.
        rustix::io::fcntl_setfd(&fd, rustix::io::FdFlags::CLOEXEC)?;
        std::os::unix::net::UnixListener::from(fd)
    } else if let Some(path) = listen.strip_prefix("unix:") {
        // A socket left by an earlier run would make bind fail.
        let _ = std::fs::remove_file(path);
        std::os::unix::net::UnixListener::bind(path)?
    } else {
        return Err(std::io::Error::other(format!(
            "cannot listen on {listen}, only unix:PATH and fd:N are supported"
        )));
    };
    listener.set_nonblocking(true)?;
    Ok(listener)
}

#[cfg(target_os = "linux")]
async fn serve(
    listener: std::os::unix::net::UnixListener,
    cfg: Config,
    secret: [u8; 32],
) -> std::io::Result<()> {
    let listener = tokio::net::UnixListener::from_std(listener)?;
    let drone = Drone::new(cfg, secret);
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

#[cfg(not(target_os = "linux"))]
fn main() -> std::process::ExitCode {
    eprintln!("hive-drone runs only on Linux");
    std::process::ExitCode::FAILURE
}
