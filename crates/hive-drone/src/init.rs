//! A small init for when the drone is the first process in a container's PID namespace.
//!
//! PID 1 inherits every process whose parent dies, and has to wait on them or they stay as
//! zombies. The drone runs its own children through tokio, which waits on each by its pid, so it
//! cannot also wait on any child at all without taking an exit status tokio is waiting for. So
//! PID 1 is this loop instead: it starts the drone as its one child, waits on everything that
//! ends, passes the signals that ask for a stop on to the drone, and exits the way the drone did.
//! The kernel then kills whatever else is left in the namespace.

use rustix::process::{Pid, Signal as KillSignal, WaitOptions, kill_process};
use std::ffi::OsString;
use std::process::{Command, ExitCode};
use tokio::signal::unix::{SignalKind, signal};

/// Starts `/proc/self/exe` with `args` and stays its parent until it ends.
pub fn run(args: Vec<OsString>) -> ExitCode {
    // Every process in the container is a descendant and runs as the same user, so without this
    // any of them could trace PID 1. The drone does the same for itself when it hardens.
    if let Err(e) =
        rustix::process::set_dumpable_behavior(rustix::process::DumpableBehavior::NotDumpable)
    {
        eprintln!("hive-drone init: {e}");
        return ExitCode::FAILURE;
    }
    let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("hive-drone init: starting the runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    rt.block_on(async move {
        // The handlers go in before the child starts, so an exit it makes at once is not missed.
        let mut children = match signal(SignalKind::child()) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("hive-drone init: {e}");
                return ExitCode::FAILURE;
            }
        };
        let (mut term, mut int, mut hup, mut quit) = match (
            signal(SignalKind::terminate()),
            signal(SignalKind::interrupt()),
            signal(SignalKind::hangup()),
            signal(SignalKind::quit()),
        ) {
            (Ok(a), Ok(b), Ok(c), Ok(d)) => (a, b, c, d),
            _ => {
                eprintln!("hive-drone init: cannot handle the stop signals");
                return ExitCode::FAILURE;
            }
        };
        let child = match Command::new("/proc/self/exe").args(args).spawn() {
            Ok(child) => child,
            Err(e) => {
                eprintln!("hive-drone init: starting the drone: {e}");
                return ExitCode::FAILURE;
            }
        };
        let Some(drone) = i32::try_from(child.id()).ok().and_then(Pid::from_raw) else {
            return ExitCode::FAILURE;
        };
        // Reaped here by pid, never through the handle.
        drop(child);
        loop {
            if let Some(code) = reap(drone) {
                return code;
            }
            let sig = tokio::select! {
                _ = children.recv() => continue,
                Some(()) = term.recv() => KillSignal::TERM,
                Some(()) = int.recv() => KillSignal::INT,
                Some(()) = hup.recv() => KillSignal::HUP,
                Some(()) = quit.recv() => KillSignal::QUIT,
            };
            let _ = kill_process(drone, sig);
        }
    })
}

/// Waits on every child that has ended, and returns how to exit once the drone is one of them.
fn reap(drone: Pid) -> Option<ExitCode> {
    let mut out = None;
    while let Ok(Some((pid, status))) = rustix::process::wait(WaitOptions::NOHANG) {
        if pid == drone {
            let code = status
                .exit_status()
                .or_else(|| status.terminating_signal().map(|s| 128 + s))
                .unwrap_or(1);
            out = Some(ExitCode::from(u8::try_from(code).unwrap_or(1)));
        }
    }
    out
}
