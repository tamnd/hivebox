//! The `hive-uffd` binary.
//!
//! ```text
//! hive-uffd --socket PATH --memory FILE [--trace FILE]
//! ```
//!
//! It listens on `PATH` and serves every VMM that connects from the memory file `FILE`, one thread
//! per VM. With `--trace`, each restore first fills in the pages in the trace file. While there is
//! no trace file yet, the first restore to end writes the pages it faulted in there, so the ones
//! after it start from them.
//!
//! When `FILE` is on tmpfs or hugetlbfs, a VMM that maps it privately and registers for minor
//! faults gets its pages mapped in from the page cache, not copied, so all its VMs share them.

#![forbid(unsafe_code)]

use std::process::ExitCode;

const USAGE: &str = "usage: hive-uffd --socket PATH --memory FILE [--trace FILE]";

#[cfg(target_os = "linux")]
fn main() -> ExitCode {
    use hive_uffd::{Memory, Session, Trace};
    use std::os::unix::net::UnixListener;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    let (mut socket, mut memory, mut trace) = (None, None, None);
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let slot = match arg.as_str() {
            "--socket" => &mut socket,
            "--memory" => &mut memory,
            "--trace" => &mut trace,
            "--version" | "-V" => {
                println!("hive-uffd {}", env!("CARGO_PKG_VERSION"));
                return ExitCode::SUCCESS;
            }
            other => {
                eprintln!("hive-uffd: unknown argument {other}\n{USAGE}");
                return ExitCode::from(2);
            }
        };
        *slot = it.next().map(PathBuf::from);
    }
    let (Some(socket), Some(memory)) = (socket, memory) else {
        eprintln!("hive-uffd: {USAGE}");
        return ExitCode::from(2);
    };
    let memory = match Memory::open(&memory) {
        Ok(m) => Arc::new(m),
        Err(e) => {
            eprintln!("hive-uffd: {}: {e}", memory.display());
            return ExitCode::FAILURE;
        }
    };
    let _ = std::fs::remove_file(&socket);
    let listener = match UnixListener::bind(&socket) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("hive-uffd: {}: {e}", socket.display());
            return ExitCode::FAILURE;
        }
    };
    let saved = Arc::new(Mutex::new(trace.as_deref().and_then(|p| Trace::load(p).ok())));
    let mut vm = 0u64;
    for stream in listener.incoming() {
        let stream = match stream {
            Ok(s) => s,
            Err(e) => {
                eprintln!("hive-uffd: accept: {e}");
                continue;
            }
        };
        vm += 1;
        let (memory, saved, path) = (memory.clone(), saved.clone(), trace.clone());
        std::thread::spawn(move || {
            let mut s = match Session::accept(stream, memory) {
                Ok(s) => s,
                Err(e) => return eprintln!("hive-uffd: vm {vm}: {e}"),
            };
            let started = std::time::Instant::now();
            let prefetch = saved.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
            if let Some(t) = &prefetch
                && let Err(e) = s.prefetch(t)
            {
                eprintln!("hive-uffd: vm {vm}: prefetch: {e}");
            }
            if let Err(e) = s.serve() {
                eprintln!("hive-uffd: vm {vm}: {e}");
            }
            let st = s.stats();
            eprintln!(
                "hive-uffd: vm {vm} ended after {:.1} s: {} faults, {} zero, {} MiB prefetched, slowest {} us",
                started.elapsed().as_secs_f64(),
                st.faults,
                st.zero_faults,
                st.prefetched >> 20,
                st.slowest.as_micros()
            );
            let mut saved = saved.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if let (None, Some(path)) = (saved.as_ref(), path) {
                let t = s.trace();
                match t.save(&path) {
                    Ok(()) => *saved = Some(t),
                    Err(e) => eprintln!("hive-uffd: {}: {e}", path.display()),
                }
            }
        });
    }
    ExitCode::SUCCESS
}

#[cfg(not(target_os = "linux"))]
fn main() -> ExitCode {
    eprintln!("hive-uffd only runs on Linux\n{USAGE}");
    ExitCode::FAILURE
}
