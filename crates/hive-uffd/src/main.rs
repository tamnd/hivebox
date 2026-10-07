//! The `hive-uffd` binary.
//!
//! ```text
//! hive-uffd --socket PATH --memory FILE [--trace FILE]
//! ```
//!
//! It listens on `PATH` and serves every VMM that connects from the memory file `FILE`, one thread
//! per VM. With `--trace`, each restore first fills in the pages in the trace file. While there is
//! no trace file yet, the first restore to end writes the pages it faulted in there, so the ones
//! after it start from them. After that, a page later restores fault on joins the trace once two
//! of them have, and the file is written again.
//!
//! When `FILE` is on tmpfs or hugetlbfs, a VMM that maps it privately and registers for minor
//! faults gets its pages mapped in from the page cache, not copied, so all its VMs share them.
//!
//! The process that listens is a watchdog and serves nothing itself. It reads each VMM's regions
//! and userfaultfd, keeps a copy of both, and hands the VM to a worker, which is this binary run
//! with `--worker` and the control socket as its stdin. When the worker dies, the watchdog starts
//! another and hands it every VM that has not hung up, along with the memory each one's guest gave
//! back, so the VMs only see their faults wait. The
//! new worker starts at once, unless the last one lived less than 10 s too, and then after a pause
//! that doubles from 100 ms up to 5 s.

#![forbid(unsafe_code)]

use std::process::ExitCode;

const USAGE: &str = "usage: hive-uffd --socket PATH --memory FILE [--trace FILE]";

#[cfg(target_os = "linux")]
fn main() -> ExitCode {
    use std::path::PathBuf;

    let (mut socket, mut memory, mut trace, mut worker) = (None, None, None, false);
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let slot = match arg.as_str() {
            "--socket" => &mut socket,
            "--memory" => &mut memory,
            "--trace" => &mut trace,
            "--worker" => {
                worker = true;
                continue;
            }
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
    match (socket, memory) {
        (_, Some(memory)) if worker => linux::work(&memory, trace),
        (Some(socket), Some(memory)) => linux::watch(&socket, &memory, trace),
        _ => {
            eprintln!("hive-uffd: {USAGE}");
            ExitCode::from(2)
        }
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use hive_uffd::{Hello, Memory, Merged, Report, Session, Trace};
    use std::collections::BTreeMap;
    use std::os::fd::{AsFd, OwnedFd};
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::path::{Path, PathBuf};
    use std::process::{Command, ExitCode, Stdio};
    use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
    use std::time::{Duration, Instant};

    /// How many restores have to miss a page before the trace takes it in.
    const MISSED_IN: u32 = 2;
    /// How long a worker has to live for its death to be taken as bad luck, not a crash loop.
    const SETTLED: Duration = Duration::from_secs(10);
    const SHORTEST_PAUSE: Duration = Duration::from_millis(100);
    const LONGEST_PAUSE: Duration = Duration::from_secs(5);

    /// What the watchdog knows: the worker's control socket while one runs, and every VM that has
    /// not hung up, by number.
    #[derive(Default)]
    struct Watch {
        worker: Option<UnixStream>,
        live: BTreeMap<u64, Hello>,
    }

    fn lock(m: &Mutex<Watch>) -> MutexGuard<'_, Watch> {
        m.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn watch(socket: &Path, memory: &Path, trace: Option<PathBuf>) -> ExitCode {
        // The worker opens the file itself, and this only says early when it cannot.
        if let Err(e) = Memory::open(memory) {
            eprintln!("hive-uffd: {}: {e}", memory.display());
            return ExitCode::FAILURE;
        }
        let _ = std::fs::remove_file(socket);
        let listener = match UnixListener::bind(socket) {
            Ok(l) => l,
            Err(e) => {
                eprintln!("hive-uffd: {}: {e}", socket.display());
                return ExitCode::FAILURE;
            }
        };
        let exe = match std::env::current_exe() {
            Ok(exe) => exe,
            Err(e) => {
                eprintln!("hive-uffd: finding this binary: {e}");
                return ExitCode::FAILURE;
            }
        };
        let state = Arc::new(Mutex::new(Watch::default()));
        let accepting = state.clone();
        std::thread::spawn(move || accept(&listener, &accepting));
        let mut pause = Duration::ZERO;
        loop {
            let started = Instant::now();
            if let Err(e) = run_worker(&exe, memory, trace.as_deref(), &state) {
                eprintln!("hive-uffd: worker: {e}");
            }
            if started.elapsed() >= SETTLED {
                pause = Duration::ZERO;
            } else {
                std::thread::sleep(pause);
                pause = (pause * 2).clamp(SHORTEST_PAUSE, LONGEST_PAUSE);
            }
        }
    }

    /// Takes VMMs as they connect, keeps each one's regions and userfaultfd, and hands it to the
    /// worker. A VM that cannot be handed over now goes to the next worker.
    fn accept(listener: &UnixListener, state: &Arc<Mutex<Watch>>) {
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
            let (id, state) = (vm, state.clone());
            // A VMM that connects and says nothing holds up only its own thread.
            std::thread::spawn(move || {
                let hello = match Hello::read(stream) {
                    Ok(h) => h,
                    Err(e) => return eprintln!("hive-uffd: vm {id}: {e}"),
                };
                let mut st = lock(&state);
                if let Some(w) = &st.worker
                    && let Err(e) = hello.send(w, id)
                {
                    eprintln!("hive-uffd: vm {id}: handing it to the worker: {e}");
                }
                st.live.insert(id, hello);
            });
        }
    }

    /// Starts a worker, hands it every live VM, and waits for it to end.
    fn run_worker(
        exe: &Path,
        memory: &Path,
        trace: Option<&Path>,
        state: &Arc<Mutex<Watch>>,
    ) -> std::io::Result<()> {
        let (ours, theirs) = hive_uffd::pair()?;
        let mut cmd = Command::new(exe);
        cmd.arg("--worker").arg("--memory").arg(memory);
        if let Some(t) = trace {
            cmd.arg("--trace").arg(t);
        }
        cmd.stdin(Stdio::from(OwnedFd::from(theirs)));
        let mut child = cmd.spawn()?;
        // The worker's end has to be closed here too, or its death never reads as a hang up.
        drop(cmd);
        let pid = child.id();
        {
            let mut st = lock(state);
            for (id, hello) in &st.live {
                if let Err(e) = hello.send(&ours, *id) {
                    eprintln!("hive-uffd: vm {id}: handing it to worker {pid}: {e}");
                }
            }
            if !st.live.is_empty() {
                eprintln!("hive-uffd: worker {pid} took over {} VMs", st.live.len());
            }
            st.worker = Some(ours.try_clone()?);
        }
        let ended = state.clone();
        let reader = std::thread::spawn(move || {
            while let Ok(Some(report)) = Report::receive(&ours) {
                let mut st = lock(&ended);
                match report {
                    Report::Done(id) => drop(st.live.remove(&id)),
                    Report::Removed { id, start, end } => {
                        if let Some(hello) = st.live.get_mut(&id) {
                            hello.gave_back(start, end);
                        }
                    }
                }
            }
        });
        let status = child.wait()?;
        lock(state).worker = None;
        let _ = reader.join();
        eprintln!("hive-uffd: worker {pid} ended: {status}");
        Ok(())
    }

    pub(super) fn work(memory: &Path, trace: Option<PathBuf>) -> ExitCode {
        let control = match std::io::stdin().as_fd().try_clone_to_owned() {
            Ok(fd) => Arc::new(UnixStream::from(fd)),
            Err(e) => {
                eprintln!("hive-uffd: worker: the control socket: {e}");
                return ExitCode::FAILURE;
            }
        };
        let memory = match Memory::open(memory) {
            Ok(m) => Arc::new(m),
            Err(e) => {
                eprintln!("hive-uffd: {}: {e}", memory.display());
                return ExitCode::FAILURE;
            }
        };
        let loaded = trace.as_deref().and_then(|p| Trace::load(p).ok());
        let saved = Arc::new(Mutex::new(Merged::new(loaded, MISSED_IN)));
        let mut serving = Vec::new();
        loop {
            let (vm, hello) = match Hello::receive(&control) {
                Ok(Some(v)) => v,
                // The watchdog is gone, so no more VMs come. The ones here are served to the end.
                Ok(None) => break,
                Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
                    eprintln!("hive-uffd: worker: {e}");
                    continue;
                }
                Err(e) => {
                    eprintln!("hive-uffd: worker: {e}");
                    break;
                }
            };
            let (memory, saved, path, control) =
                (memory.clone(), saved.clone(), trace.clone(), control.clone());
            serving.push(std::thread::spawn(move || {
                serve(vm, hello, memory, &saved, path.as_deref(), &control);
                let _ = Report::Done(vm).send(&control);
            }));
        }
        for t in serving {
            let _ = t.join();
        }
        ExitCode::SUCCESS
    }

    fn serve(
        vm: u64,
        hello: Hello,
        memory: Arc<Memory>,
        saved: &Mutex<Merged>,
        path: Option<&Path>,
        control: &Arc<UnixStream>,
    ) {
        let mut s = match Session::start(hello, memory) {
            Ok(s) => s,
            Err(e) => return eprintln!("hive-uffd: vm {vm}: {e}"),
        };
        s.report_to(control.clone(), vm);
        let started = Instant::now();
        let prefetch = saved.lock().unwrap_or_else(PoisonError::into_inner).trace().cloned();
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
        let mut saved = saved.lock().unwrap_or_else(PoisonError::into_inner);
        if saved.add(&s.trace()) > 0
            && let (Some(t), Some(path)) = (saved.trace(), path)
            && let Err(e) = t.save(path)
        {
            eprintln!("hive-uffd: {}: {e}", path.display());
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn main() -> ExitCode {
    eprintln!("hive-uffd only runs on Linux\n{USAGE}");
    ExitCode::FAILURE
}
