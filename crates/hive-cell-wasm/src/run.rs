//! Running one command in a wasm cell: a new instance of the program its first word names, with
//! the cell's directory as `/`, a memory limit, a time limit and its output kept the way the
//! drone keeps a process's.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime};

use bytes::Bytes;
use futures::future::BoxFuture;
use hive_drone::{Ring, Runner};
use hive_proto::drone::api::{RunRequest, RunResult};
use hive_types::{Error, Reason};
use tokio::io::AsyncWrite;
use tokio::sync::{OnceCell, watch};
use wasmtime::{Engine, InstancePre, Linker, Module, ResourceLimiter, Store, Trap};
use wasmtime_wasi::cli::{IsTerminal, StdoutStream};
use wasmtime_wasi::p2::pipe::MemoryInputPipe;
use wasmtime_wasi::p2::{OutputStream, Pollable, StreamResult};
use wasmtime_wasi::preview1::WasiP1Ctx;
use wasmtime_wasi::{DirPerms, FilePerms, I32Exit, WasiCtxBuilder};

use crate::words;

/// The exit code of a program that trapped, as the `wasmtime` command gives it.
pub(crate) const TRAPPED: i32 = 134;
/// The most table elements a program may grow to.
const MAX_TABLE: usize = 1 << 20;
/// The most of a trap's message kept on stderr.
const MAX_TRAP: usize = 4096;
const SIGKILL: i32 = 9;

/// What every wasm cell on the node shares: the engine, the programs compiled so far and where
/// they come from.
pub(crate) struct Shared {
    pub(crate) engine: Engine,
    pub(crate) linker: Linker<Ctx>,
    pub(crate) modules: PathBuf,
    pub(crate) mounts: Vec<(String, PathBuf)>,
    programs: Mutex<HashMap<String, Arc<Entry>>>,
}

impl fmt::Debug for Shared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared").field("modules", &self.modules).finish_non_exhaustive()
    }
}

/// A program, compiled once and instantiated for every run.
pub(crate) struct Program {
    pre: InstancePre<Ctx>,
}

// One program file as it was when it was compiled. A file that changes is compiled again.
struct Entry {
    stamp: (u64, SystemTime),
    compiled: OnceCell<Result<Arc<Program>, String>>,
}

/// Why a program could not be had.
#[derive(Debug)]
pub(crate) enum Missing {
    /// There is no file for it.
    NotFound,
    /// There is one, and it is not a module wasmtime takes.
    Bad(String),
}

impl Shared {
    pub(crate) fn new(
        engine: Engine,
        linker: Linker<Ctx>,
        modules: PathBuf,
        mounts: Vec<(String, PathBuf)>,
    ) -> Self {
        Self { engine, linker, modules, mounts, programs: Mutex::new(HashMap::new()) }
    }

    /// The program called `name`, from `name.wasm` in the modules directory. The first cell to
    /// ask compiles it, and the rest wait for that.
    pub(crate) async fn program(&self, name: &str) -> Result<Arc<Program>, Missing> {
        if !is_program_name(name) {
            return Err(Missing::NotFound);
        }
        let path = self.modules.join(format!("{name}.wasm"));
        let meta = match tokio::fs::metadata(&path).await {
            Ok(m) if m.is_file() => m,
            Ok(_) => return Err(Missing::NotFound),
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Err(Missing::NotFound),
            Err(e) => return Err(Missing::Bad(e.to_string())),
        };
        let stamp = (meta.len(), meta.modified().unwrap_or(SystemTime::UNIX_EPOCH));
        let entry = {
            let mut programs = self.programs.lock().unwrap_or_else(PoisonError::into_inner);
            let entry = programs
                .entry(name.to_owned())
                .or_insert_with(|| Arc::new(Entry { stamp, compiled: OnceCell::new() }));
            if entry.stamp != stamp {
                *entry = Arc::new(Entry { stamp, compiled: OnceCell::new() });
            }
            entry.clone()
        };
        let compiled = entry
            .compiled
            .get_or_init(|| async {
                let engine = self.engine.clone();
                let linker = self.linker.clone();
                let compile = move || -> Result<Arc<Program>, String> {
                    let module = Module::from_file(&engine, &path).map_err(|e| format!("{e:#}"))?;
                    let pre = linker.instantiate_pre(&module).map_err(|e| format!("{e:#}"))?;
                    Ok(Arc::new(Program { pre }))
                };
                tokio::task::spawn_blocking(compile).await.unwrap_or_else(|e| Err(e.to_string()))
            })
            .await;
        compiled.clone().map_err(Missing::Bad)
    }
}

/// A plain file name with no path in it.
pub(crate) fn is_program_name(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with('.')
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b"._-+".contains(&b))
}

/// The program a command's first word names: the last part of a path, without `.wasm`, so
/// `python`, `/usr/bin/python` and `python.wasm` are all `python`.
fn program_name(word: &str) -> &str {
    let base = Path::new(word).file_name().and_then(|n| n.to_str()).unwrap_or(word);
    base.strip_suffix(".wasm").unwrap_or(base)
}

/// What one cell has used, read by `CellDriver::metrics`.
#[derive(Debug, Default)]
pub(crate) struct Usage {
    pub(crate) mem: AtomicU64,
    pub(crate) peak: AtomicU64,
    pub(crate) running: AtomicU64,
    pub(crate) cpu_nanos: AtomicU64,
}

/// The store's data for one run.
pub(crate) struct Ctx {
    wasi: WasiP1Ctx,
    limits: Limits,
}

impl Ctx {
    pub(crate) fn wasi(&mut self) -> &mut WasiP1Ctx {
        &mut self.wasi
    }
}

// Holds a run to the cell's memory and counts what it takes.
struct Limits {
    max: usize,
    mine: u64,
    usage: Arc<Usage>,
}

impl ResourceLimiter for Limits {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        if desired > self.max {
            return Ok(false);
        }
        let grown = desired.saturating_sub(current) as u64;
        self.mine += grown;
        let now = self.usage.mem.fetch_add(grown, Ordering::Relaxed) + grown;
        self.usage.peak.fetch_max(now, Ordering::Relaxed);
        Ok(true)
    }

    fn table_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        Ok(desired <= MAX_TABLE)
    }
}

impl Drop for Limits {
    fn drop(&mut self) {
        self.usage.mem.fetch_sub(self.mine, Ordering::Relaxed);
    }
}

/// One stream of a run's output. It never refuses a write, so a program that writes more than
/// is kept still runs to the end, and what it wrote is cut as the drone cuts a process's.
#[derive(Clone)]
struct Capture(Arc<Mutex<Ring>>);

impl Capture {
    fn new(limit: usize) -> Self {
        Self(Arc::new(Mutex::new(Ring::new(limit))))
    }

    fn push(&self, data: &[u8]) {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).push(data);
    }

    /// What was kept, whether anything was dropped, and how much was written.
    fn take(&self) -> (Bytes, bool, u64) {
        let mut ring = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        (ring.take(), ring.truncated(), ring.total())
    }
}

impl IsTerminal for Capture {
    fn is_terminal(&self) -> bool {
        false
    }
}

impl StdoutStream for Capture {
    fn p2_stream(&self) -> Box<dyn OutputStream> {
        Box::new(self.clone())
    }

    fn async_stream(&self) -> Box<dyn AsyncWrite + Send + Sync> {
        Box::new(self.clone())
    }
}

#[wasmtime_wasi::async_trait]
impl Pollable for Capture {
    async fn ready(&mut self) {}
}

impl OutputStream for Capture {
    fn write(&mut self, bytes: Bytes) -> StreamResult<()> {
        self.push(&bytes);
        Ok(())
    }

    fn flush(&mut self) -> StreamResult<()> {
        Ok(())
    }

    fn check_write(&mut self) -> StreamResult<usize> {
        Ok(1 << 20)
    }
}

impl AsyncWrite for Capture {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.push(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Runs one cell's commands, for its drone.
#[derive(Clone, Debug)]
pub(crate) struct CellRunner(Arc<CellInner>);

#[derive(Debug)]
struct CellInner {
    shared: Arc<Shared>,
    root: PathBuf,
    env: Vec<(String, String)>,
    mem: usize,
    defaults: hive_drone::Config,
    usage: Arc<Usage>,
    stopped: watch::Receiver<bool>,
}

impl CellRunner {
    pub(crate) fn new(
        shared: Arc<Shared>,
        root: PathBuf,
        env: Vec<(String, String)>,
        mem: usize,
        usage: Arc<Usage>,
        stopped: watch::Receiver<bool>,
    ) -> Self {
        let defaults = hive_drone::Config::default();
        Self(Arc::new(CellInner { shared, root, env, mem, defaults, usage, stopped }))
    }
}

impl Runner for CellRunner {
    fn run(&self, req: RunRequest) -> BoxFuture<'static, Result<RunResult, Error>> {
        let inner = self.0.clone();
        Box::pin(async move { inner.run(req).await })
    }
}

// How a run ended, short of the program returning.
enum Cut {
    TimedOut,
    Stopped,
}

impl CellInner {
    async fn run(&self, req: RunRequest) -> Result<RunResult, Error> {
        let started = Instant::now();
        let c = req.command.unwrap_or_default();
        if !c.cwd.is_empty() && c.cwd != "/" {
            return Err(Error::new(
                Reason::InvalidArgument,
                "wasm programs start in /, so give them paths from there and no working directory",
            ));
        }
        let argv = if c.argv.is_empty() { words::split(&c.shell)? } else { c.argv };
        let Some(first) = argv.first() else {
            return Err(Error::new(Reason::InvalidArgument, "the command is empty"));
        };
        let name = program_name(first);
        let program = match self.shared.program(name).await {
            Ok(p) => p,
            Err(Missing::NotFound) => {
                return Ok(failed(127, &format!("{first}: no such program"), started));
            }
            Err(Missing::Bad(e)) => return Ok(failed(126, &format!("{first}: {e}"), started)),
        };
        let limit = if c.max_output_bytes == 0 {
            self.defaults.default_output
        } else {
            usize::try_from(c.max_output_bytes).unwrap_or(usize::MAX)
        }
        .min(self.defaults.max_output);
        let timeout = if c.timeout_ms == 0 {
            self.defaults.default_timeout
        } else {
            Duration::from_millis(c.timeout_ms)
        };
        let (out, err) = (Capture::new(limit), Capture::new(limit));
        let wasi = {
            let mut b = WasiCtxBuilder::new();
            b.args(&argv).stdin(MemoryInputPipe::new(req.stdin)).stdout(out.clone());
            b.stderr(err.clone());
            // The command's own variables win over the cell's.
            let mut env: BTreeMap<&str, &str> =
                self.env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            env.extend(c.env.iter().map(|(k, v)| (k.as_str(), v.as_str())));
            for (k, v) in env {
                b.env(k, v);
            }
            let fs = |e: wasmtime::Error| Error::new(Reason::Internal, format!("{e:#}"));
            b.preopened_dir(&self.root, "/", DirPerms::all(), FilePerms::all()).map_err(fs)?;
            for (guest, host) in &self.shared.mounts {
                b.preopened_dir(host, guest, DirPerms::READ, FilePerms::READ).map_err(fs)?;
            }
            b.build_p1()
        };
        let limits = Limits { max: self.mem, mine: 0, usage: self.usage.clone() };
        let mut store = Store::new(&self.shared.engine, Ctx { wasi, limits });
        store.limiter(|c| &mut c.limits);
        // Give the thread back at every tick, so a busy program shares the runtime's threads
        // and can be cut off at its timeout or when its cell stops.
        store.epoch_deadline_async_yield_and_update(1);
        let _running = Running::new(&self.usage);
        let mut stopped = self.stopped.clone();
        let call = Metered {
            inner: Box::pin(async {
                let instance = match program.pre.instantiate_async(&mut store).await {
                    Ok(i) => i,
                    Err(e) => return Err((126, format!("{first} cannot start: {e:#}"))),
                };
                let Ok(main) = instance.get_typed_func::<(), ()>(&mut store, "_start") else {
                    return Err((126, format!("{first} is not a WASI command, it has no _start")));
                };
                match main.call_async(&mut store, ()).await {
                    Ok(()) => Ok(0),
                    Err(e) => match e.downcast_ref::<I32Exit>() {
                        Some(exit) => Ok(exit.0),
                        None if e.downcast_ref::<Trap>().is_some() => {
                            Err((TRAPPED, format!("{first}: wasm trap: {e:#}")))
                        }
                        None => Err((TRAPPED, format!("{first}: {e:#}"))),
                    },
                }
            }),
            usage: &self.usage,
        };
        let ended = tokio::select! {
            r = tokio::time::timeout(timeout, call) => r.map_err(|_| Cut::TimedOut),
            _ = stopped.wait_for(|s| *s) => Err(Cut::Stopped),
        };
        drop(store);
        let (exit_code, signal, timed_out) = match ended {
            Ok(Ok(code)) => (code, 0, false),
            Ok(Err((code, message))) => {
                let mut message = message.into_bytes();
                message.truncate(MAX_TRAP);
                message.push(b'\n');
                err.push(&message);
                (code, 0, false)
            }
            Err(Cut::TimedOut) => (-1, SIGKILL, true),
            Err(Cut::Stopped) => {
                return Err(Error::new(Reason::CellNotRunning, "the cell stopped"));
            }
        };
        let (stdout, out_cut, stdout_bytes) = out.take();
        let (stderr, err_cut, stderr_bytes) = err.take();
        Ok(RunResult {
            exit_code,
            signal,
            stdout,
            stderr,
            truncated: out_cut || err_cut,
            timed_out,
            wall_nanos: started.elapsed().as_nanos() as u64,
            stdout_bytes,
            stderr_bytes,
        })
    }
}

// What a shell reports for a program it cannot run, with the reason on stderr.
fn failed(exit_code: i32, message: &str, started: Instant) -> RunResult {
    let message = format!("{message}\n");
    RunResult {
        exit_code,
        stderr_bytes: message.len() as u64,
        stderr: message.into(),
        wall_nanos: started.elapsed().as_nanos() as u64,
        ..RunResult::default()
    }
}

// Counts a run as running for as long as it lives.
struct Running<'a>(&'a Usage);

impl<'a> Running<'a> {
    fn new(usage: &'a Usage) -> Self {
        usage.running.fetch_add(1, Ordering::Relaxed);
        Self(usage)
    }
}

impl Drop for Running<'_> {
    fn drop(&mut self) {
        self.0.running.fetch_sub(1, Ordering::Relaxed);
    }
}

// Adds the CPU time the thread spends in each poll of `inner` to the cell's. A program only runs
// inside a poll, so this is its CPU time, whichever threads it ran on.
struct Metered<'a, F> {
    inner: Pin<Box<F>>,
    usage: &'a Usage,
}

impl<F: Future> Future for Metered<'_, F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let before = thread_cpu();
        let out = self.inner.as_mut().poll(cx);
        self.usage.cpu_nanos.fetch_add(thread_cpu().saturating_sub(before), Ordering::Relaxed);
        out
    }
}

fn thread_cpu() -> u64 {
    let t = rustix::time::clock_gettime(rustix::time::ClockId::ThreadCPUTime);
    (t.tv_sec as u64).saturating_mul(1_000_000_000).saturating_add(t.tv_nsec as u64)
}

#[cfg(test)]
mod tests {
    use super::{is_program_name, program_name};

    #[test]
    fn the_first_word_names_a_program_file() {
        assert_eq!(program_name("python"), "python");
        assert_eq!(program_name("/usr/local/bin/python"), "python");
        assert_eq!(program_name("./judge.wasm"), "judge");
        for ok in ["python", "python3.12", "g++", "a_b-c"] {
            assert!(is_program_name(ok), "{ok}");
        }
        for bad in ["", ".", "..", ".hidden", "a/b", "a b", "a\0"] {
            assert!(!is_program_name(bad), "{bad:?}");
        }
    }
}
