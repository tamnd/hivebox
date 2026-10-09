//! Reward plugins: WebAssembly components of the world `hivebox:reward/grader`, in
//! `wit/reward.wit`, that turn a verification into a reward. A grader imports nothing and runs
//! with a memory limit and a time limit, so a task can bring its own without the node trusting it.

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime};

use hive_types::{Error, Reason};
use tokio::sync::OnceCell;
use wasmtime::component::{Component, Linker};
use wasmtime::{Engine, Store, StoreLimits, StoreLimitsBuilder, Trap};

use crate::TICK;
use crate::run::is_program_name;

#[allow(missing_docs, missing_debug_implementations, unreachable_pub, unused_qualifications)]
mod bindings {
    wasmtime::component::bindgen!({ path: "wit/reward.wit", world: "grader" });
}

use bindings::exports::hivebox::reward::score as wit;

/// How graders run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraderConfig {
    /// Where the graders are, each a component named `<name>.wasm`. A file that changes is
    /// compiled again on its next use.
    pub dir: PathBuf,
    /// The longest one grade may take.
    pub timeout: Duration,
    /// The most memory a grader may have, in MiB.
    pub mem_mib: u64,
}

impl Default for GraderConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("/var/lib/hivebox/graders"),
            timeout: Duration::from_secs(10),
            mem_mib: 256,
        }
    }
}

/// One run of the verifier's command, as a grader sees it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Run {
    /// Its exit code, or -1 when a signal or its timeout ended it.
    pub exit_code: i32,
    /// Its timeout ended it.
    pub timed_out: bool,
    /// What it wrote to stdout, as much as the node kept.
    pub stdout: Vec<u8>,
    /// The same for stderr.
    pub stderr: Vec<u8>,
    /// How long it ran, in milliseconds.
    pub wall_ms: u64,
}

/// What a grader is given.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Input {
    /// The task's own data as the caller sent it.
    pub task: Vec<u8>,
    /// Every run, in order.
    pub runs: Vec<Run>,
    /// The files the caller asked for by path, and what was in each, if it was there.
    pub files: Vec<(String, Option<Vec<u8>>)>,
    /// Every run passed.
    pub passed: bool,
    /// The protected paths the subject changed.
    pub tampered: Vec<String>,
}

/// A grader's grade.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Grade {
    /// The reward, a finite number.
    pub reward: f64,
    /// What the grader said about it.
    pub detail: String,
}

/// The node's graders, with an engine of their own.
pub struct Graders {
    engine: Engine,
    linker: Linker<State>,
    cfg: GraderConfig,
    compiled: Mutex<HashMap<String, Arc<Entry>>>,
}

impl fmt::Debug for Graders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Graders").field("cfg", &self.cfg).finish_non_exhaustive()
    }
}

struct State {
    limits: StoreLimits,
}

// One grader file as it was when it was compiled.
struct Entry {
    stamp: (u64, SystemTime),
    pre: OnceCell<Result<bindings::GraderPre<State>, String>>,
}

impl Graders {
    /// Graders as `cfg` says, with a thread that moves their engine's epoch on every [`TICK`]
    /// for as long as the engine is in use.
    ///
    /// # Errors
    ///
    /// `INVALID_ARGUMENT` when `cfg` asks for no time or no memory, or more than 4 GiB.
    pub fn new(cfg: GraderConfig) -> Result<Self, Error> {
        if cfg.timeout.is_zero() || cfg.mem_mib == 0 || cfg.mem_mib > 4096 {
            return Err(Error::new(
                Reason::InvalidArgument,
                "graders need some time, and from 1 to 4096 MiB",
            ));
        }
        let mut wc = wasmtime::Config::new();
        wc.epoch_interruption(true);
        let engine = Engine::new(&wc)
            .map_err(|e| Error::new(Reason::Internal, format!("the graders' engine: {e:#}")))?;
        let weak = engine.weak();
        std::thread::Builder::new()
            .name("hive-grader-epoch".into())
            .spawn(move || {
                while let Some(engine) = weak.upgrade() {
                    engine.increment_epoch();
                    drop(engine);
                    std::thread::sleep(TICK);
                }
            })
            .map_err(|e| Error::new(Reason::Internal, format!("the graders' epoch thread: {e}")))?;
        let linker = Linker::new(&engine);
        Ok(Self { engine, linker, cfg, compiled: Mutex::new(HashMap::new()) })
    }

    /// Compiles the grader called `name` if it has not been, so a verification can find out it
    /// has no such grader before it does anything else.
    ///
    /// # Errors
    ///
    /// `INVALID_ARGUMENT` when there is no such grader, or it is not a component of the world
    /// `hivebox:reward/grader`.
    pub async fn check(&self, name: &str) -> Result<(), Error> {
        self.pre(name).await.map(drop)
    }

    /// Grades `input` with the grader called `name`. The outer error is about the grader and the
    /// inner one about this grade: the grader returned an error, trapped, ran out of time or
    /// gave a reward that is not a finite number.
    ///
    /// # Errors
    ///
    /// As [`Graders::check`].
    pub async fn score(&self, name: &str, input: Input) -> Result<Result<Grade, String>, Error> {
        let pre = self.pre(name).await?;
        let mem = usize::try_from(self.cfg.mem_mib << 20).unwrap_or(usize::MAX);
        let ticks = self.cfg.timeout.as_millis().div_ceil(TICK.as_millis()) as u64;
        let timeout = self.cfg.timeout;
        let engine = self.engine.clone();
        let grade = move || -> Result<Grade, String> {
            let limits = StoreLimitsBuilder::new().memory_size(mem).table_elements(1 << 20).build();
            let mut store = Store::new(&engine, State { limits });
            store.limiter(|s| &mut s.limits);
            store.set_epoch_deadline(ticks);
            store.epoch_deadline_trap();
            let input = wit::Input {
                task: input.task,
                runs: input
                    .runs
                    .into_iter()
                    .map(|r| wit::Run {
                        exit_code: r.exit_code,
                        timed_out: r.timed_out,
                        stdout: r.stdout,
                        stderr: r.stderr,
                        wall_ms: r.wall_ms,
                    })
                    .collect(),
                files: input.files,
                passed: input.passed,
                tampered: input.tampered,
            };
            // The trap alone, without the backtrace that comes with it.
            let trapped = |e: wasmtime::Error| match e.downcast_ref::<Trap>() {
                Some(Trap::Interrupt) => format!("the grader ran past its {timeout:?}"),
                Some(trap) => format!("the grader hit a {trap}"),
                None => format!("the grader failed: {e:#}"),
            };
            let grader = pre.instantiate(&mut store).map_err(trapped)?;
            let grade =
                grader.hivebox_reward_score().call_score(&mut store, &input).map_err(trapped)??;
            if !grade.reward.is_finite() {
                return Err(format!("the grader gave a reward of {}", grade.reward));
            }
            Ok(Grade { reward: grade.reward, detail: grade.detail })
        };
        tokio::task::spawn_blocking(grade)
            .await
            .map_err(|e| Error::new(Reason::Internal, format!("grading: {e}")))
    }

    async fn pre(&self, name: &str) -> Result<bindings::GraderPre<State>, Error> {
        let missing = || Error::new(Reason::InvalidArgument, format!("no grader named {name:?}"));
        if !is_program_name(name) {
            return Err(missing());
        }
        let path = self.cfg.dir.join(format!("{name}.wasm"));
        let meta = match tokio::fs::metadata(&path).await {
            Ok(m) if m.is_file() => m,
            Ok(_) => return Err(missing()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Err(missing()),
            Err(e) => return Err(Error::new(Reason::Internal, format!("grader {name}: {e}"))),
        };
        let stamp = (meta.len(), meta.modified().unwrap_or(SystemTime::UNIX_EPOCH));
        let entry = {
            let mut compiled = self.compiled.lock().unwrap_or_else(PoisonError::into_inner);
            let entry = compiled
                .entry(name.to_owned())
                .or_insert_with(|| Arc::new(Entry { stamp, pre: OnceCell::new() }));
            if entry.stamp != stamp {
                *entry = Arc::new(Entry { stamp, pre: OnceCell::new() });
            }
            entry.clone()
        };
        let pre = entry
            .pre
            .get_or_init(|| async {
                let engine = self.engine.clone();
                let linker = self.linker.clone();
                let compile = move || -> Result<bindings::GraderPre<State>, String> {
                    let component =
                        Component::from_file(&engine, &path).map_err(|e| format!("{e:#}"))?;
                    let pre = linker.instantiate_pre(&component).map_err(|e| format!("{e:#}"))?;
                    bindings::GraderPre::new(pre).map_err(|e| format!("{e:#}"))
                };
                tokio::task::spawn_blocking(compile).await.unwrap_or_else(|e| Err(e.to_string()))
            })
            .await;
        pre.clone().map_err(|e| Error::new(Reason::InvalidArgument, format!("grader {name}: {e}")))
    }
}
