//! Policy plugins: WebAssembly components of the world `hivebox:policy/policy`, in
//! `wit/policy.wit`, that a node asks before it makes a cell. A policy allows the cell, changes a
//! few things about it, or turns it away with a reason. It imports nothing and runs with a memory
//! limit and a time limit, so an operator can write one in any language that builds components.

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime};

use hive_types::{CellSpec, Error, Reason, Source};
use tokio::sync::OnceCell;
use wasmtime::component::{Component, Linker};
use wasmtime::{Engine, Store, StoreLimits, StoreLimitsBuilder, Trap};

use crate::TICK;
use crate::run::is_program_name;

#[allow(missing_docs, missing_debug_implementations, unreachable_pub, unused_qualifications)]
mod bindings {
    wasmtime::component::bindgen!({ path: "wit/policy.wit", world: "policy" });
}

use bindings::exports::hivebox::policy::admit as wit;

/// How policies run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolicyConfig {
    /// Where the policies are, each a component named `<name>.wasm`. A file that changes is
    /// compiled again on its next use.
    pub dir: PathBuf,
    /// The longest one verdict may take.
    pub timeout: Duration,
    /// The most memory a policy may have, in MiB.
    pub mem_mib: u64,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("/var/lib/hivebox/policies"),
            timeout: Duration::from_millis(100),
            mem_mib: 64,
        }
    }
}

/// What a policy changes about a cell before it is made.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Change {
    /// Its network profile instead.
    pub network_profile: Option<String>,
    /// Its longest life instead.
    pub hard_ttl: Option<Duration>,
    /// Labels to set on it, over any it has with the same key.
    pub labels: Vec<(String, String)>,
}

impl Change {
    /// Makes the change to `spec`.
    pub fn apply(self, spec: &mut CellSpec) {
        if let Some(p) = self.network_profile {
            spec.network_profile = p;
        }
        if let Some(t) = self.hard_ttl {
            spec.hard_ttl = Some(t);
        }
        spec.labels.extend(self.labels);
    }
}

/// What a policy said about a cell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Make it as it is.
    Allow,
    /// Make it with these changes.
    Change(Change),
    /// Do not make it, for this reason.
    Deny(String),
}

/// The node's policies, with an engine of their own.
pub struct Policies {
    engine: Engine,
    linker: Linker<State>,
    cfg: PolicyConfig,
    compiled: Mutex<HashMap<String, Arc<Entry>>>,
}

impl fmt::Debug for Policies {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Policies").field("cfg", &self.cfg).finish_non_exhaustive()
    }
}

struct State {
    limits: StoreLimits,
}

// One policy file as it was when it was compiled.
struct Entry {
    stamp: (u64, SystemTime),
    pre: OnceCell<Result<bindings::PolicyPre<State>, String>>,
}

impl Policies {
    /// Policies as `cfg` says, with a thread that moves their engine's epoch on every [`TICK`]
    /// for as long as the engine is in use.
    ///
    /// # Errors
    ///
    /// `INVALID_ARGUMENT` when `cfg` asks for no time or no memory, or more than 4 GiB.
    pub fn new(cfg: PolicyConfig) -> Result<Self, Error> {
        if cfg.timeout.is_zero() || cfg.mem_mib == 0 || cfg.mem_mib > 4096 {
            return Err(Error::new(
                Reason::InvalidArgument,
                "policies need some time, and from 1 to 4096 MiB",
            ));
        }
        let mut wc = wasmtime::Config::new();
        wc.epoch_interruption(true);
        let engine = Engine::new(&wc)
            .map_err(|e| Error::new(Reason::Internal, format!("the policies' engine: {e:#}")))?;
        let weak = engine.weak();
        std::thread::Builder::new()
            .name("hive-policy-epoch".into())
            .spawn(move || {
                while let Some(engine) = weak.upgrade() {
                    engine.increment_epoch();
                    drop(engine);
                    std::thread::sleep(TICK);
                }
            })
            .map_err(|e| {
                Error::new(Reason::Internal, format!("the policies' epoch thread: {e}"))
            })?;
        let linker = Linker::new(&engine);
        Ok(Self { engine, linker, cfg, compiled: Mutex::new(HashMap::new()) })
    }

    /// Compiles the policy called `name` if it has not been, so a node can refuse to start with
    /// a policy it cannot run.
    ///
    /// # Errors
    ///
    /// `INVALID_ARGUMENT` when there is no such policy, or it is not a component of the world
    /// `hivebox:policy/policy`.
    pub async fn check(&self, name: &str) -> Result<(), Error> {
        self.pre(name).await.map(drop)
    }

    /// Asks the policy called `name` about a cell `project` wants made as `spec` says. The outer
    /// error is about the policy and the inner one about this verdict: the policy trapped or ran
    /// out of time.
    ///
    /// # Errors
    ///
    /// As [`Policies::check`].
    pub async fn decide(
        &self,
        name: &str,
        project: &str,
        spec: &CellSpec,
    ) -> Result<Result<Verdict, String>, Error> {
        let pre = self.pre(name).await?;
        let request = request(project, spec);
        let mem = usize::try_from(self.cfg.mem_mib << 20).unwrap_or(usize::MAX);
        let ticks = self.cfg.timeout.as_millis().div_ceil(TICK.as_millis()) as u64;
        let timeout = self.cfg.timeout;
        let engine = self.engine.clone();
        let decide = move || -> Result<Verdict, String> {
            let limits = StoreLimitsBuilder::new().memory_size(mem).table_elements(1 << 20).build();
            let mut store = Store::new(&engine, State { limits });
            store.limiter(|s| &mut s.limits);
            store.set_epoch_deadline(ticks);
            store.epoch_deadline_trap();
            // The trap alone, without the backtrace that comes with it.
            let trapped = |e: wasmtime::Error| match e.downcast_ref::<Trap>() {
                Some(Trap::Interrupt) => format!("it ran past its {timeout:?}"),
                Some(trap) => format!("it hit a {trap}"),
                None => format!("it failed: {e:#}"),
            };
            let policy = pre.instantiate(&mut store).map_err(trapped)?;
            let verdict =
                policy.hivebox_policy_admit().call_check(&mut store, &request).map_err(trapped)?;
            Ok(match verdict {
                wit::Verdict::Allow => Verdict::Allow,
                wit::Verdict::Change(c) => Verdict::Change(Change {
                    network_profile: c.network_profile,
                    hard_ttl: c.hard_ttl_s.map(Duration::from_secs),
                    labels: c.labels,
                }),
                wit::Verdict::Deny(why) => Verdict::Deny(why),
            })
        };
        tokio::task::spawn_blocking(decide)
            .await
            .map_err(|e| Error::new(Reason::Internal, format!("policy {name}: {e}")))
    }

    async fn pre(&self, name: &str) -> Result<bindings::PolicyPre<State>, Error> {
        let missing = || Error::new(Reason::InvalidArgument, format!("no policy named {name:?}"));
        if !is_program_name(name) {
            return Err(missing());
        }
        let path = self.cfg.dir.join(format!("{name}.wasm"));
        let meta = match tokio::fs::metadata(&path).await {
            Ok(m) if m.is_file() => m,
            Ok(_) => return Err(missing()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Err(missing()),
            Err(e) => return Err(Error::new(Reason::Internal, format!("policy {name}: {e}"))),
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
                let compile = move || -> Result<bindings::PolicyPre<State>, String> {
                    let component =
                        Component::from_file(&engine, &path).map_err(|e| format!("{e:#}"))?;
                    let pre = linker.instantiate_pre(&component).map_err(|e| format!("{e:#}"))?;
                    bindings::PolicyPre::new(pre).map_err(|e| format!("{e:#}"))
                };
                tokio::task::spawn_blocking(compile).await.unwrap_or_else(|e| Err(e.to_string()))
            })
            .await;
        pre.clone().map_err(|e| Error::new(Reason::InvalidArgument, format!("policy {name}: {e}")))
    }
}

fn request(project: &str, spec: &CellSpec) -> wit::Request {
    wit::Request {
        project: project.to_owned(),
        source: match &spec.source {
            Source::Template(t) => wit::Source::Template(t.clone()),
            Source::Image(i) => wit::Source::Image(i.clone()),
            Source::Snapshot(s) => wit::Source::Snapshot(s.clone()),
        },
        backend: spec.backend.as_str().to_owned(),
        vcpu_milli: spec.resources.vcpu_milli,
        mem_mib: spec.resources.mem_mib,
        disk_gib: spec.resources.disk_gib,
        qos: spec.qos.as_str().to_owned(),
        network_profile: spec.network_profile.clone(),
        hard_ttl_s: spec.hard_ttl.map(|t| t.as_secs()),
        trusted_image: spec.trusted_image,
        labels: spec.labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        env: spec.env.keys().cloned().collect(),
    }
}
