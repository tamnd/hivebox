//! The driver the comb calls. It gets each container's root filesystem and config ready itself,
//! and hands the create to a worker process.

use crate::spec::{self, Host};
use crate::worker::{Create, Reply};
use futures::future::BoxFuture;
use hive_cell::{
    CellDriver, CellHandle, DriverCaps, ExitInfo, GuestChannel, Liveness, NodeFit, PauseMode,
    Result, RootfsPlan, Slot, SnapshotCaps, cgroup,
};
use hive_types::{Backend, CellId, CellSpec, Error, Reason};
use std::collections::{BTreeMap, HashMap};
use std::ffi::OsString;
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{Notify, mpsc, oneshot};

/// Where cgroup v2 is mounted. A cell's cgroup is given to libcontainer relative to it.
const CGROUP_ROOT: &str = "/sys/fs/cgroup";
/// Less cold page cache than this in a cell is not worth a trim.
const TRIM_FLOOR: u64 = 1 << 20;

/// How one node runs its container cells.
#[derive(Clone, Debug)]
pub struct Config {
    /// The command that starts a worker. The comb runs its own binary with `--oci-worker`.
    pub worker: Vec<OsString>,
    /// Workers kept running, which is how many creates go on at once.
    pub workers: usize,
    /// The drone binary put in every container. It has to be static, since it runs on the
    /// image's libc or none.
    pub drone: PathBuf,
    /// Where each cell gets a directory for its bundle, its state and its log.
    pub state_dir: PathBuf,
    /// The first host id that root in a cell maps to. Images are shifted to it when they are
    /// imported.
    pub uid_base: u32,
    /// How many ids each cell has.
    pub uid_count: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            worker: vec!["/proc/self/exe".into(), "--oci-worker".into()],
            workers: 8,
            drone: PathBuf::from("/usr/lib/hivebox/hive-drone"),
            state_dir: PathBuf::from("/run/hivebox/oci"),
            uid_base: 1_000_000,
            uid_count: 65536,
        }
    }
}

/// Container cells through youki's libcontainer.
#[derive(Debug)]
pub struct OciDriver {
    cfg: Config,
    jobs: mpsc::Sender<Job>,
    exits: Arc<Exits>,
    /// Each prepared cell's first secret, until its start hands it to the drone. Kept here
    /// rather than in the handle, since the comb writes handles to its WAL.
    secrets: Mutex<HashMap<CellId, [u8; 32]>>,
}

#[derive(Debug)]
struct Job {
    create: Create,
    done: oneshot::Sender<std::result::Result<i32, String>>,
}

/// How ended containers ended, as the workers report them, until a stop takes the answer.
#[derive(Debug, Default)]
struct Exits {
    ended: Mutex<HashMap<u32, ExitInfo>>,
    news: Notify,
}

impl OciDriver {
    /// Starts the workers. It has to be called in a Tokio runtime.
    ///
    /// # Errors
    ///
    /// The state directory cannot be made.
    pub fn new(cfg: Config) -> std::io::Result<Self> {
        std::fs::DirBuilder::new().recursive(true).mode(0o711).create(&cfg.state_dir)?;
        let (jobs, rx) = mpsc::channel(1024);
        let rx = Arc::new(tokio::sync::Mutex::new(rx));
        let exits = Arc::new(Exits::default());
        for _ in 0..cfg.workers.max(1) {
            tokio::spawn(run_worker(cfg.worker.clone(), rx.clone(), exits.clone()));
        }
        Ok(Self { cfg, jobs, exits, secrets: Mutex::default() })
    }

    fn dir(&self, id: CellId) -> PathBuf {
        self.cfg.state_dir.join(id.to_string())
    }

    fn prepare_now(
        &self,
        id: CellId,
        spec: &CellSpec,
        rootfs: &RootfsPlan,
        slot: &Slot,
    ) -> std::io::Result<CellHandle> {
        let dir = self.dir(id);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::DirBuilder::new().mode(0o711).create(&dir)?;
        let root = dir.join("rootfs");
        std::fs::DirBuilder::new().mode(0o755).create(&root)?;
        let (uid, gid) = (Some(uid(self.cfg.uid_base)), Some(gid(self.cfg.uid_base)));
        // The upper's own owner and mode are what the cell sees on its `/`.
        std::fs::create_dir_all(&rootfs.upper)?;
        std::fs::set_permissions(&rootfs.upper, std::fs::Permissions::from_mode(0o755))?;
        rustix::fs::chown(&rootfs.upper, uid, gid)?;
        let work = slot.dir.join("work");
        std::fs::create_dir_all(&work)?;
        let lowers: Vec<String> = rootfs.lowers.iter().map(|l| l.display().to_string()).collect();
        // volatile skips every sync, which a cell's scratch layer never needs.
        let options = std::ffi::CString::new(format!(
            "lowerdir={},upperdir={},workdir={},volatile",
            lowers.join(":"),
            rootfs.upper.display(),
            work.display()
        ))
        .map_err(std::io::Error::other)?;
        rustix::mount::mount(
            "overlay",
            &root,
            "overlay",
            rustix::mount::MountFlags::empty(),
            options.as_c_str(),
        )
        .map_err(|e| std::io::Error::other(format!("mounting the root filesystem: {e}")))?;
        let cgroup = slot.cgroup.strip_prefix(CGROUP_ROOT).map_err(|_| {
            std::io::Error::other(format!("{} is not under {CGROUP_ROOT}", slot.cgroup.display()))
        })?;
        for (name, text) in [("hosts", spec::HOSTS), ("hostname", "cell\n")] {
            let path = dir.join(name);
            std::fs::write(&path, text)?;
            rustix::fs::chown(&path, uid, gid)?;
        }
        let resolv = dir.join("resolv.conf");
        if let Some(ns) = slot.nameserver {
            std::fs::write(&resolv, format!("nameserver {ns}\noptions timeout:2 attempts:2\n"))?;
        }
        let host = Host {
            drone: &self.cfg.drone,
            cgroup: &Path::new("/").join(cgroup),
            uid_base: self.cfg.uid_base,
            uid_count: self.cfg.uid_count,
            resolv: slot.nameserver.map(|_| resolv.as_path()),
            etc: &dir,
        };
        let config = spec::config(spec, &host);
        std::fs::write(dir.join("config.json"), serde_json::to_vec(&config)?)?;
        Ok(CellHandle {
            id,
            backend: Backend::Container,
            pid: None,
            channel: GuestChannel::Unix(dir.join("drone.sock")),
            cgroup: slot.cgroup.clone(),
            netns: slot.netns.clone(),
            extra: BTreeMap::new(),
        })
    }

    /// Takes down whatever `prepare` made. Fine to call on a cell that is half made or gone.
    /// Forgets the cell and removes its directory. The unmount and the delete run on the blocking
    /// pool: a bulk stop does a thousand of these at once, and on the runtime's own threads they
    /// held up every other call on the node.
    async fn clean(&self, id: CellId) {
        self.secrets.lock().unwrap_or_else(PoisonError::into_inner).remove(&id);
        let dir = self.dir(id);
        let _ = tokio::task::spawn_blocking(move || {
            let _ = rustix::mount::unmount(dir.join("rootfs"), rustix::mount::UnmountFlags::DETACH);
            let _ = std::fs::remove_dir_all(&dir);
        })
        .await;
    }

    async fn wait_gone(&self, pidfd: &OwnedFd, limit: Duration) -> bool {
        let Ok(fd) = AsyncFd::with_interest(pidfd.as_fd(), tokio::io::Interest::READABLE) else {
            return false;
        };
        tokio::time::timeout(limit, fd.readable()).await.is_ok_and(|r| r.is_ok())
    }

    async fn exit_of(&self, pid: u32, wait: Duration) -> Option<ExitInfo> {
        let until = Instant::now() + wait;
        loop {
            let news = self.exits.news.notified();
            if let Some(e) =
                self.exits.ended.lock().unwrap_or_else(PoisonError::into_inner).remove(&pid)
            {
                return Some(e);
            }
            let left = until.checked_duration_since(Instant::now())?;
            let _ = tokio::time::timeout(left, news).await;
        }
    }
}

impl CellDriver for OciDriver {
    fn backend(&self) -> Backend {
        Backend::Container
    }

    fn caps(&self) -> DriverCaps {
        DriverCaps {
            pause: true,
            snapshot: SnapshotCaps::None,
            fork: false,
            resize: false,
            gpu: false,
            trim: true,
        }
    }

    fn probe(&self) -> BoxFuture<'_, Result<NodeFit>> {
        Box::pin(async move {
            let mut notes = Vec::new();
            let mut ready = true;
            let mut need = |ok: bool, what: String| {
                ready &= ok;
                notes.push(what);
            };
            let v2 = Path::new(CGROUP_ROOT).join("cgroup.controllers").exists();
            need(v2, format!("cgroup v2: {}", if v2 { "yes" } else { "no" }));
            let drone = self.cfg.drone.is_file();
            need(
                drone,
                format!(
                    "drone at {}: {}",
                    self.cfg.drone.display(),
                    if drone { "yes" } else { "missing" }
                ),
            );
            let users = std::fs::read_to_string("/proc/sys/user/max_user_namespaces")
                .ok()
                .and_then(|s| s.trim().parse::<u64>().ok())
                .unwrap_or(0);
            need(users > 0, format!("user namespaces: {users}"));
            let overlay = std::fs::read_to_string("/proc/filesystems")
                .is_ok_and(|s| s.lines().any(|l| l.ends_with("\toverlay")));
            need(overlay, format!("overlayfs: {}", if overlay { "yes" } else { "no" }));
            Ok(NodeFit { ready, notes })
        })
    }

    fn prepare<'a>(
        &'a self,
        id: CellId,
        spec: &'a CellSpec,
        rootfs: &'a RootfsPlan,
        slot: &'a Slot,
    ) -> BoxFuture<'a, Result<CellHandle>> {
        Box::pin(async move {
            if slot.cgroup.as_os_str().is_empty() {
                return Err(Error::new(Reason::Internal, "a container cell needs a cgroup"));
            }
            // A mount and a few small files, a millisecond or so, so it runs right here.
            let h = match self.prepare_now(id, spec, rootfs, slot) {
                Ok(h) => h,
                Err(e) => {
                    self.clean(id).await;
                    return Err(Error::new(
                        Reason::Internal,
                        format!("preparing the container: {e}"),
                    ));
                }
            };
            self.secrets.lock().unwrap_or_else(PoisonError::into_inner).insert(id, slot.secret);
            Ok(h)
        })
    }

    fn start<'a>(&'a self, h: &'a mut CellHandle) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let dir = self.dir(h.id);
            let secret = self
                .secrets
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&h.id)
                .ok_or_else(|| {
                    Error::new(Reason::Internal, "the cell was not prepared, or started before")
                })?;
            let GuestChannel::Unix(socket) = h.channel.clone() else {
                return Err(Error::new(Reason::Internal, "a container's channel is a unix socket"));
            };
            let netns = h.netns.clone();
            let create = Create { seq: 0, id: h.id.to_string(), dir, socket, netns, secret };
            let (done, answer) = oneshot::channel();
            let failed =
                |e: String| Error::new(Reason::Internal, format!("starting the container: {e}"));
            self.jobs
                .send(Job { create, done })
                .await
                .map_err(|_| failed("the workers are gone".into()))?;
            let pid =
                answer.await.map_err(|_| failed("its worker ended".into()))?.map_err(failed)?;
            let pid = u32::try_from(pid).map_err(|_| failed(format!("pid {pid}")))?;
            h.pid = Some(pid);
            if let Some(start) = start_time(pid) {
                h.extra.insert("start".into(), start.to_string());
            }
            Ok(())
        })
    }

    fn pause<'a>(&'a self, h: &'a CellHandle, mode: PauseMode) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if mode == PauseMode::SnapshotKill {
                return Err(Error::new(
                    Reason::PolicyDenied,
                    "a container cannot be snapshotted yet",
                ));
            }
            let io = |e: std::io::Error| Error::new(Reason::Internal, format!("pausing: {e}"));
            cgroup::freeze(&h.cgroup, true).map_err(io)?;
            // The kernel freezes in the background. A cell that is busy in the kernel can take a
            // little while.
            let until = Instant::now() + Duration::from_secs(5);
            while !cgroup::frozen(&h.cgroup).map_err(io)? {
                if Instant::now() > until {
                    return Err(Error::new(Reason::Internal, "the cell did not freeze within 5s"));
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            if mode == PauseMode::Reclaim {
                // Best effort: whatever the kernel can push out now goes, the page cache back to
                // disk and the rest to swap where there is any. The kernel gives up early with
                // EAGAIN once it cannot find more, which is fine.
                cgroup::swap(&h.cgroup, true);
                let used = cgroup::metrics(&h.cgroup).mem_bytes;
                let _ = cgroup::write(&h.cgroup, "memory.reclaim", &used.to_string());
            }
            Ok(())
        })
    }

    fn resume<'a>(&'a self, h: &'a CellHandle) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            cgroup::freeze(&h.cgroup, false)
                .map_err(|e| Error::new(Reason::Internal, format!("resuming: {e}")))?;
            // Pages already in swap stay there until touched, and new ones stop going out.
            cgroup::swap(&h.cgroup, false);
            Ok(())
        })
    }

    fn trim<'a>(&'a self, h: &'a CellHandle) -> BoxFuture<'a, Result<u64>> {
        Box::pin(async move {
            // A reclaim writes dirty pages out before it returns, so it stays off the runtime.
            let dir = h.cgroup.clone();
            tokio::task::spawn_blocking(move || cgroup::trim(&dir, TRIM_FLOOR))
                .await
                .map_err(|e| Error::new(Reason::Internal, format!("trimming: {e}")))
        })
    }

    fn stop<'a>(&'a self, h: &'a CellHandle, grace: Duration) -> BoxFuture<'a, Result<ExitInfo>> {
        Box::pin(async move {
            let mut exit = ExitInfo::default();
            if let Some(pid) = h.pid {
                if let Some(pidfd) = pidfd(pid).ok().filter(|_| alive(h)) {
                    let _ = cgroup::freeze(&h.cgroup, false);
                    let mut gone = false;
                    if !grace.is_zero() {
                        // Init passes the stop on to the drone, and the kernel takes down the rest
                        // of the cell once init exits.
                        let _ = rustix::process::pidfd_send_signal(
                            &pidfd,
                            rustix::process::Signal::TERM,
                        );
                        gone = self.wait_gone(&pidfd, grace).await;
                    }
                    if !gone {
                        if cgroup::kill(&h.cgroup).is_err() {
                            let _ = rustix::process::pidfd_send_signal(
                                &pidfd,
                                rustix::process::Signal::KILL,
                            );
                        }
                        self.wait_gone(&pidfd, Duration::from_secs(5)).await;
                    }
                }
                // The worker reports the exit a moment after the pidfd says so.
                let wait = if alive(h) { Duration::ZERO } else { Duration::from_millis(500) };
                exit = self.exit_of(pid, wait).await.unwrap_or_default();
            }
            exit.oom = cgroup::oom_kills(&h.cgroup) > 0;
            self.clean(h.id).await;
            Ok(exit)
        })
    }

    fn check<'a>(&'a self, h: &'a CellHandle) -> BoxFuture<'a, Result<Liveness>> {
        Box::pin(async move {
            let Some(pid) = h.pid else {
                return Ok(Liveness::Gone(ExitInfo::default()));
            };
            if !alive(h) {
                let mut exit = self
                    .exits
                    .ended
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .get(&pid)
                    .copied()
                    .unwrap_or_default();
                exit.oom = cgroup::oom_kills(&h.cgroup) > 0;
                return Ok(Liveness::Gone(exit));
            }
            if cgroup::frozen(&h.cgroup).unwrap_or(false) {
                return Ok(Liveness::Paused);
            }
            Ok(Liveness::Alive)
        })
    }
}

/// Whether the handle's init is still running: the pid is there, is the same process that was
/// started, and has not exited yet.
fn alive(h: &CellHandle) -> bool {
    let Some(pid) = h.pid else { return false };
    let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else { return false };
    let Some((state, start)) = stat_fields(&stat) else { return false };
    let same = h.extra.get("start").is_none_or(|s| s.parse() == Ok(start));
    same && state != 'Z' && state != 'X'
}

/// The state letter and the start time from a `/proc/PID/stat` line.
fn stat_fields(stat: &str) -> Option<(char, u64)> {
    // The command name can hold spaces and brackets, so the fields start after the last `)`.
    let rest = &stat[stat.rfind(')')? + 2..];
    let mut fields = rest.split(' ');
    let state = fields.next()?.chars().next()?;
    // starttime is field 22 of the line, the 20th after the state.
    let start = fields.nth(18)?.parse().ok()?;
    Some((state, start))
}

fn start_time(pid: u32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    stat_fields(&stat).map(|(_, start)| start)
}

fn pidfd(pid: u32) -> std::io::Result<OwnedFd> {
    let pid = i32::try_from(pid).ok().and_then(rustix::process::Pid::from_raw);
    let pid = pid.ok_or_else(|| std::io::Error::other("bad pid"))?;
    Ok(rustix::process::pidfd_open(pid, rustix::process::PidfdFlags::empty())?)
}

fn uid(n: u32) -> rustix::fs::Uid {
    rustix::fs::Uid::from_raw(n)
}

fn gid(n: u32) -> rustix::fs::Gid {
    rustix::fs::Gid::from_raw(n)
}

/// Keeps one worker running and feeds it jobs, one at a time. A worker that dies is started again,
/// and the job it had fails.
async fn run_worker(
    cmd: Vec<OsString>,
    jobs: Arc<tokio::sync::Mutex<mpsc::Receiver<Job>>>,
    exits: Arc<Exits>,
) {
    let mut backoff = Duration::from_millis(10);
    loop {
        let Some((mut child, mut stdin, mut replies)) = spawn_worker(&cmd, exits.clone()) else {
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(5));
            continue;
        };
        loop {
            let job = jobs.lock().await.recv().await;
            let Some(job) = job else {
                // The driver is gone, so the worker goes too.
                drop(stdin);
                let _ = child.wait().await;
                return;
            };
            let mut line = serde_json::to_vec(&job.create).expect("a create serializes");
            line.push(b'\n');
            if stdin.write_all(&line).await.is_err() {
                let _ = job.done.send(Err("the worker ended".into()));
                break;
            }
            match replies.recv().await {
                Some(r) => {
                    backoff = Duration::from_millis(10);
                    let _ = job.done.send(r);
                }
                None => {
                    let _ = job.done.send(Err("the worker ended".into()));
                    break;
                }
            }
        }
        let _ = child.kill().await;
    }
}

type Replies = mpsc::Receiver<std::result::Result<i32, String>>;

fn spawn_worker(cmd: &[OsString], exits: Arc<Exits>) -> Option<(Child, ChildStdin, Replies)> {
    let (program, args) = cmd.split_first()?;
    let mut child = Command::new(program)
        .args(args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| eprintln!("hive-cell-oci: starting a worker: {e}"))
        .ok()?;
    let stdin = child.stdin.take()?;
    let stdout = child.stdout.take()?;
    let (tx, rx) = mpsc::channel(1);
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            match serde_json::from_str::<Reply>(&line) {
                Ok(Reply::Created { pid, .. }) => {
                    let _ = tx.send(Ok(pid)).await;
                }
                Ok(Reply::Failed { error, .. }) => {
                    let _ = tx.send(Err(error)).await;
                }
                Ok(Reply::Exited { pid, code, signal }) => {
                    if let Ok(pid) = u32::try_from(pid) {
                        let exit = ExitInfo { code, signal, oom: false };
                        exits
                            .ended
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .insert(pid, exit);
                        exits.news.notify_waiters();
                    }
                }
                Err(e) => eprintln!("hive-cell-oci: a worker said {line:?}: {e}"),
            }
        }
    });
    Some((child, stdin, rx))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stat_lines_with_odd_names_parse() {
        let line = "42 (a) b (c)) S 1 42 42 0 -1 4194560 100 0 0 0 1 2 0 0 20 0 1 0 987654 1000 10";
        assert_eq!(stat_fields(line), Some(('S', 987_654)));
        assert_eq!(stat_fields("garbage"), None);
    }
}
