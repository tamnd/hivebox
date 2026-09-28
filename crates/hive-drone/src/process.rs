//! Running commands, to completion or streamed, from `spec/09_guest_agent.md`, section 3.
//!
//! Every command gets its own process group, so a timeout or a signal reaches everything it
//! started and not just the first process. The environment starts empty and gets the drone's base
//! environment and then the caller's, so nothing about the drone leaks into a command by accident.

use crate::Config;
use crate::ring::Ring;
use bytes::Bytes;
use hive_proto::drone::Stream;
use hive_proto::drone::api::{Command, MAX_CHUNK, RunRequest, RunResult, tag, tagged};
use hive_types::{Error, Reason};
use rustix::process::{Pid, Signal};
use std::os::unix::process::ExitStatusExt;
use std::path::Path;
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStdin};
use tokio::sync::mpsc;

/// How long to keep reading output after a command exits. A background job that inherited the
/// pipes can hold them open for as long as it runs, and the answer should not wait for it.
const DRAIN: Duration = Duration::from_millis(100);
const READ_BUF: usize = 32 * 1024;

/// Runs a command to completion.
pub(crate) async fn run(cfg: &Config, req: RunRequest) -> Result<RunResult, Error> {
    let c = req.command.unwrap_or_default();
    let mut cmd = build(cfg, &c)?;
    let limit = cfg.output_limit(c.max_output_bytes);
    cmd.stdin(if req.stdin.is_empty() { Stdio::null() } else { Stdio::piped() });
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let started = Instant::now();
    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(e) => return Ok(spawn_failed(cfg, &c, &e, started)),
    };
    let mut group = Group::new(&child);
    if let Some(mut stdin) = child.stdin.take() {
        let data = req.stdin;
        // A command that never reads its input must not hold up the answer.
        tokio::spawn(async move {
            let _ = stdin.write_all(&data).await;
        });
    }
    let out = Arc::new(Mutex::new(Ring::new(limit)));
    let err = Arc::new(Mutex::new(Ring::new(limit)));
    let mut out_task = tokio::spawn(pump(child.stdout.take(), out.clone()));
    let mut err_task = tokio::spawn(pump(child.stderr.take(), err.clone()));
    let timed_out;
    let status = match tokio::time::timeout(cfg.timeout(c.timeout_ms), child.wait()).await {
        Ok(status) => {
            timed_out = false;
            status
        }
        Err(_) => {
            timed_out = true;
            group.kill();
            child.wait().await
        }
    }
    .map_err(|e| Error::new(Reason::Internal, format!("waiting for the command: {e}")))?;
    let wall = started.elapsed();
    group.exited();
    let _ = tokio::time::timeout(DRAIN, async {
        let _ = (&mut out_task).await;
        let _ = (&mut err_task).await;
    })
    .await;
    out_task.abort();
    err_task.abort();
    let (mut out, mut err) = (lock(&out), lock(&err));
    let mut result = exit(status, timed_out, wall);
    result.truncated = out.truncated() || err.truncated();
    result.stdout_bytes = out.total();
    result.stderr_bytes = err.total();
    result.stdout = out.take();
    result.stderr = err.take();
    Ok(result)
}

/// Runs a command with its input and output streamed over `stream`. See `api::tag` for the frames.
pub(crate) async fn start(cfg: &Config, c: Command, stream: &mut Stream) -> Result<(), Error> {
    let mut cmd = build(cfg, &c)?;
    cmd.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let started = Instant::now();
    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(e) => {
            let mut result = spawn_failed(cfg, &c, &e, started);
            let message = std::mem::take(&mut result.stderr);
            stream.send(tagged(tag::STDERR, &message[..message.len().min(MAX_CHUNK)])).await?;
            return stream.send_last(result.exit_frame()).await;
        }
    };
    let mut group = Group::new(&child);
    let pid = child.id().unwrap_or(0);
    stream.send(tagged(tag::PID, &pid.to_be_bytes())).await?;

    let (stdin_tx, stdin_rx) = mpsc::channel(16);
    tokio::spawn(feed(child.stdin.take(), stdin_rx));
    let mut stdin = Some(stdin_tx);
    let mut stdout = child.stdout.take();
    let mut stderr = child.stderr.take();
    let (mut out_buf, mut err_buf) = (vec![0u8; READ_BUF], vec![0u8; READ_BUF]);
    let (mut out_bytes, mut err_bytes) = (0u64, 0u64);
    let mut input_open = true;
    let mut status: Option<ExitStatus> = None;
    let mut timed_out = false;
    let mut wall = Duration::ZERO;
    // The timeout until the command exits, then the end of the drain window.
    let mut wake = tokio::time::Instant::now() + cfg.timeout(c.timeout_ms);

    while status.is_none() || stdout.is_some() || stderr.is_some() {
        // Input is only taken when there is room for it, so a command that does not read its
        // stdin pushes back on the node instead of on its own output.
        let room = stdin.as_ref().is_none_or(|tx| tx.capacity() > 0);
        tokio::select! {
            n = read(&mut stdout, &mut out_buf) => {
                if n == 0 {
                    stdout = None;
                } else {
                    out_bytes += n as u64;
                    stream.send(tagged(tag::STDOUT, &out_buf[..n])).await?;
                }
            }
            n = read(&mut stderr, &mut err_buf) => {
                if n == 0 {
                    stderr = None;
                } else {
                    err_bytes += n as u64;
                    stream.send(tagged(tag::STDERR, &err_buf[..n])).await?;
                }
            }
            msg = stream.recv(), if input_open && room => match msg? {
                None => {
                    input_open = false;
                    stdin = None;
                }
                Some(msg) => input(&msg, &mut stdin, &group)?,
            },
            s = child.wait(), if status.is_none() => {
                status = Some(s.map_err(|e| {
                    Error::new(Reason::Internal, format!("waiting for the command: {e}"))
                })?);
                wall = started.elapsed();
                group.exited();
                wake = tokio::time::Instant::now() + DRAIN;
            }
            () = tokio::time::sleep_until(wake), if status.is_some() || !timed_out => {
                if status.is_some() {
                    break;
                }
                timed_out = true;
                group.kill();
            }
        }
    }
    let Some(status) = status else {
        return Err(Error::new(Reason::Internal, "the command loop ended before the command"));
    };
    let mut result = exit(status, timed_out, wall);
    result.stdout_bytes = out_bytes;
    result.stderr_bytes = err_bytes;
    stream.send_last(result.exit_frame()).await
}

fn input(msg: &Bytes, stdin: &mut Option<mpsc::Sender<Bytes>>, group: &Group) -> Result<(), Error> {
    let Some((&t, rest)) = msg.split_first() else {
        return Ok(());
    };
    match t {
        tag::STDIN => {
            if let Some(tx) = stdin {
                // There is room, checked before receiving. A closed pipe means the command
                // stopped reading, and the bytes have nowhere to go.
                let _ = tx.try_send(msg.slice(1..));
            }
        }
        tag::EOF => *stdin = None,
        tag::SIGNAL => {
            let raw = <[u8; 4]>::try_from(rest)
                .map(i32::from_be_bytes)
                .map_err(|_| invalid("a signal frame that is not four bytes"))?;
            let sig = Signal::from_named_raw(raw)
                .ok_or_else(|| invalid(format!("{raw} is not a signal")))?;
            group.signal(sig);
        }
        other => return Err(invalid(format!("unknown frame tag {other}"))),
    }
    Ok(())
}

fn build(cfg: &Config, c: &Command) -> Result<tokio::process::Command, Error> {
    let mut cmd = if let Some((program, args)) = c.argv.split_first() {
        let mut cmd = tokio::process::Command::new(program);
        cmd.args(args);
        cmd
    } else if !c.shell.is_empty() {
        let mut cmd = tokio::process::Command::new(&cfg.shell);
        cmd.arg("-c").arg(&c.shell);
        cmd
    } else {
        return Err(invalid("the command has neither argv nor shell"));
    };
    cmd.env_clear();
    cmd.envs(cfg.base_env.iter().map(|(k, v)| (k, v)));
    cmd.envs(&c.env);
    cmd.current_dir(if c.cwd.is_empty() { cfg.workdir.as_path() } else { Path::new(&c.cwd) });
    cmd.process_group(0);
    cmd.kill_on_drop(true);
    if let Some(gid) = c.gid.or(cfg.gid) {
        cmd.gid(gid);
    }
    if let Some(uid) = c.uid.or(cfg.uid) {
        cmd.uid(uid);
    }
    Ok(cmd)
}

// What a shell reports for a command it cannot find, with the reason on stderr.
fn spawn_failed(cfg: &Config, c: &Command, e: &std::io::Error, started: Instant) -> RunResult {
    let program = c.argv.first().map_or_else(|| cfg.shell.display().to_string(), Clone::clone);
    let message = format!("hive-drone: cannot run {program}: {e}\n");
    RunResult {
        exit_code: 127,
        stderr_bytes: message.len() as u64,
        stderr: message.into(),
        wall_nanos: started.elapsed().as_nanos() as u64,
        ..RunResult::default()
    }
}

fn exit(status: ExitStatus, timed_out: bool, wall: Duration) -> RunResult {
    RunResult {
        exit_code: status.code().unwrap_or(-1),
        signal: status.signal().unwrap_or(0),
        timed_out,
        wall_nanos: wall.as_nanos() as u64,
        ..RunResult::default()
    }
}

async fn pump<R: AsyncRead + Unpin>(source: Option<R>, ring: Arc<Mutex<Ring>>) {
    let Some(mut source) = source else { return };
    let mut buf = vec![0u8; READ_BUF];
    while let Ok(n @ 1..) = source.read(&mut buf).await {
        lock(&ring).push(&buf[..n]);
    }
}

async fn feed(stdin: Option<ChildStdin>, mut rx: mpsc::Receiver<Bytes>) {
    let Some(mut stdin) = stdin else { return };
    while let Some(data) = rx.recv().await {
        if stdin.write_all(&data).await.is_err() {
            return;
        }
    }
}

// Waits forever on a pipe that is already closed, so select! stops polling it.
async fn read<R: AsyncRead + Unpin>(source: &mut Option<R>, buf: &mut [u8]) -> usize {
    match source {
        Some(r) => r.read(buf).await.unwrap_or(0),
        None => std::future::pending().await,
    }
}

// The rings are only pushed to and taken from, so one left behind by a panic is still whole.
fn lock(ring: &Mutex<Ring>) -> MutexGuard<'_, Ring> {
    ring.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A command's process group, killed when dropped unless the command has exited. A caller that
/// goes away, or a connection that drops, takes the command and its children with it.
#[derive(Debug)]
struct Group {
    pid: Option<Pid>,
    running: bool,
}

impl Group {
    fn new(child: &Child) -> Self {
        let pid = child.id().and_then(|id| Pid::from_raw(id as i32));
        Self { pid, running: true }
    }

    fn signal(&self, sig: Signal) {
        if let Some(pid) = self.pid.filter(|_| self.running) {
            // The group may already be gone, which is fine.
            let _ = rustix::process::kill_process_group(pid, sig);
        }
    }

    fn kill(&self) {
        self.signal(Signal::KILL);
    }

    // After the leader exits its pid can be reused, so the group is left alone from then on.
    // Children it left running in the background keep running, as they would under a shell.
    fn exited(&mut self) {
        self.running = false;
    }
}

impl Drop for Group {
    fn drop(&mut self) {
        if self.running {
            self.kill();
        }
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(Reason::InvalidArgument, message)
}
