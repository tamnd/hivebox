//! Persistent shells, from `spec/09_guest_agent.md`, section 4.
//!
//! A session is one shell that lives across calls, so `cd`, variables and functions carry over
//! from one command to the next the way they do in a terminal. Stdout and stderr share one pipe,
//! so output comes back interleaved as it was written.
//!
//! The drone knows a command has finished because every command is wrapped like this:
//!
//! ```text
//! { <command>
//! } </dev/null; __hive_rc=$?; printf '\036%s:%d\036' '<token>' "$__hive_rc"
//! ```
//!
//! The token is 128 random bits, new for every command, and never put in the environment, so the
//! command cannot print the marker early and fake its own exit code. The drone cuts the marker
//! out of the output and reads the exit code from it. If the marker does not come before the
//! timeout, or the shell exits, the shell's process group is killed and the next call starts a
//! new shell.

use crate::process::{DRAIN, Group, READ_BUF, invalid, setup};
use crate::{Config, ring::Ring};
use hive_proto::drone::api::{
    SessionCreate, SessionInfo, SessionRun, SessionRunResult, SessionSend, SessionSendResult,
};
use hive_rt::{OsRng, Rng};
use hive_types::{Error, Reason};
use rustix::pipe::PipeFlags;
use rustix::process::{Pid, Signal};
use std::collections::HashMap;
use std::fmt::Write as _;
use std::os::unix::process::ExitStatusExt;
use std::path::Path;
use std::process::{ExitStatus, Stdio};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::unix::pipe;
use tokio::process::{Child, ChildStdin};

/// Most sessions one drone keeps at once.
pub(crate) const MAX_SESSIONS: usize = 64;
const MARK: u8 = 0x1e;
// The longest exit code the marker can carry, with room for a sign.
const MAX_CODE_LEN: usize = 12;
const DEFAULT_QUIET: Duration = Duration::from_millis(200);

#[derive(Debug, Default)]
pub(crate) struct Sessions {
    map: Mutex<HashMap<String, Arc<Session>>>,
}

#[derive(Debug)]
struct Session {
    spec: SessionCreate,
    // The shell's pid while there is one, so a close can kill it while a call holds the lock.
    pid: AtomicI32,
    shell: tokio::sync::Mutex<Option<Shell>>,
}

#[derive(Debug)]
struct Shell {
    child: Child,
    stdin: ChildStdin,
    output: pipe::Receiver,
    group: Group,
    // Output read past the end of the last command, which belongs to the next one.
    leftover: Vec<u8>,
}

enum End {
    Marker(i32),
    Exited(ExitStatus),
    Closed,
    TimedOut,
}

impl Sessions {
    fn table(&self) -> MutexGuard<'_, HashMap<String, Arc<Session>>> {
        self.map.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn len(&self) -> usize {
        self.table().len()
    }

    pub(crate) fn create(&self, cfg: &Config, spec: SessionCreate) -> Result<SessionInfo, Error> {
        if self.len() >= MAX_SESSIONS {
            return Err(Error::new(
                Reason::CapacityUnavailable,
                format!("{MAX_SESSIONS} sessions are open already"),
            ));
        }
        let pid = AtomicI32::new(0);
        // Starting the shell now means a bad shell or directory fails here and not on first use.
        let shell = spawn(cfg, &spec, &pid)?;
        let session = Arc::new(Session { spec, pid, shell: tokio::sync::Mutex::new(Some(shell)) });
        let mut id = String::with_capacity(32);
        for b in &OsRng.secret()[..16] {
            let _ = write!(id, "{b:02x}");
        }
        self.table().insert(id.clone(), session);
        Ok(SessionInfo { id })
    }

    pub(crate) fn close(&self, id: &str) -> Result<(), Error> {
        let session = self.table().remove(id).ok_or_else(|| not_found(id))?;
        // A call in progress holds the shell, so kill it by pid. The call then sees the shell
        // exit and returns, and the shell is reaped when the last reference goes.
        if let Some(pid) = Pid::from_raw(session.pid.load(Ordering::Acquire)) {
            let _ = rustix::process::kill_process_group(pid, Signal::KILL);
        }
        Ok(())
    }

    fn get(&self, id: &str) -> Result<Arc<Session>, Error> {
        self.table().get(id).cloned().ok_or_else(|| not_found(id))
    }

    pub(crate) async fn run(
        &self,
        cfg: &Config,
        req: SessionRun,
    ) -> Result<SessionRunResult, Error> {
        let session = self.get(&req.id)?;
        let mut guard = session.shell.lock().await;
        let shell = match &mut *guard {
            Some(shell) => shell,
            None => guard.insert(spawn(cfg, &session.spec, &session.pid)?),
        };
        let mut token = String::with_capacity(32);
        for b in &OsRng.secret()[..16] {
            let _ = write!(token, "{b:02x}");
        }
        let command = if req.command.trim().is_empty() { ":" } else { req.command.as_str() };
        let script = format!(
            "{{ {command}\n}} </dev/null; __hive_rc=$?; printf '\\036%s:%d\\036' '{token}' \"$__hive_rc\"\n"
        );
        let marker = format!("\x1e{token}:").into_bytes();
        let mut ring = Ring::new(cfg.output_limit(req.max_output_bytes));
        let started = Instant::now();
        let deadline = tokio::time::Instant::now() + cfg.timeout(req.timeout_ms);
        let mut pending = std::mem::take(&mut shell.leftover);
        let end = if shell.stdin.write_all(script.as_bytes()).await.is_err() {
            End::Closed
        } else {
            let mut buf = vec![0u8; READ_BUF];
            loop {
                match scan(&pending, &marker) {
                    Scan::Found { at, code, end } => {
                        ring.push(&pending[..at]);
                        shell.leftover = pending[end..].to_vec();
                        break End::Marker(code);
                    }
                    Scan::Keep(n) => {
                        ring.push(&pending[..n]);
                        pending.drain(..n);
                    }
                }
                tokio::select! {
                    n = shell.output.read(&mut buf) => match n {
                        Ok(0) | Err(_) => break End::Closed,
                        Ok(n) => pending.extend_from_slice(&buf[..n]),
                    },
                    status = shell.child.wait() => match status {
                        Ok(status) => break End::Exited(status),
                        Err(_) => break End::Closed,
                    },
                    () = tokio::time::sleep_until(deadline) => break End::TimedOut,
                }
            }
        };
        let wall = started.elapsed();
        let (exit_code, timed_out, restarted) = match end {
            End::Marker(code) => (code, false, false),
            End::Exited(status) => {
                // What the shell wrote on its way out is still in the pipe.
                ring.push(&pending);
                drain(&mut shell.output, &mut ring).await;
                (exit_code(status), false, true)
            }
            // The shell closed its output, which usually means it is exiting. Give it a moment
            // so the exit code is the shell's and not a guess.
            End::Closed => match tokio::time::timeout(DRAIN, shell.child.wait()).await {
                Ok(Ok(status)) => {
                    ring.push(&pending);
                    (exit_code(status), false, true)
                }
                _ => (-1, false, true),
            },
            End::TimedOut => {
                ring.push(&pending);
                (-1, true, true)
            }
        };
        if restarted {
            stop(&mut guard, &session.pid).await;
        }
        Ok(SessionRunResult {
            exit_code,
            truncated: ring.truncated(),
            timed_out,
            wall_nanos: wall.as_nanos() as u64,
            output_bytes: ring.total(),
            restarted,
            output: ring.take(),
        })
    }

    pub(crate) async fn send(
        &self,
        cfg: &Config,
        req: SessionSend,
    ) -> Result<SessionSendResult, Error> {
        let session = self.get(&req.id)?;
        let mut guard = session.shell.lock().await;
        let shell = match &mut *guard {
            Some(shell) => shell,
            None => guard.insert(spawn(cfg, &session.spec, &session.pid)?),
        };
        let expect = req.expect.as_bytes();
        let quiet = match (req.quiet_ms, expect.is_empty()) {
            (0, true) => Some(DEFAULT_QUIET),
            (0, false) => None,
            (ms, _) => Some(Duration::from_millis(ms)),
        };
        let mut ring = Ring::new(cfg.output_limit(req.max_output_bytes));
        // The tail of the output so far, long enough to find `expect` across two reads.
        let mut window = Vec::new();
        let leftover = std::mem::take(&mut shell.leftover);
        let mut matched = false;
        if let Some(n) = see(&leftover, expect, &mut window, &mut ring) {
            shell.leftover = leftover[n..].to_vec();
            matched = true;
        }
        let deadline = tokio::time::Instant::now() + cfg.timeout(req.timeout_ms);
        let mut last = tokio::time::Instant::now();
        let mut timed_out = false;
        let mut restarted = false;
        if !req.input.is_empty() && shell.stdin.write_all(&req.input).await.is_err() {
            restarted = true;
        }
        let mut buf = vec![0u8; READ_BUF];
        while !matched && !restarted {
            let wake = quiet.map_or(deadline, |q| deadline.min(last + q));
            tokio::select! {
                n = shell.output.read(&mut buf) => match n {
                    Ok(0) | Err(_) => restarted = true,
                    Ok(n) => {
                        last = tokio::time::Instant::now();
                        if let Some(end) = see(&buf[..n], expect, &mut window, &mut ring) {
                            shell.leftover = buf[end..n].to_vec();
                            matched = true;
                        }
                    }
                },
                status = shell.child.wait() => {
                    let _ = status;
                    drain(&mut shell.output, &mut ring).await;
                    restarted = true;
                }
                () = tokio::time::sleep_until(wake) => {
                    timed_out = wake == deadline;
                    break;
                }
            }
        }
        if restarted {
            stop(&mut guard, &session.pid).await;
        }
        Ok(SessionSendResult {
            truncated: ring.truncated(),
            matched,
            timed_out,
            output_bytes: ring.total(),
            restarted,
            output: ring.take(),
        })
    }
}

fn spawn(cfg: &Config, spec: &SessionCreate, pid: &AtomicI32) -> Result<Shell, Error> {
    let program =
        if spec.shell.is_empty() { cfg.session_shell.as_path() } else { Path::new(&spec.shell) };
    let mut cmd = tokio::process::Command::new(program);
    if program.file_name().is_some_and(|n| n == "bash") {
        cmd.args(["--noprofile", "--norc"]);
    }
    setup(&mut cmd, cfg, &spec.env, &spec.cwd, spec.uid, spec.gid);
    // One pipe for stdout and stderr, so output interleaves as it was written. Both ends are
    // close on exec, so no other command the drone starts holds the write end open.
    let (read, write) = rustix::pipe::pipe_with(PipeFlags::CLOEXEC).map_err(internal)?;
    let write2 = write.try_clone().map_err(internal)?;
    cmd.stdin(Stdio::piped()).stdout(Stdio::from(write)).stderr(Stdio::from(write2));
    let spawned = cmd.spawn();
    // The command holds the drone's copies of the write end until it is dropped, and the reader
    // only sees the end of output once every copy is closed.
    drop(cmd);
    let mut child = spawned.map_err(|e| {
        invalid(format!("cannot start the session shell {}: {e}", program.display()))
    })?;
    let stdin = child.stdin.take().ok_or_else(|| internal("the shell has no stdin"))?;
    let output = pipe::Receiver::from_owned_fd(read).map_err(internal)?;
    let group = Group::new(&child);
    pid.store(child.id().map_or(0, |id| id as i32), Ordering::Release);
    Ok(Shell { child, stdin, output, group, leftover: Vec::new() })
}

// Kills what is left of the shell and forgets it, so the next call starts a new one.
async fn stop(slot: &mut Option<Shell>, pid: &AtomicI32) {
    pid.store(0, Ordering::Release);
    if let Some(mut shell) = slot.take() {
        shell.group.kill();
        let _ = shell.child.wait().await;
        shell.group.exited();
    }
}

async fn drain(output: &mut pipe::Receiver, ring: &mut Ring) {
    let mut buf = vec![0u8; READ_BUF];
    let _ = tokio::time::timeout(DRAIN, async {
        while let Ok(n @ 1..) = output.read(&mut buf).await {
            ring.push(&buf[..n]);
        }
    })
    .await;
}

// Keeps `data` up to the end of the first match of `expect`, if any, and returns where that
// match ends. What comes after it belongs to the next call. `window` holds the end of the output
// seen so far, so a match split across two reads is found.
fn see(data: &[u8], expect: &[u8], window: &mut Vec<u8>, ring: &mut Ring) -> Option<usize> {
    if expect.is_empty() {
        ring.push(data);
        return None;
    }
    let old = window.len();
    window.extend_from_slice(data);
    // The window held less than a whole match before, so any match ends inside `data`.
    if let Some(at) = window.windows(expect.len()).position(|w| w == expect) {
        let end = at + expect.len() - old;
        ring.push(&data[..end]);
        window.clear();
        return Some(end);
    }
    ring.push(data);
    let keep = expect.len() - 1;
    if window.len() > keep {
        window.drain(..window.len() - keep);
    }
    None
}

#[derive(Debug, PartialEq)]
enum Scan {
    /// The marker is at `at` and ends before `end`.
    Found { at: usize, code: i32, end: usize },
    /// No marker yet. The first `n` bytes are output, and the rest might be the start of one.
    Keep(usize),
}

fn scan(data: &[u8], marker: &[u8]) -> Scan {
    let mut from = 0;
    while let Some(i) = data[from..].iter().position(|&b| b == MARK).map(|i| i + from) {
        let rest = &data[i..];
        if rest.len() < marker.len() {
            if marker.starts_with(rest) {
                return Scan::Keep(i);
            }
        } else if rest.starts_with(marker) {
            let tail = &rest[marker.len()..];
            match tail.iter().position(|&b| b == MARK) {
                Some(j) => {
                    if let Some(code) =
                        std::str::from_utf8(&tail[..j]).ok().and_then(|s| s.parse().ok())
                    {
                        return Scan::Found { at: i, code, end: i + marker.len() + j + 1 };
                    }
                }
                None if tail.len() <= MAX_CODE_LEN => return Scan::Keep(i),
                None => {}
            }
        }
        from = i + 1;
    }
    Scan::Keep(data.len())
}

fn exit_code(status: ExitStatus) -> i32 {
    status.code().or_else(|| status.signal().map(|s| 128 + s)).unwrap_or(-1)
}

fn not_found(id: &str) -> Error {
    Error::new(Reason::InvalidArgument, format!("no session {id:?}"))
}

fn internal(e: impl ToString) -> Error {
    Error::new(Reason::Internal, e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    const M: &[u8] = b"\x1eabc:";

    #[test]
    fn the_marker_is_found_and_cut_out() {
        let data = b"hello\n\x1eabc:3\x1erest";
        assert_eq!(scan(data, M), Scan::Found { at: 6, code: 3, end: 13 });
    }

    #[test]
    fn a_marker_split_across_reads_is_held_back() {
        assert_eq!(scan(b"out\x1eab", M), Scan::Keep(3));
        assert_eq!(scan(b"out\x1eabc:12", M), Scan::Keep(3));
        assert_eq!(scan(b"out\x1e", M), Scan::Keep(3));
    }

    #[test]
    fn a_marker_with_the_wrong_token_is_output() {
        assert_eq!(scan(b"\x1exyz:0\x1e", M), Scan::Keep(6));
        assert_eq!(scan(b"a\x1eb\x1eabc:0\x1e", M), Scan::Found { at: 3, code: 0, end: 10 });
    }

    #[test]
    fn a_match_split_across_reads_is_found_and_cut() {
        let (mut window, mut ring) = (Vec::new(), Ring::new(1 << 20));
        assert_eq!(see(b"abc>", b">>> ", &mut window, &mut ring), None);
        assert_eq!(see(b">> rest", b">>> ", &mut window, &mut ring), Some(3));
        assert_eq!(&ring.take()[..], b"abc>>> ");
    }

    #[test]
    fn a_garbled_code_is_not_an_end() {
        assert_eq!(scan(b"\x1eabc:x\x1e", M), Scan::Keep(6));
        assert_eq!(scan(b"\x1eabc:0000000000000000000", M), Scan::Keep(24));
    }
}
