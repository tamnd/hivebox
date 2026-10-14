//! The process that makes containers.
//!
//! libcontainer starts a container with a raw `clone3`, and the child runs Rust code, allocations
//! included, before it execs. After a clone from a process with many threads, any lock another
//! thread held stays locked in the child for good, the allocator's among them, so the comb with
//! its runtime threads cannot do this itself. A worker is a plain single threaded process that
//! does nothing else: the driver starts a few of them, sends each a create as one line of JSON on
//! stdin, and reads the answers and the exits of the containers it made on stdout.
//!
//! Each worker makes its containers from its own private mount namespace, which it cuts down
//! when it starts, so the table every create copies does not grow with the cells on the node. A
//! create comes with its cell's network namespace and root filesystem as descriptors, since
//! neither is mounted where the worker can see it. The root is attached in the worker's namespace
//! for the length of the create, and the container keeps its own copy after.
//!
//! libcontainer makes each container's init a sibling of the intermediate process with no exit
//! signal, so the init is the worker's child but a "clone" child that only `__WALL` waits for.
//! The worker holds a pidfd for each one, waits on it when it ends and reports how.

use nix::sys::wait::{Id, WaitPidFlag, WaitStatus, waitid};
use rustix::event::{PollFd, PollFlags, poll};
use rustix::mount::{MountPropagationFlags, MoveMountFlags, UnmountFlags};
use rustix::net::{RecvAncillaryBuffer, RecvAncillaryMessage, RecvFlags, recvmsg};
use rustix::process::{Pid, PidfdFlags, pidfd_open};
use rustix::thread::{LinkNameSpaceType::Network, move_into_link_name_space};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::io::{IoSliceMut, Write};
use std::mem::MaybeUninit;
use std::os::fd::{AsFd, AsRawFd, OwnedFd};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

/// One create, from the driver to a worker.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Create {
    /// Matches the answer to the request.
    pub(crate) seq: u64,
    /// The container id, which is the cell id.
    pub(crate) id: String,
    /// The cell's directory. It holds the bundle, libcontainer's state and the log.
    pub(crate) dir: PathBuf,
    /// The cell's network namespace, which the container starts in. `None` is the host's. It
    /// comes as a descriptor with the create, and the path is for messages.
    pub(crate) netns: Option<PathBuf>,
    /// Whether the root filesystem comes as a mount on a descriptor with the create, after the
    /// network namespace's.
    #[serde(default)]
    pub(crate) rootfs: bool,
    /// Where the drone's socket goes. The worker binds it here, outside the container, and the
    /// drone gets it as descriptor 3.
    pub(crate) socket: PathBuf,
    /// The drone's first secret, which it reads from stdin.
    pub(crate) secret: [u8; 32],
}

/// What a worker says, one per line.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Reply {
    /// The create with this `seq` worked, and the container's init is `pid`.
    Created { seq: u64, pid: i32 },
    /// The create with this `seq` failed.
    Failed { seq: u64, error: String },
    /// A container this worker made has ended.
    Exited { pid: i32, code: Option<i32>, signal: Option<i32> },
}

/// Runs a worker on stdin and stdout until stdin closes.
#[must_use]
pub fn main() -> ExitCode {
    // Descriptors 0 to 2 are the pipes to the driver, so the lowest free one is 3.
    let three = null();
    if three.as_raw_fd() != 3 {
        eprintln!("hive-oci worker: descriptor 3 is taken, so containers cannot get their socket");
        return ExitCode::FAILURE;
    }
    if let Err(e) = private_mounts() {
        eprintln!("hive-oci worker: making its own mount namespace: {e}");
        return ExitCode::FAILURE;
    }
    let host_net = match std::fs::File::open("/proc/self/ns/net") {
        Ok(f) => f.into(),
        Err(e) => {
            eprintln!("hive-oci worker: opening its own network namespace: {e}");
            return ExitCode::FAILURE;
        }
    };
    let mut worker =
        Worker { three, host_net, inits: Vec::new(), input: Vec::new(), fds: VecDeque::new() };
    match worker.run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("hive-oci worker: {e}");
            ExitCode::FAILURE
        }
    }
}

struct Worker {
    /// Descriptor 3, on `/dev/null` between creates so nothing else lands on it, and on the
    /// drone's socket during one, since that is where libcontainer passes it on.
    three: OwnedFd,
    /// The network namespace the worker started in, to go back to after each create.
    host_net: OwnedFd,
    /// Every container init this worker made that has not ended, with a pidfd for it.
    inits: Vec<(i32, OwnedFd)>,
    /// Bytes read from stdin that do not make a whole line yet.
    input: Vec<u8>,
    /// Descriptors that came on stdin, in order, for the next create.
    fds: VecDeque<OwnedFd>,
}

impl Worker {
    fn run(&mut self) -> std::io::Result<()> {
        let stdin = std::io::stdin();
        let mut buf = vec![0u8; 64 << 10];
        loop {
            let (input, ended) = {
                let mut fds = Vec::with_capacity(self.inits.len() + 1);
                fds.push(PollFd::new(&stdin, PollFlags::IN));
                fds.extend(self.inits.iter().map(|(_, fd)| PollFd::new(fd, PollFlags::IN)));
                match poll(&mut fds, None) {
                    Ok(_) => {}
                    Err(rustix::io::Errno::INTR) => continue,
                    Err(e) => return Err(e.into()),
                }
                let input = !fds[0].revents().is_empty();
                let ended: Vec<usize> = fds[1..]
                    .iter()
                    .enumerate()
                    .filter(|(_, f)| !f.revents().is_empty())
                    .map(|(i, _)| i)
                    .collect();
                (input, ended)
            };
            // Highest first, so each removal leaves the lower indices where they were.
            for i in ended.into_iter().rev() {
                let (pid, fd) = self.inits.swap_remove(i);
                self.reap(pid, &fd)?;
            }
            if input {
                // Poll said there is something, so this does not block. None is the end.
                let n = self.receive(&mut buf)?;
                if n == 0 {
                    return Ok(());
                }
                self.input.extend_from_slice(&buf[..n]);
                while let Some(end) = self.input.iter().position(|b| *b == b'\n') {
                    let line: Vec<u8> = self.input.drain(..=end).collect();
                    let reply = match serde_json::from_slice::<Create>(&line) {
                        Ok(c) => self.create(c),
                        Err(e) => return Err(std::io::Error::other(format!("bad request: {e}"))),
                    };
                    say(&reply)?;
                }
            }
        }
    }

    /// Reads what the driver sent, and keeps any descriptors that came with it.
    fn receive(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(2))];
        let mut control = RecvAncillaryBuffer::new(&mut space);
        let got = recvmsg(
            std::io::stdin(),
            &mut [IoSliceMut::new(buf)],
            &mut control,
            RecvFlags::CMSG_CLOEXEC,
        )?;
        for m in control.drain() {
            if let RecvAncillaryMessage::ScmRights(fds) = m {
                self.fds.extend(fds);
            }
        }
        Ok(got.bytes)
    }

    fn create(&mut self, c: Create) -> Reply {
        let netns = if c.netns.is_some() { self.fds.pop_front().map(Some) } else { Some(None) };
        let root = if c.rootfs { self.fds.pop_front().map(Some) } else { Some(None) };
        // Anything more belongs to no create.
        self.fds.clear();
        let (Some(netns), Some(root)) = (netns, root) else {
            let error = "a descriptor the create needs did not come with it".into();
            return Reply::Failed { seq: c.seq, error };
        };
        let rootfs = c.dir.join("rootfs");
        let made = attach(root.as_ref(), &rootfs)
            .and_then(|()| self.join(netns.as_ref(), c.netns.as_deref()))
            .and_then(|()| create(&mut self.three, &c));
        if root.is_some() {
            // The container has its own copy by now, or never will.
            let _ = rustix::mount::unmount(&rootfs, UnmountFlags::DETACH);
        }
        if netns.is_some()
            && let Err(e) = move_into_link_name_space(self.host_net.as_fd(), Some(Network))
        {
            // Every later create would land in this cell's network, so the worker goes and the
            // driver starts a new one.
            eprintln!("hive-oci worker: going back to the host network: {e}");
            std::process::exit(1);
        }
        match made {
            Ok(pid) => match Pid::from_raw(pid).map(|p| pidfd_open(p, PidfdFlags::empty())) {
                Some(Ok(fd)) => {
                    self.inits.push((pid, fd));
                    Reply::Created { seq: c.seq, pid }
                }
                // Only this worker can wait on the init, so it is there, even if it has ended.
                _ => Reply::Failed { seq: c.seq, error: format!("no pidfd for {pid}") },
            },
            Err(error) => Reply::Failed { seq: c.seq, error },
        }
    }

    /// Moves the worker into `netns`, so the container it makes next starts there. libcontainer
    /// cannot join it for the container, since it makes the user namespace first, and from inside
    /// that the host's network namespaces are out of reach.
    fn join(&self, netns: Option<&OwnedFd>, path: Option<&Path>) -> Result<(), String> {
        let Some(fd) = netns else { return Ok(()) };
        let path = path.unwrap_or(Path::new("the network namespace"));
        move_into_link_name_space(fd.as_fd(), Some(Network))
            .map_err(|e| format!("joining {}: {e}", path.display()))
    }

    fn reap(&mut self, pid: i32, fd: &OwnedFd) -> std::io::Result<()> {
        let flags = WaitPidFlag::WEXITED | WaitPidFlag::WNOHANG | WaitPidFlag::__WALL;
        let (code, signal) = match waitid(Id::PIDFd(fd.as_fd()), flags) {
            Ok(WaitStatus::Exited(_, code)) => (Some(code), None),
            Ok(WaitStatus::Signaled(_, sig, _)) => (None, Some(sig as i32)),
            // Readable but not waitable means it is gone some other way. Say so all the same.
            _ => (None, None),
        };
        say(&Reply::Exited { pid, code, signal })
    }
}

/// Moves the worker into a mount namespace of its own that gets nothing from the host's after
/// this, and drops the network namespaces bound there, which are one for each cell and which it
/// only ever gets as descriptors.
fn private_mounts() -> std::io::Result<()> {
    nix::sched::unshare(nix::sched::CloneFlags::CLONE_NEWNS)?;
    rustix::mount::mount_change("/", MountPropagationFlags::PRIVATE | MountPropagationFlags::REC)?;
    let table = std::fs::read_to_string("/proc/self/mountinfo")?;
    for point in table.lines().rev().filter_map(nsfs_point) {
        let _ = rustix::mount::unmount(point, UnmountFlags::DETACH);
    }
    Ok(())
}

/// Where the mount in a line of `mountinfo` is, if it is a namespace file.
fn nsfs_point(line: &str) -> Option<String> {
    let (left, right) = line.split_once(" - ")?;
    (right.split(' ').next()? == "nsfs").then_some(())?;
    Some(unescape(left.split(' ').nth(4)?))
}

/// A `mountinfo` path, with the octal escapes the kernel puts in for spaces and the like undone.
fn unescape(path: &str) -> String {
    let b = path.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        let octal = b.get(i + 1..i + 4).filter(|d| d.iter().all(|c| (b'0'..=b'7').contains(c)));
        match (b[i], octal) {
            (b'\\', Some(d)) => {
                out.push((d[0] - b'0') * 64 + (d[1] - b'0') * 8 + (d[2] - b'0'));
                i += 4;
            }
            (c, _) => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Attaches the mount on `root`, if there is one, at `at`.
fn attach(root: Option<&OwnedFd>, at: &Path) -> Result<(), String> {
    let Some(root) = root else { return Ok(()) };
    rustix::mount::move_mount(
        root,
        "",
        rustix::fs::CWD,
        at,
        MoveMountFlags::MOVE_MOUNT_F_EMPTY_PATH,
    )
    .map_err(|e| format!("attaching the root filesystem: {e}"))
}

fn say(reply: &Reply) -> std::io::Result<()> {
    let mut line = serde_json::to_vec(reply).map_err(std::io::Error::other)?;
    line.push(b'\n');
    let mut out = std::io::stdout().lock();
    out.write_all(&line)?;
    out.flush()
}

/// Makes and starts one container, and returns its init's pid.
fn create(three: &mut OwnedFd, c: &Create) -> Result<i32, String> {
    use libcontainer::container::builder::ContainerBuilder;
    use libcontainer::syscall::syscall::SyscallType;

    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(c.dir.join("drone.log"))
        .map_err(|e| format!("opening the log: {e}"))?;
    let log2 = log.try_clone().map_err(|e| e.to_string())?;
    // The drone reads its secret from stdin. A pipe holds far more than 32 bytes, so the write
    // end is filled and closed before the drone ever starts.
    let (stdin, mut secret) = std::io::pipe().map_err(|e| e.to_string())?;
    secret.write_all(&c.secret).map_err(|e| e.to_string())?;
    drop(secret);
    let _ = std::fs::remove_file(&c.socket);
    let listener = UnixListener::bind(&c.socket)
        .map_err(|e| format!("binding {}: {e}", c.socket.display()))?;
    // libcontainer keeps descriptors from 3 up to the number preserved open in the container, so
    // the socket sits on 3 for the length of the create.
    rustix::io::dup2(&listener, three).map_err(|e| format!("moving the socket: {e}"))?;
    drop(listener);
    let built = ContainerBuilder::new(c.id.clone(), SyscallType::default())
        .with_root_path(&c.dir)
        .and_then(ContainerBuilder::validate_id)
        .map(|b| {
            b.with_stdin(stdin)
                .with_stdout(log)
                .with_stderr(log2)
                .with_preserved_fds(1)
                .as_init(&c.dir)
                .with_systemd(false)
                .with_detach(true)
        })
        .and_then(libcontainer::container::init_builder::InitContainerBuilder::build);
    let parked = rustix::io::dup2(null(), three);
    let mut container = built.map_err(|e| format!("making the container: {e}"))?;
    parked.map_err(|e| format!("clearing descriptor 3: {e}"))?;
    container.start().map_err(|e| format!("starting the container: {e}"))?;
    container.pid().map(nix::unistd::Pid::as_raw).ok_or_else(|| "the container has no pid".into())
}

fn null() -> OwnedFd {
    std::fs::File::open("/dev/null").expect("/dev/null").into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_namespace_files_are_dropped() {
        let ns = "1102 29 0:4 net:[4026532899] /root/hb\\040x/netns/a rw shared:5 - nsfs nsfs rw";
        let ov = "1103 29 0:91 / /run/hivebox/oci/a/rootfs rw shared:6 - overlay overlay rw";
        assert_eq!(nsfs_point(ns).as_deref(), Some("/root/hb x/netns/a"));
        assert_eq!(nsfs_point(ov), None);
        assert_eq!(unescape("a\\134b\\01"), "a\\b\\01");
    }
}
