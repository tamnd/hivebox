//! Network namespaces a cell can be put in, kept alive by a bind mount on a file, the way
//! `ip netns add` does it.
//!
//! A new namespace has only a loopback device, and it is down. [`create`] brings it up, which is all
//! the `none` network profile needs. Wiring a veth pair and the egress program comes with
//! `hive-guard`.

use rustix::mount::{UnmountFlags, mount_bind, unmount};
use rustix::net::{AddressFamily, RecvFlags, SendFlags, SocketFlags, SocketType};
use rustix::thread::UnshareFlags;
use std::io;
use std::path::{Path, PathBuf};

/// Threads [`create`] spreads a batch over. On 6.8 kernels two threads make nearly twice as many
/// namespaces a second as one, and eight make no more than two on an idle host, but a busy host
/// gains up to eight.
const MAKERS: usize = 4;
/// Fewest paths worth a thread of their own.
const PER_THREAD: usize = 4;

/// Makes a network namespace at each of `paths`, with loopback up, and returns what happened to
/// each, in order. The files must not exist yet. Making one moves the calling thread into it, so
/// they are made on threads of their own, up to `MAKERS` of them.
pub fn create(paths: &[PathBuf]) -> Vec<io::Result<()>> {
    let per = paths.len().div_ceil(MAKERS).max(PER_THREAD);
    std::thread::scope(|s| {
        let threads: Vec<_> = paths
            .chunks(per)
            .map(|chunk| {
                let t = std::thread::Builder::new()
                    .name("hive-netns".into())
                    .spawn_scoped(s, move || chunk.iter().map(|p| make(p)).collect::<Vec<_>>());
                (t, chunk.len())
            })
            .collect();
        threads
            .into_iter()
            .flat_map(|(t, n)| {
                let made = t.and_then(|t| {
                    t.join().map_err(|_| io::Error::other("the netns thread panicked"))
                });
                match made {
                    Ok(results) => results,
                    Err(e) => {
                        (0..n).map(|_| Err(io::Error::new(e.kind(), e.to_string()))).collect()
                    }
                }
            })
            .collect()
    })
}

fn make(path: &Path) -> io::Result<()> {
    // SAFETY: `unshare_unsafe` is unsafe because `FILES` gives this thread a file table of its own,
    // so descriptors other threads open would not be valid here. Only `NEWNET` is passed, which
    // moves this thread alone into a new namespace and changes nothing else about it.
    #[expect(unsafe_code, reason = "the only way to make a network namespace")]
    unsafe {
        rustix::thread::unshare_unsafe(UnshareFlags::NEWNET)?;
    }
    std::fs::OpenOptions::new().write(true).create_new(true).open(path)?;
    let done =
        mount_bind("/proc/thread-self/ns/net", path).map_err(io::Error::from).and_then(|()| {
            loopback_up()
                .map_err(|e| io::Error::new(e.kind(), format!("bringing up loopback: {e}")))
        });
    if done.is_err() {
        let _ = remove(path);
    }
    done
}

/// Sets `IFF_UP` on interface 1, which is loopback in every network namespace, with one
/// `RTM_NEWLINK` message to the kernel. It goes to the namespace the calling thread is in.
fn loopback_up() -> io::Result<()> {
    const RTM_NEWLINK: u16 = 16;
    const NLM_F_REQUEST: u16 = 1;
    const NLM_F_ACK: u16 = 4;
    const NLMSG_ERROR: u16 = 2;
    const IFF_UP: u32 = 1;
    const LOOPBACK: i32 = 1;

    let sock = rustix::net::socket_with(
        AddressFamily::NETLINK,
        SocketType::RAW,
        SocketFlags::CLOEXEC,
        // No protocol means NETLINK_ROUTE.
        None,
    )?;
    // struct nlmsghdr then struct ifinfomsg, in the host's byte order.
    let mut msg = Vec::with_capacity(32);
    msg.extend_from_slice(&32u32.to_ne_bytes());
    msg.extend_from_slice(&RTM_NEWLINK.to_ne_bytes());
    msg.extend_from_slice(&(NLM_F_REQUEST | NLM_F_ACK).to_ne_bytes());
    msg.extend_from_slice(&1u32.to_ne_bytes()); // sequence
    msg.extend_from_slice(&0u32.to_ne_bytes()); // port id, filled in by the kernel
    msg.extend_from_slice(&[0, 0]); // family, padding
    msg.extend_from_slice(&0u16.to_ne_bytes()); // device type
    msg.extend_from_slice(&LOOPBACK.to_ne_bytes());
    msg.extend_from_slice(&IFF_UP.to_ne_bytes()); // flags
    msg.extend_from_slice(&IFF_UP.to_ne_bytes()); // the flags to change
    // An unconnected netlink socket sends to the kernel.
    rustix::net::send(&sock, &msg, SendFlags::empty())?;

    let mut ack = [0u8; 64];
    let (n, _) = rustix::net::recv(&sock, &mut ack, RecvFlags::empty())?;
    let field = |at: usize, len: usize| ack.get(at..at + len).filter(|_| at + len <= n);
    let kind = field(4, 2).map(|b| u16::from_ne_bytes([b[0], b[1]]));
    let errno = field(16, 4).map(|b| i32::from_ne_bytes([b[0], b[1], b[2], b[3]]));
    match (kind, errno) {
        (Some(NLMSG_ERROR), Some(0)) => Ok(()),
        (Some(NLMSG_ERROR), Some(e)) => Err(io::Error::from_raw_os_error(-e)),
        _ => Err(io::Error::other("the kernel's answer was not an acknowledgement")),
    }
}

/// Unmounts the namespace at `path` and removes the file. The kernel frees the namespace once the
/// last process in it is gone. A path that does not exist is fine.
pub fn remove(path: &Path) -> io::Result<()> {
    match unmount(path, UnmountFlags::DETACH) {
        Ok(()) => {}
        Err(rustix::io::Errno::NOENT) => return Ok(()),
        // Not a mount point: made but never mounted, so only the file is left.
        Err(rustix::io::Errno::INVAL) => {}
        Err(e) => return Err(e.into()),
    }
    match std::fs::remove_file(path) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

/// Runs `f` on a thread of its own that has joined the namespace at `path`.
///
/// # Errors
///
/// Fails if the namespace cannot be opened or joined.
pub fn within<T: Send + 'static>(
    path: &Path,
    f: impl FnOnce() -> T + Send + 'static,
) -> io::Result<T> {
    let ns = std::fs::File::open(path)?;
    std::thread::spawn(move || -> io::Result<T> {
        rustix::thread::move_into_link_name_space(
            std::os::fd::AsFd::as_fd(&ns),
            Some(rustix::thread::LinkNameSpaceType::Network),
        )?;
        Ok(f())
    })
    .join()
    .map_err(|_| io::Error::other("the thread inside the namespace panicked"))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};

    fn root() -> bool {
        rustix::process::geteuid().is_root()
    }

    fn scratch() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("hive-netns-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn a_new_namespace_has_only_loopback_and_it_works() {
        if !root() {
            eprintln!("skipped: needs root");
            return;
        }
        let dir = scratch();
        let ns = dir.join("a");
        create(std::slice::from_ref(&ns)).pop().unwrap().unwrap();

        let (devices, echoed) = within(&ns, || {
            let devices = std::fs::read_to_string("/proc/thread-self/net/dev").unwrap();
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let mut c = TcpStream::connect(l.local_addr().unwrap()).unwrap();
            let (mut s, _) = l.accept().unwrap();
            c.write_all(b"ping").unwrap();
            let mut buf = [0; 4];
            s.read_exact(&mut buf).unwrap();
            (devices, buf)
        })
        .unwrap();
        let names: Vec<&str> =
            devices.lines().skip(2).filter_map(|l| l.split(':').next()).map(str::trim).collect();
        assert_eq!(names, ["lo"]);
        assert_eq!(&echoed, b"ping");

        // Two namespaces do not see each other's sockets.
        let other = dir.join("b");
        create(std::slice::from_ref(&other)).pop().unwrap().unwrap();
        let port = within(&ns, || {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = l.local_addr().unwrap().port();
            std::mem::forget(l);
            port
        })
        .unwrap();
        let reached =
            within(&other, move || TcpStream::connect(("127.0.0.1", port)).is_ok()).unwrap();
        assert!(!reached);

        remove(&ns).unwrap();
        remove(&other).unwrap();
        assert!(!ns.exists() && !other.exists());
        remove(&ns).unwrap();
        std::fs::remove_dir(&dir).unwrap();
    }

    #[test]
    fn an_existing_file_is_not_overwritten() {
        if !root() {
            eprintln!("skipped: needs root");
            return;
        }
        let dir = scratch();
        let taken = dir.join("taken");
        std::fs::write(&taken, "mine").unwrap();
        let results = create(&[taken.clone(), dir.join("free")]);
        assert_eq!(results[0].as_ref().unwrap_err().kind(), io::ErrorKind::AlreadyExists);
        assert!(results[1].is_ok());
        assert_eq!(std::fs::read_to_string(&taken).unwrap(), "mine");
        remove(&dir.join("free")).unwrap();
        std::fs::remove_file(&taken).unwrap();
        std::fs::remove_dir(&dir).unwrap();
    }

    #[test]
    fn making_one_without_root_fails_cleanly() {
        if root() {
            return;
        }
        let dir = scratch();
        let ns = dir.join("a");
        assert!(create(std::slice::from_ref(&ns)).pop().unwrap().is_err());
        assert!(!ns.exists());
        std::fs::remove_dir(&dir).unwrap();
    }

    #[test]
    #[ignore = "a measurement, not a test"]
    fn speed() {
        use std::time::Instant;
        const N: usize = 400;
        let dir = scratch();
        let paths: Vec<PathBuf> = (0..N).map(|i| dir.join(format!("cell-{i}"))).collect();
        // One thread against the spread, in turns, so a busy host weighs on both alike.
        let mut rates = [Vec::new(), Vec::new()];
        for (i, chunk) in paths.chunks(N / 8).enumerate() {
            let chunk = chunk.to_vec();
            let t0 = Instant::now();
            let results = if i % 2 == 0 {
                std::thread::spawn(move || chunk.iter().map(|p| make(p)).collect::<Vec<_>>())
                    .join()
                    .unwrap()
            } else {
                create(&chunk)
            };
            results.into_iter().for_each(|r| r.unwrap());
            rates[i % 2].push((N / 8) as f64 / t0.elapsed().as_secs_f64());
        }
        for (name, r) in ["one thread", "create"].iter().zip(&rates) {
            let r: Vec<String> = r.iter().map(|x| format!("{x:.0}")).collect();
            eprintln!("made {} at a time on {name}: {}/s", N / 8, r.join(", "));
        }

        let t0 = Instant::now();
        for p in &paths[..20] {
            remove(p).unwrap();
        }
        eprintln!("removed 20 one at a time: {:?} each", t0.elapsed() / 20);

        for threads in [8, 64] {
            let batch: Vec<PathBuf> =
                if threads == 8 { paths[20..120].to_vec() } else { paths[120..].to_vec() };
            let n = batch.len();
            let t0 = Instant::now();
            std::thread::scope(|s| {
                for chunk in batch.chunks(n.div_ceil(threads)) {
                    s.spawn(move || chunk.iter().for_each(|p| remove(p).unwrap()));
                }
            });
            let took = t0.elapsed();
            eprintln!(
                "removed {n} on {threads} threads: {took:?}, {:.0}/s",
                n as f64 / took.as_secs_f64()
            );
        }
        std::fs::remove_dir(&dir).unwrap();
    }
}
