//! Starting QEMU in the cell's cgroup and network namespace, in a user namespace of its own.
//!
//! The user namespace maps the ids the cell's files are owned by, `uid_base` and up, to 0 and up,
//! so QEMU's 9p server sees and makes files with the owners the guest means. QEMU itself does not
//! run as one of those ids, which every container cell's processes run as too. It runs as one
//! more id mapped to a host uid of its own for each virtual machine, with only the capabilities
//! that serving the cell's files takes. A QEMU a guest breaks out into can then not signal or
//! trace any other cell's processes, and has no capability over any file the cell does not own.
//!
//! It also gets a mount namespace of its own with an empty tmpfs over each of the hivebox state
//! directories, so the other cells' files are not even there to find. The few files QEMU needs
//! from those, its root filesystem, kernel and initramfs, are passed in as open descriptors.
//!
//! The maps can only be written from outside the new namespace, so a thread here writes them while
//! the child waits on a pipe between its `unshare` and its `exec`.

use rustix::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};
use rustix::thread::{CapabilitySet, CapabilitySets, LinkNameSpaceType, UnshareFlags};
use std::ffi::CString;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use tokio::process::{Child, Command};

/// What QEMU keeps: enough to make, read, write and change the owner and mode of the files the
/// cell owns, which is what the 9p server's passthrough mode does.
const CAPS: CapabilitySet = CapabilitySet::CHOWN
    .union(CapabilitySet::DAC_OVERRIDE)
    .union(CapabilitySet::DAC_READ_SEARCH)
    .union(CapabilitySet::FOWNER)
    .union(CapabilitySet::FSETID)
    .union(CapabilitySet::SETFCAP);

/// Where and as whom QEMU runs.
#[derive(Debug)]
pub(crate) struct Jail {
    /// The cell's `cgroup.procs`, open for writing.
    pub(crate) procs: OwnedFd,
    /// The cell's network namespace, if it has one.
    pub(crate) netns: Option<OwnedFd>,
    /// The host id the cell's root is.
    pub(crate) uid_base: u32,
    /// How many ids the cell has.
    pub(crate) uid_count: u32,
    /// The host uid and gid QEMU runs as.
    pub(crate) vmm: u32,
    /// Supplementary groups QEMU keeps, such as the group of `/dev/kvm`.
    pub(crate) groups: Vec<u32>,
    /// Directories hidden under an empty tmpfs.
    pub(crate) hide: Vec<PathBuf>,
    /// Descriptors QEMU gets, at the same numbers.
    pub(crate) keep: Vec<RawFd>,
}

/// Starts `cmd` in the jail.
pub(crate) fn spawn(mut cmd: Command, jail: Jail) -> io::Result<Child> {
    let (pid_r, pid_w) = rustix::pipe::pipe_with(rustix::pipe::PipeFlags::CLOEXEC)?;
    let (go_r, go_w) = rustix::pipe::pipe_with(rustix::pipe::PipeFlags::CLOEXEC)?;
    let maps =
        format!("0 {} {}\n{} {} 1\n", jail.uid_base, jail.uid_count, jail.uid_count, jail.vmm);
    let writer = std::thread::spawn(move || write_maps(&pid_r, go_w, &maps));

    // Everything the child uses is made here, since between fork and exec it may not allocate.
    let hide: Vec<CString> = jail
        .hide
        .iter()
        .filter(|p| p.is_dir())
        .map(|p| CString::new(p.as_os_str().as_bytes()).map_err(io::Error::other))
        .collect::<io::Result<_>>()?;
    let groups: Vec<rustix::fs::Gid> =
        jail.groups.iter().map(|g| rustix::fs::Gid::from_raw(*g)).collect();
    let me = rustix::fs::Uid::from_raw(jail.uid_count);
    let my_group = rustix::fs::Gid::from_raw(jail.uid_count);
    let procs = jail.procs.as_raw_fd();
    let netns = jail.netns.as_ref().map(AsRawFd::as_raw_fd);
    let (pid_w_fd, go_r_fd) = (pid_w.as_raw_fd(), go_r.as_raw_fd());
    let keep = jail.keep.clone();
    // SAFETY: the closure runs in the child between fork and exec, where only async signal safe
    // calls are allowed. It makes plain system calls through rustix on descriptors and strings
    // made above, and allocates nothing. The descriptors it borrows stay open in the parent until
    // `spawn` returns, so they are open in the child too. Neither `unshare` takes the descriptor
    // table or anything else the code here relies on staying shared.
    unsafe {
        cmd.pre_exec(move || {
            let fd = |raw: RawFd| BorrowedFd::borrow_raw(raw);
            rustix::io::write(fd(procs), b"0")?;
            if let Some(ns) = netns {
                rustix::thread::move_into_link_name_space(
                    fd(ns),
                    Some(LinkNameSpaceType::Network),
                )?;
            }
            rustix::thread::unshare_unsafe(UnshareFlags::NEWNS)?;
            rustix::mount::mount_change(
                c"/",
                rustix::mount::MountPropagationFlags::REC
                    | rustix::mount::MountPropagationFlags::PRIVATE,
            )?;
            for dir in &hide {
                rustix::mount::mount(
                    c"tmpfs",
                    dir.as_c_str(),
                    c"tmpfs",
                    rustix::mount::MountFlags::NOSUID
                        | rustix::mount::MountFlags::NODEV
                        | rustix::mount::MountFlags::NOEXEC,
                    Some(c"mode=0700,size=4k"),
                )?;
            }
            rustix::thread::set_thread_groups(&groups)?;
            rustix::thread::unshare_unsafe(UnshareFlags::NEWUSER)?;
            let pid = rustix::process::getpid().as_raw_nonzero().get();
            rustix::io::write(fd(pid_w_fd), &pid.to_le_bytes())?;
            let mut go = [0u8; 1];
            if rustix::io::read(fd(go_r_fd), &mut go)? != 1 {
                return Err(rustix::io::Errno::PERM.into());
            }
            rustix::thread::set_keep_capabilities(true)?;
            rustix::thread::set_thread_res_gid(my_group, my_group, my_group)?;
            rustix::thread::set_thread_res_uid(me, me, me)?;
            rustix::thread::set_capabilities(
                None,
                CapabilitySets { effective: CAPS, permitted: CAPS, inheritable: CAPS },
            )?;
            for cap in CAPS.iter() {
                rustix::thread::configure_capability_in_ambient_set(cap, true)?;
            }
            rustix::thread::set_no_new_privs(true)?;
            for &k in &keep {
                rustix::io::fcntl_setfd(fd(k), rustix::io::FdFlags::empty())?;
            }
            Ok(())
        });
    }
    let child = cmd.spawn();
    // The child has its own copies now. With these closed, the writer sees the end of the pipe if
    // the child failed before it said its pid.
    drop((pid_w, go_r));
    let wrote = writer.join().map_err(|_| io::Error::other("the map writer panicked"))?;
    match (child, wrote) {
        (Ok(child), _) => Ok(child),
        (Err(e), Ok(())) => Err(e),
        (Err(_), Err(e)) => Err(e),
    }
}

/// Waits for the child's pid, writes its maps, and lets it go on.
fn write_maps(pid: &OwnedFd, go: OwnedFd, maps: &str) -> io::Result<()> {
    let mut b = [0u8; 4];
    let mut got = 0;
    while got < 4 {
        match rustix::io::read(pid.as_fd(), &mut b[got..])? {
            0 => return Ok(()),
            n => got += n,
        }
    }
    let pid = i32::from_le_bytes(b);
    for map in ["uid_map", "gid_map"] {
        std::fs::write(format!("/proc/{pid}/{map}"), maps)
            .map_err(|e| io::Error::other(format!("writing QEMU's {map}: {e}")))?;
    }
    rustix::io::write(&go, &[1])?;
    Ok(())
}
