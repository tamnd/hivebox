//! Project quotas, which hold what a cell writes under a directory to a size, from
//! `spec/08_node_agent.md`, section 7. A directory gets a project id with the inherit flag, so
//! everything made under it later has the same id, and the filesystem counts and limits each id's
//! blocks and inodes. XFS mounted with `prjquota` and ext4 with the `project` and `quota` features
//! do this. Every call takes a path on the filesystem, so no block device has to be found.

#![allow(unsafe_code)]

use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::path::Path;

/// `PRJQUOTA` in `<linux/quota.h>`.
const PRJQUOTA: u32 = 2;
/// `Q_GETQUOTA` and `Q_SETQUOTA`, the generic calls.
const Q_GETQUOTA: u32 = 0x80_0007;
const Q_SETQUOTA: u32 = 0x80_0008;
/// `Q_XGETQSTATV`, the state of each kind of quota, which ext4 answers too.
const Q_XGETQSTATV: u32 = (b'X' as u32) << 8 | 8;
/// `FS_QUOTA_PDQ_ENFD` in the state's flags: project limits are enforced, not just counted.
const PDQ_ENFD: u16 = 0x0020;
/// `QIF_BLIMITS | QIF_ILIMITS`: a set call changes the block and inode limits only.
const QIF_LIMITS: u32 = 1 | 4;
/// The block limits are in units of this many bytes.
const QIF_DQBLKSIZE: u64 = 1024;
/// `FS_IOC_FSGETXATTR` and `FS_IOC_FSSETXATTR`.
const FS_IOC_FSGETXATTR: libc::c_ulong = 0x801c_581f;
const FS_IOC_FSSETXATTR: libc::c_ulong = 0x401c_5820;
/// `FS_XFLAG_PROJINHERIT`: what is made under the directory gets its project id.
const PROJINHERIT: u32 = 0x200;

/// `struct if_dqblk`.
#[repr(C)]
#[derive(Default)]
struct Dqblk {
    bhardlimit: u64,
    bsoftlimit: u64,
    curspace: u64,
    ihardlimit: u64,
    isoftlimit: u64,
    curinodes: u64,
    btime: u64,
    itime: u64,
    valid: u32,
}

/// `struct fsxattr`.
#[repr(C)]
#[derive(Default)]
struct Fsxattr {
    xflags: u32,
    extsize: u32,
    nextents: u32,
    projid: u32,
    cowextsize: u32,
    pad: [u8; 8],
}

/// What one project holds now.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Usage {
    /// Bytes in use.
    pub bytes: u64,
    /// Files, directories and links.
    pub inodes: u64,
}

fn quotactl(on: &File, cmd: u32, id: u32, addr: *mut libc::c_void) -> io::Result<()> {
    let cmd = cmd << 8 | PRJQUOTA;
    // SAFETY: `quotactl_fd` reads or writes the one structure `addr` points to, which the caller
    // passes for that command and which outlives the call, and the descriptor is open.
    let r = unsafe { libc::syscall(libc::SYS_quotactl_fd, on.as_raw_fd(), cmd, id, addr) };
    if r < 0 { Err(io::Error::last_os_error()) } else { Ok(()) }
}

/// Whether the filesystem `dir` is on enforces project limits.
///
/// # Errors
///
/// `dir` cannot be opened, or the filesystem has no project quotas at all, which most report as
/// `ENOSYS`, `ENOTTY` or `ESRCH`.
pub fn enforced(dir: &Path) -> io::Result<bool> {
    let on = File::open(dir)?;
    // `struct fs_quota_statv` is 160 bytes, and its version, 1, goes in the first.
    let mut state = [0u8; 160];
    state[0] = 1;
    quotactl(&on, Q_XGETQSTATV, 0, state.as_mut_ptr().cast())?;
    Ok(u16::from_ne_bytes([state[2], state[3]]) & PDQ_ENFD != 0)
}

/// Gives the empty directory `dir` the project id `project`, inherited by all made under it.
///
/// # Errors
///
/// `dir` cannot be opened, or the filesystem has no project ids.
pub fn tag(dir: &Path, project: u32) -> io::Result<()> {
    let d = File::open(dir)?;
    let mut attr = Fsxattr::default();
    // SAFETY: both calls read or write the one `struct fsxattr`, which lives across them.
    unsafe {
        if libc::ioctl(d.as_raw_fd(), FS_IOC_FSGETXATTR, &raw mut attr) < 0 {
            return Err(io::Error::last_os_error());
        }
        attr.projid = project;
        attr.xflags |= PROJINHERIT;
        if libc::ioctl(d.as_raw_fd(), FS_IOC_FSSETXATTR, &raw const attr) < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Holds `project` to `bytes` and `inodes` on the filesystem `dir` is on. Writes past the limit
/// fail with `EDQUOT` on ext4 and `ENOSPC` on XFS.
///
/// # Errors
///
/// `dir` cannot be opened, or the limit cannot be set.
pub fn limit(dir: &Path, project: u32, bytes: u64, inodes: u64) -> io::Result<()> {
    let on = File::open(dir)?;
    let blocks = bytes.div_ceil(QIF_DQBLKSIZE);
    let mut q = Dqblk {
        bhardlimit: blocks,
        bsoftlimit: blocks,
        ihardlimit: inodes,
        isoftlimit: inodes,
        valid: QIF_LIMITS,
        ..Dqblk::default()
    };
    quotactl(&on, Q_SETQUOTA, project, (&raw mut q).cast())
}

/// What `project` holds on the filesystem `dir` is on. A project the filesystem has no record of
/// holds nothing.
///
/// # Errors
///
/// `dir` cannot be opened, or the filesystem has no project quotas.
pub fn usage(dir: &Path, project: u32) -> io::Result<Usage> {
    let on = File::open(dir)?;
    let mut q = Dqblk::default();
    match quotactl(&on, Q_GETQUOTA, project, (&raw mut q).cast()) {
        Ok(()) => Ok(Usage { bytes: q.curspace, inodes: q.curinodes }),
        // XFS has no record of an id nothing ever had.
        Err(e) if e.raw_os_error() == Some(libc::ENOENT) => Ok(Usage::default()),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_structures_are_the_kernels_size() {
        assert_eq!(size_of::<Dqblk>(), 72);
        assert_eq!(size_of::<Fsxattr>(), 28);
        // _IOR('X', 31, struct fsxattr) and _IOW('X', 32, struct fsxattr).
        let ioc = |dir: libc::c_ulong, nr: libc::c_ulong| {
            dir << 30
                | (size_of::<Fsxattr>() as libc::c_ulong) << 16
                | libc::c_ulong::from(b'X') << 8
                | nr
        };
        assert_eq!(ioc(2, 31), FS_IOC_FSGETXATTR);
        assert_eq!(ioc(1, 32), FS_IOC_FSSETXATTR);
    }

    #[test]
    fn a_filesystem_without_project_quotas_says_so() {
        // /proc has no quotas of any kind, so this is an error and never a yes.
        assert!(!enforced(Path::new("/proc")).unwrap_or(false));
    }
}
