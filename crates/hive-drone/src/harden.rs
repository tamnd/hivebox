//! Locks the drone down before it serves anything, so every command it runs inherits the same
//! limits. Two things are applied, both for good, to the calling thread, which has to be the only
//! one: a Landlock ruleset that makes some paths read only, and three seccomp filters, joined
//! into one program.
//!
//! The first filter is an allowlist, and a syscall not on it fails with ENOSYS. That is the answer
//! libc expects from an old kernel, so it falls back on its own, as it does from `clone3` to
//! `clone`. The second filter names the calls that give a way out or reach deep into the kernel
//! (mounts, namespaces, BPF, keyrings, io_uring, modules) and fails them with EPERM, which is what
//! a program that checks for a missing privilege expects. When both filters answer with an errno,
//! the kernel takes the one installed last, so the named calls get EPERM.
//!
//! The third looks only at `ioctl`, which reaches every driver and filesystem in the kernel. It
//! lets through the requests normal programs make, for terminals, sockets, file flags and
//! reflinks, and fails the rest with ENOTTY, the answer for a request the file does not know. That
//! takes in `XFS_IOC_SWAPEXT`, which in DSec swapped the blocks of a file the agent could not read
//! into one it could and shut the filesystem down. Pushing input into a terminal, changing its
//! line discipline and moving extents on ext4 fail with EPERM.
//!
//! On kernel 5.11 and later, a syscall allowed without a look at its arguments is cached, and after
//! its first use it costs close to nothing. Only `clone`, `personality` and `ioctl` are looked at
//! more closely, and those run every filter installed, so joined it is one program to run and not
//! three.

use landlock::{
    ABI, Access, AccessFs, Ruleset, RulesetAttr, RulesetCreatedAttr, RulesetStatus,
    path_beneath_rules,
};
use seccompiler::{
    BpfProgram, SeccompAction, SeccompCmpArgLen, SeccompCmpOp, SeccompCondition, SeccompFilter,
    SeccompRule, TargetArch, sock_filter,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// What was applied, for the drone to report when it starts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hardened {
    /// Whether the kernel enforced the whole Landlock ruleset, only part of it, or none, or that
    /// there was nothing to protect.
    pub landlock: &'static str,
    /// Syscalls on the allowlist, for this architecture.
    pub allowed: usize,
    /// Syscalls failed with EPERM.
    pub denied: usize,
    /// Kinds of `ioctl` request let through: families by their type byte, and single requests.
    pub ioctls: usize,
}

/// Makes `protect` read only for the drone and everything it starts, then installs the seccomp
/// filters. Each path in `protect` has to be absolute. Anything that exists beside the path to a
/// protected one stays writable, and so does anything created later below a writable directory.
/// A new entry in a directory on the way to a protected path cannot be made, since those
/// directories are read only too.
pub fn apply(protect: &[PathBuf]) -> Result<Hardened, String> {
    // With nothing to protect, a ruleset would allow everything and only cost a lookup per open.
    let landlock = if protect.is_empty() { "not needed" } else { landlock(protect)? };
    // A process that is not dumpable can only be traced by one with CAP_SYS_PTRACE, which a
    // cell's root does not get, so nothing the drone runs can read the secret out of its memory.
    rustix::process::set_dumpable_behavior(rustix::process::DumpableBehavior::NotDumpable)
        .map_err(|e| format!("making the drone not dumpable: {e}"))?;
    let arch = TargetArch::try_from(std::env::consts::ARCH)
        .map_err(|e| format!("seccomp on {}: {e:?}", std::env::consts::ARCH))?;
    let allow = allow_filter(arch)?;
    let deny = deny_filter(arch)?;
    let filter = join(vec![ioctl_filter(), deny, allow]);
    seccompiler::apply_filter(&filter).map_err(|e| format!("installing the filter: {e}"))?;
    Ok(Hardened {
        landlock,
        allowed: ALLOW.len() + ARGS.len(),
        denied: DENY.len(),
        ioctls: IOCTL_TYPES.len() + IOCTLS.len(),
    })
}

fn landlock(protect: &[PathBuf]) -> Result<&'static str, String> {
    let abi = ABI::V4;
    let all = AccessFs::from_all(abi);
    let read = AccessFs::from_read(abi);
    let writable = writable(Path::new("/"), protect)?;
    let status = Ruleset::default()
        .handle_access(all)
        .and_then(|r| r.create())
        .and_then(|r| r.add_rules(path_beneath_rules(["/"], read)))
        .and_then(|r| r.add_rules(path_beneath_rules(&writable, all)))
        .and_then(|r| r.restrict_self())
        .map_err(|e| format!("landlock: {e}"))?;
    Ok(match status.ruleset {
        RulesetStatus::FullyEnforced => "fully enforced",
        RulesetStatus::PartiallyEnforced => "partly enforced",
        RulesetStatus::NotEnforced => "not enforced",
    })
}

/// Every entry under `root` that is not a symlink, a protected path or on the way to one. Those get
/// full access, and the rest keeps only read access.
fn writable(root: &Path, protect: &[PathBuf]) -> Result<Vec<PathBuf>, String> {
    for p in protect {
        if !p.is_absolute() {
            return Err(format!("{} is not an absolute path", p.display()));
        }
    }
    let mut out = Vec::new();
    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        let entries =
            std::fs::read_dir(&dir).map_err(|e| format!("listing {}: {e}", dir.display()))?;
        for entry in entries {
            let entry = entry.map_err(|e| format!("listing {}: {e}", dir.display()))?;
            let path = entry.path();
            // A rule opens its path and follows a symlink, which could lead into a protected
            // path. What a symlink points at gets its own rule if it is writable.
            if entry.file_type().is_ok_and(|t| t.is_symlink()) || protect.iter().any(|p| p == &path)
            {
                continue;
            }
            if protect.iter().any(|p| p.starts_with(&path)) {
                dirs.push(path);
            } else {
                out.push(path);
            }
        }
    }
    Ok(out)
}

fn allow_filter(arch: TargetArch) -> Result<BpfProgram, String> {
    let mut rules: BTreeMap<i64, Vec<SeccompRule>> =
        ALLOW.iter().map(|&nr| (nr, Vec::new())).collect();
    // A new namespace is refused. The exit signal lives in the low byte of the flags, so
    // CLONE_NEWTIME, which shares it, cannot be told apart and is not checked, as in Docker.
    let namespaces = (libc::CLONE_NEWNS
        | libc::CLONE_NEWUTS
        | libc::CLONE_NEWIPC
        | libc::CLONE_NEWUSER
        | libc::CLONE_NEWPID
        | libc::CLONE_NEWNET
        | libc::CLONE_NEWCGROUP) as u64;
    rules.insert(libc::SYS_clone, vec![rule(0, SeccompCmpOp::MaskedEq(namespaces), 0)?]);
    // Only the personalities Docker allows: the default, a query, and the two that change the
    // layout of the address space a little.
    let personas = [0, 0x8, 0x20000, 0x20008, 0xffff_ffff];
    let persona =
        personas.iter().map(|&v| rule(0, SeccompCmpOp::Eq, v)).collect::<Result<Vec<_>, _>>()?;
    rules.insert(libc::SYS_personality, persona);
    compile(rules, SeccompAction::Errno(libc::ENOSYS as u32), SeccompAction::Allow, arch)
}

fn deny_filter(arch: TargetArch) -> Result<BpfProgram, String> {
    let rules = DENY.iter().map(|&nr| (nr, Vec::new())).collect();
    compile(rules, SeccompAction::Allow, SeccompAction::Errno(libc::EPERM as u32), arch)
}

/// `filters` as one program, which asks each in turn and the next only when it allows the call
/// with [`SECCOMP_RET_ALLOW`].
/// The kernel runs filters stacked on one another newest first and keeps the strongest answer, or
/// the newest of equal ones, which for these, where the only answers are an errno, allowing the
/// call and killing a process from another architecture before anything else, comes to the same.
fn join(filters: Vec<BpfProgram>) -> BpfProgram {
    let last = filters.len().saturating_sub(1);
    let mut prog: BpfProgram = Vec::with_capacity(filters.iter().map(Vec::len).sum());
    for (n, filter) in filters.into_iter().enumerate() {
        let next = prog.len() + filter.len();
        for mut ins in filter {
            if n < last && ins.code == RET && ins.k == SECCOMP_RET_ALLOW {
                ins = sock_filter { code: JA, jt: 0, jf: 0, k: (next - prog.len() - 1) as u32 };
            }
            prog.push(ins);
        }
    }
    prog
}

const LD: u16 = (libc::BPF_LD | libc::BPF_W | libc::BPF_ABS) as u16;
const JA: u16 = (libc::BPF_JMP | libc::BPF_JA) as u16;
const JEQ: u16 = (libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K) as u16;
const AND: u16 = (libc::BPF_ALU | libc::BPF_AND | libc::BPF_K) as u16;
const RET: u16 = (libc::BPF_RET | libc::BPF_K) as u16;

/// Where a check in [`ioctl_filter`] goes.
#[derive(Clone, Copy)]
enum To {
    /// On to the next check either way.
    Next,
    /// Allowed when it matches, without asking the filters after this one.
    Allow,
    /// Refused with EPERM when it matches.
    Refuse,
    /// Left to the filters after this one when it does not match.
    PassUnless,
}

/// The `ioctl` filter, written out by hand, since a filter compiled from rules cannot say "every
/// request but these". Only the low 32 bits of the request are looked at, as the kernel reads it
/// as an `unsigned int`, so high bits set on purpose change nothing.
fn ioctl_filter() -> BpfProgram {
    // The offsets into `struct seccomp_data`, and the low half of `args[1]`, little endian.
    const NR: u32 = 0;
    const ARCH: u32 = 4;
    const REQUEST: u32 = 24;
    let op = |code, k| sock_filter { code, jt: 0, jf: 0, k };
    // The checks, each with where it jumps when it matches, then the three ways out.
    let mut checks: Vec<(sock_filter, To)> = vec![
        (op(LD, ARCH), To::Next),
        // A syscall from another architecture is left to the other filters, which refuse it.
        (op(JEQ, AUDIT_ARCH), To::PassUnless),
        (op(LD, NR), To::Next),
        (op(JEQ, libc::SYS_ioctl as u32), To::PassUnless),
        (op(LD, REQUEST), To::Next),
    ];
    checks.extend(IOCTLS_REFUSED.iter().map(|&r| (op(JEQ, r), To::Refuse)));
    checks.extend(IOCTLS.iter().map(|&r| (op(JEQ, r), To::Allow)));
    // The type byte and the number, whatever the size and direction.
    checks.push((op(AND, 0xffff), To::Next));
    checks.extend(IOCTL_NUMBERS_REFUSED.iter().map(|&r| (op(JEQ, r), To::Refuse)));
    checks.push((op(AND, 0xff00), To::Next));
    checks.extend(IOCTL_TYPES.iter().map(|&t| (op(JEQ, u32::from(t) << 8), To::Allow)));
    let (refuse, allow, pass) = (checks.len() + 1, checks.len() + 2, checks.len() + 3);
    let mut prog: BpfProgram = Vec::with_capacity(pass + 1);
    for (i, (mut ins, to)) in checks.into_iter().enumerate() {
        // The program is a few dozen long, so every jump fits in a byte.
        let jump = |to: usize| (to - i - 1) as u8;
        match to {
            To::Next => {}
            To::Allow => ins.jt = jump(allow),
            To::Refuse => ins.jt = jump(refuse),
            To::PassUnless => ins.jf = jump(pass),
        }
        prog.push(ins);
    }
    prog.push(op(RET, SECCOMP_RET_ERRNO | libc::ENOTTY as u32));
    prog.push(op(RET, SECCOMP_RET_ERRNO | libc::EPERM as u32));
    prog.push(op(RET, SECCOMP_RET_ALLOW_HERE));
    prog.push(op(RET, SECCOMP_RET_ALLOW));
    prog
}

const SECCOMP_RET_ERRNO: u32 = 0x0005_0000;
const SECCOMP_RET_ALLOW: u32 = 0x7fff_0000;
/// Allows the call, since the kernel ignores the low bits of an allow, and tells [`join`] not to
/// ask the filters after this one. The `ioctl` filter answers with it, as the others allow
/// `ioctl` whatever its arguments, and that way an `ioctl` runs about 20 instructions and not about
/// 140.
const SECCOMP_RET_ALLOW_HERE: u32 = SECCOMP_RET_ALLOW | 1;

/// The `AUDIT_ARCH_*` a syscall from this architecture carries.
#[cfg(target_arch = "x86_64")]
const AUDIT_ARCH: u32 = 0xc000_003e;
#[cfg(target_arch = "aarch64")]
const AUDIT_ARCH: u32 = 0xc000_00b7;
#[cfg(target_arch = "riscv64")]
const AUDIT_ARCH: u32 = 0xc000_00f3;

/// Families of `ioctl` request let through, by their type byte: terminals and the `FIO*` calls
/// on any file (`T`), sockets (0x89), the file flags, `FIEMAP`, fscrypt and fs-verity (`f`), and
/// the inode generation (`v`).
const IOCTL_TYPES: [u8; 4] = [b'T', 0x89, b'f', b'v'];

/// Single requests let through from other families: `FICLONE`, `FICLONERANGE` and
/// `FIDEDUPERANGE`, which `cp` uses for reflinks and which only reach files the caller has open,
/// and `RNDGETENTCNT`, which some programs ask of `/dev/random`.
const IOCTLS: [u32; 4] = [0x4004_9409, 0x4020_940d, 0xc018_9436, 0x8004_5200];

/// Requests in an allowed family that are refused: `TIOCSTI`, which pushes input into a terminal
/// as if typed, `TIOCSETD`, which loads a line discipline, and `TIOCLINUX`.
const IOCTLS_REFUSED: [u32; 3] = [0x5412, 0x5423, 0x541c];

/// Requests refused by type byte and number, whatever their size: `EXT4_IOC_MOVE_EXT`, which
/// swaps blocks between files as `XFS_IOC_SWAPEXT` does, and `EXT4_IOC_SWAP_BOOT`.
const IOCTL_NUMBERS_REFUSED: [u32; 2] = [0x660f, 0x6611];

fn rule(arg: u8, op: SeccompCmpOp, value: u64) -> Result<SeccompRule, String> {
    let len = match op {
        SeccompCmpOp::MaskedEq(_) => SeccompCmpArgLen::Qword,
        _ => SeccompCmpArgLen::Dword,
    };
    let cond = SeccompCondition::new(arg, len, op, value).map_err(|e| e.to_string())?;
    SeccompRule::new(vec![cond]).map_err(|e| e.to_string())
}

fn compile(
    rules: BTreeMap<i64, Vec<SeccompRule>>,
    mismatch: SeccompAction,
    matched: SeccompAction,
    arch: TargetArch,
) -> Result<BpfProgram, String> {
    let filter = SeccompFilter::new(rules, mismatch, matched, arch).map_err(|e| e.to_string())?;
    filter.try_into().map_err(|e: seccompiler::BackendError| e.to_string())
}

/// Calls with an argument check, counted in [`Hardened::allowed`].
const ARGS: [i64; 2] = [libc::SYS_clone, libc::SYS_personality];

/// Everything a normal program, a compiler, a test runner or a debugger uses. It is close to
/// Docker's default profile, without the calls Docker allows only with extra capabilities.
const ALLOW: &[i64] = &[
    // Files and descriptors.
    libc::SYS_read,
    libc::SYS_write,
    libc::SYS_readv,
    libc::SYS_writev,
    libc::SYS_pread64,
    libc::SYS_pwrite64,
    libc::SYS_preadv,
    libc::SYS_pwritev,
    libc::SYS_preadv2,
    libc::SYS_pwritev2,
    libc::SYS_openat,
    libc::SYS_openat2,
    libc::SYS_close,
    libc::SYS_close_range,
    libc::SYS_lseek,
    libc::SYS_fstat,
    libc::SYS_newfstatat,
    libc::SYS_statx,
    libc::SYS_statfs,
    libc::SYS_fstatfs,
    libc::SYS_getdents64,
    libc::SYS_faccessat,
    libc::SYS_faccessat2,
    libc::SYS_readlinkat,
    libc::SYS_mkdirat,
    libc::SYS_mknodat,
    libc::SYS_unlinkat,
    libc::SYS_renameat2,
    libc::SYS_linkat,
    libc::SYS_symlinkat,
    libc::SYS_fchmod,
    libc::SYS_fchmodat,
    libc::SYS_fchown,
    libc::SYS_fchownat,
    libc::SYS_truncate,
    libc::SYS_ftruncate,
    libc::SYS_fallocate,
    libc::SYS_fadvise64,
    libc::SYS_fsync,
    libc::SYS_fdatasync,
    libc::SYS_sync,
    libc::SYS_syncfs,
    libc::SYS_sync_file_range,
    libc::SYS_utimensat,
    libc::SYS_chdir,
    libc::SYS_fchdir,
    libc::SYS_getcwd,
    libc::SYS_chroot,
    libc::SYS_umask,
    libc::SYS_dup,
    libc::SYS_dup3,
    libc::SYS_fcntl,
    libc::SYS_flock,
    libc::SYS_ioctl,
    libc::SYS_pipe2,
    libc::SYS_splice,
    libc::SYS_tee,
    libc::SYS_vmsplice,
    libc::SYS_sendfile,
    libc::SYS_copy_file_range,
    libc::SYS_readahead,
    libc::SYS_getxattr,
    libc::SYS_lgetxattr,
    libc::SYS_fgetxattr,
    libc::SYS_setxattr,
    libc::SYS_lsetxattr,
    libc::SYS_fsetxattr,
    libc::SYS_listxattr,
    libc::SYS_llistxattr,
    libc::SYS_flistxattr,
    libc::SYS_removexattr,
    libc::SYS_lremovexattr,
    libc::SYS_fremovexattr,
    libc::SYS_inotify_init1,
    libc::SYS_inotify_add_watch,
    libc::SYS_inotify_rm_watch,
    libc::SYS_memfd_create,
    // Memory.
    libc::SYS_mmap,
    libc::SYS_munmap,
    libc::SYS_mprotect,
    libc::SYS_mremap,
    libc::SYS_madvise,
    libc::SYS_msync,
    libc::SYS_mincore,
    libc::SYS_mlock,
    libc::SYS_mlock2,
    libc::SYS_munlock,
    libc::SYS_mlockall,
    libc::SYS_munlockall,
    libc::SYS_brk,
    libc::SYS_membarrier,
    libc::SYS_get_mempolicy,
    // Processes, users and scheduling.
    libc::SYS_execve,
    libc::SYS_execveat,
    libc::SYS_exit,
    libc::SYS_exit_group,
    libc::SYS_wait4,
    libc::SYS_waitid,
    libc::SYS_kill,
    libc::SYS_tkill,
    libc::SYS_tgkill,
    libc::SYS_set_tid_address,
    libc::SYS_set_robust_list,
    libc::SYS_get_robust_list,
    libc::SYS_futex,
    libc::SYS_rseq,
    libc::SYS_getpid,
    libc::SYS_getppid,
    libc::SYS_gettid,
    libc::SYS_getpgid,
    libc::SYS_setpgid,
    libc::SYS_getsid,
    libc::SYS_setsid,
    libc::SYS_getuid,
    libc::SYS_geteuid,
    libc::SYS_getgid,
    libc::SYS_getegid,
    libc::SYS_getresuid,
    libc::SYS_getresgid,
    libc::SYS_getgroups,
    libc::SYS_setuid,
    libc::SYS_setgid,
    libc::SYS_setreuid,
    libc::SYS_setregid,
    libc::SYS_setresuid,
    libc::SYS_setresgid,
    libc::SYS_setfsuid,
    libc::SYS_setfsgid,
    libc::SYS_setgroups,
    libc::SYS_capget,
    libc::SYS_capset,
    libc::SYS_prctl,
    libc::SYS_getpriority,
    libc::SYS_setpriority,
    libc::SYS_sched_yield,
    libc::SYS_sched_getaffinity,
    libc::SYS_sched_setaffinity,
    libc::SYS_sched_getparam,
    libc::SYS_sched_setparam,
    libc::SYS_sched_getscheduler,
    libc::SYS_sched_setscheduler,
    libc::SYS_sched_get_priority_max,
    libc::SYS_sched_get_priority_min,
    libc::SYS_sched_rr_get_interval,
    libc::SYS_sched_getattr,
    libc::SYS_sched_setattr,
    libc::SYS_getrlimit,
    libc::SYS_setrlimit,
    libc::SYS_prlimit64,
    libc::SYS_getrusage,
    libc::SYS_times,
    libc::SYS_sysinfo,
    libc::SYS_uname,
    libc::SYS_sethostname,
    libc::SYS_setdomainname,
    libc::SYS_getcpu,
    libc::SYS_getrandom,
    libc::SYS_pidfd_open,
    libc::SYS_pidfd_send_signal,
    libc::SYS_pidfd_getfd,
    libc::SYS_ptrace,
    libc::SYS_process_vm_readv,
    libc::SYS_process_vm_writev,
    libc::SYS_ioprio_get,
    libc::SYS_ioprio_set,
    libc::SYS_seccomp,
    libc::SYS_landlock_create_ruleset,
    libc::SYS_landlock_add_rule,
    libc::SYS_landlock_restrict_self,
    // Signals.
    libc::SYS_rt_sigaction,
    libc::SYS_rt_sigprocmask,
    libc::SYS_rt_sigreturn,
    libc::SYS_rt_sigpending,
    libc::SYS_rt_sigtimedwait,
    libc::SYS_rt_sigqueueinfo,
    libc::SYS_rt_tgsigqueueinfo,
    libc::SYS_rt_sigsuspend,
    libc::SYS_sigaltstack,
    libc::SYS_signalfd4,
    libc::SYS_restart_syscall,
    // Time.
    libc::SYS_clock_gettime,
    libc::SYS_clock_getres,
    libc::SYS_clock_nanosleep,
    libc::SYS_nanosleep,
    libc::SYS_gettimeofday,
    libc::SYS_getitimer,
    libc::SYS_setitimer,
    libc::SYS_timer_create,
    libc::SYS_timer_settime,
    libc::SYS_timer_gettime,
    libc::SYS_timer_getoverrun,
    libc::SYS_timer_delete,
    libc::SYS_timerfd_create,
    libc::SYS_timerfd_settime,
    libc::SYS_timerfd_gettime,
    // Waiting on many things.
    libc::SYS_epoll_create1,
    libc::SYS_epoll_ctl,
    libc::SYS_epoll_pwait,
    libc::SYS_epoll_pwait2,
    libc::SYS_eventfd2,
    libc::SYS_ppoll,
    libc::SYS_pselect6,
    // Sockets.
    libc::SYS_socket,
    libc::SYS_socketpair,
    libc::SYS_bind,
    libc::SYS_listen,
    libc::SYS_accept,
    libc::SYS_accept4,
    libc::SYS_connect,
    libc::SYS_getsockname,
    libc::SYS_getpeername,
    libc::SYS_sendto,
    libc::SYS_recvfrom,
    libc::SYS_sendmsg,
    libc::SYS_recvmsg,
    libc::SYS_sendmmsg,
    libc::SYS_recvmmsg,
    libc::SYS_setsockopt,
    libc::SYS_getsockopt,
    libc::SYS_shutdown,
    // Async IO and System V IPC, both kept inside the cell's own namespaces.
    libc::SYS_io_setup,
    libc::SYS_io_destroy,
    libc::SYS_io_submit,
    libc::SYS_io_cancel,
    libc::SYS_io_getevents,
    libc::SYS_msgget,
    libc::SYS_msgsnd,
    libc::SYS_msgrcv,
    libc::SYS_msgctl,
    libc::SYS_semget,
    libc::SYS_semop,
    libc::SYS_semtimedop,
    libc::SYS_semctl,
    libc::SYS_shmget,
    libc::SYS_shmat,
    libc::SYS_shmdt,
    libc::SYS_shmctl,
    libc::SYS_mq_open,
    libc::SYS_mq_unlink,
    libc::SYS_mq_timedsend,
    libc::SYS_mq_timedreceive,
    libc::SYS_mq_notify,
    libc::SYS_mq_getsetattr,
    // The older calls x86_64 still has and newer architectures only have as *at forms.
    #[cfg(target_arch = "x86_64")]
    libc::SYS_open,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_creat,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_stat,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_lstat,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_access,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_pipe,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_dup2,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_poll,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_select,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_epoll_create,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_epoll_wait,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_eventfd,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_signalfd,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_inotify_init,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_mkdir,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_rmdir,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_rename,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_renameat,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_link,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_unlink,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_symlink,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_readlink,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_chmod,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_chown,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_lchown,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_utime,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_utimes,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_futimesat,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_getdents,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_fork,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_vfork,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_arch_prctl,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_alarm,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_pause,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_time,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_getpgrp,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_mknod,
];

/// Calls that answer EPERM rather than ENOSYS, so a program can tell it was refused.
const DENY: &[i64] = &[
    libc::SYS_mount,
    libc::SYS_umount2,
    libc::SYS_pivot_root,
    libc::SYS_unshare,
    libc::SYS_setns,
    libc::SYS_fsopen,
    libc::SYS_fsconfig,
    libc::SYS_fsmount,
    libc::SYS_fspick,
    libc::SYS_move_mount,
    libc::SYS_open_tree,
    libc::SYS_mount_setattr,
    libc::SYS_open_by_handle_at,
    libc::SYS_name_to_handle_at,
    libc::SYS_bpf,
    libc::SYS_perf_event_open,
    libc::SYS_userfaultfd,
    libc::SYS_io_uring_setup,
    libc::SYS_io_uring_enter,
    libc::SYS_io_uring_register,
    libc::SYS_keyctl,
    libc::SYS_add_key,
    libc::SYS_request_key,
    libc::SYS_init_module,
    libc::SYS_finit_module,
    libc::SYS_delete_module,
    libc::SYS_kexec_load,
    libc::SYS_kexec_file_load,
    libc::SYS_reboot,
    libc::SYS_swapon,
    libc::SYS_swapoff,
    libc::SYS_acct,
    libc::SYS_quotactl,
    libc::SYS_syslog,
    libc::SYS_vhangup,
    libc::SYS_settimeofday,
    libc::SYS_clock_settime,
    libc::SYS_clock_adjtime,
    libc::SYS_adjtimex,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_iopl,
    #[cfg(target_arch = "x86_64")]
    libc::SYS_ioperm,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_call_is_both_allowed_and_denied() {
        let mut allow: Vec<i64> = ALLOW.iter().chain(&ARGS).copied().collect();
        allow.sort_unstable();
        let before = allow.len();
        allow.dedup();
        assert_eq!(before, allow.len(), "a call is on the allowlist twice");
        for nr in DENY {
            assert!(allow.binary_search(nr).is_err(), "{nr} is both allowed and denied");
        }
    }

    #[test]
    fn both_filters_compile() {
        let arch = TargetArch::try_from(std::env::consts::ARCH).unwrap();
        assert!(!allow_filter(arch).unwrap().is_empty());
        assert!(!deny_filter(arch).unwrap().is_empty());
    }

    #[test]
    fn only_the_way_to_a_protected_path_is_read_only() {
        let root = std::env::temp_dir().join(format!("hive-harden-{}", std::process::id()));
        for d in ["a/keep", "a/b", "c"] {
            std::fs::create_dir_all(root.join(d)).unwrap();
        }
        std::fs::write(root.join("a/file"), "").unwrap();
        std::os::unix::fs::symlink(root.join("a/keep"), root.join("a/link")).unwrap();
        let mut w = writable(&root, &[root.join("a/keep")]).unwrap();
        w.sort();
        assert_eq!(w, vec![root.join("a/b"), root.join("a/file"), root.join("c")]);
        assert!(writable(&root, &["relative".into()]).is_err());
        std::fs::remove_dir_all(&root).unwrap();
    }

    /// Runs `prog` on a syscall, as the kernel would, for the few instructions it uses.
    fn run(prog: &[sock_filter], arch: u32, nr: i64, request: u64) -> u32 {
        let jump = |c: u32| libc::BPF_JMP | c | libc::BPF_K;
        let mut data = [0u8; 64];
        data[0..4].copy_from_slice(&(nr as u32).to_le_bytes());
        data[4..8].copy_from_slice(&arch.to_le_bytes());
        data[24..32].copy_from_slice(&request.to_le_bytes());
        let (mut a, mut pc) = (0u32, 0usize);
        loop {
            let ins = &prog[pc];
            pc += 1;
            match u32::from(ins.code) {
                c if c == libc::BPF_LD | libc::BPF_W | libc::BPF_ABS => {
                    let k = ins.k as usize;
                    a = u32::from_le_bytes(data[k..k + 4].try_into().unwrap());
                }
                c if c == libc::BPF_JMP | libc::BPF_JA => pc += ins.k as usize,
                c if c == jump(libc::BPF_JEQ) => {
                    pc += usize::from(if a == ins.k { ins.jt } else { ins.jf });
                }
                c if c == jump(libc::BPF_JGT) => {
                    pc += usize::from(if a > ins.k { ins.jt } else { ins.jf });
                }
                c if c == jump(libc::BPF_JGE) => {
                    pc += usize::from(if a >= ins.k { ins.jt } else { ins.jf });
                }
                c if c == libc::BPF_ALU | libc::BPF_AND | libc::BPF_K => a &= ins.k,
                c if c == libc::BPF_RET | libc::BPF_K => return ins.k,
                c => panic!("instruction {c:#x}"),
            }
        }
    }

    #[test]
    fn ioctls_normal_programs_make_pass_and_the_rest_are_refused() {
        let prog = ioctl_filter();
        let ioctl = |request| run(&prog, AUDIT_ARCH, libc::SYS_ioctl, request);
        let (ok, enotty, eperm) = (
            SECCOMP_RET_ALLOW_HERE,
            SECCOMP_RET_ERRNO | libc::ENOTTY as u32,
            SECCOMP_RET_ERRNO | libc::EPERM as u32,
        );
        for (name, request, want) in [
            ("TCGETS", 0x5401, ok),
            ("TIOCGWINSZ", 0x5413, ok),
            ("FIONREAD", 0x541b, ok),
            ("FIOCLEX", 0x5451, ok),
            ("TIOCGPTN", 0x8004_5430, ok),
            ("SIOCGIFCONF", 0x8912, ok),
            ("FS_IOC_GETFLAGS", 0x8008_6601, ok),
            ("FS_IOC_FIEMAP", 0xc020_660b, ok),
            ("FS_IOC_GETVERSION", 0x8008_7601, ok),
            ("FICLONE", 0x4004_9409, ok),
            ("RNDGETENTCNT", 0x8004_5200, ok),
            ("TIOCSTI", 0x5412, eperm),
            ("TIOCSTI with the high bits set", 0xffff_ffff_0000_5412, eperm),
            ("TIOCSETD", 0x5423, eperm),
            ("EXT4_IOC_MOVE_EXT", 0xc028_660f, eperm),
            ("XFS_IOC_SWAPEXT", 0xc0c0_586d, enotty),
            ("FIFREEZE", 0xc004_5877, enotty),
            ("BTRFS_IOC_SNAP_CREATE", 0x5000_9401, enotty),
            ("NS_GET_USERNS", 0xb701, enotty),
            ("LOOP_SET_FD", 0x4c00, enotty),
        ] {
            assert_eq!(ioctl(request), want, "{name}");
        }
        let pass = SECCOMP_RET_ALLOW;
        assert_eq!(run(&prog, AUDIT_ARCH, libc::SYS_read, 0xc0c0_586d), pass, "another syscall");
        assert_eq!(run(&prog, 0x4000_0003, libc::SYS_ioctl, 0xc0c0_586d), pass, "another arch");
    }

    #[test]
    fn the_joined_filter_answers_as_the_three_stacked_would() {
        let arch = TargetArch::try_from(std::env::consts::ARCH).unwrap();
        let filters = [ioctl_filter(), deny_filter(arch).unwrap(), allow_filter(arch).unwrap()];
        let joined = join(filters.to_vec());
        assert!(joined.len() < 4096, "{} instructions", joined.len());
        // The kernel keeps the answer with the lowest action as a signed number, newest first.
        let stacked = |arch, nr, request| {
            let action = |r: u32| (r & 0xffff_0000) as i32;
            filters
                .iter()
                .map(|f| run(f, arch, nr, request))
                .fold(SECCOMP_RET_ALLOW, |kept, r| if action(r) < action(kept) { r } else { kept })
        };
        for arch in [AUDIT_ARCH, 0x4000_0003] {
            for nr in 0..=600 {
                for request in [0, 0x5412, 0x541b, 0xc0c0_586d, 0x0001_0000_0000] {
                    // An allow is an allow, whatever its low bits.
                    let got = match run(&joined, arch, nr, request) {
                        SECCOMP_RET_ALLOW_HERE => SECCOMP_RET_ALLOW,
                        r => r,
                    };
                    let want = match stacked(arch, nr, request) {
                        SECCOMP_RET_ALLOW_HERE => SECCOMP_RET_ALLOW,
                        r => r,
                    };
                    assert_eq!(got, want, "{arch:#x} {nr} {request:#x}");
                }
            }
        }
    }
}
