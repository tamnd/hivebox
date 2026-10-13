//! The driver the comb calls. It mounts each cell's root filesystem on the host, builds the
//! cell's initramfs in memory, and starts QEMU in the jail.

use crate::initramfs;
use crate::jail::{self, Jail};
use futures::future::BoxFuture;
use hive_cell::pid::{alive, pidfd, start_time};
use hive_cell::{
    CellDriver, CellHandle, DriverCaps, ExitInfo, GuestChannel, Liveness, NodeFit, PauseMode,
    Result, RootfsPlan, Slot, SnapshotCaps, cgroup,
};
use hive_types::{Backend, CellId, CellSpec, Error, Reason};
use rustix::fs::{MemfdFlags, Mode, OFlags, SealFlags};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write;
use std::os::fd::{AsFd, AsRawFd, OwnedFd};
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};
use tokio::io::unix::AsyncFd;
use tokio::sync::Notify;

/// The vsock port the drone listens on in every guest.
pub const DRONE_PORT: u32 = 52;
/// The 9p tag the guest mounts its root filesystem from, the drone's `vm::ROOT_TAG`.
const ROOT_TAG: &str = "root";
/// TCG's translation cache, in MiB. Its default is a GiB, more than most cells have in all.
const TB_MIB: u32 = 64;
/// Less cold page cache than this in a cell is not worth a trim.
const TRIM_FLOOR: u64 = 1 << 20;
/// The commands' search path when the cell sets none.
const PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

/// How one node runs its virtual machines.
#[derive(Clone, Debug)]
pub struct Config {
    /// The QEMU binary.
    pub qemu: PathBuf,
    /// The guest kernel, an x86 bzImage. The driver reads it once, so it can be readable by root
    /// alone.
    pub kernel: PathBuf,
    /// The firmware, or `None` for QEMU's own default.
    pub bios: Option<PathBuf>,
    /// Where QEMU finds its firmware and option ROMs, or `None` for where it was built to look.
    pub firmware_dir: Option<PathBuf>,
    /// Kernel modules the guest loads, in this order, before it mounts its root filesystem. A
    /// kernel with 9p, virtio and vsock built in needs none. Modules ending in `.zst`, `.xz` or
    /// `.gz` are loaded compressed, which the kernel has to be built to allow.
    pub modules: Vec<PathBuf>,
    /// The drone, which runs as the guest's `/init`. It has to be static.
    pub drone: PathBuf,
    /// QEMU's machine type. `microvm` boots fastest. Any other type gets the devices on PCI.
    pub machine: String,
    /// QEMU's accelerator: `kvm`, or `tcg` where there is no KVM, which is far slower.
    pub accel: String,
    /// Where each cell gets a directory for its root filesystem's mount and its console log.
    pub state_dir: PathBuf,
    /// The host id root in a cell is, the same as for containers, since cells share images.
    pub uid_base: u32,
    /// How many ids each cell has.
    pub uid_count: u32,
    /// The host uid and gid the first virtual machine's QEMU runs as. Each one at the same time
    /// gets the next, up to `max_vms`, and none may be in the cells' range.
    pub vmm_uid_base: u32,
    /// The vsock context id of the first virtual machine. The others get the next ones. Context
    /// ids are shared by everything on the host that uses vsock.
    pub cid_base: u32,
    /// How many virtual machines can run at once.
    pub max_vms: u32,
    /// Directories QEMU sees empty, so a QEMU a guest breaks out into finds none of the other
    /// cells' files.
    pub hide: Vec<PathBuf>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            qemu: PathBuf::from("/usr/bin/qemu-system-x86_64"),
            kernel: PathBuf::from("/usr/lib/hivebox/vmlinuz"),
            bios: None,
            firmware_dir: None,
            modules: Vec::new(),
            drone: PathBuf::from("/usr/lib/hivebox/hive-drone"),
            machine: "microvm".into(),
            accel: "kvm".into(),
            state_dir: PathBuf::from("/run/hivebox/qemu"),
            uid_base: 1_000_000,
            uid_count: 65536,
            vmm_uid_base: 2_000_000,
            cid_base: 100_000,
            max_vms: 4096,
            hide: vec![PathBuf::from("/run/hivebox"), PathBuf::from("/var/lib/hivebox")],
        }
    }
}

/// Full virtual machines through QEMU.
#[derive(Debug)]
pub struct QemuDriver {
    cfg: Config,
    kernel: OwnedFd,
    bios: Option<OwnedFd>,
    /// The initramfs every cell starts with.
    base: Vec<u8>,
    /// The group of `/dev/kvm`, which QEMU keeps so it can open it.
    kvm_group: Option<u32>,
    /// Each cell's slot, which gives it its context id and QEMU's uid.
    slots: Mutex<HashMap<CellId, u32>>,
    /// What each prepared cell boots with, until its start takes it. The secret is kept here
    /// rather than in the handle, since the comb writes handles to its WAL.
    boots: Mutex<HashMap<CellId, Boot>>,
    exits: Arc<Exits>,
}

#[derive(Debug)]
struct Boot {
    secret: [u8; 32],
    args: Vec<String>,
    vcpus: u32,
    mem_mib: u32,
}

/// How ended QEMUs ended, until a stop takes the answer.
#[derive(Debug, Default)]
struct Exits {
    ended: Mutex<HashMap<u32, ExitInfo>>,
    news: Notify,
}

impl QemuDriver {
    /// Reads the kernel, the firmware, the drone and the modules, and makes the state directory.
    ///
    /// # Errors
    ///
    /// A file cannot be read, the id ranges overlap, or the state directory cannot be made.
    pub fn new(cfg: Config) -> std::io::Result<Self> {
        let bad = |what: String| std::io::Error::new(std::io::ErrorKind::InvalidInput, what);
        let cells = u64::from(cfg.uid_base)..u64::from(cfg.uid_base) + u64::from(cfg.uid_count);
        let vmms =
            u64::from(cfg.vmm_uid_base)..u64::from(cfg.vmm_uid_base) + u64::from(cfg.max_vms);
        if cells.start < vmms.end && vmms.start < cells.end {
            return Err(bad(format!("QEMU's uids {vmms:?} overlap the cells' {cells:?}")));
        }
        if cfg.cid_base < 3
            || u64::from(cfg.cid_base) + u64::from(cfg.max_vms) >= u64::from(u32::MAX)
        {
            return Err(bad(format!("context ids from {} are not all free to use", cfg.cid_base)));
        }
        let read = |p: &Path| {
            std::fs::read(p)
                .map_err(|e| std::io::Error::new(e.kind(), format!("{}: {e}", p.display())))
        };
        let kernel = sealed("kernel", &read(&cfg.kernel)?)?;
        let bios = cfg.bios.as_deref().map(|b| sealed("bios", &read(b)?)).transpose()?;
        let base = initramfs::base(&cfg.drone, &cfg.modules)?;
        let kvm_group = (cfg.accel == "kvm")
            .then(|| rustix::fs::stat("/dev/kvm").ok().map(|s| s.st_gid))
            .flatten();
        std::fs::DirBuilder::new().recursive(true).mode(0o700).create(&cfg.state_dir)?;
        Ok(Self {
            cfg,
            kernel,
            bios,
            base,
            kvm_group,
            slots: Mutex::default(),
            boots: Mutex::default(),
            exits: Arc::default(),
        })
    }

    fn dir(&self, id: CellId) -> PathBuf {
        self.cfg.state_dir.join(id.to_string())
    }

    /// The lowest slot no cell has.
    fn take_slot(&self, id: CellId) -> std::io::Result<u32> {
        let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        let taken: HashSet<u32> = slots.values().copied().collect();
        let free = (0..self.cfg.max_vms)
            .find(|s| !taken.contains(s))
            .ok_or_else(|| std::io::Error::other("every virtual machine slot is taken"))?;
        slots.insert(id, free);
        Ok(free)
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
        std::fs::DirBuilder::new().mode(0o700).create(&dir)?;
        let root = dir.join("rootfs");
        std::fs::DirBuilder::new().mode(0o755).create(&root)?;
        let owner = Some(rustix::fs::Uid::from_raw(self.cfg.uid_base));
        let group = Some(rustix::fs::Gid::from_raw(self.cfg.uid_base));
        // The upper's own owner and mode are what the guest sees on its `/`.
        std::fs::create_dir_all(&rootfs.upper)?;
        std::fs::set_permissions(&rootfs.upper, std::fs::Permissions::from_mode(0o755))?;
        rustix::fs::chown(&rootfs.upper, owner, group)?;
        let work = slot.dir.join("work");
        std::fs::create_dir_all(&work)?;
        if let Some(seed) = &rootfs.seed {
            hive_cell::tree::copy(seed, &rootfs.upper)?;
        }
        let lowers: Vec<String> = rootfs.lowers.iter().map(|l| l.display().to_string()).collect();
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
            Some(options.as_c_str()),
        )
        .map_err(|e| std::io::Error::other(format!("mounting the root filesystem: {e}")))?;
        let cid = self.cfg.cid_base + self.take_slot(id)?;
        let r = &spec.resources;
        let boot = Boot {
            secret: slot.secret,
            args: drone_args(spec),
            vcpus: r.vcpu_milli.div_ceil(1000).max(1),
            mem_mib: guest_mib(r.mem_mib, self.cfg.accel == "tcg"),
        };
        self.boots.lock().unwrap_or_else(PoisonError::into_inner).insert(id, boot);
        Ok(CellHandle {
            id,
            backend: Backend::Fullvm,
            pid: None,
            channel: GuestChannel::VsockCid { cid, port: DRONE_PORT },
            cgroup: slot.cgroup.clone(),
            netns: slot.netns.clone(),
            extra: BTreeMap::new(),
        })
    }

    fn start_now(&self, h: &CellHandle, boot: &Boot) -> std::io::Result<tokio::process::Child> {
        let GuestChannel::VsockCid { cid, .. } = h.channel else {
            return Err(std::io::Error::other("a virtual machine's channel is a vsock context id"));
        };
        let slot = cid - self.cfg.cid_base;
        let dir = self.dir(h.id);
        let mut initrd = self.base.clone();
        initrd.extend_from_slice(&initramfs::cell(&boot.args, &boot.secret));
        let initrd = sealed("initrd", &initrd)?;
        let root = rustix::fs::open(
            dir.join("rootfs"),
            OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        let vhost =
            rustix::fs::open("/dev/vhost-vsock", OFlags::RDWR | OFlags::CLOEXEC, Mode::empty())
                .map_err(|e| std::io::Error::other(format!("opening /dev/vhost-vsock: {e}")))?;
        let procs = rustix::fs::open(
            h.cgroup.join("cgroup.procs"),
            OFlags::WRONLY | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        let netns = h
            .netns
            .as_ref()
            .map(|n| rustix::fs::open(n, OFlags::RDONLY | OFlags::CLOEXEC, Mode::empty()))
            .transpose()?;
        let console = std::fs::File::create(dir.join("console.log"))?;

        let fd = |f: &OwnedFd| format!("/proc/self/fd/{}", f.as_raw_fd());
        let bus = if self.cfg.machine.starts_with("microvm") { "device" } else { "pci" };
        let tcg = self.cfg.accel == "tcg";
        let mut cmd = tokio::process::Command::new(&self.cfg.qemu);
        cmd.args(["-machine", &self.cfg.machine])
            .args([
                "-accel",
                &if tcg { format!("tcg,tb-size={TB_MIB}") } else { self.cfg.accel.clone() },
            ])
            .args(["-cpu", if tcg { "max" } else { "host" }])
            .args(["-smp", &boot.vcpus.to_string(), "-m", &format!("{}M", boot.mem_mib)])
            .args(["-nodefaults", "-no-user-config", "-display", "none", "-serial", "stdio"])
            // The guest's init ending panics the kernel, which reboots, which ends QEMU.
            .arg("-no-reboot")
            .args([
                "-sandbox",
                "on,obsolete=deny,elevateprivileges=deny,spawn=deny,resourcecontrol=deny",
            ])
            .args(["-kernel", &fd(&self.kernel), "-initrd", &fd(&initrd)])
            .args(["-append", "console=ttyS0 panic=-1 quiet -- --vm"])
            .arg("-fsdev")
            .arg(format!("local,id=root,path={},security_model=passthrough", fd(&root)))
            .args(["-device", &format!("virtio-9p-{bus},fsdev=root,mount_tag={ROOT_TAG}")])
            .arg("-device")
            .arg(format!("vhost-vsock-{bus},guest-cid={cid},vhostfd={}", vhost.as_raw_fd()))
            .stdin(std::process::Stdio::null())
            .stdout(console.try_clone()?)
            .stderr(console)
            .kill_on_drop(false);
        let mut keep =
            vec![self.kernel.as_raw_fd(), initrd.as_raw_fd(), root.as_raw_fd(), vhost.as_raw_fd()];
        if let Some(dir) = &self.cfg.firmware_dir {
            cmd.arg("-L").arg(dir);
        }
        if let Some(b) = &self.bios {
            cmd.args(["-bios", &fd(b)]);
            keep.push(b.as_raw_fd());
        }
        let jail = Jail {
            procs,
            netns,
            uid_base: self.cfg.uid_base,
            uid_count: self.cfg.uid_count,
            vmm: self.cfg.vmm_uid_base + slot,
            groups: self.kvm_group.into_iter().collect(),
            hide: self.cfg.hide.clone(),
            keep,
        };
        // The descriptors QEMU needs are open here until it has started with its own copies.
        let child = jail::spawn(cmd, jail);
        drop((initrd, root, vhost));
        child
    }

    /// Takes down whatever `prepare` made. Fine to call on a cell that is half made or gone.
    async fn clean(&self, id: CellId) {
        self.boots.lock().unwrap_or_else(PoisonError::into_inner).remove(&id);
        self.slots.lock().unwrap_or_else(PoisonError::into_inner).remove(&id);
        let dir = self.dir(id);
        let _ = tokio::task::spawn_blocking(move || {
            let _ = rustix::mount::unmount(dir.join("rootfs"), rustix::mount::UnmountFlags::DETACH);
            let _ = std::fs::remove_dir_all(&dir);
        })
        .await;
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

/// The guest's memory, out of the cell's. QEMU itself needs some of the cgroup's memory, and a
/// guest that filled all of it would have QEMU killed. Under TCG the translation cache is more.
fn guest_mib(cell_mib: u32, tcg: bool) -> u32 {
    let qemu = (cell_mib / 8).clamp(64, 512) + if tcg { TB_MIB } else { 0 };
    cell_mib.saturating_sub(qemu).max(64)
}

/// The drone's arguments in the guest.
fn drone_args(spec: &CellSpec) -> Vec<String> {
    let mut args =
        vec!["--listen".to_string(), format!("vsock:{DRONE_PORT}"), "--secret-stdin".into()];
    let mut env = vec![("PATH", PATH), ("HOME", "/root"), ("LANG", "C.UTF-8")];
    env.retain(|(k, _)| !spec.env.contains_key(*k));
    for (k, v) in env.into_iter().chain(spec.env.iter().map(|(k, v)| (k.as_str(), v.as_str()))) {
        args.push("--env".into());
        args.push(format!("{k}={v}"));
    }
    args
}

/// `data` in a sealed memfd, which QEMU opens through `/proc/self/fd` as a file of its own.
fn sealed(name: &str, data: &[u8]) -> std::io::Result<OwnedFd> {
    let fd = rustix::fs::memfd_create(name, MemfdFlags::CLOEXEC | MemfdFlags::ALLOW_SEALING)?;
    let mut f = std::fs::File::from(fd);
    f.write_all(data)?;
    let fd = OwnedFd::from(f);
    rustix::fs::fcntl_add_seals(
        &fd,
        SealFlags::SHRINK | SealFlags::GROW | SealFlags::WRITE | SealFlags::SEAL,
    )?;
    Ok(fd)
}

async fn wait_gone(pidfd: &OwnedFd, limit: Duration) -> bool {
    let Ok(fd) = AsyncFd::with_interest(pidfd.as_fd(), tokio::io::Interest::READABLE) else {
        return false;
    };
    tokio::time::timeout(limit, fd.readable()).await.is_ok_and(|r| r.is_ok())
}

impl CellDriver for QemuDriver {
    fn backend(&self) -> Backend {
        Backend::Fullvm
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
            let yes = |ok: bool| if ok { "yes" } else { "missing" };
            let qemu = self.cfg.qemu.is_file();
            need(qemu, format!("qemu at {}: {}", self.cfg.qemu.display(), yes(qemu)));
            let vsock = Path::new("/dev/vhost-vsock").exists();
            need(vsock, format!("/dev/vhost-vsock: {}", yes(vsock)));
            if self.cfg.accel == "kvm" {
                let kvm = Path::new("/dev/kvm").exists();
                need(kvm, format!("/dev/kvm: {}", yes(kvm)));
            }
            let overlay = std::fs::read_to_string("/proc/filesystems")
                .is_ok_and(|s| s.lines().any(|l| l.ends_with("\toverlay")));
            need(overlay, format!("overlayfs: {}", yes(overlay)));
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
                return Err(Error::new(Reason::Internal, "a virtual machine needs a cgroup"));
            }
            match self.prepare_now(id, spec, rootfs, slot) {
                Ok(h) => Ok(h),
                Err(e) => {
                    self.clean(id).await;
                    Err(Error::new(Reason::Internal, format!("preparing the virtual machine: {e}")))
                }
            }
        })
    }

    fn start<'a>(&'a self, h: &'a mut CellHandle) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let boot = self
                .boots
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&h.id)
                .ok_or_else(|| {
                    Error::new(Reason::Internal, "the cell was not prepared, or started before")
                })?;
            let mut child = self.start_now(h, &boot).map_err(|e| {
                Error::new(Reason::Internal, format!("starting the virtual machine: {e}"))
            })?;
            let pid =
                child.id().ok_or_else(|| Error::new(Reason::Internal, "QEMU ended at once"))?;
            h.pid = Some(pid);
            if let Some(start) = start_time(pid) {
                h.extra.insert(hive_cell::pid::START.into(), start.to_string());
            }
            let exits = self.exits.clone();
            tokio::spawn(async move {
                use std::os::unix::process::ExitStatusExt;
                let exit = match child.wait().await {
                    Ok(s) => ExitInfo { code: s.code(), signal: s.signal(), oom: false },
                    Err(_) => ExitInfo::default(),
                };
                exits.ended.lock().unwrap_or_else(PoisonError::into_inner).insert(pid, exit);
                exits.news.notify_waiters();
            });
            Ok(())
        })
    }

    fn pause<'a>(&'a self, h: &'a CellHandle, mode: PauseMode) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if mode == PauseMode::SnapshotKill {
                return Err(Error::new(
                    Reason::PolicyDenied,
                    "a virtual machine cannot be snapshotted yet",
                ));
            }
            // Freezing QEMU's cgroup stops the guest's vCPUs with it, the same as a container.
            let io = |e: std::io::Error| Error::new(Reason::Internal, format!("pausing: {e}"));
            cgroup::freeze(&h.cgroup, true).map_err(io)?;
            let until = Instant::now() + Duration::from_secs(5);
            while !cgroup::frozen(&h.cgroup).map_err(io)? {
                if Instant::now() > until {
                    return Err(Error::new(Reason::Internal, "the cell did not freeze within 5s"));
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            if mode == PauseMode::Reclaim {
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
            cgroup::swap(&h.cgroup, false);
            Ok(())
        })
    }

    fn trim<'a>(&'a self, h: &'a CellHandle) -> BoxFuture<'a, Result<u64>> {
        Box::pin(async move {
            let dir = h.cgroup.clone();
            tokio::task::spawn_blocking(move || cgroup::trim(&dir, TRIM_FLOOR))
                .await
                .map_err(|e| Error::new(Reason::Internal, format!("trimming: {e}")))
        })
    }

    fn stop<'a>(&'a self, h: &'a CellHandle, _grace: Duration) -> BoxFuture<'a, Result<ExitInfo>> {
        Box::pin(async move {
            let mut exit = ExitInfo::default();
            if let Some(pid) = h.pid {
                // The guest's writes are in the upper as soon as they return, so there is
                // nothing for a grace period to save, and QEMU ends at once on a TERM.
                if let Some(pidfd) = pidfd(pid).ok().filter(|_| alive(h)) {
                    let _ = cgroup::freeze(&h.cgroup, false);
                    let _ =
                        rustix::process::pidfd_send_signal(&pidfd, rustix::process::Signal::TERM);
                    if !wait_gone(&pidfd, Duration::from_secs(5)).await {
                        if cgroup::kill(&h.cgroup).is_err() {
                            let _ = rustix::process::pidfd_send_signal(
                                &pidfd,
                                rustix::process::Signal::KILL,
                            );
                        }
                        wait_gone(&pidfd, Duration::from_secs(5)).await;
                    }
                }
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
            // A cell from before a restart keeps its slot.
            if let GuestChannel::VsockCid { cid, .. } = h.channel
                && let Some(slot) = cid.checked_sub(self.cfg.cid_base)
            {
                self.slots.lock().unwrap_or_else(PoisonError::into_inner).insert(h.id, slot);
            }
            if cgroup::frozen(&h.cgroup).unwrap_or(false) {
                return Ok(Liveness::Paused);
            }
            if cgroup::oom_kills(&h.cgroup) > 0 {
                let exit = ExitInfo { signal: Some(9), oom: true, ..ExitInfo::default() };
                return Ok(Liveness::Gone(exit));
            }
            Ok(Liveness::Alive)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_guest_leaves_qemu_room_in_the_cell() {
        assert_eq!(guest_mib(512, false), 448);
        assert_eq!(guest_mib(2048, false), 1792);
        assert_eq!(guest_mib(16384, false), 15872);
        assert_eq!(guest_mib(32, false), 64);
        assert_eq!(guest_mib(1024, true), 832);
    }

    #[test]
    fn the_drone_gets_the_cell_env_over_the_defaults() {
        let mut spec = CellSpec::new(hive_types::Source::Image("python".into()), Backend::Fullvm);
        spec.env.insert("PATH".into(), "/opt/bin".into());
        let args = drone_args(&spec);
        assert_eq!(&args[..3], ["--listen", "vsock:52", "--secret-stdin"]);
        let env: Vec<&str> = args[3..].chunks(2).map(|p| p[1].as_str()).collect();
        assert_eq!(env, ["HOME=/root", "LANG=C.UTF-8", "PATH=/opt/bin"]);
    }

    #[test]
    fn overlapping_uids_are_refused() {
        let cfg = Config { vmm_uid_base: 1_000_100, ..Config::default() };
        let e = QemuDriver::new(cfg).unwrap_err();
        assert!(e.to_string().contains("overlap"), "{e}");
        let cfg = Config { cid_base: 2, ..Config::default() };
        assert!(QemuDriver::new(cfg).unwrap_err().to_string().contains("context ids"));
    }
}
