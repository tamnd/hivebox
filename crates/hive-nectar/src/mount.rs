//! Mounting layers on a node: each blob on a read only loop device, EROFS on those through the new
//! mount API, and the mount idmapped so that root in the image is the cells' root on the host.
//!
//! This is the eager path from `spec/06_storage_images.md`, section 5.1: the blobs are already
//! whole in the cache. Loop devices are made with `LOOP_CONFIGURE` in one call, read only, with
//! direct I/O so a layer is not cached twice, and with autoclear, so the kernel frees a device as
//! soon as the last mount on it goes. Nothing needs detaching by hand, even after a crash.
//!
//! Layers are built with the owners the image has, and one user namespace per node maps them.
//! The mount is idmapped before it is attached, so no one ever sees it unshifted.

#![allow(unsafe_code)]

use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::os::fd::{AsFd, AsRawFd, OwnedFd};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use tokio::sync::OnceCell;

use crate::store::blocking;
use crate::{BlobId, BlobStore, Cache, Held, LayerRef, Manifest};

use rustix::mount::{
    FsMountFlags, FsOpenFlags, MountAttrFlags, MoveMountFlags, UnmountFlags, fsconfig_create,
    fsconfig_set_flag, fsconfig_set_string, fsmount, fsopen, move_mount, unmount,
};

const LOOP_CTL_GET_FREE: libc::c_ulong = 0x4C82;
const LOOP_CONFIGURE: libc::c_ulong = 0x4C0A;
const LO_FLAGS_READ_ONLY: u32 = 1;
const LO_FLAGS_AUTOCLEAR: u32 = 4;
const LO_FLAGS_DIRECT_IO: u32 = 16;
const MOUNT_ATTR_IDMAP: u64 = 0x0010_0000;

/// `struct loop_info64` from `linux/loop.h`.
#[repr(C)]
struct LoopInfo64 {
    device: u64,
    inode: u64,
    rdevice: u64,
    offset: u64,
    sizelimit: u64,
    number: u32,
    encrypt_type: u32,
    encrypt_key_size: u32,
    flags: u32,
    file_name: [u8; 64],
    crypt_name: [u8; 64],
    encrypt_key: [u8; 32],
    init: [u64; 2],
}

/// `struct loop_config` from `linux/loop.h`.
#[repr(C)]
struct LoopConfig {
    fd: u32,
    block_size: u32,
    info: LoopInfo64,
    reserved: [u64; 8],
}

/// `struct mount_attr` from `linux/mount.h`.
#[repr(C)]
struct MountAttr {
    attr_set: u64,
    attr_clr: u64,
    propagation: u64,
    userns_fd: u64,
}

/// A user namespace that maps ids `0..count` to `base..base + count`, for idmapping layers. One is
/// enough for a node, since every cell has the same range.
#[derive(Debug)]
pub struct IdMap {
    userns: OwnedFd,
    base: u32,
    count: u32,
}

impl IdMap {
    /// Makes the namespace, which takes a short lived child process, since a process with threads
    /// cannot move itself into a new user namespace.
    ///
    /// # Errors
    ///
    /// The caller cannot make user namespaces or write their maps, which takes `CAP_SETUID` and
    /// `CAP_SETGID`.
    pub fn new(base: u32, count: u32) -> io::Result<Self> {
        if count == 0 || base.checked_add(count).is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the id range runs past the last id",
            ));
        }
        let (ready_r, ready_w) = rustix::pipe::pipe_with(rustix::pipe::PipeFlags::CLOEXEC)?;
        // SAFETY: the child only makes raw system calls, which are async signal safe, and never
        // returns: it ends in `_exit` or is killed by the parent. It touches no locks or memory
        // the other threads of this process might have held at the fork.
        let pid = unsafe { libc::fork() };
        if pid < 0 {
            return Err(io::Error::last_os_error());
        }
        if pid == 0 {
            // SAFETY: as above, plain system calls on a descriptor this process owns.
            unsafe {
                let ok = libc::unshare(libc::CLONE_NEWUSER) == 0;
                let byte = u8::from(ok);
                libc::write(ready_w.as_raw_fd(), (&raw const byte).cast(), 1);
                loop {
                    libc::pause();
                }
            }
        }
        drop(ready_w);
        let made = (|| {
            let mut byte = [0u8];
            if rustix::io::read(&ready_r, &mut byte)? != 1 || byte[0] != 1 {
                return Err(io::Error::other("the child could not make a user namespace"));
            }
            let proc = PathBuf::from(format!("/proc/{pid}"));
            let map = format!("0 {base} {count}\n");
            std::fs::write(proc.join("uid_map"), &map)?;
            std::fs::write(proc.join("gid_map"), &map)?;
            Ok(OwnedFd::from(File::open(proc.join("ns/user"))?))
        })();
        // SAFETY: `pid` is the child forked above, which nothing else waits for.
        unsafe {
            libc::kill(pid, libc::SIGKILL);
            libc::waitpid(pid, std::ptr::null_mut(), 0);
        }
        Ok(Self { userns: made?, base, count })
    }

    /// The first host id.
    #[must_use]
    pub fn base(&self) -> u32 {
        self.base
    }

    /// How many ids it maps.
    #[must_use]
    pub fn count(&self) -> u32 {
        self.count
    }
}

/// A layer mounted read only at a path, until it is dropped. Dropping it detaches the mount, and
/// overlays already using it keep it alive until they go too.
#[derive(Debug)]
pub struct Mounted {
    target: PathBuf,
}

impl Mounted {
    /// Where it is mounted.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.target
    }
}

impl Drop for Mounted {
    fn drop(&mut self) {
        let _ = unmount(&self.target, UnmountFlags::DETACH);
    }
}

/// Mounts the layer made of `meta` and `data` at `target`, an empty directory. `data` is `None`
/// for a layer with no file contents, which `mkfs.erofs` builds with no extra device. With an
/// `idmap`, files the image has owned by id `n` show up as owned by `base + n`, and ids outside
/// the map show up as the overflow id.
///
/// # Errors
///
/// The blobs cannot be put on loop devices, they are not EROFS, or the mount fails.
pub fn mount_layer(
    meta: &Path,
    data: Option<&Path>,
    target: &Path,
    idmap: Option<&IdMap>,
) -> io::Result<Mounted> {
    // Both devices stay open until the mount has them, since autoclear frees a device on the last
    // close.
    let meta_dev = attach(meta)?;
    let data_dev = data.map(attach).transpose()?;
    let fs = fsopen("erofs", FsOpenFlags::FSOPEN_CLOEXEC)?;
    fsconfig_set_string(&fs, "source", &meta_dev.path)?;
    if let Some(d) = &data_dev {
        fsconfig_set_string(&fs, "device", &d.path)?;
    }
    fsconfig_set_flag(&fs, "ro")?;
    fsconfig_create(&fs)
        .map_err(|e| io::Error::other(format!("mounting {} as EROFS: {e}", meta.display())))?;
    let mnt = fsmount(
        &fs,
        FsMountFlags::FSMOUNT_CLOEXEC,
        MountAttrFlags::MOUNT_ATTR_RDONLY
            | MountAttrFlags::MOUNT_ATTR_NODEV
            | MountAttrFlags::MOUNT_ATTR_NOSUID,
    )?;
    if let Some(map) = idmap {
        set_idmap(&mnt, &map.userns)?;
    }
    move_mount(&mnt, "", rustix::fs::CWD, target, MoveMountFlags::MOVE_MOUNT_F_EMPTY_PATH)?;
    Ok(Mounted { target: target.to_owned() })
}

fn set_idmap(mnt: &OwnedFd, userns: &OwnedFd) -> io::Result<()> {
    let attr = MountAttr {
        attr_set: MOUNT_ATTR_IDMAP,
        attr_clr: 0,
        propagation: 0,
        userns_fd: userns.as_raw_fd() as u64,
    };
    // SAFETY: `attr` is a `struct mount_attr` that lives across the call, its size is passed with
    // it, and the path is an empty C string.
    let r = unsafe {
        libc::syscall(
            libc::SYS_mount_setattr,
            mnt.as_raw_fd(),
            c"".as_ptr(),
            libc::AT_EMPTY_PATH,
            &raw const attr,
            size_of::<MountAttr>(),
        )
    };
    if r < 0 {
        return Err(io::Error::other(format!(
            "idmapping the layer: {}",
            io::Error::last_os_error()
        )));
    }
    Ok(())
}

/// A loop device, held open.
struct Loop {
    path: PathBuf,
    _dev: File,
}

/// Puts `file` on a free loop device, read only, with direct I/O and autoclear.
fn attach(file: &Path) -> io::Result<Loop> {
    let backing = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT | libc::O_CLOEXEC)
        .open(file)
        .map_err(|e| io::Error::new(e.kind(), format!("{}: {e}", file.display())))?;
    let control = File::open("/dev/loop-control")?;
    // Another process may take the free device between asking and configuring, so this asks again.
    for _ in 0..64 {
        // SAFETY: LOOP_CTL_GET_FREE takes no argument and returns a device number.
        let n = unsafe { libc::ioctl(control.as_raw_fd(), LOOP_CTL_GET_FREE) };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        let path = PathBuf::from(format!("/dev/loop{n}"));
        let dev = File::options().read(true).custom_flags(libc::O_CLOEXEC).open(&path)?;
        let mut config = LoopConfig {
            fd: u32::try_from(backing.as_fd().as_raw_fd()).map_err(io::Error::other)?,
            block_size: 4096,
            // SAFETY: every field of `loop_info64` is an integer or an array of them, so all
            // zeros is a valid value.
            info: unsafe { std::mem::zeroed() },
            reserved: [0; 8],
        };
        config.info.flags = LO_FLAGS_READ_ONLY | LO_FLAGS_AUTOCLEAR | LO_FLAGS_DIRECT_IO;
        // SAFETY: `config` is a `struct loop_config` that lives across the call, and the kernel
        // only reads it.
        let r = unsafe { libc::ioctl(dev.as_raw_fd(), LOOP_CONFIGURE, &raw const config) };
        if r == 0 {
            return Ok(Loop { path, _dev: dev });
        }
        let e = io::Error::last_os_error();
        if e.raw_os_error() != Some(libc::EBUSY) {
            return Err(io::Error::new(
                e.kind(),
                format!("{}: LOOP_CONFIGURE: {e}", path.display()),
            ));
        }
    }
    Err(io::Error::other("no free loop device after 64 tries"))
}

/// The layers mounted on a node, each once however many cells use it. A layer stays mounted, with
/// its blobs pinned in the cache, for as long as this lives.
#[derive(Debug)]
pub struct Layers {
    dir: PathBuf,
    cache: Arc<Cache>,
    idmap: Option<Arc<IdMap>>,
    mounted: Mutex<HashMap<BlobId, Arc<OnceCell<Arc<Layer>>>>>,
}

#[derive(Debug)]
struct Layer {
    mounted: Mounted,
    _meta: Held,
    _data: Option<Held>,
}

impl Layers {
    /// Mounts layers under `dir`, one directory each named by the layer, with blobs from `cache`.
    /// Whatever an earlier run left mounted there is detached first. Cells already running on
    /// those mounts keep them.
    ///
    /// # Errors
    ///
    /// `dir` cannot be made or cleared.
    pub fn new(
        dir: impl Into<PathBuf>,
        cache: Arc<Cache>,
        idmap: Option<IdMap>,
    ) -> io::Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        for entry in std::fs::read_dir(&dir)? {
            let path = entry?.path();
            let _ = unmount(&path, UnmountFlags::DETACH);
            std::fs::remove_dir(&path)?;
        }
        Ok(Self { dir, cache, idmap: idmap.map(Arc::new), mounted: Mutex::default() })
    }

    /// Fetches the image's blobs from `store` into the cache where they are not there yet, mounts
    /// the layers that are not mounted yet, and returns where they are, top first as overlayfs
    /// wants them.
    ///
    /// # Errors
    ///
    /// A blob cannot be fetched or a layer cannot be mounted.
    pub async fn mount(&self, store: &dyn BlobStore, image: &Manifest) -> io::Result<Vec<PathBuf>> {
        let each = image.layers.iter().rev().map(|l| self.one(store, l));
        let layers = futures::future::try_join_all(each).await?;
        Ok(layers.iter().map(|l| l.mounted.path().to_owned()).collect())
    }

    /// How many layers are mounted.
    #[must_use]
    pub fn len(&self) -> usize {
        let m = self.mounted.lock().unwrap_or_else(PoisonError::into_inner);
        m.values().filter(|c| c.initialized()).count()
    }

    /// Whether none are.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    async fn one(&self, store: &dyn BlobStore, layer: &LayerRef) -> io::Result<Arc<Layer>> {
        let cell = self
            .mounted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(layer.digest)
            .or_default()
            .clone();
        let got = cell
            .get_or_try_init(|| async {
                let meta = self.cache.get(store, layer.meta).await?;
                let data = if layer.data_size == 0 {
                    None
                } else {
                    Some(self.cache.get(store, layer.data).await?)
                };
                let target = self.dir.join(layer.digest.to_string());
                let (paths, idmap) = (
                    (meta.path().to_owned(), data.as_ref().map(|d| d.path().to_owned())),
                    self.idmap.clone(),
                );
                let t = target.clone();
                let mounted = blocking(move || {
                    match std::fs::create_dir(&t) {
                        Err(e) if e.kind() != io::ErrorKind::AlreadyExists => return Err(e),
                        _ => {}
                    }
                    mount_layer(&paths.0, paths.1.as_deref(), &t, idmap.as_deref())
                })
                .await?;
                Ok::<_, io::Error>(Arc::new(Layer { mounted, _meta: meta, _data: data }))
            })
            .await;
        match got {
            Ok(l) => Ok(l.clone()),
            Err(e) => Err(io::Error::new(e.kind(), format!("layer {}: {e}", layer.digest))),
        }
    }
}
