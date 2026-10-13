//! The drone as the first process of a virtual machine, started by the kernel from an initramfs.
//!
//! The initramfs the fullvm driver builds holds the drone as `/init`, the kernel modules the guest
//! needs under `/hivebox/modules`, the drone's arguments in `/hivebox/args` and its first secret in
//! `/hivebox/secret`. [`boot`] loads the modules in name order, mounts the cell's root filesystem
//! from the host over 9p, moves into it and removes everything it read, so neither the secret nor
//! the arguments are left anywhere the cell can reach. The secret goes to the drone on a pipe as
//! its stdin. From there it is the same as in a container: [`crate::init::run`] starts the drone
//! and waits on orphans, and when the drone ends PID 1 ends, the kernel panics and the virtual
//! machine is gone.

use std::ffi::{CString, OsString};
use std::io::{self, Read};
use std::os::unix::ffi::OsStringExt;
use std::path::Path;
use std::process::ExitCode;

/// Where the initramfs keeps what the driver put in it.
const BOOT: &str = "/hivebox";
/// Where the root filesystem is mounted before the switch.
const NEWROOT: &str = "/newroot";
/// The 9p tag the driver exports the root filesystem under.
pub const ROOT_TAG: &str = "root";
/// The kernel's `MODULE_INIT_COMPRESSED_FILE`, for a module the kernel decompresses itself.
const COMPRESSED: i32 = 4;

/// Gets the virtual machine ready and runs the drone under the init loop. It only returns when
/// the drone has ended or the setup failed.
pub fn boot() -> ExitCode {
    match setup() {
        Ok(args) => crate::init::run(args),
        Err(e) => {
            eprintln!("hive-drone vm: {e}");
            ExitCode::FAILURE
        }
    }
}

fn setup() -> io::Result<Vec<OsString>> {
    use rustix::mount::{MountFlags, mount, mount_move};
    let at = |what: &str, e: io::Error| io::Error::new(e.kind(), format!("{what}: {e}"));
    for (fs, dir) in [("proc", "/proc"), ("sysfs", "/sys"), ("devtmpfs", "/dev")] {
        std::fs::create_dir_all(dir)?;
        mount(fs, dir, fs, MountFlags::NOSUID | MountFlags::NOEXEC, None)
            .map_err(|e| at(&format!("mounting {dir}"), e.into()))?;
    }
    load_modules(&Path::new(BOOT).join("modules"))?;
    let args =
        std::fs::read(Path::new(BOOT).join("args")).map_err(|e| at("reading the args", e))?;
    let args = args
        .split(|b| *b == 0)
        .filter(|a| !a.is_empty())
        .map(|a| OsString::from_vec(a.to_vec()))
        .collect();
    let mut secret = [0u8; 32];
    std::fs::File::open(Path::new(BOOT).join("secret"))
        .and_then(|mut f| f.read_exact(&mut secret))
        .map_err(|e| at("reading the secret", e))?;

    std::fs::create_dir_all(NEWROOT)?;
    // cache=mmap keeps nothing but mapped pages in the guest, so every write is in the cell's
    // upper on the host as soon as it returns, which is what a snapshot of the upper needs.
    let opts = CString::new("trans=virtio,version=9p2000.L,access=client,cache=mmap,msize=512000")
        .map_err(io::Error::other)?;
    mount(ROOT_TAG, NEWROOT, "9p", MountFlags::empty(), Some(opts.as_c_str()))
        .map_err(|e| at("mounting the root filesystem", e.into()))?;
    let new = Path::new(NEWROOT);
    for dir in ["proc", "sys", "dev"] {
        std::fs::create_dir_all(new.join(dir))?;
        mount_move(Path::new("/").join(dir), new.join(dir))
            .map_err(|e| at(&format!("moving /{dir}"), e.into()))?;
    }
    let pts = CString::new("newinstance,ptmxmode=0666,mode=0620").map_err(io::Error::other)?;
    for (fs, dir, flags, opts) in [
        ("devpts", "dev/pts", MountFlags::NOSUID | MountFlags::NOEXEC, Some(pts.as_c_str())),
        ("tmpfs", "dev/shm", MountFlags::NOSUID | MountFlags::NODEV, None),
        ("tmpfs", "run", MountFlags::NOSUID | MountFlags::NODEV, None),
    ] {
        std::fs::create_dir_all(new.join(dir))?;
        mount(fs, new.join(dir), fs, flags, opts)
            .map_err(|e| at(&format!("mounting /{dir}"), e.into()))?;
    }
    // The initramfs stays in memory under the new root, so what was read goes now.
    std::fs::remove_dir_all(BOOT)?;
    std::fs::remove_file("/init")?;
    rustix::system::sethostname(b"cell")?;

    std::env::set_current_dir(NEWROOT)?;
    mount_move(".", "/").map_err(|e| at("moving the root filesystem", e.into()))?;
    rustix::process::chroot(".")?;
    std::env::set_current_dir("/")?;

    // The drone reads the secret from stdin. A pipe holds far more than 32 bytes, so this write
    // never waits for a reader.
    let (read, write) = rustix::pipe::pipe_with(rustix::pipe::PipeFlags::CLOEXEC)?;
    rustix::io::write(&write, &secret)?;
    drop(write);
    rustix::stdio::dup2_stdin(&read)?;
    Ok(args)
}

/// Loads every module in `dir` in name order. One the kernel already has is fine.
fn load_modules(dir: &Path) -> io::Result<()> {
    let Ok(entries) = std::fs::read_dir(dir) else { return Ok(()) };
    let mut paths: Vec<_> = entries.map(|e| e.map(|e| e.path())).collect::<io::Result<_>>()?;
    paths.sort();
    for path in paths {
        let file = std::fs::File::open(&path)?;
        let packed = path.extension().is_some_and(|x| x == "zst" || x == "xz" || x == "gz");
        match rustix::system::finit_module(&file, c"", if packed { COMPRESSED } else { 0 }) {
            Ok(()) | Err(rustix::io::Errno::EXIST) => {}
            Err(e) => {
                return Err(io::Error::other(format!("loading {}: {e}", path.display())));
            }
        }
    }
    Ok(())
}
