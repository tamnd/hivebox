//! The initramfs a virtual machine boots from, as `newc` cpio archives the kernel unpacks.
//!
//! The part every cell shares, the drone as `/init` and the kernel modules, is built once when
//! the driver starts. Each cell's arguments and secret go in a second small archive after it,
//! which the kernel unpacks over the first, so a cell costs a copy and not a rebuild.

use std::io;
use std::path::Path;

/// One archive being written.
#[derive(Debug, Default)]
pub(crate) struct Cpio {
    out: Vec<u8>,
    ino: u32,
}

const DIR: u32 = 0o040_000;
const FILE: u32 = 0o100_000;

impl Cpio {
    pub(crate) fn dir(&mut self, name: &str, mode: u32) {
        self.entry(name, DIR | mode, &[]);
    }

    pub(crate) fn file(&mut self, name: &str, mode: u32, data: &[u8]) {
        self.entry(name, FILE | mode, data);
    }

    /// Ends the archive and returns it.
    pub(crate) fn finish(mut self) -> Vec<u8> {
        self.entry("TRAILER!!!", 0, &[]);
        self.out
    }

    fn entry(&mut self, name: &str, mode: u32, data: &[u8]) {
        self.ino += 1;
        let size = u32::try_from(data.len()).expect("an initramfs file is under 4 GiB");
        let namesize = u32::try_from(name.len() + 1).expect("a short name");
        let nlink = if mode & DIR != 0 { 2 } else { 1 };
        // magic, ino, mode, uid, gid, nlink, mtime, filesize, devmajor, devminor, rdevmajor,
        // rdevminor, namesize, check
        self.out.extend_from_slice(b"070701");
        for field in [self.ino, mode, 0, 0, nlink, 0, size, 0, 0, 0, 0, namesize, 0] {
            self.out.extend_from_slice(format!("{field:08x}").as_bytes());
        }
        self.out.extend_from_slice(name.as_bytes());
        self.out.push(0);
        self.pad();
        self.out.extend_from_slice(data);
        self.pad();
    }

    fn pad(&mut self) {
        while !self.out.len().is_multiple_of(4) {
            self.out.push(0);
        }
    }
}

/// The shared archive: the drone as `/init`, and each module under `/hivebox/modules`, numbered
/// so the drone loads them in the order given.
pub(crate) fn base(drone: &Path, modules: &[impl AsRef<Path>]) -> io::Result<Vec<u8>> {
    let mut c = Cpio::default();
    c.file("init", 0o755, &std::fs::read(drone)?);
    c.dir("hivebox", 0o700);
    c.dir("hivebox/modules", 0o700);
    for (i, m) in modules.iter().enumerate() {
        let m = m.as_ref();
        let name = m.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, format!("bad module {}", m.display()))
        })?;
        c.file(&format!("hivebox/modules/{i:03}-{name}"), 0o600, &std::fs::read(m)?);
    }
    Ok(c.finish())
}

/// One cell's archive: the drone's arguments, each ended by a NUL, and its first secret.
pub(crate) fn cell(args: &[String], secret: &[u8; 32]) -> Vec<u8> {
    let mut joined = Vec::new();
    for a in args {
        joined.extend_from_slice(a.as_bytes());
        joined.push(0);
    }
    let mut c = Cpio::default();
    c.dir("hivebox", 0o700);
    c.file("hivebox/args", 0o600, &joined);
    c.file("hivebox/secret", 0o600, secret);
    c.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reads the names, modes and contents back out of an archive.
    fn entries(b: &[u8]) -> Vec<(String, u32, Vec<u8>)> {
        let hex = |at: usize| {
            let s = std::str::from_utf8(&b[at..at + 8]).unwrap();
            usize::from_str_radix(s, 16).unwrap()
        };
        let up = |n: usize| (n + 3) & !3;
        let (mut out, mut pos) = (Vec::new(), 0);
        loop {
            assert_eq!(&b[pos..pos + 6], b"070701");
            let (mode, size, namesize) = (hex(pos + 14), hex(pos + 54), hex(pos + 94));
            let name = String::from_utf8(b[pos + 110..pos + 109 + namesize].to_vec()).unwrap();
            let data = up(pos + 110 + namesize);
            pos = up(data + size);
            if name == "TRAILER!!!" {
                assert_eq!(pos, b.len());
                return out;
            }
            out.push((name, u32::try_from(mode).unwrap(), b[data..data + size].to_vec()));
        }
    }

    #[test]
    fn a_cell_archive_holds_the_args_and_the_secret() {
        let a = cell(&["--listen".into(), "vsock:52".into()], &[7; 32]);
        assert_eq!(a.len() % 4, 0);
        let e = entries(&a);
        assert_eq!(e[0], ("hivebox".into(), 0o040_700, vec![]));
        assert_eq!(e[1], ("hivebox/args".into(), 0o100_600, b"--listen\0vsock:52\0".to_vec()));
        assert_eq!(e[2], ("hivebox/secret".into(), 0o100_600, vec![7; 32]));
    }

    #[test]
    fn the_base_numbers_the_modules_in_order() {
        let dir = std::env::temp_dir().join(format!("hive-initramfs-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        for (name, body) in [("drone", "elf"), ("b.ko.zst", "bb"), ("a.ko", "a")] {
            std::fs::write(dir.join(name), body).unwrap();
        }
        let a = base(&dir.join("drone"), &[dir.join("b.ko.zst"), dir.join("a.ko")]).unwrap();
        let names: Vec<_> = entries(&a).into_iter().map(|(n, _, d)| (n, d)).collect();
        assert_eq!(
            names,
            [
                ("init".into(), b"elf".to_vec()),
                ("hivebox".into(), vec![]),
                ("hivebox/modules".into(), vec![]),
                ("hivebox/modules/000-b.ko.zst".into(), b"bb".to_vec()),
                ("hivebox/modules/001-a.ko".into(), b"a".to_vec()),
            ]
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
