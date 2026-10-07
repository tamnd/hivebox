//! The layer writer against `mkfs.erofs`: the same tar built both ways must mount as the same
//! tree, down to modes, owners, times, links, device numbers, xattrs and file contents. It needs
//! root, a kernel with EROFS and loop devices, and `HIVE_MKFS_EROFS`, and passes without doing
//! anything otherwise. It builds a tar of its own, and every tar in `HIVE_EROFS_TARS`, a list
//! split by colons, such as the layers of a real image. When `fsck.erofs` sits next to
//! `mkfs.erofs`, it checks what the writer built too.

#![cfg(target_os = "linux")]

use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufReader;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use hive_nectar::erofs::{Built, DEFAULT_CHUNK_SIZE, Mkfs, Writer};
use hive_nectar::mount::mount_layer;

/// What a path in a mounted layer shows, less its inode number and the blocks it takes.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Shown {
    mode: u32,
    uid: u32,
    gid: u32,
    size: u64,
    mtime: (i64, i64),
    nlink: u64,
    rdev: u64,
    xattrs: BTreeMap<Vec<u8>, Vec<u8>>,
    /// The hash of a file's contents, or a symlink's target.
    body: Vec<u8>,
}

fn xattrs(path: &Path) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut names = vec![0u8; 1 << 16];
    let n = rustix::fs::llistxattr(path, &mut names[..]).unwrap();
    let mut out = BTreeMap::new();
    for name in names[..n].split(|&b| b == 0).filter(|n| !n.is_empty()) {
        let mut value = vec![0u8; 1 << 16];
        let len = rustix::fs::lgetxattr(path, name, &mut value[..]).unwrap_or_else(|e| {
            panic!("{}: {}: {e}", path.display(), String::from_utf8_lossy(name))
        });
        out.insert(name.to_vec(), value[..len].to_vec());
    }
    out
}

fn walk(root: &Path, at: &Path, out: &mut BTreeMap<PathBuf, Shown>) {
    let m = std::fs::symlink_metadata(at).unwrap();
    let ty = m.file_type();
    let body = if ty.is_file() {
        let mut h = blake3::Hasher::new();
        h.update_reader(File::open(at).unwrap()).unwrap();
        h.finalize().as_bytes().to_vec()
    } else if ty.is_symlink() {
        std::fs::read_link(at).unwrap().into_os_string().into_encoded_bytes()
    } else {
        Vec::new()
    };
    let rel = at.strip_prefix(root).unwrap().to_path_buf();
    out.insert(
        rel,
        Shown {
            mode: m.mode(),
            uid: m.uid(),
            gid: m.gid(),
            size: m.size(),
            mtime: (m.mtime(), m.mtime_nsec()),
            nlink: m.nlink(),
            rdev: if ty.is_char_device() || ty.is_block_device() { m.rdev() } else { 0 },
            xattrs: xattrs(at),
            body,
        },
    );
    if ty.is_dir() {
        for e in std::fs::read_dir(at).unwrap() {
            walk(root, &e.unwrap().path(), out);
        }
    }
}

/// Mounts `built` at `target` and reads the whole tree.
fn tree(built: &Built, target: &Path) -> BTreeMap<PathBuf, Shown> {
    std::fs::create_dir_all(target).unwrap();
    let data = std::fs::metadata(&built.data).is_ok_and(|m| m.len() > 0);
    let _mounted = mount_layer(&built.meta, data.then_some(built.data.as_path()), target, None)
        .unwrap_or_else(|e| panic!("mounting {}: {e}", built.meta.display()));
    let mut out = BTreeMap::new();
    walk(target, target, &mut out);
    out
}

/// A tar with one of each thing a layer holds.
fn sample(path: &Path) {
    use tar::EntryType::{Block, Char, Directory, Fifo, Link, Regular, Symlink, XHeader};
    let mut b = tar::Builder::new(File::create(path).unwrap());
    let header = |kind, mode, size, uid| {
        let mut h = tar::Header::new_gnu();
        h.set_entry_type(kind);
        h.set_mode(mode);
        h.set_uid(uid);
        h.set_gid(uid + 1);
        h.set_mtime(1_700_000_000);
        h.set_size(size);
        h
    };
    let mut x = 7u64;
    let noise: Vec<u8> = (0..1_500_000)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 24) as u8
        })
        .collect();
    let mut gappy = noise[..900_000].to_vec();
    gappy[100_000..800_000].fill(0);
    b.append_data(&mut header(Directory, 0o750, 0, 10), "etc/", &[][..]).unwrap();
    b.append_data(&mut header(Regular, 0o644, 5, 0), "etc/hostname", &b"hive\n"[..]).unwrap();
    b.append_data(&mut header(Regular, 0o4755, 1_500_000, 0), "usr/bin/big", &noise[..]).unwrap();
    b.append_data(&mut header(Regular, 0o644, 1_500_000, 0), "usr/bin/same", &noise[..]).unwrap();
    b.append_data(&mut header(Regular, 0o644, 900_000, 0), "usr/gappy", &gappy[..]).unwrap();
    b.append_data(&mut header(Regular, 0o600, 0, 3), "empty", &[][..]).unwrap();
    b.append_link(&mut header(Symlink, 0o777, 0, 0), "usr/bin/sh", "big").unwrap();
    b.append_link(&mut header(Symlink, 0o777, 0, 0), "usr/long", "y".repeat(3000)).unwrap();
    b.append_link(&mut header(Link, 0o644, 0, 0), "usr/bin/hard", "usr/bin/big").unwrap();
    let mut dev = header(Char, 0o666, 0, 0);
    dev.set_device_major(1).unwrap();
    dev.set_device_minor(3).unwrap();
    b.append_data(&mut dev, "dev/null", &[][..]).unwrap();
    let mut dev = header(Block, 0o660, 0, 0);
    dev.set_device_major(259).unwrap();
    dev.set_device_minor(70_000).unwrap();
    b.append_data(&mut dev, "dev/disk", &[][..]).unwrap();
    b.append_data(&mut header(Fifo, 0o644, 0, 0), "dev/fifo", &[][..]).unwrap();
    b.append_data(&mut header(Regular, 0, 0, 0), "opt/.wh.gone", &[][..]).unwrap();
    b.append_data(&mut header(Regular, 0, 0, 0), "srv/.wh..wh..opq", &[][..]).unwrap();
    // A PAX header with a name, as Go writes them. `mkfs.erofs` 1.7 takes a header with no name,
    // which is what `append_pax_extensions` writes, for the end of the tar.
    let records: &[(&str, &[u8])] = &[
        // A version 2 capability set with cap_net_bind_service permitted and effective.
        (
            "SCHILY.xattr.security.capability",
            &[1, 0, 0, 2, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        ),
        ("SCHILY.xattr.user.note", b"hello"),
        ("mtime", b"1700000000.123456789"),
    ];
    let mut body = Vec::new();
    for (key, value) in records {
        let rest = key.len() + value.len() + 3;
        let mut len = rest + 1;
        while len.to_string().len() + rest != len {
            len += 1;
        }
        body.extend_from_slice(format!("{len} {key}=").as_bytes());
        body.extend_from_slice(value);
        body.push(b'\n');
    }
    let mut pax = header(XHeader, 0o644, body.len() as u64, 0);
    b.append_data(&mut pax, "PaxHeaders/srv/caps", &body[..]).unwrap();
    b.append_data(&mut header(Regular, 0o755, 1, 0), "srv/caps", &b"c"[..]).unwrap();
    for k in 0..600 {
        let name = format!("many/a-file-with-a-rather-long-name-{k:04}");
        b.append_data(&mut header(Regular, 0o644, 4, 0), name, &b"many"[..]).unwrap();
    }
    b.append_data(&mut header(Regular, 0o644, 3, 0), "etc/hostname", &b"new"[..]).unwrap();
    b.finish().unwrap();
}

#[test]
fn the_writer_builds_what_mkfs_erofs_builds() {
    let Some(program) = std::env::var_os("HIVE_MKFS_EROFS") else {
        eprintln!("skipped: set HIVE_MKFS_EROFS to run it");
        return;
    };
    if !rustix::process::geteuid().is_root() {
        eprintln!("skipped: needs root");
        return;
    }
    let fsck = Path::new(&program).with_file_name("fsck.erofs");
    let dir = std::env::temp_dir().join(format!("hive-nectar-writer-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut tars = vec![dir.join("sample.tar")];
    sample(&tars[0]);
    if let Some(more) = std::env::var_os("HIVE_EROFS_TARS") {
        tars.extend(std::env::split_paths(&more));
    }
    let mkfs = Mkfs::new(&program, DEFAULT_CHUNK_SIZE).unwrap();
    let writer = Writer::new(DEFAULT_CHUNK_SIZE).unwrap();
    let (mut ours, mut theirs) = (Duration::ZERO, Duration::ZERO);
    for (i, tar) in tars.iter().enumerate() {
        let (a, b) = (dir.join(format!("w{i}")), dir.join(format!("m{i}")));
        std::fs::create_dir_all(&a).unwrap();
        std::fs::create_dir_all(&b).unwrap();
        let t = Instant::now();
        let built = writer.build(BufReader::new(File::open(tar).unwrap()), &a).unwrap();
        let took = t.elapsed();
        ours += took;
        let t = Instant::now();
        let made = mkfs.build(BufReader::new(File::open(tar).unwrap()), &b).unwrap();
        let mtook = t.elapsed();
        theirs += mtook;
        let size = |b: &Built| {
            std::fs::metadata(&b.meta).unwrap().len() + std::fs::metadata(&b.data).unwrap().len()
        };
        println!(
            "{}: the writer took {took:.2?} for {} bytes, mkfs.erofs {mtook:.2?} for {} bytes",
            tar.display(),
            size(&built),
            size(&made)
        );
        if fsck.exists() {
            let out = Command::new(&fsck)
                .arg(format!("--device={}", built.data.display()))
                .arg(&built.meta)
                .output()
                .unwrap();
            assert!(out.status.success(), "fsck.erofs: {}", String::from_utf8_lossy(&out.stderr));
        }
        let (got, want) = (tree(&built, &a.join("mnt")), tree(&made, &b.join("mnt")));
        let paths = |t: &BTreeMap<PathBuf, Shown>| t.keys().cloned().collect::<Vec<_>>();
        assert_eq!(paths(&got), paths(&want), "{}", tar.display());
        for (path, shown) in &want {
            let mut got = got[path].clone();
            // `mkfs.erofs` 1.7 drops the nanoseconds of a PAX mtime, and the writer keeps them.
            if shown.mtime.1 == 0 {
                got.mtime.1 = 0;
            }
            assert_eq!(&got, shown, "{} in {}", path.display(), tar.display());
        }
        if i == 0 {
            assert_eq!(got[Path::new("srv/caps")].mtime, (1_700_000_000, 123_456_789));
        }
        println!("{} paths alike", want.len());
        let _ = std::fs::remove_dir_all(&a);
        let _ = std::fs::remove_dir_all(&b);
    }
    println!("in all, the writer took {ours:.2?} and mkfs.erofs {theirs:.2?}");
    let _ = std::fs::remove_dir_all(&dir);
}
