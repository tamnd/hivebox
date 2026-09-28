//! Tar upload and download, end to end over an in-memory pipe, on real directories.

#![cfg(target_os = "linux")]

use bytes::Bytes;
use hive_drone::{Client, Config, Drone};
use hive_proto::drone::api::{FsPath, FsUpload};
use hive_types::{Error, Reason};
use std::collections::BTreeMap;
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

/// A fresh directory that is removed afterwards.
struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        static N: AtomicU32 = AtomicU32::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("hive-tar-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    fn path(&self, rel: &str) -> String {
        self.0.join(rel).to_str().unwrap().to_string()
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        // Read-only directories from an archive would stop the removal.
        let _ = Command::new("chmod").arg("-R").arg("u+rwx").arg(&self.0).status();
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn client_with(cfg: Config) -> Client {
    let (a, b) = tokio::io::duplex(1 << 20);
    tokio::spawn(Drone::new(cfg, [7; 32]).serve(a));
    Client::connect(b, &[7; 32], 0, [1; 32]).await.unwrap()
}

async fn client() -> Client {
    client_with(Config::default()).await
}

fn into(path: &str) -> FsUpload {
    FsUpload { path: path.into(), ..FsUpload::default() }
}

fn at(path: &str) -> FsPath {
    FsPath { path: path.into(), ..FsPath::default() }
}

fn errno(e: &Error) -> &str {
    assert_eq!(e.reason, Reason::FileError, "{e}");
    e.errno.as_deref().unwrap_or("")
}

fn pattern(n: usize) -> Vec<u8> {
    (0..n).map(|i| (i * 31 + i / 4099) as u8).collect()
}

// Everything about a tree that should survive a round trip, by relative path.
fn snapshot(root: &Path) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let mut todo = vec![PathBuf::new()];
    while let Some(rel) = todo.pop() {
        for entry in std::fs::read_dir(root.join(&rel)).unwrap() {
            let entry = entry.unwrap();
            let rel = rel.join(entry.file_name());
            let meta = std::fs::symlink_metadata(root.join(&rel)).unwrap();
            let mode = meta.mode() & 0o7777;
            let what = if meta.file_type().is_symlink() {
                let t = std::fs::read_link(root.join(&rel)).unwrap();
                format!("link {}", t.display())
            } else if meta.is_dir() {
                todo.push(rel.clone());
                format!("dir {mode:o}")
            } else {
                let data = std::fs::read(root.join(&rel)).unwrap();
                let sum = data.iter().fold(0u64, |a, &b| a.wrapping_mul(31).wrapping_add(b.into()));
                format!("file {mode:o} {} {sum:x} {}", data.len(), meta.mtime())
            };
            out.insert(rel.display().to_string(), what);
        }
    }
    out
}

// A tree with one of everything an archive carries.
fn sample(root: &Path) {
    std::fs::create_dir_all(root.join("src/deep/er")).unwrap();
    std::fs::write(root.join("README"), "hello\n").unwrap();
    std::fs::write(root.join("empty"), "").unwrap();
    std::fs::write(root.join("src/big.bin"), pattern(3 << 20)).unwrap();
    std::fs::write(root.join("src/deep/er/x"), "x").unwrap();
    let run = root.join("run.sh");
    std::fs::write(&run, "#!/bin/sh\necho hi\n").unwrap();
    std::fs::set_permissions(&run, std::fs::Permissions::from_mode(0o4755)).unwrap();
    let long = "n".repeat(120);
    std::fs::create_dir(root.join(&long)).unwrap();
    std::fs::write(root.join(&long).join("m".repeat(150)), "long names").unwrap();
    symlink("src/big.bin", root.join("link")).unwrap();
    symlink("/nowhere/at/all", root.join("dangling")).unwrap();
    std::fs::hard_link(root.join("README"), root.join("README.hard")).unwrap();
    let locked = root.join("locked");
    std::fs::create_dir(&locked).unwrap();
    std::fs::write(locked.join("inside"), "in").unwrap();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o555)).unwrap();
}

#[tokio::test]
async fn trees_round_trip() {
    let s = Scratch::new();
    let c = client().await;
    let src = s.0.join("src");
    sample(&src);
    let tar = c.fs_download(&at(src.to_str().unwrap()), 1 << 30).await.unwrap();
    let out = s.path("out");
    let done =
        c.fs_upload(&FsUpload { make_parents: true, ..into(&out) }, tar.clone()).await.unwrap();
    assert_eq!(snapshot(&src), snapshot(Path::new(&out)));
    assert_eq!(done.skipped, 0);
    // Eight files, two symlinks and five directories, with the hard link packed as a file.
    assert_eq!(done.entries, 15);
    assert_eq!(done.bytes, (3 << 20) + 6 + 6 + 1 + 18 + 10 + 2);
    // The same tree packs to the same bytes.
    let again = c.fs_download(&at(src.to_str().unwrap()), 1 << 30).await.unwrap();
    assert_eq!(tar, again);
}

#[tokio::test]
async fn system_tar_reads_and_writes_the_same_archives() {
    let s = Scratch::new();
    let c = client().await;
    let src = s.0.join("src");
    sample(&src);
    // What GNU tar makes, with its "./" names, unpacks the same.
    let made = Command::new("tar").arg("-C").arg(&src).args(["-cf", "-", "."]).output().unwrap();
    assert!(made.status.success());
    let out = s.path("from-tar");
    c.fs_upload(&FsUpload { make_parents: true, ..into(&out) }, made.stdout.into()).await.unwrap();
    assert_eq!(snapshot(&src), snapshot(Path::new(&out)));
    // And what the drone makes, GNU tar unpacks the same.
    let tar = c.fs_download(&at(src.to_str().unwrap()), 1 << 30).await.unwrap();
    let back = s.0.join("by-tar");
    std::fs::create_dir(&back).unwrap();
    let mut x = Command::new("tar")
        .arg("-C")
        .arg(&back)
        .args(["-xpf", "-"])
        .stdin(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    std::io::Write::write_all(x.stdin.as_mut().unwrap(), &tar).unwrap();
    drop(x.stdin.take());
    assert!(x.wait().unwrap().success());
    assert_eq!(snapshot(&src), snapshot(&back));
}

#[tokio::test]
async fn one_file_or_link_goes_in_under_its_name() {
    let s = Scratch::new();
    let c = client().await;
    std::fs::write(s.0.join("a.txt"), "aaa").unwrap();
    symlink("a.txt", s.0.join("l")).unwrap();
    for (name, want) in [("a.txt", tar::EntryType::Regular), ("l", tar::EntryType::Symlink)] {
        let tar = c.fs_download(&at(&s.path(name)), 1 << 20).await.unwrap();
        let mut a = tar::Archive::new(&tar[..]);
        let entries: Vec<_> = a
            .entries()
            .unwrap()
            .map(|e| {
                let e = e.unwrap();
                (e.path().unwrap().display().to_string(), e.header().entry_type())
            })
            .collect();
        assert_eq!(entries, [(name.to_string(), want)]);
    }
}

// An archive entry with any name at all, which the tar crate's own builder would refuse.
fn raw(tar: &mut tar::Builder<Vec<u8>>, name: &str, ty: tar::EntryType, link: &str, data: &[u8]) {
    let mut h = tar::Header::new_old();
    h.as_old_mut().name[..name.len()].copy_from_slice(name.as_bytes());
    h.as_old_mut().linkname[..link.len()].copy_from_slice(link.as_bytes());
    h.set_entry_type(ty);
    h.set_mode(0o644);
    h.set_size(data.len() as u64);
    h.set_cksum();
    tar.append(&h, data).unwrap();
}

#[tokio::test]
async fn hostile_archives_stay_in_the_destination() {
    let s = Scratch::new();
    let c = client().await;
    let dest = s.0.join("dest");
    std::fs::create_dir(&dest).unwrap();
    let outside = s.0.join("outside");
    std::fs::write(&outside, "keep").unwrap();
    // A symlink already in the destination, pointing out of it.
    symlink(&outside, dest.join("pre")).unwrap();

    use tar::EntryType as T;
    let mut b = tar::Builder::new(Vec::new());
    raw(&mut b, "../escaped", T::Regular, "", b"x");
    raw(&mut b, "a/../../escaped2", T::Regular, "", b"x");
    raw(&mut b, "/abs/file", T::Regular, "", b"abs");
    raw(&mut b, "root", T::Symlink, "/", b"");
    raw(&mut b, "root/through-root", T::Regular, "", b"t");
    raw(&mut b, "up", T::Symlink, "../../../../..", b"");
    raw(&mut b, "up/through-up", T::Regular, "", b"u");
    raw(&mut b, "pre", T::Regular, "", b"replaced");
    raw(&mut b, "hard", T::Link, "../outside", b"");
    raw(&mut b, "dev", T::Char, "", b"");
    raw(&mut b, "fifo", T::Fifo, "", b"");
    let tar = b.into_inner().unwrap();

    let done = c.fs_upload(&into(dest.to_str().unwrap()), tar.into()).await.unwrap();
    assert_eq!(done.skipped, 5, "the two climbing names, the climbing link, a device, a pipe");
    assert_eq!(std::fs::read(&outside).unwrap(), b"keep");
    assert!(!s.0.join("escaped").exists() && !s.0.join("escaped2").exists());
    assert_eq!(std::fs::read(dest.join("abs/file")).unwrap(), b"abs");
    // Both links lead to the destination itself, so that is where the files went.
    assert_eq!(std::fs::read(dest.join("through-root")).unwrap(), b"t");
    assert_eq!(std::fs::read(dest.join("through-up")).unwrap(), b"u");
    let pre = std::fs::symlink_metadata(dest.join("pre")).unwrap();
    assert!(pre.is_file());
    assert_eq!(std::fs::read(dest.join("pre")).unwrap(), b"replaced");
}

#[tokio::test]
async fn bad_uploads_fail_cleanly() {
    let s = Scratch::new();
    let c = client().await;
    let missing = s.path("missing");
    let e = c.fs_upload(&into(&missing), Bytes::new()).await.unwrap_err();
    assert_eq!(errno(&e), "ENOENT");

    let dest = s.path("d");
    std::fs::create_dir(&dest).unwrap();
    let e = c.fs_upload(&into(&dest), Bytes::from(vec![0x55; 4096])).await.unwrap_err();
    assert_eq!(e.reason, Reason::InvalidArgument, "{e}");

    // A directory where the archive has a file.
    std::fs::create_dir(s.0.join("d/x")).unwrap();
    let mut b = tar::Builder::new(Vec::new());
    raw(&mut b, "x", tar::EntryType::Regular, "", b"x");
    let e = c.fs_upload(&into(&dest), b.into_inner().unwrap().into()).await.unwrap_err();
    assert_eq!(errno(&e), "EISDIR");

    // An upload dropped halfway leaves the channel working.
    let mut w = c.fs_upload_start(&into(&dest)).await.unwrap();
    w.write(Bytes::from(vec![0; 700])).await.unwrap();
    drop(w);
    let e = c.fs_download(&at(&missing), 1 << 20).await.unwrap_err();
    assert_eq!(errno(&e), "ENOENT");

    let jailed = client_with(Config { roots: vec![s.0.join("d")], ..Config::default() }).await;
    let e = jailed.fs_upload(&into("/etc"), Bytes::new()).await.unwrap_err();
    assert_eq!(e.reason, Reason::PolicyDenied);
    let e = jailed.fs_download(&at("/etc"), 1 << 20).await.unwrap_err();
    assert_eq!(e.reason, Reason::PolicyDenied);
}

#[tokio::test]
async fn big_archives_stream_both_ways() {
    let s = Scratch::new();
    let c = client().await;
    let src = s.0.join("src");
    std::fs::create_dir(&src).unwrap();
    for i in 0..200 {
        std::fs::write(src.join(format!("f{i:03}")), pattern(4096 + i * 997)).unwrap();
    }
    std::fs::write(src.join("large"), pattern(40 << 20)).unwrap();
    let mut r = c.fs_download_start(&at(src.to_str().unwrap())).await.unwrap();
    let out = s.path("out");
    let mut w = c.fs_upload_start(&FsUpload { make_parents: true, ..into(&out) }).await.unwrap();
    while let Some(chunk) = r.next().await.unwrap() {
        w.write(chunk).await.unwrap();
    }
    let done = w.finish().await.unwrap();
    assert_eq!(done.entries, 201);
    assert_eq!(snapshot(&src), snapshot(Path::new(&out)));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, not a test"]
async fn throughput() {
    let s = Scratch::new();
    let (a, b) = tokio::net::UnixStream::pair().unwrap();
    tokio::spawn(Drone::new(Config::default(), [7; 32]).serve(a));
    let c = Client::connect(b, &[7; 32], 0, [1; 32]).await.unwrap();
    for (what, files, size) in
        [("10,000 files of 4 KiB", 10_000, 4096), ("one 512 MiB file", 1, 512 << 20)]
    {
        let src = s.0.join(format!("src-{files}"));
        std::fs::create_dir(&src).unwrap();
        let data = pattern(size);
        for i in 0..files {
            std::fs::write(src.join(format!("f{i:05}")), &data).unwrap();
        }
        let mib = (files * size) as f64 / f64::from(1 << 20);
        let t = Instant::now();
        let tar = c.fs_download(&at(src.to_str().unwrap()), 1 << 30).await.unwrap();
        let down = t.elapsed();
        let out = s.path(&format!("out-{files}"));
        let t = Instant::now();
        let up = FsUpload { make_parents: true, ..into(&out) };
        c.fs_upload(&up, tar).await.unwrap();
        let up = t.elapsed();
        println!(
            "tar {what}: download {down:?} ({:.0} MiB/s), upload {up:?} ({:.0} MiB/s)",
            mib / down.as_secs_f64(),
            mib / up.as_secs_f64()
        );
    }
}
