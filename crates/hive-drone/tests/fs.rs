//! File operations, end to end over an in-memory pipe, on real directories.

#![cfg(target_os = "linux")]

use bytes::Bytes;
use hive_drone::{Client, Config, Drone};
use hive_proto::drone::api::{
    FileKind, FsChmod, FsList, FsMkdir, FsPath, FsRead, FsRename, FsWrite,
};
use hive_types::{Error, Reason};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

/// A fresh directory that is removed afterwards.
struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        static N: AtomicU32 = AtomicU32::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("hive-fs-{}-{n}", std::process::id()));
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

fn write(path: &str, data: impl Into<Bytes>) -> FsWrite {
    FsWrite { path: path.into(), data: data.into(), ..FsWrite::default() }
}

fn at(path: &str) -> FsPath {
    FsPath { path: path.into(), ..FsPath::default() }
}

async fn read(c: &Client, path: &str) -> Result<Bytes, Error> {
    c.fs_read(&FsRead { path: path.into(), ..FsRead::default() }, 1 << 30).await
}

fn errno(e: &Error) -> &str {
    assert_eq!(e.reason, Reason::FileError, "{e}");
    e.errno.as_deref().unwrap_or("")
}

fn pattern(n: usize) -> Vec<u8> {
    (0..n).map(|i| (i * 31 + i / 4099) as u8).collect()
}

#[tokio::test]
async fn small_files_round_trip() {
    let s = Scratch::new();
    let c = client().await;
    let p = s.path("hello.txt");
    let info = c.fs_write(&write(&p, "hi there\n")).await.unwrap();
    assert_eq!((info.path.as_str(), info.size, info.mode), (p.as_str(), 9, 0o644));
    assert_eq!(info.kind, FileKind::File as i32);
    assert_eq!(read(&c, &p).await.unwrap(), "hi there\n");
    assert_eq!(std::fs::read(&p).unwrap(), b"hi there\n");
    let empty = s.path("empty");
    c.fs_write(&write(&empty, "")).await.unwrap();
    assert_eq!(read(&c, &empty).await.unwrap(), "");
}

#[tokio::test]
async fn big_files_stream_both_ways() {
    let s = Scratch::new();
    let c = client().await;
    let p = s.path("big.bin");
    let data = pattern(20 << 20);
    let info = c.fs_write(&write(&p, data.clone())).await.unwrap();
    assert_eq!(info.size, data.len() as u64);
    assert!(std::fs::read(&p).unwrap() == data);

    let mut r = c.fs_open(&FsRead { path: p.clone(), ..FsRead::default() }).await.unwrap();
    let mut got = Vec::new();
    while let Some(chunk) = r.next().await.unwrap() {
        got.extend_from_slice(&chunk);
    }
    assert!(got == data);

    let part = FsRead { path: p.clone(), offset: 1_000_003, length: 777_777 };
    assert!(c.fs_read(&part, 1 << 30).await.unwrap()[..] == data[1_000_003..1_777_780]);
    let past = FsRead { path: p.clone(), offset: 1 << 40, length: 0 };
    assert!(c.fs_read(&past, 1 << 30).await.unwrap().is_empty());
    let e = c.fs_read(&FsRead { path: p, ..FsRead::default() }, 1000).await.unwrap_err();
    assert_eq!(e.reason, Reason::OutputLimit);
}

#[tokio::test]
async fn writes_replace_the_file_whole() {
    let s = Scratch::new();
    let c = client().await;
    let p = s.path("f");
    c.fs_write(&FsWrite { mode: 0o600, ..write(&p, "old") }).await.unwrap();
    // An existing file keeps its bits unless the write sets them.
    let info = c.fs_write(&write(&p, "newer")).await.unwrap();
    assert_eq!((info.mode, info.size), (0o600, 5));
    let info = c.fs_write(&FsWrite { mode: 0o4755, ..write(&p, "newest") }).await.unwrap();
    assert_eq!(info.mode, 0o4755);

    // A writer dropped halfway leaves the old file and no temporary file behind.
    let mut w = c.fs_create(&write(&p, "partial")).await.unwrap();
    w.write(Bytes::from(vec![b'x'; 1 << 20])).await.unwrap();
    drop(w);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(read(&c, &p).await.unwrap(), "newest");
    let names: Vec<_> = std::fs::read_dir(&s.0).unwrap().map(|e| e.unwrap().file_name()).collect();
    assert_eq!(names, ["f"]);
}

#[tokio::test]
async fn appends_and_writes_through_symlinks_change_the_file_in_place() {
    let s = Scratch::new();
    let c = client().await;
    let p = s.path("log");
    c.fs_write(&FsWrite { append: true, ..write(&p, "one\n") }).await.unwrap();
    c.fs_write(&FsWrite { append: true, ..write(&p, "two\n") }).await.unwrap();
    assert_eq!(read(&c, &p).await.unwrap(), "one\ntwo\n");
    let link = s.path("link");
    std::os::unix::fs::symlink("log", &link).unwrap();
    c.fs_write(&write(&link, "three\n")).await.unwrap();
    assert_eq!(std::fs::read(&p).unwrap(), b"three\n");
    assert!(std::fs::symlink_metadata(&link).unwrap().file_type().is_symlink());
}

#[tokio::test]
async fn parents_are_made_on_request() {
    let s = Scratch::new();
    let c = client().await;
    let deep = s.path("a/b/c/file");
    let e = c.fs_write(&write(&deep, "x")).await.unwrap_err();
    assert_eq!(errno(&e), "ENOENT");
    c.fs_write(&FsWrite { make_parents: true, ..write(&deep, "x") }).await.unwrap();
    assert_eq!(read(&c, &deep).await.unwrap(), "x");

    let dir = s.path("m/n/o");
    let mk = FsMkdir { path: dir.clone(), mode: 0o700, parents: true, ..FsMkdir::default() };
    let info = c.fs_mkdir(&mk).await.unwrap();
    assert_eq!((info.kind, info.mode), (FileKind::Dir as i32, 0o700));
    // mkdir -p on something that is there is fine, and plain mkdir is not.
    c.fs_mkdir(&mk).await.unwrap();
    let e = c.fs_mkdir(&FsMkdir { parents: false, ..mk }).await.unwrap_err();
    assert_eq!(errno(&e), "EEXIST");
}

#[tokio::test]
async fn failures_name_their_errno() {
    let s = Scratch::new();
    let c = client().await;
    assert_eq!(errno(&read(&c, &s.path("nope")).await.unwrap_err()), "ENOENT");
    assert_eq!(errno(&read(&c, &s.path("")).await.unwrap_err()), "EISDIR");
    std::fs::create_dir(s.0.join("d")).unwrap();
    std::fs::write(s.0.join("d/f"), "x").unwrap();
    let e = c.fs_write(&write(&s.path("d"), "x")).await.unwrap_err();
    assert_eq!(errno(&e), "EISDIR");
    let e = c.fs_remove(&at(&s.path("d"))).await.unwrap_err();
    assert_eq!(errno(&e), "ENOTEMPTY");
    let e = c.fs_list(&FsList { path: s.path("d/f"), depth: 1 }).await.unwrap_err();
    assert_eq!(errno(&e), "ENOTDIR");
    let e = c.fs_stat(&at(&s.path("d/f/g"))).await.unwrap_err();
    assert_eq!(errno(&e), "ENOTDIR");
    let e = c.fs_stat(&at("")).await.unwrap_err();
    assert_eq!(e.reason, Reason::InvalidArgument);
}

#[tokio::test]
async fn listings_are_sorted_and_do_not_enter_symlinks() {
    let s = Scratch::new();
    let c = client().await;
    for f in ["b/x", "b/y/z", "a"] {
        let p = s.0.join(f);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, f).unwrap();
    }
    std::os::unix::fs::symlink("b", s.0.join("l")).unwrap();
    let base = s.path("");
    let names = |r: &hive_proto::drone::api::FsListResult| {
        r.entries
            .iter()
            .map(|e| e.path.strip_prefix(&base).unwrap().to_string())
            .collect::<Vec<_>>()
    };
    let one = c.fs_list(&FsList { path: base.clone(), depth: 0 }).await.unwrap();
    assert_eq!(names(&one), ["a", "b", "l"]);
    assert!(!one.truncated);
    let l = &one.entries[2];
    assert_eq!((l.kind, l.symlink_target.as_str()), (FileKind::Symlink as i32, "b"));
    let all = c.fs_list(&FsList { path: base.clone(), depth: 5 }).await.unwrap();
    assert_eq!(names(&all), ["a", "b", "b/x", "b/y", "b/y/z", "l"]);
    assert_eq!(all.entries[4].size, 5);
}

#[tokio::test]
async fn recursive_removal_stays_off_symlinks() {
    let s = Scratch::new();
    let c = client().await;
    std::fs::create_dir_all(s.0.join("keep")).unwrap();
    std::fs::write(s.0.join("keep/precious"), "x").unwrap();
    std::fs::create_dir_all(s.0.join("tree/a/b/c")).unwrap();
    std::fs::write(s.0.join("tree/a/b/c/f"), "x").unwrap();
    std::os::unix::fs::symlink(s.0.join("keep"), s.0.join("tree/a/out")).unwrap();
    c.fs_remove(&FsPath { recursive: true, ..at(&s.path("tree")) }).await.unwrap();
    assert!(!s.0.join("tree").exists());
    assert!(s.0.join("keep/precious").exists());
    // A symlink itself is removed, never what it points to.
    std::os::unix::fs::symlink("keep", s.0.join("l")).unwrap();
    c.fs_remove(&FsPath { recursive: true, ..at(&s.path("l")) }).await.unwrap();
    assert!(s.0.join("keep/precious").exists());
    let e = c.fs_remove(&at(&s.path("l"))).await.unwrap_err();
    assert_eq!(errno(&e), "ENOENT");
}

#[tokio::test]
async fn renames_and_permission_changes() {
    let s = Scratch::new();
    let c = client().await;
    let (a, b) = (s.path("a"), s.path("b"));
    c.fs_write(&write(&a, "A")).await.unwrap();
    c.fs_write(&write(&b, "B")).await.unwrap();
    let e = c.fs_rename(&FsRename { from: a.clone(), to: b.clone(), overwrite: false }).await;
    assert_eq!(errno(&e.unwrap_err()), "EEXIST");
    let info =
        c.fs_rename(&FsRename { from: a.clone(), to: b.clone(), overwrite: true }).await.unwrap();
    assert_eq!((info.path.as_str(), info.size), (b.as_str(), 1));
    assert_eq!(read(&c, &b).await.unwrap(), "A");
    assert_eq!(errno(&c.fs_stat(&at(&a)).await.unwrap_err()), "ENOENT");

    let info = c.fs_chmod(&FsChmod { path: b.clone(), mode: 0o751 }).await.unwrap();
    assert_eq!(info.mode, 0o751);
    // Links have no bits of their own, so a chmod goes to the target.
    std::os::unix::fs::symlink("b", s.0.join("l")).unwrap();
    c.fs_chmod(&FsChmod { path: s.path("l"), mode: 0o600 }).await.unwrap();
    assert_eq!(c.fs_stat(&at(&b)).await.unwrap().mode, 0o600);
    let link = c.fs_stat(&at(&s.path("l"))).await.unwrap();
    assert_eq!((link.kind, link.symlink_target.as_str()), (FileKind::Symlink as i32, "b"));
    let through = c.fs_stat(&FsPath { follow: true, ..at(&s.path("l")) }).await.unwrap();
    assert_eq!((through.kind, through.mode), (FileKind::File as i32, 0o600));
}

#[tokio::test]
async fn paths_never_leave_the_roots() {
    let s = Scratch::new();
    let root = s.0.join("root");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(s.0.join("secret"), "outside").unwrap();
    std::os::unix::fs::symlink(&s.0, root.join("abs")).unwrap();
    std::os::unix::fs::symlink("../..", root.join("up")).unwrap();
    std::os::unix::fs::symlink("/proc/self/root", root.join("magic")).unwrap();
    let cfg = Config { roots: vec![root.clone()], workdir: root.clone(), ..Config::default() };
    let c = client_with(cfg).await;

    let e = read(&c, &s.path("secret")).await.unwrap_err();
    assert_eq!(e.reason, Reason::PolicyDenied);
    // Each of these would reach the secret if the links were followed as the kernel normally
    // does. Inside the root they lead nowhere.
    let abs = format!("abs{}", s.path("secret"));
    for p in ["up/secret", "../secret", "abs/secret", abs.as_str(), "magic/etc/hostname"] {
        let e = read(&c, p).await.unwrap_err();
        assert_eq!(e.reason, Reason::FileError, "{p}: {e}");
        assert!(matches!(e.errno.as_deref(), Some("ENOENT" | "ELOOP" | "EXDEV")), "{p}: {e}");
    }
    // Writing through the same link makes a new file inside the root instead.
    c.fs_write(&write("up/secret", "gotcha")).await.unwrap();
    assert_eq!(std::fs::read(root.join("secret")).unwrap(), b"gotcha");
    // The link dangles inside the root, so there is nothing to make parents under.
    let e = c.fs_write(&FsWrite { make_parents: true, ..write("abs/x/y", "z") }).await;
    assert_eq!(e.unwrap_err().reason, Reason::FileError);
    assert_eq!(std::fs::read(s.0.join("secret")).unwrap(), b"outside");

    // Relative paths start at the working directory, and links inside the root still work.
    std::fs::write(root.join("inside"), "in").unwrap();
    std::os::unix::fs::symlink("/inside", root.join("abs-in")).unwrap();
    assert_eq!(read(&c, "inside").await.unwrap(), "in");
    assert_eq!(read(&c, "abs-in").await.unwrap(), "in");
    assert_eq!(read(&c, "up/inside").await.unwrap(), "in");
    let e = c.fs_remove(&at(root.to_str().unwrap())).await.unwrap_err();
    assert_eq!(e.reason, Reason::InvalidArgument);
}

#[tokio::test]
async fn many_files_at_once() {
    let s = Scratch::new();
    let c = client().await;
    let mut tasks = Vec::new();
    for i in 0..200 {
        let (c, p) = (c.clone(), s.path(&format!("f{i}")));
        tasks.push(tokio::spawn(async move {
            let data = pattern(1000 + i * 997);
            c.fs_write(&write(&p, data.clone())).await.unwrap();
            assert!(read(&c, &p).await.unwrap()[..] == data[..]);
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let all = c.fs_list(&FsList { path: s.path(""), depth: 1 }).await.unwrap();
    assert_eq!(all.entries.len(), 200);
}

/// `cargo test --release -p hive-drone --test fs -- --ignored --nocapture latency`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, not a test"]
async fn latency() {
    let s = Scratch::new();
    let (a, b) = tokio::net::UnixStream::pair().unwrap();
    tokio::spawn(Drone::new(Config::default(), [7; 32]).serve(a));
    let c = Client::connect(b, &[7; 32], 0, [1; 32]).await.unwrap();
    let small = s.path("small");
    let four_k = Bytes::from(pattern(4096));
    c.fs_write(&write(&small, four_k.clone())).await.unwrap();

    let n = 5000;
    for what in ["stat", "read 4 KiB", "write 4 KiB"] {
        let mut lat = Vec::with_capacity(n);
        let t = Instant::now();
        for _ in 0..n {
            let one = Instant::now();
            match what {
                "stat" => drop(c.fs_stat(&at(&small)).await.unwrap()),
                "read 4 KiB" => drop(read(&c, &small).await.unwrap()),
                _ => drop(c.fs_write(&write(&small, four_k.clone())).await.unwrap()),
            }
            lat.push(one.elapsed());
        }
        let secs = t.elapsed().as_secs_f64();
        lat.sort_unstable();
        println!(
            "fs {what}: p50 {:?}, p99 {:?}, {:.0} per second",
            lat[n / 2],
            lat[n * 99 / 100],
            n as f64 / secs
        );
    }

    let big = s.path("big");
    let size = 512usize << 20;
    let data = Bytes::from(pattern(size));
    let t = Instant::now();
    c.fs_write(&write(&big, data)).await.unwrap();
    let mib = (size >> 20) as f64;
    println!("fs write 512 MiB: {:.0} MiB/s", mib / t.elapsed().as_secs_f64());
    let t = Instant::now();
    let mut r = c.fs_open(&FsRead { path: big.clone(), ..FsRead::default() }).await.unwrap();
    let mut got = 0;
    while let Some(chunk) = r.next().await.unwrap() {
        got += chunk.len();
    }
    assert_eq!(got, size);
    println!("fs read 512 MiB: {:.0} MiB/s", mib / t.elapsed().as_secs_f64());

    let dir = s.0.join("many");
    std::fs::create_dir(&dir).unwrap();
    for i in 0..10_000 {
        std::fs::write(dir.join(format!("file-{i:05}")), "").unwrap();
    }
    let t = Instant::now();
    let r = c.fs_list(&FsList { path: dir.to_str().unwrap().into(), depth: 1 }).await.unwrap();
    assert_eq!(r.entries.len(), 10_000);
    println!("fs list 10,000 entries: {:?}", t.elapsed());
    let t = Instant::now();
    c.fs_remove(&FsPath { recursive: true, ..at(dir.to_str().unwrap()) }).await.unwrap();
    println!("fs remove 10,000 entries: {:?}", t.elapsed());
    assert!(!Path::new(&dir).exists());
}
