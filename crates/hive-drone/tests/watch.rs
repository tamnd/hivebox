//! Directory watches, end to end over an in-memory pipe, on real directories.

#![cfg(target_os = "linux")]

use hive_drone::{Client, Config, Drone, Watcher};
use hive_proto::drone::api::{FileKind, FsEvent, FsEventKind, FsPath};
use hive_types::{Error, Reason};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

/// A fresh directory that is removed afterwards.
struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        static N: AtomicU32 = AtomicU32::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("hive-watch-{}-{n}", std::process::id()));
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

async fn watch(c: &Client, path: &str, recursive: bool) -> Watcher {
    c.fs_watch(&FsPath { path: path.into(), recursive, ..FsPath::default() }).await.unwrap()
}

fn has(events: &[FsEvent], kind: FsEventKind, path: &str) -> bool {
    events.iter().any(|e| e.kind == kind as i32 && e.path == path)
}

// Collects events until `done` holds for everything seen so far, or fails after five seconds.
async fn until(w: &mut Watcher, done: impl Fn(&[FsEvent]) -> bool) -> Vec<FsEvent> {
    let mut seen = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !done(&seen) {
        let left = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(left, w.next()).await {
            Ok(Ok(Some(batch))) => seen.extend(batch.events),
            Ok(Ok(None)) => panic!("the watch ended, after {seen:?}"),
            Ok(Err(e)) => panic!("the watch failed with {e}, after {seen:?}"),
            Err(_) => panic!("no match in time, saw {seen:?}"),
        }
    }
    seen
}

// Waits a little and returns whatever came in that time.
async fn quiet(w: &mut Watcher) -> Vec<FsEvent> {
    let mut seen = Vec::new();
    while let Ok(Ok(Some(batch))) = tokio::time::timeout(Duration::from_millis(300), w.next()).await
    {
        seen.extend(batch.events);
    }
    seen
}

#[tokio::test]
async fn every_kind_of_change_is_seen() {
    let s = Scratch::new();
    let c = client().await;
    let mut w = watch(&c, s.0.to_str().unwrap(), false).await;
    assert_eq!(w.info().kind, FileKind::Dir as i32);
    let (a, b) = (s.path("a"), s.path("b"));
    std::fs::write(&a, "1").unwrap();
    std::fs::write(&a, "22").unwrap();
    std::fs::set_permissions(&a, std::fs::Permissions::from_mode(0o600)).unwrap();
    std::fs::rename(&a, &b).unwrap();
    std::fs::remove_file(&b).unwrap();
    std::fs::create_dir(s.0.join("d")).unwrap();
    let seen = until(&mut w, |e| {
        has(e, FsEventKind::Remove, &b) && e.iter().any(|x| x.path.ends_with("/d"))
    })
    .await;
    use FsEventKind::{Chmod, Create, Rename, Write};
    let want = [
        (Create, &a),
        (Write, &a),
        (Chmod, &a),
        (Rename, &a),
        (Create, &b),
        (FsEventKind::Remove, &b),
    ];
    let got: Vec<_> = seen.iter().filter(|e| !e.path.ends_with("/d")).collect();
    let mut at = 0;
    for (kind, path) in want {
        let found = got[at..].iter().position(|e| e.kind == kind as i32 && &e.path == path);
        at += found.unwrap_or_else(|| panic!("no {kind:?} {path} in order in {seen:?}")) + 1;
    }
    let d = seen.iter().find(|e| e.path.ends_with("/d")).unwrap();
    assert!(d.is_dir && d.kind == Create as i32);
}

#[tokio::test]
async fn a_plain_watch_stays_on_its_own_directory() {
    let s = Scratch::new();
    std::fs::create_dir(s.0.join("sub")).unwrap();
    let c = client().await;
    let mut w = watch(&c, s.0.to_str().unwrap(), false).await;
    std::fs::write(s.0.join("sub/inner"), "x").unwrap();
    std::fs::write(s.0.join("outer"), "x").unwrap();
    let seen = until(&mut w, |e| has(e, FsEventKind::Create, &s.path("outer"))).await;
    assert!(!seen.iter().any(|e| e.path.contains("inner")), "{seen:?}");
}

#[tokio::test]
async fn a_recursive_watch_follows_new_directories() {
    let s = Scratch::new();
    std::fs::create_dir(s.0.join("old")).unwrap();
    let c = client().await;
    let mut w = watch(&c, s.0.to_str().unwrap(), true).await;
    // Made in one go, faster than the watch on each new level can be added.
    std::fs::create_dir_all(s.0.join("a/b/c")).unwrap();
    std::fs::write(s.0.join("a/b/c/f"), "x").unwrap();
    std::fs::write(s.0.join("old/g"), "x").unwrap();
    let f = s.path("a/b/c/f");
    let g = s.path("old/g");
    until(&mut w, |e| has(e, FsEventKind::Create, &f) && has(e, FsEventKind::Create, &g)).await;
    // Watches are now in place all the way down.
    std::fs::write(&f, "more").unwrap();
    until(&mut w, |e| has(e, FsEventKind::Write, &f)).await;
}

#[tokio::test]
async fn moved_directories_are_tracked() {
    let s = Scratch::new();
    let outside = Scratch::new();
    std::fs::create_dir_all(s.0.join("here/deep")).unwrap();
    std::fs::create_dir_all(outside.0.join("incoming/deep")).unwrap();
    let c = client().await;
    let mut w = watch(&c, s.0.to_str().unwrap(), true).await;

    // A directory moved away is no longer watched.
    let away = outside.0.join("away");
    std::fs::rename(s.0.join("here"), &away).unwrap();
    until(&mut w, |e| has(e, FsEventKind::Rename, &s.path("here"))).await;
    std::fs::write(away.join("deep/ghost"), "x").unwrap();
    // A directory moved in is watched from then on, all the way down.
    std::fs::rename(outside.0.join("incoming"), s.0.join("arrived")).unwrap();
    until(&mut w, |e| has(e, FsEventKind::Create, &s.path("arrived"))).await;
    std::fs::write(s.0.join("arrived/deep/new"), "x").unwrap();
    let seen = until(&mut w, |e| has(e, FsEventKind::Create, &s.path("arrived/deep/new"))).await;
    assert!(!seen.iter().any(|e| e.path.contains("ghost")), "{seen:?}");
    let seen = quiet(&mut w).await;
    assert!(!seen.iter().any(|e| e.path.contains("ghost")), "{seen:?}");
}

#[tokio::test]
async fn the_watch_ends_when_its_directory_goes() {
    let s = Scratch::new();
    let dir = s.0.join("gone");
    std::fs::create_dir_all(dir.join("sub")).unwrap();
    let c = client().await;
    let mut w = watch(&c, dir.to_str().unwrap(), true).await;
    std::fs::remove_dir_all(&dir).unwrap();
    let mut seen = Vec::new();
    let ended = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(batch) = w.next().await.unwrap() {
            seen.extend(batch.events);
        }
    })
    .await;
    assert!(ended.is_ok(), "still running after {seen:?}");
    assert!(has(&seen, FsEventKind::Remove, dir.to_str().unwrap()), "{seen:?}");
}

#[tokio::test]
async fn closing_a_watch_is_clean() {
    let s = Scratch::new();
    let c = client().await;
    let w = watch(&c, s.0.to_str().unwrap(), true).await;
    w.close().await.unwrap();
    // Plenty of watches one after another, so none of them leak.
    for _ in 0..200 {
        let w = watch(&c, s.0.to_str().unwrap(), false).await;
        drop(w);
    }
    let w = watch(&c, s.0.to_str().unwrap(), false).await;
    w.close().await.unwrap();
}

fn errno(e: &Error) -> &str {
    assert_eq!(e.reason, Reason::FileError, "{e}");
    e.errno.as_deref().unwrap_or("")
}

#[tokio::test]
async fn only_directories_inside_the_roots_can_be_watched() {
    let s = Scratch::new();
    std::fs::write(s.0.join("file"), "x").unwrap();
    let c = client_with(Config { roots: vec![s.0.clone()], ..Config::default() }).await;
    let at = |p: &str| FsPath { path: p.into(), ..FsPath::default() };
    let e = c.fs_watch(&at(&s.path("file"))).await.unwrap_err();
    assert_eq!(errno(&e), "ENOTDIR");
    let e = c.fs_watch(&at(&s.path("missing"))).await.unwrap_err();
    assert_eq!(errno(&e), "ENOENT");
    let e = c.fs_watch(&at("/etc")).await.unwrap_err();
    assert_eq!(e.reason, Reason::PolicyDenied);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, not a test"]
async fn latency() {
    let s = Scratch::new();
    let (a, b) = tokio::net::UnixStream::pair().unwrap();
    tokio::spawn(Drone::new(Config::default(), [7; 32]).serve(a));
    let c = Client::connect(b, &[7; 32], 0, [1; 32]).await.unwrap();
    let mut w = watch(&c, s.0.to_str().unwrap(), true).await;
    let n = 2000;
    let mut lat = Vec::with_capacity(n);
    for i in 0..n {
        let p = s.path(&format!("f{i}"));
        let t = Instant::now();
        std::fs::write(&p, "x").unwrap();
        until(&mut w, |e| has(e, FsEventKind::Create, &p)).await;
        lat.push(t.elapsed());
    }
    lat.sort_unstable();
    println!("watch, write to event: p50 {:?}, p99 {:?}", lat[n / 2], lat[n * 99 / 100]);

    // A burst of changes, to see how many arrive and how fast.
    let burst = 50_000;
    let dir = s.0.join("burst");
    std::fs::create_dir(&dir).unwrap();
    let last = dir.join(format!("f{}", burst - 1)).to_str().unwrap().to_string();
    let t = Instant::now();
    let writer = std::thread::spawn(move || {
        for i in 0..burst {
            std::fs::write(dir.join(format!("f{i}")), "x").unwrap();
        }
    });
    let mut seen = 0usize;
    let mut overflowed = false;
    // Ends at the last file's write, or when nothing more comes after an overflow lost it.
    while let Ok(batch) = tokio::time::timeout(Duration::from_secs(2), w.next()).await {
        let batch = batch.unwrap().unwrap();
        seen += batch.events.len();
        overflowed |= batch.events.iter().any(|e| e.kind == FsEventKind::Overflow as i32);
        if batch.events.iter().any(|e| e.path == last && e.kind == FsEventKind::Write as i32) {
            break;
        }
    }
    writer.join().unwrap();
    println!(
        "watch, burst of {burst} new files: {seen} events in {:?}, overflow {overflowed}",
        t.elapsed()
    );
}
