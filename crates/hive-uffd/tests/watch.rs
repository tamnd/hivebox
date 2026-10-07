//! The watchdog: a worker killed while a VM touches its memory is replaced, and the VM carries on
//! with the right pages. It needs a userfaultfd, which root always has, and passes without doing
//! anything otherwise.

#![cfg(target_os = "linux")]

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use hive_uffd::Guest;

const PAGE: usize = 4096;
const PAGES: usize = 8192;

fn expect(p: usize) -> u8 {
    (p % 251 + 1) as u8
}

/// Whether this process may make a userfaultfd: root may, and others only where the sysctl
/// allows it.
fn allowed() -> bool {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let root =
        status.lines().any(|l| l.starts_with("Uid:") && l.split_whitespace().nth(1) == Some("0"));
    let open = std::fs::read_to_string("/proc/sys/vm/unprivileged_userfaultfd")
        .is_ok_and(|v| v.trim() == "1");
    root || open
}

/// The one child of `pid`, once it has one that is not `not`.
fn worker(pid: u32, not: Option<u32>) -> u32 {
    let path = format!("/proc/{pid}/task/{pid}/children");
    let began = Instant::now();
    loop {
        let text = std::fs::read_to_string(&path).unwrap();
        let kids: Vec<u32> = text.split_whitespace().filter_map(|k| k.parse().ok()).collect();
        if let [one] = kids[..]
            && Some(one) != not
        {
            return one;
        }
        assert!(began.elapsed() < Duration::from_secs(20), "no new worker under {pid}: {kids:?}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn kill(pid: u32) {
    let ok = Command::new("kill").arg("-KILL").arg(pid.to_string()).status().unwrap().success();
    assert!(ok, "kill {pid}");
}

struct Watchdog(Child, PathBuf);

impl Drop for Watchdog {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
        let _ = std::fs::remove_dir_all(&self.1);
    }
}

fn start(dir: &Path) -> Watchdog {
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();
    let mem = dir.join("mem");
    let data: Vec<u8> = (0..PAGES).flat_map(|p| [expect(p); PAGE]).collect();
    std::fs::write(&mem, data).unwrap();
    let sock = dir.join("uffd.sock");
    let child = Command::new(env!("CARGO_BIN_EXE_hive-uffd"))
        .arg("--socket")
        .arg(&sock)
        .arg("--memory")
        .arg(&mem)
        .spawn()
        .unwrap();
    let w = Watchdog(child, dir.to_path_buf());
    let began = Instant::now();
    while !sock.exists() {
        assert!(began.elapsed() < Duration::from_secs(20), "the socket never showed up");
        std::thread::sleep(Duration::from_millis(5));
    }
    w
}

#[test]
fn a_killed_worker_is_replaced_and_the_vm_carries_on() {
    if !allowed() {
        eprintln!("skipped: no userfaultfd for this process");
        return;
    }
    let dir = std::env::temp_dir().join(format!("hive-uffd-watch-{}", std::process::id()));
    let w = start(&dir);
    let guest = Arc::new(Guest::connect(&dir.join("uffd.sock"), PAGES * PAGE).unwrap());
    let first = worker(w.0.id(), None);
    assert_eq!(guest.touch(0), expect(0));

    // The first page after the kill waits for a new worker.
    kill(first);
    let t = Instant::now();
    assert_eq!(guest.touch(PAGE), expect(1));
    let took = t.elapsed();
    let second = worker(w.0.id(), Some(first));

    // A VM thread touches pages while its worker is killed under it.
    let done = Arc::new(AtomicUsize::new(2));
    let toucher = {
        let (guest, done) = (guest.clone(), done.clone());
        std::thread::spawn(move || {
            for p in 2..PAGES {
                assert_eq!(guest.touch(p * PAGE), expect(p), "page {p}");
                done.store(p + 1, Ordering::Relaxed);
            }
        })
    };
    while done.load(Ordering::Relaxed) < PAGES / 4 && !toucher.is_finished() {
        std::thread::yield_now();
    }
    kill(second);
    toucher.join().unwrap();
    let third = worker(w.0.id(), Some(second));
    assert_ne!(third, first);
    println!("the first page after the worker was killed came in {took:.1?}");
    drop(guest);
}
