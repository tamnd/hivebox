//! The drone binary with `--init` and `--harden`, as a container runs it.

#![cfg(target_os = "linux")]

use hive_drone::Client;
use hive_proto::drone::api::{Command, RunRequest};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};

const SECRET: [u8; 32] = [9; 32];

struct Running {
    dir: PathBuf,
    child: std::process::Child,
    client: Client,
}

impl Drop for Running {
    fn drop(&mut self) {
        // A stop through init, which passes it on to the drone. Killing init alone would leave the
        // drone running, since outside a PID namespace nothing takes it down with its parent.
        let pid = rustix::process::Pid::from_child(&self.child);
        let _ = rustix::process::kill_process(pid, rustix::process::Signal::TERM);
        let until = Instant::now() + Duration::from_secs(5);
        while self.child.try_wait().ok().flatten().is_none() && Instant::now() < until {
            std::thread::sleep(Duration::from_millis(5));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn start(name: &str, harden: bool) -> Running {
    let dir = std::env::temp_dir().join(format!("hive-drone-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("protected")).unwrap();
    std::fs::create_dir_all(dir.join("work")).unwrap();
    std::fs::write(dir.join("protected/keep"), "kept\n").unwrap();
    std::fs::write(dir.join("secret"), SECRET).unwrap();
    let socket = dir.join("drone.sock");
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_hive-drone"));
    cmd.arg("--init")
        .arg("--listen")
        .arg(format!("unix:{}", socket.display()))
        .arg("--secret-file")
        .arg(dir.join("secret"))
        .arg("--workdir")
        .arg(dir.join("work"))
        .args(["--env", "HIVE_TEST=yes", "--env", "PATH=/usr/bin:/bin:/usr/sbin:/sbin"])
        .stdin(Stdio::null());
    if harden {
        cmd.arg("--harden").arg("--protect").arg(dir.join("protected"));
    }
    let child = cmd.spawn().unwrap();
    let until = Instant::now() + Duration::from_secs(10);
    let stream = loop {
        if let Ok(s) = tokio::net::UnixStream::connect(&socket).await {
            break s;
        }
        assert!(Instant::now() < until, "the drone never listened");
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    let client = Client::connect(stream, &SECRET, 0, [1; 32]).await.unwrap();
    Running { dir, child, client }
}

fn sh(script: &str) -> RunRequest {
    RunRequest {
        command: Some(Command { shell: script.into(), ..Command::default() }),
        ..Default::default()
    }
}

async fn out(c: &Client, script: &str) -> (i32, String) {
    let r = c.run(&sh(script)).await.unwrap();
    let text =
        format!("{}{}", String::from_utf8_lossy(&r.stdout), String::from_utf8_lossy(&r.stderr));
    (r.exit_code, text)
}

#[tokio::test]
async fn a_hardened_drone_runs_commands_but_not_escapes() {
    let d = start("hard", true).await;
    let c = &d.client;
    assert!(!d.dir.join("secret").exists(), "the secret file is still there");

    let (code, text) = out(c, "grep -E '^(Seccomp|NoNewPrivs):' /proc/self/status").await;
    assert_eq!(code, 0, "{text}");
    assert!(text.contains("Seccomp:\t2") && text.contains("NoNewPrivs:\t1"), "{text}");

    let (code, text) = out(c, "echo $HIVE_TEST $PATH").await;
    assert_eq!((code, text.as_str()), (0, "yes /usr/bin:/bin:/usr/sbin:/sbin\n"));

    // Normal work goes on beside the protected path.
    let (code, text) = out(c, "echo made > f && cat f && ls / >/dev/null && sort </dev/null").await;
    assert_eq!((code, text.as_str()), (0, "made\n"));

    // The protected path reads but does not change.
    let p = d.dir.join("protected");
    let (code, text) = out(c, &format!("cat {}/keep", p.display())).await;
    assert_eq!((code, text.as_str()), (0, "kept\n"));
    for script in [
        format!("echo x > {}/new", p.display()),
        format!("echo x >> {}/keep", p.display()),
        format!("rm {}/keep", p.display()),
        format!("mv {}/keep {}/work/", p.display(), d.dir.display()),
    ] {
        let (code, _) = out(c, &script).await;
        assert_ne!(code, 0, "{script} worked");
    }
    assert_eq!(std::fs::read_to_string(p.join("keep")).unwrap(), "kept\n");

    // Namespaces and mounts are refused with EPERM, so the tools say so.
    for script in ["unshare -U true", "unshare -m true"] {
        let (code, text) = out(c, script).await;
        assert_ne!(code, 0, "{script} worked");
        assert!(text.contains("not permitted"), "{script}: {text}");
    }
    let (code, _) = out(c, "mount -t tmpfs none work").await;
    assert_ne!(code, 0, "mount worked");
}

/// Each request, then what it got: `ok` or the errno's name.
const IOCTLS: &str = r#"python3 -c '
import errno, fcntl, os, struct
f = os.open("f", os.O_RDWR | os.O_CREAT)
def code(req, arg=bytes(64)):
    try:
        fcntl.ioctl(f, req, arg)
        return "ok"
    except OSError as e:
        return errno.errorcode[e.errno]
r, w = os.pipe()
os.write(w, b"abc")
print("FIONREAD", struct.unpack("i", fcntl.ioctl(r, 0x541B, bytes(4)))[0])
print("FS_IOC_GETFLAGS", code(0x80086601))
print("FS_IOC_FSGETXATTR", code(0x801C581F))
print("XFS_IOC_SWAPEXT", code(0xC0C0586D, bytes(192)))
print("TIOCSTI", code(0x5412, b"x"))
print("EXT4_IOC_MOVE_EXT", code(0xC028660F, bytes(40)))
'"#;

#[tokio::test]
async fn a_hardened_drone_lets_the_usual_ioctls_through_and_not_the_rest() {
    if !std::process::Command::new("python3").arg("-V").output().is_ok_and(|o| o.status.success()) {
        eprintln!("skipped: needs python3");
        return;
    }
    let answers = |text: &str| -> std::collections::HashMap<String, String> {
        text.lines().filter_map(|l| l.split_once(' ')).map(|(k, v)| (k.into(), v.into())).collect()
    };
    let plain = start("ioctl-plain", false).await;
    let (code, text) = out(&plain.client, IOCTLS).await;
    assert_eq!(code, 0, "{text}");
    let plain = answers(&text);
    let hard = start("ioctl-hard", true).await;
    let (code, text) = out(&hard.client, IOCTLS).await;
    assert_eq!(code, 0, "{text}");
    let hard = answers(&text);
    assert_eq!(hard["FIONREAD"], "3");
    assert_eq!(hard["FS_IOC_GETFLAGS"], plain["FS_IOC_GETFLAGS"], "file flags read as before");
    // A filesystem that answers it unhardened shows the whole `X` family is cut off.
    if plain["FS_IOC_FSGETXATTR"] == "ok" {
        assert_eq!(hard["FS_IOC_FSGETXATTR"], "ENOTTY");
    }
    assert_eq!(hard["XFS_IOC_SWAPEXT"], "ENOTTY");
    assert_eq!((plain["TIOCSTI"].as_str(), hard["TIOCSTI"].as_str()), ("ENOTTY", "EPERM"));
    assert_eq!(hard["EXT4_IOC_MOVE_EXT"], "EPERM");
}

#[tokio::test]
async fn init_passes_a_stop_on_and_exits_as_the_drone_did() {
    let mut d = start("init", false).await;
    let (code, _) = out(&d.client, "true").await;
    assert_eq!(code, 0);
    let pid = rustix::process::Pid::from_child(&d.child);
    rustix::process::kill_process(pid, rustix::process::Signal::TERM).unwrap();
    let until = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(s) = d.child.try_wait().unwrap() {
            break s;
        }
        assert!(Instant::now() < until, "init did not exit");
        std::thread::sleep(Duration::from_millis(5));
    };
    assert_eq!(status.code(), Some(128 + 15));
}

#[tokio::test]
async fn a_second_secret_source_is_refused() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_hive-drone"))
        .args(["--listen", "unix:/nonexistent/s", "--secret-stdin", "--secret-file", "/x"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_hive-drone"))
        .args(["--listen", "unix:/nonexistent/s", "--secret-stdin", "--protect", "/x"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
}

/// A million `FIONREAD`s on a pipe, and the same loop with no syscall in it.
const IOCTL_COST: &str = r#"python3 -c '
import fcntl, os, time
r, w = os.pipe()
b = bytearray(4)
t = time.perf_counter()
for _ in range(1000000): fcntl.ioctl(r, 0x541B, b)
a = time.perf_counter() - t
t = time.perf_counter()
for _ in range(1000000): len(b)
e = time.perf_counter() - t
print("1M ioctls in %.0f ms (%.0f ns each past the loop)" % (a * 1e3, (a - e) * 1e3))
' 2>/dev/null || echo no python"#;

/// What hardening costs, next to the same drone without it. Run it with
/// `cargo test --release -p hive-drone --test harden -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "a measurement, not a test"]
async fn harden_cost() {
    let plain = start("cost-plain", false).await;
    let hard = start("cost-hard", true).await;
    let n = 300;
    for (name, d) in
        [("plain", &plain), ("hardened", &hard), ("plain", &plain), ("hardened", &hard)]
    {
        let mut runs = Vec::with_capacity(n);
        for _ in 0..n {
            let t = Instant::now();
            let r = d
                .client
                .run(&RunRequest {
                    command: Some(Command { argv: vec!["true".into()], ..Command::default() }),
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(r.exit_code, 0);
            runs.push(t.elapsed());
        }
        runs.sort();
        // One byte at a time, so it is two syscalls per byte and little else.
        let t = Instant::now();
        let (code, _) =
            out(&d.client, "dd if=/dev/zero of=/dev/null bs=1 count=1000000 2>/dev/null").await;
        assert_eq!(code, 0);
        let dd = t.elapsed();
        let t = Instant::now();
        let (code, _) =
            out(&d.client, "i=0; while [ $i -lt 300 ]; do sh -c : ; i=$((i+1)); done").await;
        assert_eq!(code, 0);
        let forks = t.elapsed();
        // An ioctl the filter looks at the request of, from python, and python's own loop alone.
        let (code, ioctls) = out(&d.client, IOCTL_COST).await;
        assert_eq!(code, 0, "{ioctls}");
        println!(
            "{name}: run true p50 {:?} p99 {:?}, 2M syscalls in {dd:?} ({:.0} ns each), 300 fork and exec in {forks:?}, {}",
            runs[n / 2],
            runs[n * 99 / 100],
            dd.as_nanos() as f64 / 2e6,
            ioctls.trim()
        );
    }
}
