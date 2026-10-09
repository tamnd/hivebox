//! Wasm cells end to end: programs in a modules directory, run through each cell's drone the
//! way the node agent runs them.

#![cfg(target_os = "linux")]

use std::path::PathBuf;
use std::time::{Duration, Instant};

use bytes::Bytes;
use hive_cell::{CellDriver, CellHandle, GuestChannel, Liveness, RootfsPlan, Slot};
use hive_cell_wasm::{Config, WasmDriver};
use hive_drone::Client;
use hive_proto::drone::api::{Command, FsRead, FsWrite, RunRequest, SessionCreate};
use hive_types::{Backend, CellId, CellSpec, Reason, Source};
use tokio::net::UnixStream;

const HELLO: &str = r#"(module
  (import "wasi_snapshot_preview1" "fd_write" (func $write (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 16) "hello from wasm\n")
  (func (export "_start")
    (i32.store (i32.const 0) (i32.const 16))
    (i32.store (i32.const 4) (i32.const 16))
    (drop (call $write (i32.const 1) (i32.const 0) (i32.const 1) (i32.const 8)))))"#;

// Writes its arguments to stdout as they come, each ended by a NUL.
const ARGS: &str = r#"(module
  (import "wasi_snapshot_preview1" "args_sizes_get" (func $sizes (param i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "args_get" (func $get (param i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "fd_write" (func $write (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (func (export "_start")
    (drop (call $sizes (i32.const 0) (i32.const 4)))
    (drop (call $get (i32.const 256) (i32.const 1024)))
    (i32.store (i32.const 16) (i32.const 1024))
    (i32.store (i32.const 20) (i32.load (i32.const 4)))
    (drop (call $write (i32.const 1) (i32.const 16) (i32.const 1) (i32.const 24)))))"#;

// The same for its environment.
const ENV: &str = r#"(module
  (import "wasi_snapshot_preview1" "environ_sizes_get" (func $sizes (param i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "environ_get" (func $get (param i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "fd_write" (func $write (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (func (export "_start")
    (drop (call $sizes (i32.const 0) (i32.const 4)))
    (drop (call $get (i32.const 256) (i32.const 1024)))
    (i32.store (i32.const 16) (i32.const 1024))
    (i32.store (i32.const 20) (i32.load (i32.const 4)))
    (drop (call $write (i32.const 1) (i32.const 16) (i32.const 1) (i32.const 24)))))"#;

// Copies stdin to stdout.
const CAT: &str = r#"(module
  (import "wasi_snapshot_preview1" "fd_read" (func $read (param i32 i32 i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "fd_write" (func $write (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (func (export "_start")
    (local $n i32)
    (loop $more
      (i32.store (i32.const 0) (i32.const 1024))
      (i32.store (i32.const 4) (i32.const 4096))
      (drop (call $read (i32.const 0) (i32.const 0) (i32.const 1) (i32.const 8)))
      (local.set $n (i32.load (i32.const 8)))
      (if (local.get $n) (then
        (i32.store (i32.const 4) (local.get $n))
        (drop (call $write (i32.const 1) (i32.const 0) (i32.const 1) (i32.const 12)))
        (br $more))))))"#;

// Copies /in.txt to a new /out.txt, and exits with 10 and up when a step fails.
const COPY: &str = r#"(module
  (import "wasi_snapshot_preview1" "path_open"
    (func $open (param i32 i32 i32 i32 i32 i64 i64 i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "fd_read" (func $read (param i32 i32 i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "fd_write" (func $write (param i32 i32 i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "proc_exit" (func $exit (param i32)))
  (memory (export "memory") 1)
  (data (i32.const 0) "in.txt")
  (data (i32.const 16) "out.txt")
  (func (export "_start")
    (local $n i32)
    (if (call $open (i32.const 3) (i32.const 0) (i32.const 0) (i32.const 6) (i32.const 0)
          (i64.const 2) (i64.const 0) (i32.const 0) (i32.const 32))
      (then (call $exit (i32.const 10))))
    (if (call $open (i32.const 3) (i32.const 0) (i32.const 16) (i32.const 7) (i32.const 9)
          (i64.const 64) (i64.const 0) (i32.const 0) (i32.const 36))
      (then (call $exit (i32.const 11))))
    (loop $more
      (i32.store (i32.const 40) (i32.const 1024))
      (i32.store (i32.const 44) (i32.const 4096))
      (if (call $read (i32.load (i32.const 32)) (i32.const 40) (i32.const 1) (i32.const 48))
        (then (call $exit (i32.const 12))))
      (local.set $n (i32.load (i32.const 48)))
      (if (local.get $n) (then
        (i32.store (i32.const 44) (local.get $n))
        (if (call $write (i32.load (i32.const 36)) (i32.const 40) (i32.const 1) (i32.const 52))
          (then (call $exit (i32.const 13))))
        (br $more))))))"#;

const EXIT: &str = r#"(module
  (import "wasi_snapshot_preview1" "proc_exit" (func $exit (param i32)))
  (memory (export "memory") 1)
  (func (export "_start") (call $exit (i32.const 3))))"#;

const TRAP: &str = r#"(module (memory (export "memory") 1) (func (export "_start") unreachable))"#;

const SPIN: &str =
    r#"(module (memory (export "memory") 1) (func (export "_start") (loop $l (br $l))))"#;

// Grows its memory 4 MiB at a time until it cannot, and exits with how many times it could.
const GROW: &str = r#"(module
  (import "wasi_snapshot_preview1" "proc_exit" (func $exit (param i32)))
  (memory (export "memory") 1)
  (func (export "_start")
    (local $n i32)
    (block $done
      (loop $more
        (br_if $done (i32.eq (memory.grow (i32.const 64)) (i32.const -1)))
        (local.set $n (i32.add (local.get $n) (i32.const 1)))
        (br $more)))
    (call $exit (local.get $n))))"#;

const LIB: &str = r#"(module (func (export "add") (param i32 i32) (result i32)
  (i32.add (local.get 0) (local.get 1))))"#;

struct Fixture {
    driver: WasmDriver,
    dir: PathBuf,
    modules: PathBuf,
}

impl Fixture {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("hive-wasm-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let modules = dir.join("modules");
        std::fs::create_dir_all(&modules).unwrap();
        for (name, text) in [
            ("hello", HELLO),
            ("args", ARGS),
            ("env", ENV),
            ("cat", CAT),
            ("copy", COPY),
            ("exit", EXIT),
            ("trap", TRAP),
            ("spin", SPIN),
            ("grow", GROW),
            ("lib", LIB),
        ] {
            std::fs::write(modules.join(format!("{name}.wasm")), text).unwrap();
        }
        let cfg = Config {
            modules: modules.clone(),
            instances: 32,
            max_mem_mib: 64,
            ..Config::default()
        };
        Self { driver: WasmDriver::new(cfg).unwrap(), dir, modules }
    }

    async fn cell(
        &self,
        seq: u64,
        spec: &CellSpec,
    ) -> Result<(CellHandle, Client), hive_types::Error> {
        let id = CellId::new(1, 1, 1, seq, 0).unwrap();
        let slot = Slot {
            dir: self.dir.join(format!("c{seq}")),
            secret: [seq as u8; 32],
            ..Slot::default()
        };
        std::fs::create_dir_all(&slot.dir).unwrap();
        let mut h = self.driver.prepare(id, spec, &RootfsPlan::default(), &slot).await?;
        self.driver.start(&mut h).await?;
        let GuestChannel::Unix(sock) = &h.channel else { panic!("{:?}", h.channel) };
        let io = UnixStream::connect(sock).await.unwrap();
        let client = Client::connect(io, &slot.secret, 0, [9; 32]).await.unwrap();
        Ok((h, client))
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn spec(image: &str, mem_mib: u32) -> CellSpec {
    let mut s = CellSpec::new(Source::Image(image.into()), Backend::Fncall);
    s.resources.mem_mib = mem_mib;
    s
}

fn shell(line: &str) -> RunRequest {
    RunRequest {
        command: Some(Command { shell: line.into(), ..Command::default() }),
        stdin: Bytes::new(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_command_runs_the_program_its_first_word_names() {
    let f = Fixture::new("names");
    let (_h, c) = f.cell(1, &spec("hello", 16)).await.unwrap();
    for line in ["hello", "/usr/bin/hello", "hello.wasm"] {
        let r = c.run(&shell(line)).await.unwrap();
        assert_eq!((r.exit_code, &r.stdout[..]), (0, &b"hello from wasm\n"[..]), "{line}");
        assert_eq!(r.stdout_bytes, 16);
    }
    let r = c.run(&shell("args 'a b' c\\ d \"\"")).await.unwrap();
    assert_eq!(&r.stdout[..], b"args\0a b\0c d\0\0");
    let argv = RunRequest {
        command: Some(Command {
            argv: vec!["args".into(), "$HOME | x".into()],
            ..Command::default()
        }),
        stdin: Bytes::new(),
    };
    assert_eq!(&c.run(&argv).await.unwrap().stdout[..], b"args\0$HOME | x\0");
    let r = c.run(&shell("nothing here")).await.unwrap();
    assert_eq!(r.exit_code, 127);
    assert_eq!(&r.stderr[..], b"nothing: no such program\n");
    let r = c.run(&shell("lib")).await.unwrap();
    assert_eq!(r.exit_code, 126, "{:?}", r.stderr);
    let e = c.run(&shell("hello | cat")).await.unwrap_err();
    assert_eq!(e.reason, Reason::InvalidArgument);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_cell_and_the_command_set_the_environment() {
    let f = Fixture::new("env");
    let mut s = spec("env", 16);
    s.env.insert("A".into(), "cell".into());
    s.env.insert("B".into(), "cell".into());
    let (_h, c) = f.cell(1, &s).await.unwrap();
    let mut req = shell("env");
    req.command.as_mut().unwrap().env.insert("B".into(), "run".into());
    let r = c.run(&req).await.unwrap();
    let vars: Vec<&[u8]> = r.stdout.split(|&b| b == 0).filter(|v| !v.is_empty()).collect();
    assert_eq!(vars, [&b"A=cell"[..], b"B=run"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn files_written_either_side_are_seen_on_the_other() {
    let f = Fixture::new("files");
    let (_h, c) = f.cell(1, &spec("copy", 16)).await.unwrap();
    let data = Bytes::from(vec![b'x'; 10_000]);
    let w =
        FsWrite { path: "/in.txt".into(), data: data.clone(), mode: 0o644, ..FsWrite::default() };
    c.fs_write(&w).await.unwrap();
    let r = c.run(&shell("copy")).await.unwrap();
    assert_eq!(r.exit_code, 0, "{:?}", r.stderr);
    let got = c.fs_read(&FsRead { path: "/out.txt".into(), ..FsRead::default() }, 1 << 20).await;
    assert_eq!(got.unwrap(), data);
    assert_eq!(std::fs::read(f.dir.join("c1/root/out.txt")).unwrap(), data);
    // Nothing above / is in reach of either side.
    let outside = FsRead { path: "/../../modules/hello.wasm".into(), ..FsRead::default() };
    assert!(c.fs_read(&outside, 1 << 20).await.is_err());
    let mut req = shell("cat");
    req.stdin = Bytes::from_static(b"from stdin");
    assert_eq!(&c.run(&req).await.unwrap().stdout[..], b"from stdin");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exits_traps_and_timeouts_come_back_as_a_process_would_give_them() {
    let f = Fixture::new("ends");
    let (_h, c) = f.cell(1, &spec("hello", 16)).await.unwrap();
    assert_eq!(c.run(&shell("exit")).await.unwrap().exit_code, 3);
    let r = c.run(&shell("trap")).await.unwrap();
    assert_eq!(r.exit_code, 134);
    assert!(String::from_utf8_lossy(&r.stderr).contains("wasm trap"), "{:?}", r.stderr);
    let mut req = shell("spin");
    req.command.as_mut().unwrap().timeout_ms = 300;
    let t = Instant::now();
    let r = c.run(&req).await.unwrap();
    assert!(r.timed_out);
    assert_eq!((r.exit_code, r.signal), (-1, 9));
    assert!(t.elapsed() < Duration::from_secs(5), "{:?}", t.elapsed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_program_grows_only_to_the_cells_memory() {
    let f = Fixture::new("mem");
    let (h, c) = f.cell(1, &spec("grow", 16)).await.unwrap();
    // One page to start and three 4 MiB grows fit in 16 MiB, and a fourth does not.
    assert_eq!(c.run(&shell("grow")).await.unwrap().exit_code, 3);
    let m = f.driver.metrics(&h);
    assert_eq!((m.mem_bytes, m.mem_peak, m.pids), (0, 193 * 65536, 0));
    let (h2, c2) = f.cell(2, &spec("grow", 64)).await.unwrap();
    assert_eq!(c2.run(&shell("grow")).await.unwrap().exit_code, 15);
    assert!(f.driver.metrics(&h2).cpu_usec > 0);
    assert_eq!(
        f.driver
            .prepare(
                CellId::new(1, 1, 1, 3, 0).unwrap(),
                &spec("grow", 65),
                &RootfsPlan::default(),
                &Slot::default()
            )
            .await
            .unwrap_err()
            .reason,
        Reason::InvalidArgument
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn what_a_wasm_cell_cannot_do_is_refused() {
    let f = Fixture::new("refused");
    let e = f.cell(1, &spec("missing", 16)).await.unwrap_err();
    assert_eq!(e.reason, Reason::ImageUnavailable);
    let snap = CellSpec::new(Source::Snapshot("s".into()), Backend::Fncall);
    assert_eq!(f.cell(2, &snap).await.unwrap_err().reason, Reason::PolicyDenied);
    let mut net = spec("hello", 16);
    net.network_profile = "llm".into();
    assert_eq!(f.cell(3, &net).await.unwrap_err().reason, Reason::PolicyDenied);
    let (_h, c) = f.cell(4, &spec("hello", 16)).await.unwrap();
    let e = c.session_create(&SessionCreate::default()).await.unwrap_err();
    assert_eq!(e.reason, Reason::PolicyDenied);
    let mut req = shell("hello");
    req.command.as_mut().unwrap().cwd = "/work".into();
    assert_eq!(c.run(&req).await.unwrap_err().reason, Reason::InvalidArgument);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stopping_a_cell_cuts_off_what_it_runs() {
    let f = Fixture::new("stop");
    let (h, c) = f.cell(1, &spec("spin", 16)).await.unwrap();
    let running = tokio::spawn(async move { c.run(&shell("spin")).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let m = f.driver.metrics(&h);
    assert_eq!(m.pids, 1);
    let t = Instant::now();
    f.driver.stop(&h, Duration::ZERO).await.unwrap();
    assert!(running.await.unwrap().is_err());
    assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());
    assert_eq!(f.driver.check(&h).await.unwrap(), Liveness::Gone(Default::default()));
    let GuestChannel::Unix(sock) = &h.channel else { unreachable!() };
    assert!(!sock.exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_changed_program_is_compiled_again() {
    let f = Fixture::new("change");
    let (_h, c) = f.cell(1, &spec("hello", 16)).await.unwrap();
    assert_eq!(&c.run(&shell("hello")).await.unwrap().stdout[..], b"hello from wasm\n");
    let changed = HELLO.replace("hello from wasm\\n", "a new program!!\\n");
    std::fs::write(f.modules.join("hello.wasm"), changed).unwrap();
    assert_eq!(&c.run(&shell("hello")).await.unwrap().stdout[..], b"a new program!!\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_cells_run_at_once() {
    let f = Fixture::new("many");
    let mut clients = Vec::new();
    for seq in 1..=8 {
        clients.push(f.cell(seq, &spec("hello", 16)).await.unwrap().1);
    }
    let runs = clients
        .iter()
        .flat_map(|c| (0..8).map(move |_| async move { c.run(&shell("hello")).await }));
    let results = futures::future::join_all(runs).await;
    assert_eq!(results.len(), 64);
    assert!(results.iter().all(|r| r.as_ref().is_ok_and(|r| r.exit_code == 0)));
}
