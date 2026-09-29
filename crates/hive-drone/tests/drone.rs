//! The drone and its client, end to end over an in-memory pipe.

#![cfg(target_os = "linux")]

use bytes::Bytes;
use hive_drone::{Client, Config, Drone, Output};
use hive_proto::drone::api::{Command, RunRequest};
use hive_types::Reason;
use std::sync::Arc;
use std::time::{Duration, Instant};

const SECRET: [u8; 32] = [7; 32];

async fn connect(drone: &Arc<Drone>, secret: [u8; 32]) -> std::io::Result<Client> {
    let (a, b) = tokio::io::duplex(1 << 20);
    tokio::spawn(drone.clone().serve(a));
    Client::connect(b, &secret, 0, [1; 32]).await
}

async fn pair() -> Client {
    connect(&Drone::new(Config::default(), SECRET), SECRET).await.unwrap()
}

fn sh(script: &str) -> RunRequest {
    RunRequest {
        command: Some(Command { shell: script.into(), ..Command::default() }),
        ..Default::default()
    }
}

#[tokio::test]
async fn echo_comes_back_on_stdout() {
    let c = pair().await;
    let r = c.run(&sh("echo hello; echo oops >&2")).await.unwrap();
    assert_eq!(r.exit_code, 0);
    assert_eq!(&r.stdout[..], b"hello\n");
    assert_eq!(&r.stderr[..], b"oops\n");
    assert!(!r.truncated && !r.timed_out);
    assert!(r.wall_nanos > 0);
}

#[tokio::test]
async fn exit_codes_and_signals_are_reported() {
    let c = pair().await;
    assert_eq!(c.run(&sh("exit 3")).await.unwrap().exit_code, 3);
    let r = c.run(&sh("kill -TERM $$")).await.unwrap();
    assert_eq!((r.exit_code, r.signal), (-1, 15));
}

#[tokio::test]
async fn stdin_is_fed_and_closed() {
    let c = pair().await;
    let mut req = sh("wc -c");
    req.stdin = Bytes::from(vec![b'x'; 300_000]);
    let r = c.run(&req).await.unwrap();
    assert_eq!(String::from_utf8_lossy(&r.stdout).trim(), "300000");
}

#[tokio::test]
async fn argv_env_and_cwd_are_honoured() {
    let c = pair().await;
    let mut cmd = Command {
        argv: vec!["/bin/sh".into(), "-c".into(), "echo $FOO; pwd; echo ${HOSTILE:-unset}".into()],
        cwd: "/tmp".into(),
        ..Command::default()
    };
    cmd.env.insert("FOO".into(), "bar".into());
    let r = c.run(&RunRequest { command: Some(cmd), ..Default::default() }).await.unwrap();
    assert_eq!(String::from_utf8_lossy(&r.stdout), "bar\n/tmp\nunset\n");
}

#[tokio::test]
async fn a_missing_program_exits_127() {
    let c = pair().await;
    let req = RunRequest {
        command: Some(Command { argv: vec!["/no/such/program".into()], ..Command::default() }),
        ..Default::default()
    };
    let r = c.run(&req).await.unwrap();
    assert_eq!(r.exit_code, 127);
    assert!(String::from_utf8_lossy(&r.stderr).contains("/no/such/program"));
}

#[tokio::test]
async fn an_empty_command_is_refused() {
    let c = pair().await;
    let e = c.run(&RunRequest::default()).await.unwrap_err();
    assert_eq!(e.reason, Reason::InvalidArgument);
}

#[tokio::test]
async fn a_timeout_kills_the_whole_group() {
    let c = pair().await;
    let mut req = sh("sleep 30 & sleep 30; echo never");
    req.command.as_mut().unwrap().timeout_ms = 200;
    let t = Instant::now();
    let r = c.run(&req).await.unwrap();
    assert!(r.timed_out);
    assert_eq!(r.signal, 9);
    assert!(t.elapsed() < Duration::from_secs(5), "took {:?}", t.elapsed());
    assert!(r.stdout.is_empty());
}

#[tokio::test]
async fn a_background_job_does_not_hold_up_the_answer() {
    let c = pair().await;
    let t = Instant::now();
    let r = c.run(&sh("sleep 5 & echo done")).await.unwrap();
    assert_eq!(&r.stdout[..], b"done\n");
    assert!(t.elapsed() < Duration::from_secs(2), "took {:?}", t.elapsed());
}

#[tokio::test]
async fn big_output_is_truncated_in_the_middle() {
    let c = pair().await;
    let mut req = sh("echo START; head -c 5000000 /dev/zero | tr '\\0' x; echo; echo END");
    req.command.as_mut().unwrap().max_output_bytes = 200_000;
    let r = c.run(&req).await.unwrap();
    assert!(r.truncated);
    assert_eq!(r.stdout.len(), 200_000);
    assert!(r.stdout.starts_with(b"START\n"));
    assert!(r.stdout.ends_with(b"\nEND\n"));
    assert_eq!(r.stdout_bytes, 6 + 5_000_000 + 1 + 4);
}

#[tokio::test]
async fn streamed_commands_echo_their_input() {
    let c = pair().await;
    let mut p = c.start(&Command { argv: vec!["cat".into()], ..Command::default() }).await.unwrap();
    assert!(matches!(p.next().await.unwrap(), Some(Output::Started(pid)) if pid > 0));
    p.write(b"one\n").await.unwrap();
    assert_eq!(p.next().await.unwrap(), Some(Output::Stdout(Bytes::from_static(b"one\n"))));
    // Far more than the pipes and the channel windows hold, so input and output have to flow
    // at the same time.
    let (mut input, output) = p.split();
    let writer = tokio::spawn(async move {
        input.write(&vec![b'y'; 16 << 20]).await.unwrap();
        input.close_stdin().await.unwrap();
        input
    });
    let r = output.wait().await.unwrap();
    writer.await.unwrap().finish().await.unwrap();
    assert_eq!(r.exit_code, 0);
    assert_eq!(r.stdout.len(), 16 << 20);
    assert_eq!(r.stdout_bytes, 4 + (16 << 20));
}

#[tokio::test]
async fn streamed_input_flows_to_a_command_that_prints_nothing() {
    let c = pair().await;
    let cmd = Command { shell: "cat > /dev/null".into(), timeout_ms: 20_000, ..Command::default() };
    let (mut input, output) = c.start(&cmd).await.unwrap().split();
    let t = Instant::now();
    let writer = tokio::spawn(async move {
        input.write(&vec![b'y'; 16 << 20]).await.unwrap();
        input.close_stdin().await.unwrap();
        input
    });
    let r = output.wait().await.unwrap();
    writer.await.unwrap().finish().await.unwrap();
    assert_eq!((r.exit_code, r.timed_out), (0, false));
    assert!(t.elapsed() < Duration::from_secs(10), "took {:?}", t.elapsed());
}

#[tokio::test]
async fn streamed_commands_take_signals() {
    let c = pair().await;
    let mut p = c.start(&Command { shell: "sleep 30".into(), ..Command::default() }).await.unwrap();
    assert!(matches!(p.next().await.unwrap(), Some(Output::Started(_))));
    p.signal(15).await.unwrap();
    let r = p.wait().await.unwrap();
    assert_eq!(r.signal, 15);
}

#[tokio::test]
async fn streamed_commands_time_out() {
    let c = pair().await;
    let cmd = Command { shell: "sleep 30".into(), timeout_ms: 100, ..Command::default() };
    let r = c.start(&cmd).await.unwrap().wait().await.unwrap();
    assert!(r.timed_out);
    assert_eq!(r.signal, 9);
}

#[tokio::test]
async fn dropping_a_streamed_command_kills_it() {
    let dir = std::env::temp_dir().join(format!("hive-drone-drop-{}", std::process::id()));
    let _ = std::fs::remove_file(&dir);
    let c = pair().await;
    let script = format!("sleep 1; touch {}", dir.display());
    let mut p = c.start(&Command { shell: script, ..Command::default() }).await.unwrap();
    assert!(matches!(p.next().await.unwrap(), Some(Output::Started(_))));
    drop(p);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(!dir.exists(), "the command outlived its caller");
}

#[tokio::test]
async fn health_reports_the_build_and_memory() {
    let c = pair().await;
    let h = c.health().await.unwrap();
    assert!(h.build.starts_with("hive-drone "));
    if cfg!(target_os = "linux") {
        assert!(h.mem_total_bytes > 0);
        assert!(h.mem_available_bytes <= h.mem_total_bytes);
    }
}

#[tokio::test]
async fn many_commands_run_at_once() {
    let c = pair().await;
    let calls = (0..64).map(|i| {
        let c = c.clone();
        tokio::spawn(async move { c.run(&sh(&format!("echo {i}"))).await.unwrap() })
    });
    for (i, call) in calls.enumerate() {
        assert_eq!(String::from_utf8_lossy(&call.await.unwrap().stdout), format!("{i}\n"));
    }
}

#[tokio::test]
async fn a_wrong_secret_is_refused() {
    let drone = Drone::new(Config::default(), SECRET);
    assert!(connect(&drone, [8; 32]).await.is_err());
}

#[tokio::test]
async fn the_secret_rotates_and_a_lost_rotation_recovers() {
    let drone = Drone::new(Config::default(), SECRET);
    let first = connect(&drone, SECRET).await.unwrap();
    // A call answered means the drone is past the handshake and has rotated.
    first.health().await.unwrap();
    let next = first.established().next_secret;
    assert_eq!(drone.current_secret(), next);
    // A node that never learned about the rotation still gets in once with the old secret.
    let again = connect(&drone, SECRET).await.unwrap();
    again.health().await.unwrap();
    assert_eq!(drone.current_secret(), again.established().next_secret);
    // But the secret from two rotations ago is gone.
    assert!(connect(&drone, next).await.is_err());
    assert!(connect(&drone, again.established().next_secret).await.is_ok());
}

/// Exec latency and throughput on this machine. Run with
/// `cargo test --release -p hive-drone --test drone -- --ignored --nocapture exec_latency`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, not a test"]
async fn exec_latency() {
    let (a, b) = tokio::net::UnixStream::pair().unwrap();
    let drone = Drone::new(Config::default(), SECRET);
    tokio::spawn(drone.clone().serve(a));
    let c = Client::connect(b, &SECRET, 0, [1; 32]).await.unwrap();
    let t = Instant::now();
    let mut health = Vec::new();
    for _ in 0..5000 {
        let s = Instant::now();
        c.health().await.unwrap();
        health.push(s.elapsed());
    }
    report("health round trip", &mut health, t.elapsed());
    let t = Instant::now();
    let mut direct = Vec::new();
    for _ in 0..2000 {
        let s = Instant::now();
        let out = tokio::process::Command::new("/bin/true").output().await.unwrap();
        assert!(out.status.success());
        direct.push(s.elapsed());
    }
    report("spawn /bin/true straight from tokio, for comparison", &mut direct, t.elapsed());
    let req = RunRequest {
        command: Some(Command { argv: vec!["/bin/true".into()], ..Command::default() }),
        ..Default::default()
    };
    let t = Instant::now();
    let mut exec = Vec::new();
    for _ in 0..2000 {
        let s = Instant::now();
        c.run(&req).await.unwrap();
        exec.push(s.elapsed());
    }
    report("exec /bin/true, one at a time", &mut exec, t.elapsed());
    let t = Instant::now();
    let tasks: Vec<_> = (0..16)
        .map(|_| {
            let (c, req) = (c.clone(), req.clone());
            tokio::spawn(async move {
                let mut lat = Vec::new();
                for _ in 0..500 {
                    let s = Instant::now();
                    c.run(&req).await.unwrap();
                    lat.push(s.elapsed());
                }
                lat
            })
        })
        .collect();
    let mut all = Vec::new();
    for task in tasks {
        all.extend(task.await.unwrap());
    }
    report("exec /bin/true, 16 at a time", &mut all, t.elapsed());
    let mut req = sh("head -c 104857600 /dev/zero");
    req.command.as_mut().unwrap().max_output_bytes = 1;
    let t = Instant::now();
    let r = c.run(&req).await.unwrap();
    let secs = t.elapsed().as_secs_f64();
    println!("run 100 MiB of output: {:.0} MiB/s", r.stdout_bytes as f64 / secs / (1 << 20) as f64);
    let t = Instant::now();
    let cmd = Command { shell: "head -c 1073741824 /dev/zero".into(), ..Command::default() };
    let r = c.start(&cmd).await.unwrap();
    let mut p = r;
    let mut n = 0usize;
    while let Some(out) = p.next().await.unwrap() {
        if let Output::Stdout(b) = out {
            n += b.len();
        }
    }
    let secs = t.elapsed().as_secs_f64();
    println!("streamed 1 GiB of output: {:.0} MiB/s", n as f64 / secs / (1 << 20) as f64);
}

fn report(what: &str, lat: &mut [Duration], total: Duration) {
    lat.sort();
    let p = |q: f64| lat[((lat.len() as f64 * q) as usize).min(lat.len() - 1)];
    println!(
        "{what}: p50 {:?}, p99 {:?}, max {:?}, {:.0} per second",
        p(0.5),
        p(0.99),
        lat[lat.len() - 1],
        lat.len() as f64 / total.as_secs_f64()
    );
}
