//! Sessions, end to end over an in-memory pipe.

#![cfg(target_os = "linux")]

use bytes::Bytes;
use hive_drone::{Client, Config, Drone};
use hive_proto::drone::api::{SessionCreate, SessionRun, SessionRunResult, SessionSend};
use hive_types::Reason;
use std::time::{Duration, Instant};

async fn client() -> Client {
    let (a, b) = tokio::io::duplex(1 << 20);
    tokio::spawn(Drone::new(Config::default(), [7; 32]).serve(a));
    Client::connect(b, &[7; 32], 0, [1; 32]).await.unwrap()
}

async fn session(c: &Client) -> String {
    c.session_create(&SessionCreate::default()).await.unwrap().id
}

async fn run(c: &Client, id: &str, command: &str) -> SessionRunResult {
    let req = SessionRun { id: id.into(), command: command.into(), ..SessionRun::default() };
    c.session_run(&req).await.unwrap()
}

fn text(r: &SessionRunResult) -> String {
    String::from_utf8_lossy(&r.output).into_owned()
}

#[tokio::test]
async fn directory_and_variables_carry_over() {
    let c = client().await;
    let id = session(&c).await;
    run(&c, &id, "cd /tmp && export FOO=bar && greet() { echo hi $1; }").await;
    assert_eq!(text(&run(&c, &id, "pwd; echo $FOO; greet you").await), "/tmp\nbar\nhi you\n");
}

#[tokio::test]
async fn exit_codes_come_back() {
    let c = client().await;
    let id = session(&c).await;
    assert_eq!(run(&c, &id, "false").await.exit_code, 1);
    assert_eq!(run(&c, &id, "(exit 7)").await.exit_code, 7);
    assert_eq!(run(&c, &id, "true").await.exit_code, 0);
    assert_eq!(run(&c, &id, "").await.exit_code, 0);
}

#[tokio::test]
async fn output_is_interleaved_and_exact() {
    let c = client().await;
    let id = session(&c).await;
    assert_eq!(text(&run(&c, &id, "echo a; echo b >&2; echo c").await), "a\nb\nc\n");
    assert_eq!(text(&run(&c, &id, "printf abc").await), "abc");
    assert_eq!(text(&run(&c, &id, "printf 'x\\036y'").await), "x\x1ey");
}

#[tokio::test]
async fn a_command_cannot_fake_its_end() {
    let c = client().await;
    let id = session(&c).await;
    let r =
        run(&c, &id, "printf '\\0360123456789abcdef0123456789abcdef:0\\036'; exit_later=1; false")
            .await;
    assert_eq!(r.exit_code, 1);
    assert_eq!(r.output.len(), 1 + 32 + 3);
}

#[tokio::test]
async fn stdin_is_empty() {
    let c = client().await;
    let id = session(&c).await;
    let t = Instant::now();
    let r = run(&c, &id, "cat; echo after").await;
    assert_eq!(text(&r), "after\n");
    assert!(t.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn a_background_job_does_not_block() {
    let c = client().await;
    let id = session(&c).await;
    let t = Instant::now();
    let r = run(&c, &id, "sleep 10 & echo started").await;
    assert_eq!(text(&r), "started\n");
    assert!(t.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn a_timeout_restarts_the_shell() {
    let c = client().await;
    let id = session(&c).await;
    run(&c, &id, "cd /tmp").await;
    let req = SessionRun {
        id: id.clone(),
        command: "echo before; sleep 30".into(),
        timeout_ms: 200,
        ..SessionRun::default()
    };
    let t = Instant::now();
    let r = c.session_run(&req).await.unwrap();
    assert!(r.timed_out && r.restarted);
    assert_eq!(text(&r), "before\n");
    assert!(t.elapsed() < Duration::from_secs(3));
    let r = run(&c, &id, "pwd").await;
    assert!(!r.restarted);
    assert_eq!(text(&r), "/\n");
}

#[tokio::test]
async fn exiting_the_shell_restarts_it() {
    let c = client().await;
    let id = session(&c).await;
    let r = run(&c, &id, "echo bye; exit 5").await;
    assert_eq!((r.exit_code, r.restarted), (5, true));
    assert_eq!(text(&r), "bye\n");
    assert_eq!(text(&run(&c, &id, "echo back").await), "back\n");
}

#[tokio::test]
async fn big_output_is_truncated() {
    let c = client().await;
    let id = session(&c).await;
    let req = SessionRun {
        id: id.clone(),
        command: "head -c 3000000 /dev/zero | tr '\\0' x; echo; echo END".into(),
        max_output_bytes: 100_000,
        ..SessionRun::default()
    };
    let r = c.session_run(&req).await.unwrap();
    assert!(r.truncated);
    assert_eq!(r.output_bytes, 3_000_000 + 1 + 4);
    assert!(r.output.ends_with(b"\nEND\n"));
    assert_eq!(r.exit_code, 0);
}

#[tokio::test]
async fn send_drives_an_interactive_program() {
    let c = client().await;
    let id = session(&c).await;
    let send = |input: &str, expect: &str| SessionSend {
        id: id.clone(),
        input: Bytes::from(input.to_string()),
        expect: expect.into(),
        timeout_ms: 5000,
        ..SessionSend::default()
    };
    let r = c.session_send(&send("read name; echo got $name\n", "")).await.unwrap();
    assert!(r.output.is_empty() && !r.matched && !r.timed_out);
    let r = c.session_send(&send("hello\n", "got hello")).await.unwrap();
    assert!(r.matched);
    assert_eq!(&r.output[..], b"got hello");
    // The shell is back at its prompt, so run works again.
    // The newline after the match was left for the next call, which sees it first.
    assert_eq!(text(&run(&c, &id, "echo $name").await), "\nhello\n");
}

#[tokio::test]
async fn send_times_out_waiting_for_a_match() {
    let c = client().await;
    let id = session(&c).await;
    let req = SessionSend {
        id,
        input: "echo nope\n".into(),
        expect: "never".into(),
        timeout_ms: 300,
        ..SessionSend::default()
    };
    let r = c.session_send(&req).await.unwrap();
    assert!(r.timed_out && !r.matched);
    assert_eq!(&r.output[..], b"nope\n");
}

#[tokio::test]
async fn closing_ends_the_session_and_a_running_command() {
    let c = client().await;
    let id = session(&c).await;
    let long = {
        let (c, id) = (c.clone(), id.clone());
        tokio::spawn(async move { run(&c, &id, "sleep 30").await })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    let t = Instant::now();
    c.session_close(&id).await.unwrap();
    let r = long.await.unwrap();
    assert!(r.restarted);
    assert!(t.elapsed() < Duration::from_secs(3));
    let req = SessionRun { id: id.clone(), command: "true".into(), ..SessionRun::default() };
    assert_eq!(c.session_run(&req).await.unwrap_err().reason, Reason::InvalidArgument);
    assert_eq!(c.session_close(&id).await.unwrap_err().reason, Reason::InvalidArgument);
}

#[tokio::test]
async fn sessions_are_independent_and_counted() {
    let c = client().await;
    let ids = [session(&c).await, session(&c).await, session(&c).await];
    for (i, id) in ids.iter().enumerate() {
        run(&c, id, &format!("N={i}")).await;
    }
    let runs = ids.iter().map(|id| {
        let (c, id) = (c.clone(), id.clone());
        tokio::spawn(async move { text(&run(&c, &id, "sleep 0.2; echo $N").await) })
    });
    for (i, r) in runs.enumerate() {
        assert_eq!(r.await.unwrap(), format!("{i}\n"));
    }
    assert_eq!(c.health().await.unwrap().sessions, 3);
}

#[tokio::test]
async fn a_bad_shell_fails_at_create() {
    let c = client().await;
    let spec = SessionCreate { shell: "/no/such/shell".into(), ..SessionCreate::default() };
    assert_eq!(c.session_create(&spec).await.unwrap_err().reason, Reason::InvalidArgument);
}

/// Session command latency on this machine. Run with
/// `cargo test --release -p hive-drone --test session -- --ignored --nocapture latency`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, not a test"]
async fn latency() {
    let (a, b) = tokio::net::UnixStream::pair().unwrap();
    tokio::spawn(Drone::new(Config::default(), [7; 32]).serve(a));
    let c = Client::connect(b, &[7; 32], 0, [1; 32]).await.unwrap();
    let id = session(&c).await;
    for (what, command) in [
        ("builtin true", "true"),
        ("cd and echo", "cd /tmp; echo hi"),
        ("exec /bin/true", "/bin/true"),
    ] {
        let mut lat = Vec::new();
        let t = Instant::now();
        for _ in 0..2000 {
            let s = Instant::now();
            run(&c, &id, command).await;
            lat.push(s.elapsed());
        }
        let total = t.elapsed();
        lat.sort();
        println!(
            "session {what}: p50 {:?}, p99 {:?}, {:.0} per second",
            lat[lat.len() / 2],
            lat[lat.len() * 99 / 100],
            lat.len() as f64 / total.as_secs_f64()
        );
    }
    let req = SessionRun {
        id,
        command: "head -c 104857600 /dev/zero".into(),
        max_output_bytes: 1,
        ..SessionRun::default()
    };
    let t = Instant::now();
    let r = c.session_run(&req).await.unwrap();
    println!(
        "session output: {:.0} MiB/s",
        r.output_bytes as f64 / t.elapsed().as_secs_f64() / (1 << 20) as f64
    );
}
