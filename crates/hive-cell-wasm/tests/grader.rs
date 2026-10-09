//! Reward plugins: components of the grader world given a verification and turned into a reward,
//! within their time and memory.

#![cfg(target_os = "linux")]

#[path = "support/graders.rs"]
mod graders;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use hive_cell_wasm::{GraderConfig, Graders, Input, Run};
use hive_types::Reason;

struct Fixture {
    graders: Graders,
    dir: PathBuf,
}

impl Fixture {
    fn new(name: &str, mem_mib: u64) -> Self {
        let dir = std::env::temp_dir().join(format!("hive-grader-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        for name in ["fraction", "exact", "echo", "fail", "trap", "spin", "hog", "nan"] {
            std::fs::write(dir.join(format!("{name}.wasm")), graders::component(name)).unwrap();
        }
        let cfg = GraderConfig { dir: dir.clone(), timeout: Duration::from_millis(300), mem_mib };
        Self { graders: Graders::new(cfg).unwrap(), dir }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn run(exit_code: i32, stdout: &str) -> Run {
    Run { exit_code, stdout: stdout.into(), wall_ms: 5, ..Run::default() }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_grader_turns_the_runs_into_a_reward() {
    let f = Fixture::new("reward", 64);
    let timed_out = Run { exit_code: -1, timed_out: true, ..Run::default() };
    let runs = vec![run(0, "41\n"), timed_out, run(1, "x\n"), run(0, "42\n")];
    let input = Input { task: b"42\n".to_vec(), runs, ..Input::default() };
    let g = f.graders.score("fraction", input.clone()).await.unwrap().unwrap();
    assert_eq!((g.reward, g.detail.as_str()), (0.5, "fraction"));
    let g = f.graders.score("exact", input.clone()).await.unwrap().unwrap();
    assert_eq!((g.reward, g.detail.as_str()), (1.0, "exact"));
    let wrong = Input { task: b"43\n".to_vec(), ..input };
    assert_eq!(f.graders.score("exact", wrong).await.unwrap().unwrap().reward, 0.0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_grader_sees_everything_it_is_given() {
    let f = Fixture::new("echo", 64);
    let input = Input {
        task: "the task, as text".into(),
        runs: vec![run(0, "a"), run(0, "b")],
        files: vec![("out.txt".into(), Some(b"data".to_vec())), ("gone".into(), None)],
        passed: true,
        tampered: vec!["tests/a.py".into(), "conftest.py".into(), "x".into()],
    };
    let g = f.graders.score("echo", input).await.unwrap().unwrap();
    assert_eq!(g.reward, 20000.0 + 1000.0 + 200.0 + 30.0 + 1.0);
    assert_eq!(g.detail, "the task, as text");
    let g = f.graders.score("echo", Input::default()).await.unwrap().unwrap();
    assert_eq!((g.reward, g.detail.as_str()), (0.0, ""));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_grader_that_cannot_grade_gives_no_reward() {
    let f = Fixture::new("fail", 64);
    let input = Input { runs: vec![run(0, "")], ..Input::default() };
    let e = f.graders.score("fail", input.clone()).await.unwrap().unwrap_err();
    assert_eq!(e, "no answer");
    let e = f.graders.score("trap", input.clone()).await.unwrap().unwrap_err();
    assert_eq!(e, "the grader hit a wasm trap: wasm `unreachable` instruction executed");
    let e = f.graders.score("nan", input.clone()).await.unwrap().unwrap_err();
    assert_eq!(e, "the grader gave a reward of NaN");
    // No runs is 0 over 0.
    let e = f.graders.score("fraction", Input::default()).await.unwrap().unwrap_err();
    assert!(e.contains("NaN"), "{e}");

    let t = Instant::now();
    let e = f.graders.score("spin", input.clone()).await.unwrap().unwrap_err();
    let took = t.elapsed();
    assert_eq!(e, "the grader ran past its 300ms");
    assert!(took >= Duration::from_millis(290) && took < Duration::from_secs(5), "{took:?}");
    // The engine is fine after.
    assert_eq!(f.graders.score("fraction", input).await.unwrap().unwrap().reward, 1.0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_grader_gets_no_more_memory_than_it_is_allowed() {
    let input = Input { runs: vec![run(0, "")], ..Input::default() };
    let small = Fixture::new("small", 64);
    let e = small.graders.score("hog", input.clone()).await.unwrap().unwrap_err();
    assert!(e.starts_with("the grader hit a wasm trap"), "{e}");
    let big = Fixture::new("big", 256);
    assert_eq!(big.graders.score("hog", input).await.unwrap().unwrap().reward, 1.0);
}

#[tokio::test(flavor = "multi_thread")]
async fn only_components_of_the_grader_world_are_graders() {
    let f = Fixture::new("world", 64);
    let bad = |e: hive_types::Error| {
        assert_eq!(e.reason, Reason::InvalidArgument, "{e:?}");
        e.message
    };
    for name in ["none", "../fraction", ".hidden", ""] {
        let m = bad(f.graders.check(name).await.unwrap_err());
        assert!(m.starts_with("no grader named"), "{m}");
    }
    std::fs::write(f.dir.join("core.wasm"), "(module)").unwrap();
    std::fs::write(f.dir.join("other.wasm"), "(component)").unwrap();
    std::fs::write(f.dir.join("junk.wasm"), "not wasm").unwrap();
    for name in ["core", "other", "junk"] {
        let m = bad(f.graders.check(name).await.unwrap_err());
        assert!(m.starts_with(&format!("grader {name}: ")), "{m}");
        let m = bad(f.graders.score(name, Input::default()).await.unwrap_err());
        assert!(m.starts_with(&format!("grader {name}: ")), "{m}");
    }
    f.graders.check("fraction").await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_grader_that_changes_is_compiled_again() {
    let f = Fixture::new("change", 64);
    let input = Input { task: b"x".to_vec(), runs: vec![run(0, "x")], ..Input::default() };
    let t = Instant::now();
    assert_eq!(f.graders.score("exact", input.clone()).await.unwrap().unwrap().reward, 1.0);
    let first = t.elapsed();
    let t = Instant::now();
    for _ in 0..100 {
        f.graders.score("exact", input.clone()).await.unwrap().unwrap();
    }
    let after = t.elapsed() / 100;
    println!("first grade {first:?}, then {after:?} each");
    assert!(after < first, "{after:?} vs {first:?}");
    // Another grader under the same name, with a different length so the change is seen even
    // within the file system's time granularity.
    std::fs::write(f.dir.join("exact.wasm"), graders::component("echo")).unwrap();
    let g = f.graders.score("exact", input).await.unwrap().unwrap();
    assert_eq!((g.reward, g.detail.as_str()), (10000.0, "x"));
}
