//! Policy plugins: components of the policy world asked about a cell, which allow it, change it
//! or turn it away, within their time and memory.

#![cfg(target_os = "linux")]

#[path = "support/policies.rs"]
mod policies;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use hive_cell_wasm::{Change, Policies, PolicyConfig, Verdict};
use hive_types::{Backend, CellSpec, Reason, Source};

struct Fixture {
    policies: Policies,
    dir: PathBuf,
}

impl Fixture {
    fn new(name: &str, mem_mib: u64) -> Self {
        let dir = std::env::temp_dir().join(format!("hive-policy-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        for name in ["allow", "small", "rl", "echo", "trap", "spin", "hog"] {
            std::fs::write(dir.join(format!("{name}.wasm")), policies::component(name)).unwrap();
        }
        let cfg = PolicyConfig { dir: dir.clone(), timeout: Duration::from_millis(100), mem_mib };
        Self { policies: Policies::new(cfg).unwrap(), dir }
    }

    async fn decide(&self, name: &str, project: &str, spec: &CellSpec) -> Verdict {
        self.policies.decide(name, project, spec).await.unwrap().unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn spec() -> CellSpec {
    let mut s = CellSpec::new(Source::Image("python".into()), Backend::Container);
    s.resources.mem_mib = 1024;
    s
}

#[tokio::test(flavor = "multi_thread")]
async fn a_policy_allows_changes_or_turns_away() {
    let f = Fixture::new("verdicts", 64);
    assert_eq!(f.decide("allow", "p", &spec()).await, Verdict::Allow);

    assert_eq!(f.decide("small", "p", &spec()).await, Verdict::Allow);
    let mut big = spec();
    big.resources.mem_mib = 8192;
    assert_eq!(f.decide("small", "p", &big).await, Verdict::Deny("more than 4 GiB".into()));

    let rl = Change {
        network_profile: Some("none".into()),
        hard_ttl: Some(Duration::from_secs(3600)),
        labels: vec![("policy".into(), "rl".into())],
    };
    assert_eq!(f.decide("rl", "web", &spec()).await, Verdict::Allow);
    assert_eq!(f.decide("rl", "rl", &spec()).await, Verdict::Allow);
    assert_eq!(f.decide("rl", "rl-train", &spec()).await, Verdict::Change(rl.clone()));
    let mut long = spec();
    long.hard_ttl = Some(Duration::from_secs(7200));
    assert_eq!(f.decide("rl", "rl-train", &long).await, Verdict::Change(rl.clone()));
    let mut short = spec();
    short.hard_ttl = Some(Duration::from_secs(600));
    let Verdict::Change(c) = f.decide("rl", "rl-train", &short).await else { panic!() };
    assert_eq!(c.hard_ttl, Some(Duration::from_secs(600)));

    let mut s = spec();
    s.network_profile = "mirrors".into();
    s.labels.insert("policy".into(), "web".into());
    s.labels.insert("step".into(), "3".into());
    rl.apply(&mut s);
    assert_eq!((s.network_profile.as_str(), s.hard_ttl), ("none", Some(Duration::from_secs(3600))));
    assert_eq!(s.labels["policy"], "rl");
    assert_eq!(s.labels["step"], "3");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_policy_sees_the_whole_request() {
    let f = Fixture::new("echo", 64);
    let mut s = CellSpec::new(Source::Snapshot("snap-41".into()), Backend::Microvm);
    s.resources.vcpu_milli = 1500;
    s.resources.mem_mib = 768;
    s.resources.disk_gib = 3;
    s.qos = hive_types::Qos::BestEffort;
    s.network_profile = "lookup".into();
    s.trusted_image = true;
    s.labels.insert("a-step".into(), "412".into());
    s.labels.insert("z".into(), "last".into());
    s.env.insert("HOME".into(), "/root".into());
    s.env.insert("TOKEN".into(), "secret".into());
    let Verdict::Change(c) = f.decide("echo", "rl-train", &s).await else { panic!() };
    assert_eq!(c.network_profile.as_deref(), Some("microvm"));
    let want = 2 * 10u64.pow(17) + 1500 * 10u64.pow(12) + 768 * 10u64.pow(6) + 3000 + 200 + 20 + 1;
    assert_eq!(c.hard_ttl, Some(Duration::from_secs(want)));
    let labels: Vec<(&str, &str)> =
        c.labels.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    assert_eq!(
        labels,
        [
            ("project", "rl-train"),
            ("source", "snap-41"),
            ("qos", "best_effort"),
            ("network", "lookup"),
            ("a-step", "412"),
            ("env", "TOKEN"),
        ]
    );
    // The values in the env never reach the policy.
    assert!(!c.labels.iter().any(|(_, v)| v.contains("secret")));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_policy_that_breaks_gives_no_verdict() {
    let f = Fixture::new("broken", 64);
    let failed = |name| {
        let f = &f;
        async move { f.policies.decide(name, "p", &spec()).await.unwrap().unwrap_err() }
    };
    assert!(failed("trap").await.contains("unreachable"), "{}", failed("trap").await);
    let t = Instant::now();
    let e = failed("spin").await;
    assert!(e.contains("ran past its 100ms"), "{e}");
    assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());
    assert!(failed("hog").await.contains("unreachable"));
    let roomy = Fixture::new("roomy", 256);
    assert_eq!(roomy.decide("hog", "p", &spec()).await, Verdict::Allow);
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_and_broken_files_are_errors_and_a_new_file_is_used() {
    let f = Fixture::new("files", 64);
    f.policies.check("rl").await.unwrap();
    for name in ["nope", "../rl", ""] {
        let e = f.policies.check(name).await.unwrap_err();
        assert_eq!(e.reason, Reason::InvalidArgument, "{name}");
    }
    std::fs::write(f.dir.join("junk.wasm"), b"not a component").unwrap();
    let e = f.policies.check("junk").await.unwrap_err();
    assert!(e.message.contains("policy junk"), "{}", e.message);

    let mut big = spec();
    big.resources.mem_mib = 8192;
    std::fs::write(f.dir.join("swap.wasm"), policies::component("allow")).unwrap();
    assert_eq!(f.decide("swap", "p", &big).await, Verdict::Allow);
    std::fs::write(f.dir.join("swap.wasm"), policies::component("small")).unwrap();
    assert!(matches!(f.decide("swap", "p", &big).await, Verdict::Deny(_)));
}

/// What a verdict costs, printed, over 2000 of them.
#[tokio::test(flavor = "multi_thread")]
async fn a_verdict_is_cheap() {
    let f = Fixture::new("cost", 64);
    f.policies.check("rl").await.unwrap();
    let mut s = spec();
    s.labels.insert("step".into(), "1".into());
    s.env.insert("HOME".into(), "/root".into());
    let mut times = Vec::new();
    for i in 0..2000 {
        let project = if i % 2 == 0 { "rl-train" } else { "web" };
        let t = Instant::now();
        f.policies.decide("rl", project, &s).await.unwrap().unwrap();
        times.push(t.elapsed());
    }
    times.sort();
    println!(
        "a verdict from the rl policy: p50 {:?} p99 {:?}",
        times[times.len() / 2],
        times[times.len() * 99 / 100]
    );
}
