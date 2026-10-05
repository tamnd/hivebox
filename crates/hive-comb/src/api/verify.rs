//! `Verify.Run`, from `spec/11_rl_integration.md` section 5: the changes a subject cell made to a
//! git checkout are checked in a fresh cell of their own, so the policy under training never sees
//! the tests that grade it.
//!
//! The diff is taken against the checkout's `HEAD` with a throwaway index, so the subject's own
//! index is left alone and new files count. Changes to protected paths are left out and reported.
//! The verifier cell gets no network, then the diff, then the caller's files such as hidden tests,
//! and runs the command as many times as asked. It is stopped when the call ends, however it ends.

use super::{Api, invalid, millis, parse_id, project, status};
use crate::comb::CreateRequest;
use bytes::Bytes;
use hive_drone::Client;
use hive_proto::convert;
use hive_proto::drone::api as drone;
use hive_proto::v1;
use hive_proto::v1::verify_server::Verify;
use hive_types::{CellId, Error, Reason};
use std::collections::BTreeMap;
use std::time::Instant;
use tonic::{Request, Response, Status};

/// Where the diff is left in the subject and put in the verifier.
const PATCH: &str = "/tmp/hive-verify.patch";
/// The biggest diff a subject may hand over.
const MAX_DIFF: usize = 32 << 20;
/// Output kept from the last run.
const OUTPUT_TAIL: usize = 64 << 10;
/// The most runs one call may ask for.
const MAX_REPEATS: u32 = 16;
/// Takes the diff with an index of its own, so the subject's staged changes count the same as
/// unstaged ones and its own index is not touched.
const TAKE_DIFF: &str = r#"set -e
export GIT_INDEX_FILE="$(mktemp -u)"
trap 'rm -f "$GIT_INDEX_FILE"' EXIT
git read-tree HEAD
git add -A
git diff --cached --binary --no-color --no-ext-diff --no-renames HEAD > /tmp/hive-verify.patch"#;

#[tonic::async_trait]
impl Verify for Api {
    async fn run(
        &self,
        req: Request<v1::VerifyRequest>,
    ) -> Result<Response<v1::VerifyResult>, Status> {
        let project = project(&req)?;
        let r = req.into_inner();
        let subject = match r.subject_cell_id.as_str() {
            "" => None,
            id => {
                let id = parse_id(id)?;
                self.owned(&project, id).map_err(status)?;
                Some(id)
            }
        };
        if subject.is_some() && r.workdir.is_empty() {
            return Err(invalid("a subject needs a workdir to take the diff in"));
        }
        if r.argv.is_empty() {
            return Err(invalid("the verifier needs a command"));
        }
        if r.repeats > MAX_REPEATS {
            return Err(invalid(format!("at most {MAX_REPEATS} repeats")));
        }
        let mut spec =
            convert::spec_from_v1(r.verifier.clone().unwrap_or_default()).map_err(status)?;
        spec.network_profile = "none".into();
        let job = Job {
            api: self,
            subject,
            timeout_ms: millis(r.timeout)?,
            scores: BTreeMap::new(),
            req: r,
        };
        Ok(Response::new(job.run(project, spec).await))
    }
}

/// One verification as it goes.
struct Job<'a> {
    api: &'a Api,
    subject: Option<CellId>,
    req: v1::VerifyRequest,
    timeout_ms: u64,
    scores: BTreeMap<String, f64>,
}

/// How a verification went before it became a result.
struct Done {
    passed: bool,
    flaky: bool,
    runs_passed: u32,
    last: Option<drone::RunResult>,
    tampered: Vec<String>,
}

impl Job<'_> {
    async fn run(mut self, project: String, spec: hive_types::CellSpec) -> v1::VerifyResult {
        let mut tampered = Vec::new();
        let mut diff = Vec::new();
        if let Some(id) = self.subject {
            let t = Instant::now();
            match self.take_diff(id).await {
                Ok(d) => (diff, tampered) = screen(&d, &self.req.protected_paths),
                Err(e) => return failed(e, tampered, self.scores),
            }
            self.time("diff", t);
            self.scores.insert("diff_bytes".into(), diff.len() as f64);
        }
        let t = Instant::now();
        let req = CreateRequest { spec, project, idem_key: None, anyway: false };
        let cell = match self.api.comb.create(req).await {
            Ok(c) => c.id,
            Err(e) => return failed(e, tampered, self.scores),
        };
        self.time("create", t);
        let result = self.check(cell, &diff).await;
        let _ = self.api.comb.stop(cell, None).await;
        match result {
            Ok(mut done) => {
                done.tampered = tampered;
                self.result(done)
            }
            Err(e) => failed(e, tampered, self.scores),
        }
    }

    /// The subject's changes to its checkout, as a patch `git apply` takes.
    async fn take_diff(&self, id: CellId) -> Result<Bytes, Error> {
        let drone = self.api.comb.drone(id).await?;
        let out = self.sh(&drone, TAKE_DIFF, 0).await?;
        if out.exit_code != 0 {
            return Err(file_error("taking the diff in the subject", &out));
        }
        let read = drone::FsRead { path: PATCH.into(), offset: 0, length: 0 };
        let diff = drone.fs_read(&read, MAX_DIFF).await?;
        let _ = drone.fs_remove(&drone::FsPath { path: PATCH.into(), ..Default::default() }).await;
        Ok(diff)
    }

    /// Puts the diff and the caller's files in the verifier cell and runs the command.
    async fn check(&mut self, cell: CellId, diff: &[u8]) -> Result<Done, Error> {
        let drone = self.api.comb.drone(cell).await?;
        let t = Instant::now();
        if !diff.is_empty() {
            let put = drone::FsWrite {
                path: PATCH.into(),
                data: Bytes::copy_from_slice(diff),
                ..Default::default()
            };
            drone.fs_write(&put).await?;
            let out =
                self.sh(&drone, "git apply --whitespace=nowarn /tmp/hive-verify.patch", 0).await?;
            if out.exit_code != 0 {
                return Err(file_error("the subject's changes do not apply", &out));
            }
        }
        for (path, data) in &self.req.files {
            let path = if path.starts_with('/') {
                path.clone()
            } else {
                format!("{}/{path}", self.req.workdir.trim_end_matches('/'))
            };
            let put = drone::FsWrite {
                path,
                data: data.clone(),
                make_parents: true,
                ..Default::default()
            };
            drone.fs_write(&put).await?;
        }
        self.time("apply", t);
        let t = Instant::now();
        let mut runs_passed = 0;
        let mut last = None;
        let repeats = self.req.repeats.max(1);
        for _ in 0..repeats {
            let command = drone::Command {
                argv: self.req.argv.clone(),
                cwd: self.req.workdir.clone(),
                timeout_ms: self.timeout_ms,
                ..Default::default()
            };
            let out = drone
                .run(&drone::RunRequest { command: Some(command), stdin: Default::default() })
                .await?;
            if out.exit_code == 0 && out.signal == 0 && !out.timed_out {
                runs_passed += 1;
            }
            last = Some(out);
        }
        self.time("run", t);
        Ok(Done {
            passed: runs_passed == repeats,
            flaky: runs_passed != 0 && runs_passed != repeats,
            runs_passed,
            last,
            tampered: Vec::new(),
        })
    }

    /// Runs a shell script in the workdir, with git trusting a checkout someone else owns.
    async fn sh(
        &self,
        drone: &Client,
        script: &str,
        timeout_ms: u64,
    ) -> Result<drone::RunResult, Error> {
        let env = [
            ("GIT_CONFIG_COUNT", "2"),
            ("GIT_CONFIG_KEY_0", "safe.directory"),
            ("GIT_CONFIG_VALUE_0", "*"),
            ("GIT_CONFIG_KEY_1", "core.quotePath"),
            ("GIT_CONFIG_VALUE_1", "false"),
        ];
        let command = drone::Command {
            shell: script.into(),
            cwd: self.req.workdir.clone(),
            env: env.into_iter().map(|(k, v)| (k.into(), v.into())).collect(),
            timeout_ms,
            ..Default::default()
        };
        drone.run(&drone::RunRequest { command: Some(command), stdin: Default::default() }).await
    }

    fn time(&mut self, step: &str, since: Instant) {
        self.scores.insert(format!("{step}_ms"), since.elapsed().as_secs_f64() * 1000.0);
    }

    fn result(mut self, done: Done) -> v1::VerifyResult {
        let mut output = Vec::new();
        let mut exit_code = 0;
        if let Some(last) = &done.last {
            output.extend_from_slice(&last.stdout);
            output.extend_from_slice(&last.stderr);
            if output.len() > OUTPUT_TAIL {
                output.drain(..output.len() - OUTPUT_TAIL);
            }
            exit_code = if last.timed_out || last.signal != 0 { -1 } else { last.exit_code };
            for (k, v) in test_counts(&last.stdout) {
                self.scores.insert(k.into(), v);
            }
        }
        v1::VerifyResult {
            passed: done.passed,
            exit_code,
            output: output.into(),
            scores: self.scores.into_iter().collect(),
            error: None,
            tampered: done.tampered,
            flaky: done.flaky,
            runs_passed: done.runs_passed,
        }
    }
}

fn failed(e: Error, tampered: Vec<String>, scores: BTreeMap<String, f64>) -> v1::VerifyResult {
    v1::VerifyResult {
        error: Some(convert::error_to_v1(&e)),
        tampered,
        scores: scores.into_iter().collect(),
        exit_code: -1,
        ..Default::default()
    }
}

/// A command in a cell that failed, which is the cell's doing rather than hivebox's.
fn file_error(what: &str, out: &drone::RunResult) -> Error {
    let stderr = String::from_utf8_lossy(&out.stderr);
    let tail = stderr.trim().lines().rev().take(5).collect::<Vec<_>>();
    let tail = tail.into_iter().rev().collect::<Vec<_>>().join("\n");
    Error::new(Reason::FileError, format!("{what}: exit {}: {tail}", out.exit_code))
}

/// The diff without the files that match a protected glob, and the paths of those files.
fn screen(diff: &[u8], protected: &[String]) -> (Vec<u8>, Vec<String>) {
    let mut kept = Vec::with_capacity(diff.len());
    let mut tampered = Vec::new();
    for part in parts(diff) {
        let paths = paths(part);
        // A part whose paths cannot be read is kept out too, since there is no telling what it
        // touches.
        let hit = if paths.is_empty() {
            Some("?".to_owned())
        } else {
            paths.into_iter().find(|p| protected.iter().any(|g| glob(g, p)))
        };
        match hit {
            Some(p) if !protected.is_empty() => tampered.push(p),
            _ => kept.extend_from_slice(part),
        }
    }
    (kept, tampered)
}

/// Splits a diff into one part per file, each starting at its `diff --git` line.
fn parts(diff: &[u8]) -> Vec<&[u8]> {
    let mut starts: Vec<usize> = Vec::new();
    for (i, w) in diff.windows(11).enumerate() {
        if w == b"diff --git " && (i == 0 || diff[i - 1] == b'\n') {
            starts.push(i);
        }
    }
    let mut out = Vec::with_capacity(starts.len());
    for (n, &s) in starts.iter().enumerate() {
        let end = starts.get(n + 1).copied().unwrap_or(diff.len());
        out.push(&diff[s..end]);
    }
    out
}

/// The paths a part touches, from its header lines, without the `a/` and `b/`.
fn paths(part: &[u8]) -> Vec<String> {
    let mut out = Vec::new();
    for line in part.split(|&b| b == b'\n') {
        if line.starts_with(b"@@") || line.starts_with(b"GIT binary patch") {
            break;
        }
        let line = String::from_utf8_lossy(line);
        let path = if let Some(rest) = line.strip_prefix("diff --git ") {
            same_halves(rest)
        } else {
            line.strip_prefix("--- ").or_else(|| line.strip_prefix("+++ ")).map(unquote)
        };
        if let Some(p) = path {
            let p = p.strip_prefix("a/").or_else(|| p.strip_prefix("b/")).unwrap_or(&p).to_owned();
            if p != "/dev/null" && !p.is_empty() && !out.contains(&p) {
                out.push(p);
            }
        }
    }
    out
}

/// The path in `a/P b/P`, the header of a file that kept its name. Quoted names are left to the
/// `---` and `+++` lines.
fn same_halves(rest: &str) -> Option<String> {
    if rest.starts_with('"') {
        return None;
    }
    let n = rest.len();
    if n < 5 || n.is_multiple_of(2) {
        return None;
    }
    let (a, b) = (&rest[..n / 2], &rest[n / 2 + 1..]);
    let (a, b) = (a.strip_prefix("a/")?, b.strip_prefix("b/")?);
    (a == b).then(|| a.to_owned())
}

/// A path as git writes it, taking off the quotes and the backslash escapes it adds to names
/// with odd characters.
fn unquote(p: &str) -> String {
    let p = p.split('\t').next().unwrap_or(p);
    let Some(inner) = p.strip_prefix('"').and_then(|p| p.strip_suffix('"')) else {
        return p.to_owned();
    };
    let mut out = String::with_capacity(inner.len());
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('t') => out.push('\t'),
            Some('n') => out.push('\n'),
            Some(c) => out.push(c),
            None => {}
        }
    }
    out
}

/// Whether `path` matches `glob`, where `*` matches within one path segment, `**` matches any
/// number of whole segments, and `?` matches one character.
fn glob(glob: &str, path: &str) -> bool {
    let g: Vec<&str> = glob.trim_start_matches("./").split('/').collect();
    let p: Vec<&str> = path.split('/').collect();
    segments(&g, &p)
}

fn segments(g: &[&str], p: &[&str]) -> bool {
    match g.split_first() {
        None => p.is_empty(),
        Some((&"**", rest)) => (0..=p.len()).any(|i| segments(rest, &p[i..])),
        Some((first, rest)) => p.split_first().is_some_and(|(q, tail)| {
            segment(first.as_bytes(), q.as_bytes()) && segments(rest, tail)
        }),
    }
}

fn segment(g: &[u8], s: &[u8]) -> bool {
    match (g.split_first(), s.split_first()) {
        (None, None) => true,
        (Some((b'*', rest)), _) => {
            segment(rest, s) || s.split_first().is_some_and(|(_, t)| segment(g, t))
        }
        (Some((b'?', rest)), Some((_, t))) => segment(rest, t),
        (Some((c, rest)), Some((d, t))) => c == d && segment(rest, t),
        _ => false,
    }
}

/// The counts on pytest's last summary line, like `3 failed, 40 passed, 2 skipped in 1.20s`.
fn test_counts(stdout: &[u8]) -> Vec<(&'static str, f64)> {
    let text = String::from_utf8_lossy(stdout);
    let Some(line) = text.lines().rev().find(|l| {
        let l = l.trim_matches(|c: char| c == '=' || c.is_whitespace());
        (l.contains(" passed") || l.contains(" failed") || l.contains(" error"))
            && l.contains(" in ")
    }) else {
        return Vec::new();
    };
    let words: Vec<&str> = line
        .split(|c: char| c == ',' || c.is_whitespace() || c == '=')
        .filter(|w| !w.is_empty())
        .collect();
    let mut out = Vec::new();
    for w in words.windows(2) {
        let Ok(n) = w[0].parse::<f64>() else { continue };
        let name = match w[1] {
            "passed" => "tests_passed",
            "failed" => "tests_failed",
            "error" | "errors" => "tests_errors",
            "skipped" => "tests_skipped",
            _ => continue,
        };
        out.push((name, n));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const DIFF: &str = "\
diff --git a/requests/sessions.py b/requests/sessions.py
index 1111111..2222222 100644
--- a/requests/sessions.py
+++ b/requests/sessions.py
@@ -1 +1 @@
-method = builtin_str(method)
+method = to_native_string(method)
diff --git a/test_requests.py b/test_requests.py
index 3333333..4444444 100644
--- a/test_requests.py
+++ b/test_requests.py
@@ -1 +1 @@
-assert False
+assert True
diff --git a/tests/unit/conftest.py b/tests/unit/conftest.py
new file mode 100644
index 0000000..5555555
--- /dev/null
+++ b/tests/unit/conftest.py
@@ -0,0 +1 @@
+import pytest
";

    #[test]
    fn globs_match_by_segment() {
        assert!(glob("tests/**", "tests/unit/a.py"));
        assert!(glob("**/conftest.py", "conftest.py"));
        assert!(glob("**/conftest.py", "a/b/conftest.py"));
        assert!(glob("test_*.py", "test_requests.py"));
        assert!(!glob("test_*.py", "sub/test_requests.py"));
        assert!(glob(".github/**", ".github/workflows/ci.yml"));
        assert!(!glob("tests/**", "src/tests.py"));
        assert!(glob("pytest.ini", "pytest.ini"));
        assert!(!glob("pytest.ini", "a/pytest.ini"));
        assert!(glob("setup.c?g", "setup.cfg"));
    }

    #[test]
    fn protected_files_are_cut_out_of_the_diff() {
        let protected = ["test_*.py".to_owned(), "**/conftest.py".to_owned()];
        let (kept, tampered) = screen(DIFF.as_bytes(), &protected);
        let kept = String::from_utf8(kept).unwrap();
        assert!(kept.starts_with("diff --git a/requests/sessions.py"));
        assert!(kept.ends_with("+method = to_native_string(method)\n"));
        assert_eq!(tampered, ["test_requests.py", "tests/unit/conftest.py"]);

        let (kept, tampered) = screen(DIFF.as_bytes(), &[]);
        assert_eq!(kept, DIFF.as_bytes());
        assert!(tampered.is_empty());
    }

    #[test]
    fn odd_names_are_read_from_the_minus_and_plus_lines() {
        let diff = "diff --git \"a/tests/we\\\"ird.py\" \"b/tests/we\\\"ird.py\"\n--- \"a/tests/we\\\"ird.py\"\n+++ \"b/tests/we\\\"ird.py\"\n@@ -1 +1 @@\n-a\n+b\n";
        assert_eq!(paths(diff.as_bytes()), ["tests/we\"ird.py"]);
        let (kept, tampered) = screen(diff.as_bytes(), &["tests/**".to_owned()]);
        assert!(kept.is_empty());
        assert_eq!(tampered, ["tests/we\"ird.py"]);
        // A binary file that kept its name has only the header line.
        let bin = "diff --git a/tests/x.bin b/tests/x.bin\nindex 1..2 100644\nGIT binary patch\nliteral 1\n";
        assert_eq!(paths(bin.as_bytes()), ["tests/x.bin"]);
    }

    #[test]
    fn pytest_summaries_become_scores() {
        let out = b"....F\n===== 1 failed, 40 passed, 2 skipped, 1 error in 1.20s =====\n";
        assert_eq!(
            test_counts(out),
            [
                ("tests_failed", 1.0),
                ("tests_passed", 40.0),
                ("tests_skipped", 2.0),
                ("tests_errors", 1.0)
            ]
        );
        assert_eq!(test_counts(b"3 passed in 0.05 seconds\n"), [("tests_passed", 3.0)]);
        assert!(test_counts(b"no tests ran\n").is_empty());
    }
}
