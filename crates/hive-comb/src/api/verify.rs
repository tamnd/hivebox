//! `Verify.Run`, from `spec/11_rl_integration.md` section 5: the changes a subject cell made to a
//! git checkout are checked in a fresh cell of their own, so the policy under training never sees
//! the tests that grade it.
//!
//! The diff is taken against the checkout's `HEAD` with a throwaway index, so the subject's own
//! index is left alone and new files count. Changes to protected paths are left out and reported.
//! The verifier takes the same diff of its own before anything is put in it, which is what the
//! image came with, such as a build directory or a package's egg-info. A file both diffs change the
//! same way is left as the image has it, and one the subject changed or put back is undone first.
//! The comb's own protected paths count on top of the caller's, and by default they are the files
//! that set up a test run, such as `conftest.py`, so a change cannot make every test pass with them.
//! The verifier cell gets no network, then the diff, then the caller's files such as hidden tests,
//! and runs the command as many times as asked. It is stopped when the call ends, however it ends.
//!
//! An exit code is easy for the subject to fake, with a `sys.exit(0)` in the code the tests load.
//! A verify that names a JUnit report passes a run only on what the report says, and a run cut
//! short leaves none.
//!
//! A verify can name a grader, a reward plugin on the node, to turn what the runs did into a
//! reward. It is given every run's output, the caller's task data and the files asked for, and
//! runs after the verifier cell is stopped, in the comb, with its own memory and time limits.

use super::junit::{self, Verdict};
use super::{Api, Args, Call, invalid, millis, parse_id, status};
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
/// Where the image's own changes the subject did not keep are put in the verifier.
const UNDO: &str = "/tmp/hive-verify-undo.patch";
/// The error when the image's changes cannot be taken back out.
const UNDONE: &str = "the image's changes the subject did not keep do not undo";
/// The error when the subject's changes cannot be put in.
const APPLIED: &str = "the subject's changes do not apply";
/// The biggest diff a subject may hand over.
const MAX_DIFF: usize = 32 << 20;
/// Output kept from the last run.
const OUTPUT_TAIL: usize = 64 << 10;
/// The biggest report read.
const MAX_REPORT: usize = 16 << 20;
/// The most runs one call may ask for.
const MAX_REPEATS: u32 = 16;
/// The most files one call may hand its grader.
const MAX_GRADER_FILES: usize = 64;
/// The biggest file a grader is handed.
const MAX_GRADER_FILE: usize = 16 << 20;
/// Takes the diff with an index of its own, so the subject's staged changes count the same as
/// unstaged ones and its own index is not touched. The index starts as a copy of the checkout's,
/// set back to `HEAD`, so git keeps what it knew of each file and only reads the ones that
/// changed. Without that it reads every file, which on a big repo in a lazy image is seconds.
/// The inode numbers and change times are not the ones the image was built with, so only the
/// size and the modification time are compared.
const TAKE_DIFF: &str = r#"set -e
index="$(git rev-parse --git-path index)"
export GIT_INDEX_FILE="$(mktemp -u)"
trap 'rm -f "$GIT_INDEX_FILE"' EXIT
if ! { cp -p "$index" "$GIT_INDEX_FILE" && git read-tree -m HEAD; } 2> /dev/null; then
  rm -f "$GIT_INDEX_FILE"
  git read-tree HEAD
fi
git -c core.checkStat=minimal -c core.trustCtime=false add -A
git diff --cached --binary --no-color --no-ext-diff --no-renames HEAD > /tmp/hive-verify.patch"#;

#[tonic::async_trait]
impl Verify for Api {
    async fn run(
        &self,
        req: Request<v1::VerifyRequest>,
    ) -> Result<Response<v1::VerifyResult>, Status> {
        let call = Call::new(self, &req, "verify.run")?;
        let r = req.into_inner();
        let cell = r.subject_cell_id.clone();
        let mut files: Vec<_> = r.files.iter().collect();
        files.sort_unstable();
        let args = files
            .into_iter()
            .fold(Args::default().num(r.files.len() as u64), |a, (k, v)| a.str(k).bytes(v))
            .str(&r.subject_cell_id)
            .strs(&r.argv)
            .str(&format!("{:?}", r.timeout))
            .str(&r.workdir)
            .strs(&r.protected_paths)
            .num(r.repeats.into())
            .str(&r.report)
            .strs(&r.must_pass);
        // Only a call that asks for a grader hashes its fields, so the calls before there were
        // graders hash the same as they did.
        let args = if r.grader.is_empty() && r.task.is_empty() && r.grader_files.is_empty() {
            args
        } else {
            args.str(&r.grader).bytes(&r.task).strs(&r.grader_files)
        };
        let job = async {
            let subject = match r.subject_cell_id.as_str() {
                "" => None,
                id => {
                    let id = parse_id(id)?;
                    self.owned(&call.project, id).map_err(status)?;
                    Some(id)
                }
            };
            if subject.is_some() && r.workdir.is_empty() {
                return Err(invalid("a subject needs a workdir to take the diff in"));
            }
            if r.argv.is_empty() {
                return Err(invalid("the verifier needs a command"));
            }
            if !r.must_pass.is_empty() && r.report.is_empty() {
                return Err(invalid("must_pass needs a report to find the tests in"));
            }
            if !r.report.is_empty() && !r.report.starts_with('/') && r.workdir.is_empty() {
                return Err(invalid("a relative report path needs a workdir"));
            }
            if r.repeats > MAX_REPEATS {
                return Err(invalid(format!("at most {MAX_REPEATS} repeats")));
            }
            if r.grader.is_empty() && (!r.task.is_empty() || !r.grader_files.is_empty()) {
                return Err(invalid("task and grader_files are for a grader, and none is named"));
            }
            if r.grader_files.len() > MAX_GRADER_FILES {
                return Err(invalid(format!("at most {MAX_GRADER_FILES} grader files")));
            }
            if r.workdir.is_empty() && r.grader_files.iter().any(|p| !p.starts_with('/')) {
                return Err(invalid("a relative grader file needs a workdir"));
            }
            if !r.grader.is_empty() {
                self.graders().map_err(status)?.check(&r.grader).await.map_err(status)?;
            }
            let mut spec =
                convert::spec_from_v1(r.verifier.clone().unwrap_or_default()).map_err(status)?;
            spec.network_profile = "none".into();
            let timeout_ms = millis(r.timeout)?;
            Ok((spec, Job { api: self, subject, timeout_ms, scores: BTreeMap::new(), req: r }))
        }
        .await;
        let (spec, job) = call.check(&cell, &args, job)?;
        // The verifier's spec as the comb reads it, whose maps are in key order.
        let args = args.str(&format!("{spec:?}"));
        let result = job.run(call.project.clone(), spec).await;
        let how = match &result.error {
            Some(e) => format!("error={}", e.reason),
            None => format!(
                "passed={} flaky={} runs_passed={} exit={}",
                result.passed, result.flaky, result.runs_passed, result.exit_code
            ),
        };
        let tampered = if result.tampered.is_empty() { "" } else { " tampered" };
        call.record(&cell, &args, format!("ok {how}{tampered}"));
        Ok(Response::new(result))
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
    /// What the last run's report said, when one was asked for.
    report: Option<Verdict>,
    tampered: Vec<String>,
    /// Every run, kept only for a grader.
    runs: Vec<drone::RunResult>,
    /// The files the grader asked for, as they were after the last run.
    files: Vec<(String, Option<Vec<u8>>)>,
}

impl Job<'_> {
    async fn run(mut self, project: String, spec: hive_types::CellSpec) -> v1::VerifyResult {
        let mut tampered = Vec::new();
        let mut diff = None;
        if let Some(id) = self.subject {
            let t = Instant::now();
            match self.take_diff(id).await {
                Ok(d) => diff = Some(d),
                Err(e) => return failed(e, tampered, self.scores),
            }
            self.time("diff", t);
        }
        let t = Instant::now();
        let req = CreateRequest { spec, project, idem_key: None, anyway: false };
        let cell = match self.api.comb.create(req).await {
            Ok(c) => c.id,
            Err(e) => return failed(e, tampered, self.scores),
        };
        self.time("create", t);
        let result = self.check(cell, diff.as_deref(), &mut tampered).await;
        let _ = self.api.comb.stop(cell, None).await;
        match result {
            Ok(mut done) => {
                done.tampered = tampered;
                let grade = self.grade(&mut done).await;
                let mut result = self.result(done);
                match grade {
                    Some(Ok((reward, detail))) => {
                        result.reward = Some(reward);
                        result.grade_detail = detail;
                    }
                    Some(Err(e)) => result.grade_error = e,
                    None => {}
                }
                result
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

    /// Puts the subject's diff and the caller's files in the verifier cell and runs the command.
    async fn check(
        &mut self,
        cell: CellId,
        diff: Option<&[u8]>,
        tampered: &mut Vec<String>,
    ) -> Result<Done, Error> {
        let drone = self.api.comb.drone(cell).await?;
        let mut patches = Vec::new();
        if let Some(diff) = diff {
            let t = Instant::now();
            let base = self.take_diff(cell).await?;
            self.time("base", t);
            let node = &self.api.comb.inner.cfg.protected_paths;
            let protected: Vec<String> =
                node.iter().chain(&self.req.protected_paths).cloned().collect();
            let s = screen(diff, &base, &protected);
            *tampered = s.tampered;
            self.scores.insert("diff_bytes".into(), s.apply.len() as f64);
            self.scores.insert("undo_bytes".into(), s.undo.len() as f64);
            patches = vec![(UNDO, s.undo, "-R ", UNDONE), (PATCH, s.apply, "", APPLIED)];
        }
        let t = Instant::now();
        for (path, patch, how, what) in patches {
            if patch.is_empty() {
                continue;
            }
            let put =
                drone::FsWrite { path: path.into(), data: patch.into(), ..Default::default() };
            drone.fs_write(&put).await?;
            let out =
                self.sh(&drone, &format!("git apply {how}--whitespace=nowarn {path}"), 0).await?;
            if out.exit_code != 0 {
                return Err(file_error(what, &out));
            }
        }
        for (path, data) in &self.req.files {
            let put = drone::FsWrite {
                path: self.path(path),
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
        let mut report = None;
        let report_path = (!self.req.report.is_empty()).then(|| self.path(&self.req.report));
        let repeats = self.req.repeats.max(1);
        let grading = !self.req.grader.is_empty();
        let mut runs = Vec::new();
        for _ in 0..repeats {
            if let Some(path) = &report_path {
                let gone = drone::FsPath { path: path.clone(), ..Default::default() };
                let _ = drone.fs_remove(&gone).await;
            }
            let command = drone::Command {
                argv: self.req.argv.clone(),
                cwd: self.req.workdir.clone(),
                timeout_ms: self.timeout_ms,
                ..Default::default()
            };
            let out = drone
                .run(&drone::RunRequest { command: Some(command), stdin: Default::default() })
                .await?;
            let mut passed = out.exit_code == 0 && out.signal == 0 && !out.timed_out;
            if let Some(path) = &report_path {
                let read = drone::FsRead { path: path.clone(), offset: 0, length: 0 };
                let xml = match drone.fs_read(&read, MAX_REPORT).await {
                    Ok(xml) => Some(xml),
                    // A report that is not there, or too big, is the run's doing.
                    Err(e) if !e.reason.is_infra() => None,
                    Err(e) => return Err(e),
                };
                let cases = xml.and_then(|x| junit::cases(&x));
                let verdict = junit::judge(cases.as_deref(), &self.req.must_pass);
                passed &= verdict.ok;
                report = Some(verdict);
            }
            runs_passed += u32::from(passed);
            if grading {
                runs.push(out.clone());
            }
            last = Some(out);
        }
        self.time("run", t);
        let mut files = Vec::with_capacity(self.req.grader_files.len());
        for path in &self.req.grader_files {
            let read = drone::FsRead { path: self.path(path), offset: 0, length: 0 };
            let data = match drone.fs_read(&read, MAX_GRADER_FILE).await {
                Ok(data) => Some(data.to_vec()),
                // A file that is not there, or too big, is the run's doing.
                Err(e) if !e.reason.is_infra() => None,
                Err(e) => return Err(e),
            };
            files.push((path.clone(), data));
        }
        Ok(Done {
            passed: runs_passed == repeats,
            flaky: runs_passed != 0 && runs_passed != repeats,
            runs_passed,
            last,
            report,
            tampered: Vec::new(),
            runs,
            files,
        })
    }

    /// What the grader made of the runs, when one was asked for.
    #[cfg(target_os = "linux")]
    async fn grade(&mut self, done: &mut Done) -> Option<Result<(f64, String), String>> {
        if self.req.grader.is_empty() {
            return None;
        }
        let input = hive_cell_wasm::Input {
            task: self.req.task.to_vec(),
            runs: std::mem::take(&mut done.runs)
                .into_iter()
                .map(|r| hive_cell_wasm::Run {
                    exit_code: if r.timed_out || r.signal != 0 { -1 } else { r.exit_code },
                    timed_out: r.timed_out,
                    stdout: r.stdout.to_vec(),
                    stderr: r.stderr.to_vec(),
                    wall_ms: r.wall_nanos / 1_000_000,
                })
                .collect(),
            files: std::mem::take(&mut done.files),
            passed: done.passed,
            tampered: done.tampered.clone(),
        };
        let t = Instant::now();
        let graders = match self.api.graders() {
            Ok(g) => g,
            Err(e) => return Some(Err(e.message)),
        };
        let grade = graders.score(&self.req.grader, input).await;
        self.time("grade", t);
        Some(grade.unwrap_or_else(|e| Err(e.message)).map(|g| (g.reward, g.detail)))
    }

    /// Off Linux there are no graders, and a call that names one is refused before it gets here.
    #[cfg(not(target_os = "linux"))]
    async fn grade(&mut self, _: &mut Done) -> Option<Result<(f64, String), String>> {
        None
    }

    /// `path` in the verifier, a relative one being under the workdir.
    fn path(&self, path: &str) -> String {
        if path.starts_with('/') {
            path.to_owned()
        } else {
            format!("{}/{path}", self.req.workdir.trim_end_matches('/'))
        }
    }

    /// Runs a shell script in the workdir, with git trusting a checkout someone else owns.
    async fn sh(
        &self,
        drone: &Client,
        script: &str,
        timeout_ms: u64,
    ) -> Result<drone::RunResult, Error> {
        git_sh(drone, &self.req.workdir, script, timeout_ms).await
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
            // The counts printed are only a fallback, as the subject's code can print anything.
            if done.report.is_none() {
                for (k, v) in test_counts(&last.stdout) {
                    self.scores.insert(k.into(), v);
                }
            }
        }
        let mut not_passed = Vec::new();
        if let Some(report) = done.report {
            self.scores.insert("report".into(), f64::from(u8::from(!report.counts.is_empty())));
            for (k, v) in report.counts {
                self.scores.insert(k.into(), v);
            }
            not_passed = report.not_passed;
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
            not_passed,
            ..Default::default()
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
/// Runs a shell script in `cwd`, with git trusting a checkout someone else owns.
pub(super) async fn git_sh(
    drone: &Client,
    cwd: &str,
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
        cwd: cwd.into(),
        env: env.into_iter().map(|(k, v)| (k.into(), v.into())).collect(),
        timeout_ms,
        ..Default::default()
    };
    drone.run(&drone::RunRequest { command: Some(command), stdin: Default::default() }).await
}

pub(super) fn file_error(what: &str, out: &drone::RunResult) -> Error {
    let stderr = String::from_utf8_lossy(&out.stderr);
    let tail = stderr.trim().lines().rev().take(5).collect::<Vec<_>>();
    let tail = tail.into_iter().rev().collect::<Vec<_>>().join("\n");
    Error::new(Reason::FileError, format!("{what}: exit {}: {tail}", out.exit_code))
}

/// What goes in the verifier, from the subject's diff and the verifier's own, both against `HEAD`.
#[derive(Debug, Default)]
struct Screened {
    /// The image's changes the subject did not keep, to take back out first.
    undo: Vec<u8>,
    /// The subject's changes, past what the image came with.
    apply: Vec<u8>,
    /// The protected paths the subject changed, which the verifier keeps as the image has them.
    tampered: Vec<String>,
}

/// Splits the subject's diff into what the verifier undoes and applies, leaving out the files that
/// match a protected glob. A file `base`, the verifier's own diff, changes the same way is left
/// alone.
fn screen(diff: &[u8], base: &[u8], protected: &[String]) -> Screened {
    // A part whose paths cannot be read is kept out too, since there is no telling what it
    // touches.
    let hit = |paths: &[String]| {
        if protected.is_empty() {
            None
        } else if paths.is_empty() {
            Some("?".to_owned())
        } else {
            paths.iter().find(|p| protected.iter().any(|g| glob(g, p))).cloned()
        }
    };
    let ours: Vec<_> = parts(diff).into_iter().map(|p| (paths(p), p)).collect();
    let image: Vec<_> = parts(base).into_iter().map(|p| (paths(p), p)).collect();
    let mut out = Screened { apply: Vec::with_capacity(diff.len()), ..Default::default() };
    for (paths, part) in &ours {
        if image.iter().any(|(_, b)| b == part) {
            continue;
        }
        match hit(paths) {
            Some(p) => out.tampered.push(p),
            None => out.apply.extend_from_slice(part),
        }
    }
    for (paths, part) in &image {
        if ours.iter().any(|(_, s)| s == part) {
            continue;
        }
        match hit(paths) {
            // A protected file the subject changed was named above.
            Some(p) if !ours.iter().any(|(s, _)| s == paths) => out.tampered.push(p),
            Some(_) => {}
            None => out.undo.extend_from_slice(part),
        }
    }
    out
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
        let s = screen(DIFF.as_bytes(), b"", &protected);
        let kept = String::from_utf8(s.apply).unwrap();
        assert!(kept.starts_with("diff --git a/requests/sessions.py"));
        assert!(kept.ends_with("+method = to_native_string(method)\n"));
        assert_eq!(s.tampered, ["test_requests.py", "tests/unit/conftest.py"]);
        assert!(s.undo.is_empty());

        let s = screen(DIFF.as_bytes(), b"", &[]);
        assert_eq!(s.apply, DIFF.as_bytes());
        assert!(s.tampered.is_empty());
    }

    #[test]
    fn the_default_paths_cut_out_what_sets_up_a_test_run() {
        let node: Vec<String> =
            crate::config::PROTECTED_PATHS.iter().map(|&p| p.to_owned()).collect();
        let s = screen(DIFF.as_bytes(), b"", &node);
        assert!(String::from_utf8(s.apply).unwrap().contains("b/test_requests.py"));
        assert_eq!(s.tampered, ["tests/unit/conftest.py"]);
        let hit = |p: &str| node.iter().any(|g| glob(g, p));
        assert!(hit("conftest.py"));
        assert!(hit("setup.cfg"));
        assert!(hit("pkg/tox.ini"));
        assert!(hit("requests.egg-info/PKG-INFO"));
        assert!(hit("venv/lib/python3.12/site-packages/pytest-8.3.dist-info/RECORD"));
        assert!(!hit("requests/sessions.py"));
        assert!(!hit("pyproject.toml"));
        assert!(!hit("tests/test_config.py"));
    }

    #[test]
    fn what_the_image_came_with_is_left_alone() {
        let built = "diff --git a/build/lib/x.py b/build/lib/x.py\nnew file mode 100644\nindex 0000000..1111111\n--- /dev/null\n+++ b/build/lib/x.py\n@@ -0,0 +1 @@\n+x = 1\n";
        let egg = "diff --git a/r.egg-info/PKG-INFO b/r.egg-info/PKG-INFO\nnew file mode 100644\nindex 0000000..2222222\n--- /dev/null\n+++ b/r.egg-info/PKG-INFO\n@@ -0,0 +1 @@\n+Name: r\n";
        let tmp = "diff --git a/tmp.txt b/tmp.txt\nnew file mode 100644\nindex 0000000..3333333\n--- /dev/null\n+++ b/tmp.txt\n@@ -0,0 +1 @@\n+t\n";
        let tmp2 = "diff --git a/tmp.txt b/tmp.txt\nnew file mode 100644\nindex 0000000..4444444\n--- /dev/null\n+++ b/tmp.txt\n@@ -0,0 +1 @@\n+u\n";
        let node: Vec<String> =
            crate::config::PROTECTED_PATHS.iter().map(|&p| p.to_owned()).collect();
        let base = format!("{built}{egg}{tmp}");

        // Kept as the image had it, so nothing is applied and the egg-info is not tampering.
        let s = screen(base.as_bytes(), base.as_bytes(), &node);
        assert!(s.apply.is_empty() && s.undo.is_empty() && s.tampered.is_empty());

        // The subject changed one file the image came with and took the build directory out.
        let ours = format!("{egg}{tmp2}{DIFF}");
        let s = screen(ours.as_bytes(), base.as_bytes(), &node);
        assert_eq!(String::from_utf8(s.undo).unwrap(), format!("{built}{tmp}"));
        assert!(String::from_utf8(s.apply).unwrap().starts_with(tmp2));
        assert_eq!(s.tampered, ["tests/unit/conftest.py"]);

        // Taking out a protected file the image came with is tampering, and it stays.
        let s = screen(tmp.as_bytes(), base.as_bytes(), &node);
        assert_eq!(String::from_utf8(s.undo).unwrap(), built);
        assert_eq!(s.tampered, ["r.egg-info/PKG-INFO"]);
    }

    #[test]
    fn odd_names_are_read_from_the_minus_and_plus_lines() {
        let diff = "diff --git \"a/tests/we\\\"ird.py\" \"b/tests/we\\\"ird.py\"\n--- \"a/tests/we\\\"ird.py\"\n+++ \"b/tests/we\\\"ird.py\"\n@@ -1 +1 @@\n-a\n+b\n";
        assert_eq!(paths(diff.as_bytes()), ["tests/we\"ird.py"]);
        let s = screen(diff.as_bytes(), b"", &["tests/**".to_owned()]);
        assert!(s.apply.is_empty());
        assert_eq!(s.tampered, ["tests/we\"ird.py"]);
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
