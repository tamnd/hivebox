//! What a trainer hands the worker: one task per line of JSON.

use serde::Deserialize;
use std::collections::BTreeMap;
use std::time::Duration;

/// One task, run `n_samples` times in cells of its own and verified each time.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Task {
    /// The trainer's name for the task. It goes on every cell as the `task` label.
    pub task_id: String,
    /// The image the samples run in.
    pub image: String,
    /// What the agent is asked to do. Commands see it as `HIVE_INSTRUCTION`.
    #[serde(default)]
    pub instruction: String,
    /// How many samples to run, as in a GRPO group.
    #[serde(default = "one")]
    pub n_samples: u32,
    /// The git checkout the agent works in, where the verifier takes the diff.
    pub workdir: String,
    /// The size of each sample's cell.
    #[serde(default)]
    pub cell: Size,
    /// More labels for every cell, such as the trainer step.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    /// Budgets for each sample.
    #[serde(default)]
    pub limits: Limits,
    /// What drives the agent.
    pub policy: Policy,
    /// How the result is checked.
    pub verify: Verify,
}

/// The size of a cell.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct Size {
    /// Memory in MiB.
    pub mem_mib: u32,
    /// CPU in thousandths of a core.
    pub vcpu_milli: u32,
}

impl Default for Size {
    fn default() -> Self {
        Self { mem_mib: 1024, vcpu_milli: 1000 }
    }
}

/// Budgets for one sample. Whichever runs out first ends the agent's turns, and the sample is
/// still verified.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    /// Most commands the agent may run.
    pub max_turns: u32,
    /// Seconds the agent may take, counted from its cell being up.
    pub max_wall_s: u64,
    /// Seconds one command may take.
    pub command_timeout_s: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self { max_turns: 50, max_wall_s: 1800, command_timeout_s: 600 }
    }
}

impl Limits {
    pub(crate) fn wall(&self) -> Duration {
        Duration::from_secs(self.max_wall_s)
    }

    pub(crate) fn command(&self) -> Duration {
        Duration::from_secs(self.command_timeout_s)
    }
}

/// What drives the agent.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Policy {
    /// Shell commands run in order in the workdir, one list per sample, starting over from the
    /// first list when there are more samples than lists. An agent harness inside the cell, the
    /// P2 pattern in the spec, is a list of one command. A replay of recorded turns or a gold
    /// patch is a list of many.
    Script(Vec<Vec<String>>),
}

/// How a sample is checked, by `Verify.Run` in a cell of its own.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Verify {
    /// The test command, run in the workdir.
    pub argv: Vec<String>,
    /// Globs the agent may not change, like `tests/**`.
    #[serde(default)]
    pub protected_paths: Vec<String>,
    /// Files such as hidden tests, by path, with their contents. A relative path is under the
    /// workdir.
    #[serde(default)]
    pub files: BTreeMap<String, String>,
    /// How many times to run the tests.
    #[serde(default = "one")]
    pub repeats: u32,
    /// The JUnit report the tests write, like `/tmp/report.xml` with `pytest --junitxml`. When
    /// set, a run passes on what the report says and not on its exit code alone.
    #[serde(default)]
    pub report: Option<String>,
    /// Tests the report has to show passing, as pytest node ids, like SWE-bench's `FAIL_TO_PASS`
    /// and `PASS_TO_PASS`.
    #[serde(default)]
    pub must_pass: Vec<String>,
    /// Seconds each run may take, or the node's default when 0.
    #[serde(default)]
    pub timeout_s: u64,
    /// The verifier's image, the task's when unset.
    #[serde(default)]
    pub image: Option<String>,
    /// The verifier cell's size, the sample cell's when unset.
    #[serde(default)]
    pub cell: Option<Size>,
    /// A sample that changed a protected path gets no reward even when the tests pass.
    #[serde(default = "yes")]
    pub zero_on_tamper: bool,
}

const fn one() -> u32 {
    1
}

const fn yes() -> bool {
    true
}

impl Task {
    /// Parses one line.
    ///
    /// # Errors
    ///
    /// The line is not a task, or the task cannot be run.
    pub fn parse(line: &str) -> Result<Self, String> {
        let t: Self = serde_json::from_str(line).map_err(|e| e.to_string())?;
        let Policy::Script(scripts) = &t.policy;
        if scripts.is_empty() {
            return Err(format!("task {}: the script has no lists", t.task_id));
        }
        if t.verify.argv.is_empty() {
            return Err(format!("task {}: the verifier has no command", t.task_id));
        }
        if t.n_samples == 0 {
            return Err(format!("task {}: n_samples is 0", t.task_id));
        }
        Ok(t)
    }

    /// The commands for sample `i`.
    pub(crate) fn script(&self, i: u32) -> &[String] {
        let Policy::Script(scripts) = &self.policy;
        &scripts[i as usize % scripts.len()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_short_task_gets_the_defaults() {
        let t = Task::parse(
            r#"{"task_id":"t1","image":"swe","workdir":"/testbed",
                "policy":{"script":[["true"],["echo a","echo b"]]},
                "verify":{"argv":["pytest"]}}"#,
        )
        .unwrap();
        assert_eq!(t.n_samples, 1);
        assert_eq!(t.cell, Size::default());
        assert_eq!(t.limits, Limits::default());
        assert_eq!(t.verify.repeats, 1);
        assert!(t.verify.zero_on_tamper);
        assert_eq!(t.script(0), ["true"]);
        assert_eq!(t.script(1), ["echo a", "echo b"]);
        assert_eq!(t.script(2), ["true"]);
    }

    #[test]
    fn bad_tasks_are_refused() {
        let base = r#""task_id":"t","image":"i","workdir":"/w""#;
        for bad in [
            format!(r#"{{{base},"policy":{{"script":[]}},"verify":{{"argv":["x"]}}}}"#),
            format!(r#"{{{base},"policy":{{"script":[["x"]]}},"verify":{{"argv":[]}}}}"#),
            format!(
                r#"{{{base},"n_samples":0,"policy":{{"script":[["x"]]}},"verify":{{"argv":["x"]}}}}"#
            ),
            format!(
                r#"{{{base},"typo":1,"policy":{{"script":[["x"]]}},"verify":{{"argv":["x"]}}}}"#
            ),
            format!(r#"{{{base},"policy":{{"llm":{{}}}},"verify":{{"argv":["x"]}}}}"#),
        ] {
            assert!(Task::parse(&bad).is_err(), "{bad}");
        }
    }
}
