//! What the worker hands back: one trajectory per sample, as a line of JSON.

use serde::Serialize;
use std::collections::BTreeMap;

/// Output kept from each command and from the verifier.
pub(crate) const OUTPUT_TAIL: usize = 4 << 10;

/// One sample from start to end.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Trajectory {
    /// The task's `task_id`.
    pub task_id: String,
    /// Which of the task's samples this is, from 0.
    pub sample: u32,
    /// The sample's cell, empty when it was never made.
    pub cell_id: String,
    /// The commands the agent ran, in order.
    pub turns: Vec<Turn>,
    /// 1 or 0, or none when the sample is masked because hivebox failed it.
    pub reward: Option<f64>,
    /// Every verifier run passed.
    pub passed: bool,
    /// Protected paths the agent changed. Those changes were not applied.
    pub tampered: Vec<String>,
    /// What the verifier said, when it ran.
    pub verify: Option<Report>,
    /// Why the agent's turns ended: `done`, `max_turns`, `max_wall` or `error`.
    pub end: &'static str,
    /// The failure was hivebox's, not the agent's, so the trainer should mask the sample.
    pub is_infra_error: bool,
    /// What failed, as `REASON: message`.
    pub error: Option<String>,
    /// Where the time went.
    pub timings: Timings,
}

/// One command.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Turn {
    /// The shell command.
    pub command: String,
    /// Its exit code, or -1 when a signal or the timeout ended it.
    pub exit_code: i32,
    /// The command's timeout ended it.
    pub timed_out: bool,
    /// The end of stdout and then stderr.
    pub output: String,
    /// How long it took.
    pub ms: u64,
}

/// The verifier's verdict.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Report {
    /// Runs of the tests that passed.
    pub runs_passed: u32,
    /// The runs did not all agree.
    pub flaky: bool,
    /// The last run's exit code.
    pub exit_code: i32,
    /// Test counts, the diff's size and step times, as `Verify.Run` has them.
    pub scores: BTreeMap<String, f64>,
    /// The end of the last run's output.
    pub output: String,
}

/// Where the time went, in milliseconds.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct Timings {
    /// Waiting for room under the worker's cell limit.
    pub queue_ms: u64,
    /// Making the sample cells, for the whole batch the sample was in.
    pub create_ms: u64,
    /// The agent's turns.
    pub agent_ms: u64,
    /// The verifier, from the diff to the last run.
    pub verify_ms: u64,
    /// From the worker taking the task to the trajectory.
    pub total_ms: u64,
}

/// The reward rule: masked when hivebox failed, 1 when the tests passed, and 0 when they did not
/// or when the agent changed a protected path and the task says that counts against it.
#[must_use]
pub fn reward(infra: bool, passed: bool, tampered: bool, zero_on_tamper: bool) -> Option<f64> {
    if infra {
        None
    } else if passed && !(tampered && zero_on_tamper) {
        Some(1.0)
    } else {
        Some(0.0)
    }
}

/// The last [`OUTPUT_TAIL`] bytes of `a` then `b`, as text.
pub(crate) fn tail(a: &[u8], b: &[u8]) -> String {
    let mut out = Vec::with_capacity(a.len() + b.len());
    out.extend_from_slice(a);
    out.extend_from_slice(b);
    if out.len() > OUTPUT_TAIL {
        out.drain(..out.len() - OUTPUT_TAIL);
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewards_follow_the_rule() {
        assert_eq!(reward(true, true, false, true), None);
        assert_eq!(reward(false, true, false, true), Some(1.0));
        assert_eq!(reward(false, false, false, true), Some(0.0));
        assert_eq!(reward(false, true, true, true), Some(0.0));
        assert_eq!(reward(false, true, true, false), Some(1.0));
    }

    #[test]
    fn output_keeps_the_end() {
        assert_eq!(tail(b"out\n", b"err\n"), "out\nerr\n");
        let long = vec![b'a'; OUTPUT_TAIL];
        let t = tail(&long, b"end");
        assert_eq!(t.len(), OUTPUT_TAIL);
        assert!(t.ends_with("aend"));
    }
}
