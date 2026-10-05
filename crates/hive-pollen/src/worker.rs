//! The rollout worker, the P3 pattern in `spec/11_rl_integration.md`: it makes each task's
//! sample cells in one batched call, drives the agent in each, verifies the result in a cell of
//! its own and hands back a trajectory as soon as each sample is done.

use crate::task::{Size, Task};
use crate::trajectory::{self, Report, Timings, Trajectory, Turn};
use futures::stream::{self, StreamExt};
use hive_proto::convert;
use hive_sdk::{Backend, Cell, CellSpec, Client, Command, Error, Resources, Source, v1};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, mpsc};

/// How long a sample cell may outlive the agent's budget, for the verifier to take its diff. A
/// cell the worker could not stop, because the worker died, ends at that point anyway.
const GRACE: Duration = Duration::from_secs(900);

/// Runs tasks with at most a set number of sample cells alive at once, so a trainer whose
/// inference is the bottleneck does not leave cells idle. Verifier cells come on top of that, at
/// most one for each live sample.
#[derive(Clone, Debug)]
pub struct Worker {
    client: Client,
    slots: Arc<Semaphore>,
    max: u32,
}

impl Worker {
    /// A worker on `client` with room for `max_inflight_cells` sample cells.
    #[must_use]
    pub fn new(client: Client, max_inflight_cells: u32) -> Self {
        let max = max_inflight_cells.max(1);
        Self { client, slots: Arc::new(Semaphore::new(max as usize)), max }
    }

    /// Runs every task and sends each trajectory to `out` as soon as its sample is done. Tasks
    /// start in order, and a task with more samples than the worker has room for runs in
    /// batches.
    pub async fn run(&self, tasks: Vec<Task>, out: mpsc::Sender<Trajectory>) {
        let start = Instant::now();
        let mut batches = Vec::new();
        for task in tasks {
            let task = Arc::new(task);
            let mut first = 0;
            while first < task.n_samples {
                let n = (task.n_samples - first).min(self.max);
                batches.push((task.clone(), first, n));
                first += n;
            }
        }
        stream::iter(batches)
            .for_each_concurrent(None, |(task, first, n)| {
                let out = out.clone();
                async move { self.batch(&task, first, n, start, &out).await }
            })
            .await;
    }

    /// Samples `first` to `first + n` of `task`.
    async fn batch(
        &self,
        task: &Task,
        first: u32,
        n: u32,
        start: Instant,
        out: &mpsc::Sender<Trajectory>,
    ) {
        // The semaphore is never closed. Each sample hands its slot back when it is done.
        let Ok(permit) = self.slots.acquire_many(n).await else { return };
        permit.forget();
        let queue_ms = ms(start.elapsed());
        let t = Instant::now();
        let made = self.client.create_many(&sample_spec(task), n, None).await;
        let create_ms = ms(t.elapsed());
        let cells = match made {
            Ok(cells) => cells,
            Err(e) => (0..n).map(|_| Err(e.clone())).collect(),
        };
        stream::iter(cells.into_iter().zip(first..))
            .for_each_concurrent(None, |(cell, i)| async move {
                let timings = Timings { queue_ms, create_ms, ..Timings::default() };
                let mut tr = match cell {
                    Ok(cell) => self.sample(task, i, &cell, timings).await,
                    Err(e) => failed(task, i, &e, timings),
                };
                tr.timings.total_ms = ms(start.elapsed());
                self.slots.add_permits(1);
                let _ = out.send(tr).await;
            })
            .await;
    }

    /// Drives the agent in `cell`, verifies what it did and stops the cell.
    async fn sample(&self, task: &Task, i: u32, cell: &Cell, timings: Timings) -> Trajectory {
        let mut tr = Trajectory {
            task_id: task.task_id.clone(),
            sample: i,
            cell_id: cell.id().to_owned(),
            end: "done",
            timings,
            ..Trajectory::default()
        };
        let t = Instant::now();
        let mut failure = None;
        for (n, command) in task.script(i).iter().enumerate() {
            if n >= task.limits.max_turns as usize {
                tr.end = "max_turns";
                break;
            }
            let left = task.limits.wall().saturating_sub(t.elapsed());
            if left.is_zero() {
                tr.end = "max_wall";
                break;
            }
            let cmd = Command::shell(command.clone())
                .cwd(task.workdir.clone())
                .env("HIVE_INSTRUCTION", task.instruction.clone())
                .timeout(left.min(task.limits.command()));
            let c = Instant::now();
            match cell.run(cmd).await {
                Ok(r) => tr.turns.push(Turn {
                    command: command.clone(),
                    exit_code: r.exit_code,
                    timed_out: r.timed_out,
                    output: trajectory::tail(&r.stdout, &r.stderr),
                    ms: ms(c.elapsed()),
                }),
                Err(e) => {
                    failure = Some(e);
                    break;
                }
            }
            if t.elapsed() >= task.limits.wall() {
                tr.end = "max_wall";
                break;
            }
        }
        tr.timings.agent_ms = ms(t.elapsed());
        if let Some(e) = failure {
            tr.end = "error";
            set_error(&mut tr, &e);
        } else {
            let t = Instant::now();
            match self.client.verify(verify_request(task, cell.id())).await {
                Ok(r) => {
                    if let Some(e) = &r.error {
                        tr.error = Some(format!("{}: {}", e.reason, e.message));
                        tr.is_infra_error = e.is_infra_error;
                    }
                    tr.passed = r.passed && r.error.is_none();
                    tr.tampered = r.tampered;
                    tr.verify = Some(Report {
                        runs_passed: r.runs_passed,
                        flaky: r.flaky,
                        exit_code: r.exit_code,
                        scores: r.scores.into_iter().collect(),
                        output: trajectory::tail(&r.output, b""),
                    });
                }
                Err(e) => set_error(&mut tr, &e),
            }
            tr.timings.verify_ms = ms(t.elapsed());
        }
        // A cell that is already gone is fine, and a stop that fails leaves the cell to its TTL.
        let _ = cell.stop().await;
        tr.reward = trajectory::reward(
            tr.is_infra_error,
            tr.passed,
            !tr.tampered.is_empty(),
            task.verify.zero_on_tamper,
        );
        tr
    }
}

/// A sample whose cell was never made.
fn failed(task: &Task, i: u32, e: &Error, timings: Timings) -> Trajectory {
    let mut tr = Trajectory {
        task_id: task.task_id.clone(),
        sample: i,
        end: "error",
        timings,
        ..Trajectory::default()
    };
    set_error(&mut tr, e);
    tr.reward = trajectory::reward(tr.is_infra_error, false, false, true);
    tr
}

fn set_error(tr: &mut Trajectory, e: &Error) {
    tr.error = Some(e.to_string());
    tr.is_infra_error = e.reason.is_infra();
}

fn spec(image: &str, size: Size) -> CellSpec {
    let mut s = CellSpec::new(Source::Image(image.to_owned()), Backend::Container);
    s.resources =
        Resources { mem_mib: size.mem_mib, vcpu_milli: size.vcpu_milli, ..Resources::DEFAULT };
    s
}

/// The spec of a task's sample cells, labelled with the task.
fn sample_spec(task: &Task) -> CellSpec {
    let mut s = spec(&task.image, task.cell);
    s.labels.clone_from(&task.labels);
    s.labels.insert("task".into(), task.task_id.clone());
    s.hard_ttl = Some(task.limits.wall() + GRACE);
    s
}

fn verify_request(task: &Task, subject: &str) -> v1::VerifyRequest {
    let v = &task.verify;
    let image = v.image.as_deref().unwrap_or(&task.image);
    v1::VerifyRequest {
        subject_cell_id: subject.to_owned(),
        verifier: Some(convert::spec_to_v1(&spec(image, v.cell.unwrap_or(task.cell)))),
        argv: v.argv.clone(),
        timeout: (v.timeout_s > 0)
            .then(|| convert::duration_to_v1(Duration::from_secs(v.timeout_s))),
        workdir: task.workdir.clone(),
        protected_paths: v.protected_paths.clone(),
        files: v.files.iter().map(|(k, c)| (k.clone(), c.clone().into_bytes().into())).collect(),
        repeats: v.repeats,
    }
}

fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task() -> Task {
        Task::parse(
            r#"{"task_id":"psf__requests-2317","image":"swe","workdir":"/testbed",
                "n_samples":3,"labels":{"step":"7"},"cell":{"mem_mib":512,"vcpu_milli":500},
                "limits":{"max_wall_s":60},
                "policy":{"script":[["true"]]},
                "verify":{"argv":["pytest","-q"],"protected_paths":["tests/**"],
                          "files":{"hidden.py":"assert 1\n"},"repeats":3,"timeout_s":30,
                          "cell":{"mem_mib":2048,"vcpu_milli":2000}}}"#,
        )
        .unwrap()
    }

    #[test]
    fn sample_cells_carry_the_task_and_a_ttl() {
        let s = sample_spec(&task());
        assert_eq!(s.labels["task"], "psf__requests-2317");
        assert_eq!(s.labels["step"], "7");
        assert_eq!(s.hard_ttl, Some(Duration::from_secs(60) + GRACE));
        assert_eq!((s.resources.mem_mib, s.resources.vcpu_milli), (512, 500));
        s.validate().unwrap();
    }

    #[test]
    fn the_verify_request_follows_the_task() {
        let r = verify_request(&task(), "c1");
        assert_eq!(r.subject_cell_id, "c1");
        assert_eq!(r.workdir, "/testbed");
        assert_eq!(r.repeats, 3);
        assert_eq!(r.timeout.unwrap().seconds, 30);
        assert_eq!(&r.files["hidden.py"][..], b"assert 1\n");
        let res = r.verifier.unwrap().resources.unwrap();
        assert_eq!((res.mem_mib, res.vcpu_milli), (2048, 2000));
    }
}
