# RL integration: hive-pollen

> hivebox exists to feed RL trainers. This file defines how rollouts, rewards and trainer control flow use the platform. It covers GRPO-style grouped rollouts, async and off-policy pipelines, weight-sync pauses, preemption, verification, and infra-error masking.

## 1. Integration patterns

| Pattern | Who drives the agent loop | Examples | hivebox role |
|---|---|---|---|
| P1 Trainer-side loop | trainer process (AgentLoop) calls LLM + tools; tools = hivebox exec | verl AgentLoop, slime, AReaL, SkyRL-gym, rLLM | SDK; cells per trajectory |
| P2 In-cell harness | agent harness (OpenHands, mini-SWE-agent, Claude-Code-like CLI, Terminus) runs inside the cell and calls the LLM via route | DSec, Harbor, ProRL Agent, MiniMax Forge | LLM route (network profile `llm`), token capture at gateway |
| P3 Rollout service | a separate `pollen` rollout service manages the env lifecycle; trainer submits tasks and receives trajectories | ROCK/ROLL, ProRL Agent server, MegaFlow | `hive-pollen` worker |

All three are supported. P3 is the reference implementation.

## 2. `hive-pollen` rollout worker

```
Trainer ──(Task{task_id, image, instruction, n_samples, policy_endpoint, limits})──▶ pollen
pollen:
  1. Images.Prefetch(image) on likely nodes (ahead of step)
  2. Cells.Create(template(image), count=n_samples, labels{step, task, group}) -- one batched call
  3. per cell: run harness (P1 or P2), stream events, enforce budgets (turns, tokens, wall)
  4. on finish: Files.Diff / Snapshots.Snapshot(DISK) -> Verify.Run (separate cell)
  5. emit Trajectory{task_id, sample_idx, messages/tokens, reward, verifier_report,
                     cause, is_infra_error, tamper_signals, timings, cell_usage}
  6. Stop cells (or keep paused for resampling/forking)
```

- The worker is async by default. Trajectories stream back as they finish (AReaL, slime, and Kimi-style partial rollout), and the trainer decides how much staleness to accept.
- The worker applies backpressure. It enforces `max_inflight_cells` per trainer to match GPU inference throughput and avoid idle sandboxes. DSec found that the rollout, not the sandbox, is usually the bottleneck.

## 3. Trainer adapters

| Framework | Integration point | Notes |
|---|---|---|
| verl | `AgentLoopBase` subclass `HiveAgentLoop` + `BaseTool` impls (`bash`, `str_replace_editor`, `submit`) | async server mode, multi-turn |
| slime | E2B-compatible endpoint (zero code) or native `hive` rollout function | slime uses E2B for SWE examples |
| AReaL | `RolloutWorkflow` using the async SDK | fully async; interruptible generation |
| ROLL / ROCK | ROCK-compatible env provider (`GEM` API) | |
| SkyRL | `SkyRL-gym` env backed by hivebox | |
| OpenRLHF / NeMo-RL | generic Gym/OpenEnv adapter | |
| Harbor / Terminal-Bench | `hivebox` environment provider | reuse datasets unchanged |
| rLLM / DeepSWE (R2E-Gym) | R2E env backed by hivebox | |

## 4. Trainer control flow hooks

- Weight sync and step boundary: `Cells.Pause(selector{step=k})` freezes cells while the inference engine reloads weights, then `Resume` continues them. If agents run in-cell (P2), the LLM gateway returns `503 retry-after` during the sync and the harnesses retry.
- Preemption or trainer crash: cells survive. With `checkpoint: on_session_idle`, the worker can resume trajectories from the last tool-call boundary, using the disk snapshot and message log stored with the trajectory.
- Resampling and tree search: `Snapshots.Fork(cell, n)` at a turn boundary gives n children with identical filesystem and memory, for branching (TVCACHE and DeltaBox-style prefix reuse). Children get labels `{parent, branch}`.
- Partial rollouts (Kimi K1.5/K2): long trajectories are paused at step end with `SnapshotKill` and resumed next step on any node.

## 5. Verification (`Verify.Run`)

```
VerifyRequest{ source: cell_id | snapshot_ref,
               base_image, diff_mode: GIT_DIFF | LAYER_DIFF,
               protected_paths: ["tests/**", "**/conftest.py", "pytest.ini", "tox.ini", "setup.cfg", ".github/**"],
               test_bundle: blob_ref,              // hidden tests, stored outside cells
               command, timeout, repeats: k, result_format: JUNIT | TAP | EXIT_CODE | CUSTOM(wasm) }
```

The flow:
1. Extract the diff from the agent cell. Take the `git diff` over tracked files plus new files, excluding protected paths. Changes to protected paths are recorded as tamper signals and not applied.
2. Create the verifier cell from `base_image` plus a verifier toolchain layer, with network `none`.
3. Apply the diff, then copy in `test_bundle`.
4. Run `command` `k` times, randomizing test order if configured.
5. Parse the structured results from a verifier-owned path.
6. Return `VerifyResult{passed, per_test[], flaky: bool, stdout_tail, cause, is_infra_error, tamper_signals}`.

Two more points:
- Reward functions can be plugins. WASM components (WIT `hivebox:reward/score`) run in T0 for custom graders such as OJ checkers or kernel-speed thresholds.
- Flaky tests are excluded. Tests whose results differ across `k` runs on the gold patch are marked in the dataset registry and left out.

What is in now: `Verify.Run` on the comb's local API, `hivectl verify` and `Client.verify()` in the SDK. The request names the subject cell, the git checkout it changed (`workdir`), the verifier cell's spec, the command, the protected globs, files to add such as hidden tests, and how many runs. The comb takes the subject's diff against `HEAD` with an index of its own, so staged, unstaged and new files all count and the subject's own index is left alone. It drops every file that matches a protected glob and names it in `tampered`, makes the verifier cell with network `none`, applies the rest with `git apply`, writes the added files and runs the command. `passed` means every run exited with 0, and `flaky` means the runs did not agree. A diff that does not apply or a workdir that is not a git checkout is a `FILE_ERROR`, which is not an infra error. `scores` has pytest's counts from the last run and the time of each step, and the verifier cell is stopped however the call ends. Not in yet: snapshot sources, layer diffs, test bundles stored ahead of time, random test order, JUnit results, WASM graders and the route through the gate.

On server3 at load 77 to 88, with the swe-requests-2317 task image, a hidden test for its bug and 32 of the task's own tests, three runs per verify and three verifies per case: the subject as it came failed 0 of 3 runs on the hidden test, the gold fix (a 396 byte diff) passed 3 of 3, and the gold fix plus edits to `test_requests.py` and a new `pytest.ini` passed 3 of 3 with both files reported as tampered. Over the 9 verifies the median step was 119 ms to take the diff, 849 ms to make the verifier cell, 17 ms to apply the diff and 4.4 s for the three runs, and a whole `hivectl verify` took a median of 6.1 s (4.4 to 11.6 s).

## 6. LLM route (network profile `llm`)

- Cells reach `llm.hive.internal`, a link-local VIP. The node DNS proxy and an eBPF redirect send that traffic to the LLM gateway. The gateway is an OpenAI/Anthropic-compatible proxy that forwards to the trainer's inference engines (vLLM/SGLang).
- The gateway records token-in and token-out per request, tagged with the cell's label (`rollout_id`). This gives exact training tokens without re-tokenization (the ProRL Agent and slime approach) and lets P2 harnesses run unmodified.
- The node proxy injects a per-cell auth header. The cell never sees the trainer credentials.

## 7. Failure classification and masking

| cause | is_infra_error | Trainer action |
|---|---|---|
| completed, agent_exit, task_timeout, turn/token budget | no | score normally |
| oom, disk_quota, pids_limit, output_limit | no (policy's fault) | score (optionally penalize) |
| policy_violation / tamper | no | penalize or filter |
| infra_node_lost, infra_image, infra_runtime, infra_internal, drone_unreachable, capacity | **yes** | **mask** (drop from loss); optionally retry task |
| verifier infra error | **yes** | mask |

The target is an infra-masked fraction of 0.1% or less. It is tracked per step in metrics. A spike points to a platform regression, not to policy behavior.

## 8. Environment building by agents (DSec RL co-design)

- The env-builder agent gets a T2 cell with network profile `build` (mirrors, github and registries).
- It installs dependencies and runs tests, then calls `Snapshots.Commit` with scrubbing (10 section 8) and provenance. The result is a new workspace layer and manifest.
- A validation job then creates a fresh cell from the manifest. It runs the tests with the gold patch (they must pass) and without it (they must fail). Only after that is the task registered in the dataset registry.
- Scale: DSec builds more than 100K workspaces per week. hivebox imaged dedups at chunk level, so the incremental cost is only the unique bytes.

## 9. Datasets supported out of the box (importers)

Supported datasets are SWE-bench (Full/Verified/Multimodal/Multilingual), SWE-Gym, R2E-Gym, SWE-smith, SWE-rebench, Multi-SWE-bench, Terminal-Bench 2 (Harbor format), and OSWorld (T3). Each has an importer (`hivectl dataset import swe-bench-verified`) that produces manifests, test bundles and templates.
