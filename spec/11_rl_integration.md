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

What is in now: the `hive-pollen` binary and library. It reads tasks as lines of JSON (task id, image, instruction, `n_samples`, workdir, cell size, labels, limits on turns, wall time and each command's time, the policy and the verify settings) and writes one trajectory per sample as a line of JSON as soon as the sample is done. It makes each task's sample cells in one `create_many` call, labelled with the task and given a hard TTL of the wall limit plus 15 minutes, keeps at most `--max-inflight` sample cells alive, runs the agent's commands in the workdir with `HIVE_INSTRUCTION` set, checks each sample with `Verify.Run` in a cell of its own and stops the cell. The reward is 1 when every verifier run passed and no protected path was changed, 0 otherwise, and masked when hivebox failed the sample. A task whose `verify` names a `grader` gets the grader's reward in place of the 1, and none when the grader could not grade it. The only policy so far is a script of shell commands per sample, which covers an agent harness that runs inside the cell (P2) and replays of recorded turns or patches. Prefetch, an agent loop that calls a model, token counts and keeping cells paused for resampling are still to come.

On server3 at a load average of 45 to 70, with two tasks of eight samples on the swe-requests-2317 image, the verifier as in §5 (a hidden test plus 32 of the task's tests, three runs), and four scripts per task that do nothing, apply the gold fix, apply it and edit `test_requests.py`, and apply a wrong fix, every one of the 48 trajectories got the right reward: 1 for the gold fix, 0 for the rest, with the edit reported as tampered. Throughput and times:

| sample cells in flight | 16 samples took | trajectories a minute | create, median | verify, median |
|---|---|---|---|---|
| 1 | 99.9 s | 9.6 | 260 ms | 5.3 s |
| 4 | 47.0 s | 20.4 | 496 ms | 9.9 s |
| 8 | 34.9 s | 27.5 | 888 ms | 14.7 s |

The verifier is most of each sample's time, and it slows down as more run at once on the 8 core host, so the gain from 4 to 8 is smaller than from 1 to 4.

## 3. Trainer adapters

| Framework | Integration point | Notes |
|---|---|---|
| verl | `AgentLoopBase` subclass `HiveAgentLoop` + `BaseTool` impls (`bash`, `str_replace_editor`, `submit`) | async server mode, multi-turn |
| slime | E2B-compatible endpoint (zero code) or native `hive` rollout function | slime uses E2B for SWE examples |
| AReaL | `RolloutWorkflow` using the async SDK | fully async; interruptible generation |
| ROLL / ROCK | ROCK-compatible env provider (`GEM` API) | |
| SkyRL | `SkyRL-gym` env backed by hivebox | |
| OpenRLHF / NeMo-RL / TRL | `hivebox.openenv.create_hivebox_app`, an OpenEnv server whose episodes are a shell in a fresh cell | scored by a check in the cell or by `Verify.Run` |
| Harbor / Terminal-Bench | `hivebox.harbor:HiveboxEnvironment`, a Harbor environment that is one cell per trial | reuse datasets unchanged, with images imported on the node |
| SWE-agent / SWE-ReX | `hivebox.swerex.HiveboxDeployment`, a SWE-ReX deployment whose runtime is a cell | no swerex server in the image |
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

What is in now: `Verify.Run` on the comb's local API, `hivectl verify` and `Client.verify()` in the SDK. The request names the subject cell, the git checkout it changed (`workdir`), the verifier cell's spec, the command, the protected globs, files to add such as hidden tests, and how many runs. The comb takes the subject's diff against `HEAD` with an index of its own, so staged, unstaged and new files all count and the subject's own index is left alone. It drops every file that matches a protected glob and names it in `tampered`, makes the verifier cell with network `none`, applies the rest with `git apply`, writes the added files and runs the command. The comb's own `[verify] protected_paths` count on top of the request's. By default they are `conftest.py`, `pytest.ini`, `.pytest.ini`, `tox.ini` and `setup.cfg` in any directory and anything under an `.egg-info` or `.dist-info` directory, the files pytest reads before it runs a test, so a caller that names no paths still gets a verify that a `conftest.py` hook cannot turn into a pass. `pyproject.toml` is not on the list, since fixes change it too, so a caller whose project keeps its pytest settings there should add it. An operator who wants only the caller's paths sets the list to `[]`. `passed` means every run exited with 0, and `flaky` means the runs did not agree. With `report`, the path of a JUnit XML report the command writes, such as pytest's `--junitxml`, a run passes only when it also left that report with tests in it, none failed or errored, and every test in `must_pass` passed. `must_pass` takes pytest node ids, as SWE-bench's `FAIL_TO_PASS` and `PASS_TO_PASS` have them. The comb removes the report before each run, so one left over or brought in by the diff does not count, and the tests in `must_pass` the last report did not show passing come back in `not_passed`. That catches a `sys.exit(0)` or `os._exit(0)` in the code under test, which ends the run with 0 before any report is written. The code under test still runs in the test process and could write a report of its own, but it would have to know the path and name every test the verifier asks for. A diff that does not apply or a workdir that is not a git checkout is a `FILE_ERROR`, which is not an infra error. `scores` has the test counts from the last run's report, or pytest's summary line without one, and the time of each step, and the verifier cell is stopped however the call ends. Not in yet: snapshot sources, layer diffs, test bundles stored ahead of time, random test order, TAP results and the route through the gate.

What is in now for reward plugins: a verify can name a `grader`, a WebAssembly component of the world `hivebox:reward/grader` in `crates/hive-cell-wasm/wit/reward.wit`, kept as `<name>.wasm` in the node's `[verify] graders` dir (`graders` in the data dir by default). The comb checks the grader is there and compiles it before it makes the verifier cell, so a wrong name costs no cell, and compiles it again when the file changes. After the last run it reads the files in `grader_files` from the verifier cell, stops the cell and calls the grader's `score` in the comb with every run's exit code, output and wall time, the caller's `task` bytes, the files (each with its bytes, or none when it was not there), `passed` and the tampered paths. The reward comes back in `reward` and the grader's note in `grade_detail`. A grader imports nothing, so it can only compute, and runs with its own memory cap (`grader_mem_mib`, 256 by default) and time limit (`grader_timeout`, 10 s by default). A grader that returns an error, traps, runs out of time or gives a reward that is not a finite number leaves `reward` unset and says why in `grade_error`, and the rest of the result stands. On server3 (8 vCPU at a load of 86 to 99), with python container verifiers: the grade step took 0.50 ms at p50 and 0.82 ms at p90 over 12 calls, and a verify of `print(6 * 7)` took 415 ms at p50 with the exact match grader and 429 ms without one, so the grader is lost in the cell's own time. The fraction grader over 16 coin flips gave exactly `runs_passed / 16` three times out of three. Sixteen runs of 1 MB of stdout each took 67 ms to grade. A spinning grader with a 1 s limit stopped at 1.24 s, a grader that wanted 128 MiB under a 64 MiB cap trapped, and a missing grader was refused in 2.7 ms. 32 graded verifies at once all got their reward, with the grade step at 1.46 ms at p50. In a tight loop in release, one grade took 244 us at p50, of which 25 us was making the instance, and 640 at once ran at 1083 grades a second. `hivectl verify --grader NAME --task TEXT --grader-file PATH`, `Client.verify(grader=, task=, grader_files=)`, `verify.grader` in pollen tasks and in the verl adapter all reach it.

On server3 at load 77 to 88, with the swe-requests-2317 task image, a hidden test for its bug and 32 of the task's own tests, three runs per verify and three verifies per case: the subject as it came failed 0 of 3 runs on the hidden test, the gold fix (a 396 byte diff) passed 3 of 3, and the gold fix plus edits to `test_requests.py` and a new `pytest.ini` passed 3 of 3 with both files reported as tampered. Over the 9 verifies the median step was 119 ms to take the diff, 849 ms to make the verifier cell, 17 ms to apply the diff and 4.4 s for the three runs, and a whole `hivectl verify` took a median of 6.1 s (4.4 to 11.6 s).

The default protected paths, on server3 at load 88 to 96 with a request that named none: a `conftest.py` hook that turns failed tests into passes, at the top or under `tests`, and a `setup.cfg` or `pytest.ini` that deselects the hidden tests all passed with the list set to `[]`, the hook even with a JUnit report and `must_pass`. With the default list all of them failed with the file named in `tampered`, the fix alone passed either way, and taking and screening the diff took a median of 70 ms against 89 ms with the list off, which is noise at that load.

## 6. LLM route (network profile `llm`)

- Cells reach `llm.hive.internal`, a link-local VIP. The node DNS proxy and an eBPF redirect send that traffic to the LLM gateway. The gateway is an OpenAI/Anthropic-compatible proxy that forwards to the trainer's inference engines (vLLM/SGLang).
- The gateway records token-in and token-out per request, tagged with the cell's label (`rollout_id`). This gives exact training tokens without re-tokenization (the ProRL Agent and slime approach) and lets P2 harnesses run unmodified.
- The node proxy injects a per-cell auth header. The cell never sees the trainer credentials.

What is in now: the gateway runs in the comb on `169.254.77.81:80`, and the DNS proxy answers `llm.hive.internal` with that address for `llm` cells only. There is no eBPF redirect, as cells connect to the address itself and the guard lets only `llm` cells through to it. The trainer drives it with the `Llm` service: `SetRoute` sets the project's engine and key, `Hold` answers new calls with 503 and Retry-After around a weight sync and waits for the calls in flight, and `Turns` returns a rollout's calls with the prompt ids, the sampled ids, their log probabilities and the token counts, in order. For chat and completions calls the gateway asks the engine for `return_token_ids`, as vLLM takes it, and for log probabilities when `[network.llm] logprobs` is set, then takes out of the answer what the cell did not ask for, streamed or not, so the agent sees what it would see from the engine itself. Anthropic style `/v1/messages` calls are forwarded and recorded without ids, since the engines do not return them there. The Python SDK has it as `hive.llm`. Through the gate, a route or a hold goes to every node and the calls left in flight are added up, and a rollout's turns are gathered from every node and put in order, with the nodes that did not answer listed, while a cell's turns come from its own node alone.

On server3 at load 87 to 104, with 16 `llm` cells in container cells from the python image and a vLLM style stub engine on the same host serving SmolLM2-135M-Instruct on the CPU through transformers (vLLM itself does not run on that VM): the echo model, which answers at once, took 2.51 ms p50 and 43.42 ms p99 from the host straight to the engine, and 3.85 ms p50 and 49.21 ms p99 from a cell through the gateway, with the gateway's own leg to the engine and back at 1.84 ms p50. 16 cells calling at once made 3,200 calls in 14.8 s, 216 calls a second at 26.1 ms p50 and 124.1 ms p99, with the comb using 1.33 ms of CPU per call, and no answer carried token ids or log probabilities back to a cell. 16 three turn chats on the real model, half of them streamed, made 48 calls and 4,804 tokens, and for all 48 the prompt ids, the output ids and the text the cell got matched what the engine logged, the log probabilities matched to the bit, and the engine saw the trainer's key rather than the cell's. With the 16 cells calling in a loop, a hold with a 120 s drain waited 39.26 s for the calls in flight and left none, the engine got no calls while it held, and all 31 calls the cells made ended well and were kept, 12 of them after waiting out the hold.

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
