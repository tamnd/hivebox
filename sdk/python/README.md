# hivebox for Python

The Python client for hivebox. It talks to a comb's local API over its Unix socket, or to a gate, with grpcio's asyncio API.

```python
import asyncio
from hivebox import AsyncHive, Spec

async def main():
    async with AsyncHive() as hive:
        async with hive.cells.create_many(Spec(image="python", mem_mib=512, labels={"step": "412"}), count=16) as group:
            for cell in group:
                r = await cell.run("python3 -c 'print(6 * 7)'", timeout=30)
                print(cell.id, r.exit_code, r.stdout)
            s = await group[0].session()
            await s.run("cd /work && export PYTHONPATH=src")
            await group[0].files.write("/work/src/a.py", "print('hi')\n")
            print(await group[0].files.read_text("/work/src/a.py"))
        # Every cell in the group is stopped on the way out.

asyncio.run(main())
```

The endpoint is the first of the argument, `$HIVE_ENDPOINT`, or `unix:$HIVE_SOCKET`, and then `unix:/run/hivebox/comb.sock`. A gate is `https://host:port`, or `http://host:port` when it has no TLS, and the token is the argument or `$HIVE_TOKEN`. The project is the argument or `$HIVE_PROJECT`, and a comb puts calls that name none in `local`.

## Fork

`fork` makes up to 16 cells from what a running or paused container cell wrote, for best of N or a tree search from a shared prefix. The files are copied while the cell runs, and the cell is frozen only while the copy is brought up to date. The children start on the cell's image with the copy, its spec and the labels you add. Only the files come along, so each child starts processes of its own.

```python
await cell.run("pip install -e /work/repo && python prepare.py", timeout=600)
kids = await cell.fork(4, labels={"branch": "b"}, idempotency_key="step-412-b")
rewards = await asyncio.gather(*(k.run(f"python try.py --seed {i}") for i, k in enumerate(kids)))
```

A retry with the same key gets the same children back. If one child fails the others are stopped and the error is raised. On XFS with reflink the copy shares blocks with the parent, so a child costs little disk until it writes.

## Verify

`hive.verify` checks what an agent did in one cell by running tests in a fresh cell of its own, with no network, so the policy never sees the tests. It is the reward step of an RL rollout.

```python
r = await hive.verify(
    ["bash", "-c", "python -m pytest -q tests/test_fix.py"],
    subject=cell,
    verifier=Spec(image="swe-requests", mem_mib=1024, cpu_milli=2000),
    workdir="/testbed",
    protected_paths=["tests/**", "**/conftest.py"],
    files={"tests/test_fix.py": hidden_test},
    repeats=3,
)
reward = None if r.is_infra_error else float(r.passed and not r.tampered)
```

The verifier takes the subject's diff against HEAD of the git checkout at `workdir`, leaves out changes to protected paths and lists them in `tampered`, writes `files` in and runs the command `repeats` times. `passed` is every run exiting with 0, `flaky` is the runs not agreeing, and `scores` has pytest's counts, the diff's size and how long each step took. With no subject the image is checked as it is, which is how a gold patch or a flaky test is checked. A check hivebox could not do comes back with `error` set, and `is_infra_error` says the sample should be masked rather than scored.

When pass or fail is not enough, `grader` names a reward plugin on the node, a WebAssembly component in its graders dir. It gets every run's output, the `task` bytes you pass, such as the expected answer, and the files in `grader_files` as the last run left them, and its reward comes back in `r.reward`. A grader that fails leaves `reward` as None and says why in `grade_error`. The world graders implement is in `crates/hive-cell-wasm/wit/reward.wit`.

```python
r = await hive.verify(["python", "solve.py"], verifier=Spec(image="python"), workdir="/work",
                      grader="oj-checker", task=expected_output, grader_files=["answer.txt"])
```

Every failure raises a subclass of `HiveError` named after its reason, like `CellNotFound`, with `is_infra_error` set when the failure was hivebox's and not the command's. A file that is missing raises `FileError`, which is also a `FileNotFoundError`, and the same goes for the other common errno values. Calls that are safe to repeat are tried up to three times when the failure was hivebox's.

## verl

`hivebox.verl` has three tools for verl's tool agent loop, `bash`, `str_replace_editor` and `submit`, and `HiveAgentLoop`, which gives each trajectory a cell of its own and makes the verifier's verdict its reward. It is written against verl 0.9.

```yaml
# tools.yaml, for actor_rollout_ref.rollout.multi_turn.tool_config_path
tools:
  - class_name: hivebox.verl.BashTool
    config: &hive
      type: native
      endpoint: https://gate.example:7401
      task:
        image: swe-requests
        workdir: /testbed
        cell: {mem_mib: 1024, vcpu_milli: 1000}
        limits: {command_timeout_s: 120, max_wall_s: 1800}
        verify:
          argv: [bash, -c, "python -m pytest -q tests/test_fix.py"]
          protected_paths: ["tests/**", "**/conftest.py"]
          cell: {mem_mib: 2048, vcpu_milli: 2000}
  - class_name: hivebox.verl.EditorTool
    config: *hive
  - class_name: hivebox.verl.SubmitTool
    config: *hive
```

```yaml
# agent_loops.yaml, for actor_rollout_ref.rollout.agent.agent_loop_config_path
- name: hive_agent
  _target_: hivebox.verl.loop.HiveAgentLoop
```

Set `agent_name` to `hive_agent` in the dataset, or `actor_rollout_ref.rollout.agent.default_agent_loop` to `hive_agent`. The `task` in the tool config holds the defaults, and a row's `extra_info["hive"]` goes on top of them, so a row only carries what differs, such as `task_id`, `image` or `verify.files` with its hidden tests. The task has the same shape as a `hive-pollen` task. The token comes from `$HIVE_TOKEN`.

The three tools share the trajectory's cell, made on the first tool call, and `bash` keeps one shell, so `cd` and `export` carry over between calls. `submit` checks the changes with `Verify.Run` and only tells the model they were submitted. When the loop ends, `HiveAgentLoop` checks a trajectory that never called `submit` (set `verify_unsubmitted: false` in the task to give it 0 instead), sets `reward_score` to 1 or 0, stops the cell and adds `hive_cell`, `hive_passed`, `hive_infra_error`, `hive_tampered`, `hive_submitted`, `hive_verify_s` and `hive_error` to the sample's extra fields. A sample hivebox failed gets 0 with `hive_infra_error` set, so the trainer can mask it.

The tools also work in verl's plain `tool_agent` loop. There they find the trajectory by verl's request id, `submit` stops the cell, and the reward is the `submit` call's tool reward. A trajectory that never submits is not checked, and its cell ends at its hard TTL, the task's `max_wall_s` plus 15 minutes.

## slime

slime's coding agent example can run its sandboxes on hivebox through the gate's E2B API, with `E2B_API_URL` and `E2B_SANDBOX_URL` set to the gate and `SLIME_AGENT_SANDBOX_IMAGE_METADATA_KEY` set to the gate's `e2b.image_key`. `hivebox.slime` moves the grading into `Verify.Run` as well. Its `run_evaluation` takes what slime's does and grades the same way: a fresh cell from the task's image, the pre commands, the diff by the same ladder of `git apply` and `patch`, then the task's `eval_cmd` or `f2p_script`, with 1 for an exit code of 0. The verifier cell has no network, and the files it needs go in with the call, so grading is one request.

```python
from examples.coding_agent_rl import swe
from hivebox import slime

slime.install(swe, slime.Grader(mem_mib=2048, cpu_milli=2000, repeats=1))
```

`install` swaps `swe.run_evaluation`, which `generate.py` calls, and keeps slime's grader for swepro and SWE-bench tasks. The client comes from `$HIVE_ENDPOINT` and `$HIVE_TOKEN`. When hivebox cannot do the check, `run_evaluation` raises `HiveError`, and `generate` aborts the sample so it is left out of training. The result is still `(reward, applied_cleanly)`, with the verdict in `.verdict` for the test counts and timings. One difference: slime runs the tests as its `agent` user, and the verifier runs them as the cell's default user.

## LLM route

Cells with the `llm` network profile reach the node's LLM gateway at `http://llm.hive.internal`, so an agent that speaks the OpenAI or Anthropic API runs there unchanged with its base URL set to `http://llm.hive.internal/v1`. The gateway sends each call to the project's inference engine with the trainer's key, which the cell never sees, and keeps the token ids the engine saw and sampled, by the cell's `rollout_id` label, so the trainer gets the exact tokens without tokenizing again.

```python
await hive.llm.route("http://10.0.0.5:30000", api_key=key)
cells = await asyncio.gather(*[
    hive.cells.create(Spec(image="swe-requests", backend="container", network_profile="llm", labels={"rollout_id": f"r{i}"}))
    for i in range(16)
])
# ... the agents run ...
for turn in await hive.llm.turns("r3", take=True):
    print(turn.seq, len(turn.prompt_ids), turn.choices[0].output_ids, turn.choices[0].logprobs)

# Around a weight sync:
left = await hive.llm.hold(retry_after=2, drain=60)
await load_new_weights()
await hive.llm.release()
```

For chat and completions calls the gateway asks the engine for `return_token_ids`, as vLLM takes it, and for log probabilities too when the node sets `[network.llm] logprobs`, then takes whatever the cell did not ask for out of the answer, streamed or not. `turns` gives a rollout's calls in order, or one cell's with `cell=`, and `take=True` removes them. When the gateway runs out of the memory it keeps turns in, it drops the rollout changed longest ago and counts the calls in `hive.llm.dropped`. `hold` answers new calls with 503 and Retry-After, which OpenAI's clients wait out, and waits up to `drain` for the calls in flight, returning how many are left. The hold ends with `release`, or by itself after `ttl` (10 minutes unless set).

Through a gate, `route`, `hold` and `release` go to every node, and `hold` returns the calls left in flight on all of them. `turns` for a rollout asks every node too, while `turns` with `cell=` asks only the node the cell is on, which is cheaper when there are many nodes and the trainer knows the cell. A node that did not answer a rollout's `turns` is listed in `hive.llm.unreached`, and its turns are still there for the next call. A comb that restarts, or a node that joins later, goes back to its own route from its config until `route` is called again.

## Development

The stubs in `hivebox/v1` are made from the protos in `crates/hive-proto/proto` by `generate.sh`, and are checked in so installing needs no protoc.

```
pip install -e '.[dev,test]'
./generate.sh
pytest
HIVE_TEST_ENDPOINT=unix:/run/hivebox/comb.sock pytest tests/test_live.py -s
```

The verify test also wants `HIVE_TEST_GIT_IMAGE`, an image with a git checkout at `HIVE_TEST_GIT_WORKDIR` (`/testbed` when unset).

```
HIVE_TEST_GIT_IMAGE=swe-requests pytest tests/test_live.py -s -k verify
```

`tests/test_verl.py` wants the same, with verl installed, and so does `tests/test_slime.py`, without slime.
