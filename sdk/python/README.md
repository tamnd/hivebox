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

Every failure raises a subclass of `HiveError` named after its reason, like `CellNotFound`, with `is_infra_error` set when the failure was hivebox's and not the command's. A file that is missing raises `FileError`, which is also a `FileNotFoundError`, and the same goes for the other common errno values. Calls that are safe to repeat are tried up to three times when the failure was hivebox's.

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
