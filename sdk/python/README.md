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

The endpoint is the first of the argument, `$HIVE_ENDPOINT`, or `unix:$HIVE_SOCKET`, and then `unix:/run/hivebox/comb.sock`. The project is the argument or `$HIVE_PROJECT`, and a comb puts calls that name none in `local`.

Every failure raises a subclass of `HiveError` named after its reason, like `CellNotFound`, with `is_infra_error` set when the failure was hivebox's and not the command's. A file that is missing raises `FileError`, which is also a `FileNotFoundError`, and the same goes for the other common errno values. Calls that are safe to repeat are tried up to three times when the failure was hivebox's.

## Development

The stubs in `hivebox/v1` are made from the protos in `crates/hive-proto/proto` by `generate.sh`, and are checked in so installing needs no protoc.

```
pip install -e '.[dev,test]'
./generate.sh
pytest
HIVE_TEST_ENDPOINT=unix:/run/hivebox/comb.sock pytest tests/test_live.py -s
```
