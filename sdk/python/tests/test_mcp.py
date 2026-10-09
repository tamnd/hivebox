"""`python -m hivebox.mcp` started over stdio by the MCP SDK's own client, as an agent's MCP client
starts it, against a real comb. Set HIVE_TEST_ENDPOINT and HIVE_TEST_IMAGE as for test_live.py,
and have mcp installed."""

import json
import os
import sys
import time

import pytest

pytest.importorskip("mcp")

from mcp import Client  # noqa: E402
from mcp.client.stdio import StdioServerParameters  # noqa: E402

import hivebox  # noqa: E402

ENDPOINT = os.environ.get("HIVE_TEST_ENDPOINT")
IMAGE = os.environ.get("HIVE_TEST_IMAGE", "python")
pytestmark = pytest.mark.skipif(not ENDPOINT, reason="set HIVE_TEST_ENDPOINT to run against a comb")


def server(*extra: str) -> Client:
    args = ["-m", "hivebox.mcp", "--endpoint", ENDPOINT, "--project", "mcp-test", "--image", IMAGE,
            "--mem-mib", "256", *extra]
    env = {k: os.environ[k] for k in ("PYTHONPATH",) if k in os.environ}
    return Client(StdioServerParameters(command=sys.executable, args=args, env=env), read_timeout_seconds=120)


async def call(client: Client, tool: str, **args):
    r = await client.call_tool(tool, args)
    assert not r.is_error, r.content
    out = r.structured_content
    return out["result"] if out is not None and set(out) == {"result"} else out


async def fails(client: Client, tool: str, **args) -> str:
    r = await client.call_tool(tool, args)
    assert r.is_error, r.structured_content
    return " ".join(c.text for c in r.content)


async def mcp_cells(stop: bool = False) -> list[tuple[str, dict]]:
    """The cells an MCP server made that are still up, as (id, labels), stopped first if asked."""
    async with hivebox.AsyncHive(ENDPOINT, project="mcp-test") as hive:
        up = [c for c in await hive.cells.list() if "hivebox-mcp" in c.labels and c.state not in ("stopping", "stopped")]
        if stop:
            for c in up:
                await c.stop()
        return [(c.id, c.labels) for c in up]


async def test_an_agent_works_in_a_cell():
    async with server("--max-cells", "3") as client:
        tools = {t.name for t in (await client.list_tools()).tools}
        assert tools == {"create_cell", "list_cells", "run_command", "read_file", "write_file", "list_files",
                         "fork_cell", "stop_cell"}
        assert IMAGE in client.instructions

        assert "is not allowed" in await fails(client, "create_cell", image="ubuntu-not-here")
        assert "is not allowed" in await fails(client, "create_cell", image=IMAGE, network_profile="open")
        assert "not a running cell" in await fails(client, "run_command", cell_id="c-nope", command="true")

        t = time.perf_counter()
        cell = (await call(client, "create_cell", image=IMAGE))["cell_id"]
        made = time.perf_counter() - t

        r = await call(client, "run_command", cell_id=cell, command="mkdir -p /work/p && cd /work/p && export N=7")
        assert r["exit_code"] == 0
        r = await call(client, "run_command", cell_id=cell, command="echo $PWD $N; echo err >&2; (exit 4)")
        assert (r["output"], r["exit_code"], r["timed_out"]) == ("/work/p 7\nerr\n", 4, False)
        r = await call(client, "run_command", cell_id=cell, command="sleep 5", timeout_s=0.3)
        assert r["timed_out"]
        # The shell that timed out was replaced, so the directory has to be set again.
        r = await call(client, "run_command", cell_id=cell, command="echo after $N; cd /work/p")
        assert r["output"] == "after\n"

        w = await call(client, "write_file", cell_id=cell, path="/work/p/src/m.py", content="print('héllo')\n")
        assert w["size"] == len("print('héllo')\n".encode())
        await call(client, "write_file", cell_id=cell, path="/work/p/src/m.py", content="print(2)\n", append=True)
        assert await call(client, "read_file", cell_id=cell, path="/work/p/src/m.py") == "print('héllo')\nprint(2)\n"
        r = await call(client, "run_command", cell_id=cell, command="python3 src/m.py")
        assert r["output"] == "héllo\n2\n"
        listed = await call(client, "list_files", cell_id=cell, path="/work/p", depth=2)
        assert {(f["path"].rsplit("/", 1)[-1], f["type"]) for f in listed} >= {("src", "dir"), ("m.py", "file")}
        assert "not a file" in await fails(client, "read_file", cell_id=cell, path="/work/p/src")
        assert "No such file" in await fails(client, "read_file", cell_id=cell, path="/work/p/nope")

        await call(client, "run_command", cell_id=cell, command="head -c 300000 /dev/zero | tr '\\0' a > big")
        assert "read it in parts" in await fails(client, "read_file", cell_id=cell, path="/work/p/big")
        part = await call(client, "read_file", cell_id=cell, path="/work/p/big", offset=299990, length=100)
        assert part == "a" * 10

        forks = await call(client, "fork_cell", cell_id=cell, count=2)
        assert len(forks) == 2
        for f in forks:
            assert await call(client, "read_file", cell_id=f["cell_id"], path="/work/p/src/m.py") == "print('héllo')\nprint(2)\n"
        await call(client, "write_file", cell_id=forks[0]["cell_id"], path="/work/p/only", content="x")
        r = await call(client, "run_command", cell_id=cell, command="ls /work/p/only")
        assert r["exit_code"] != 0
        assert "at most 3 cells" in await fails(client, "create_cell", image=IMAGE)

        times = []
        for i in range(100):
            t = time.perf_counter()
            await call(client, "run_command", cell_id=cell, command=f"echo {i}")
            times.append(time.perf_counter() - t)
        times.sort()

        await call(client, "stop_cell", cell_id=forks[1]["cell_id"])
        assert {c["cell_id"] for c in await call(client, "list_cells")} == {cell, forks[0]["cell_id"]}
        assert "not a running cell" in await fails(client, "run_command", cell_id=forks[1]["cell_id"], command="true")
        assert len(await mcp_cells()) == 2
    # The server stops what it made when the client closes it.
    assert await mcp_cells() == []
    print(f"\ncreate_cell took {made * 1000:.0f} ms; run_command took p50 {times[50] * 1000:.2f} ms "
          f"p99 {times[98] * 1000:.2f} ms over stdio")


async def test_keep_leaves_the_cells():
    async with server("--keep", "--label", "kept=yes") as client:
        cell = (await call(client, "create_cell", image=IMAGE))["cell_id"]
    left = await mcp_cells(stop=True)
    assert [(c, labels["kept"]) for c, labels in left] == [(cell, "yes")]


def test_the_tools_describe_themselves():
    from hivebox.mcp import build_server

    import asyncio

    tools = {t.name: t for t in asyncio.run(build_server().list_tools())}
    assert tools["run_command"].annotations.read_only_hint is not True
    assert tools["read_file"].annotations.read_only_hint
    assert set(tools["run_command"].input_schema["required"]) == {"cell_id", "command"}
    assert json.dumps(tools["create_cell"].output_schema)
