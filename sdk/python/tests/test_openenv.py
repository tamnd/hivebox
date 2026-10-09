"""hivebox.openenv served by uvicorn on this machine and driven by OpenEnv's own client, against
a real comb. Set HIVE_TEST_ENDPOINT and HIVE_TEST_IMAGE as for test_live.py, and have
openenv-core installed."""

import asyncio
import os
import threading
import time

import pytest

pytest.importorskip("openenv")

import uvicorn  # noqa: E402
from openenv.core.generic_client import GenericEnvClient  # noqa: E402

import hivebox  # noqa: E402
from hivebox.openenv import create_hivebox_app  # noqa: E402

ENDPOINT = os.environ.get("HIVE_TEST_ENDPOINT")
IMAGE = os.environ.get("HIVE_TEST_IMAGE", "python")
pytestmark = pytest.mark.skipif(not ENDPOINT, reason="set HIVE_TEST_ENDPOINT to run against a comb")


class Served:
    def __init__(self, **env_kw):
        app = create_hivebox_app(image=IMAGE, endpoint=ENDPOINT, project="openenv-test", mem_mib=256, **env_kw)
        self.server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning"))
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def __enter__(self):
        self.thread.start()
        while not self.server.started:
            time.sleep(0.01)
        port = self.server.servers[0].sockets[0].getsockname()[1]
        return f"http://127.0.0.1:{port}"

    def __exit__(self, *exc):
        self.server.should_exit = True
        self.thread.join(30)


async def state_of(cell_id):
    async with hivebox.AsyncHive(ENDPOINT, project="openenv-test") as hive:
        try:
            return (await hive.cells.get(cell_id)).state
        except hivebox.CellNotFound:
            return "stopped"


async def test_an_episode_keeps_its_shell_and_ends_scored():
    with Served(workdir="/tmp", check="test \"$(cat /tmp/answer)\" = 5050", max_steps=5) as url:
        async with GenericEnvClient(base_url=url) as env:
            first = await env.reset()
            cell = first.observation["cell_id"]
            assert cell and not first.done
            r = await env.step({"command": "mkdir -p w && cd w && export N=100"})
            assert r.observation["exit_code"] == 0
            r = await env.step({"command": "echo $PWD $N; (exit 3)"})
            assert (r.observation["output"], r.observation["exit_code"]) == ("/tmp/w 100\n", 3)
            r = await env.step({"command": "sleep 5", "timeout_s": 0.3})
            assert r.observation["timed_out"] and not r.done
            r = await env.step({"command": "python3 -c 'print(sum(range(101)))' > /tmp/answer", "submit": True})
            assert (r.done, r.reward, r.observation["verdict"]) == (True, 1.0, {"check_exit_code": 0})
            assert await state_of(cell) in ("stopping", "stopped")

            second = await env.reset()
            assert second.observation["cell_id"] != cell
            r = await env.step({"command": "echo 5049 > /tmp/answer", "submit": True})
            assert (r.done, r.reward) == (True, 0.0)

            await env.reset()
            for _ in range(4):
                r = await env.step({"command": "true"})
                assert not r.done
            r = await env.step({"command": "true"})
            assert (r.done, r.reward, r.observation["verdict"]) == (True, 0.0, {"out_of_steps": True})


async def test_closing_the_session_stops_its_cell():
    with Served() as url:
        async with GenericEnvClient(base_url=url) as env:
            cell = (await env.reset()).observation["cell_id"]
            assert await state_of(cell) == "running"
        for _ in range(100):
            if await state_of(cell) in ("stopping", "stopped"):
                break
            await asyncio.sleep(0.1)
        assert await state_of(cell) in ("stopping", "stopped")


async def test_sessions_run_side_by_side():
    """Eight clients at once, each with its own cell, and the time a step takes through OpenEnv,
    printed."""
    times = []

    async def episode(i):
        async with GenericEnvClient(base_url=url) as env:
            cell = (await env.reset()).observation["cell_id"]
            await env.step({"command": f"echo {i} > /tmp/me"})
            for _ in range(25):
                t = time.perf_counter()
                r = await env.step({"command": "cat /tmp/me"})
                times.append(time.perf_counter() - t)
                assert r.observation["output"] == f"{i}\n"
            return cell

    with Served() as url:
        t = time.perf_counter()
        cells = await asyncio.gather(*[episode(i) for i in range(8)])
        took = time.perf_counter() - t
    assert len(set(cells)) == 8
    times.sort()
    print(f"\n8 sessions, 200 steps in {took:.2f} s: a step took p50 {times[100] * 1000:.2f} ms "
          f"p99 {times[198] * 1000:.2f} ms")
