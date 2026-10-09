"""hivebox.sandboxfusion served by uvicorn on this machine and called with SandboxFusion's own
client, and with a plain POST the way verl calls it, against a real comb. Set
HIVE_TEST_ENDPOINT and HIVE_TEST_IMAGE as for test_live.py, and have sandbox-fusion installed."""

import asyncio
import base64
import os
import threading
import time

import pytest

pytest.importorskip("sandbox_fusion")

import aiohttp  # noqa: E402
import uvicorn  # noqa: E402
from sandbox_fusion import RunCodeRequest, RunCodeResponse  # noqa: E402
from sandbox_fusion.async_client import run_code  # noqa: E402

import hivebox  # noqa: E402
from hivebox.sandboxfusion import Language, create_app  # noqa: E402

ENDPOINT = os.environ.get("HIVE_TEST_ENDPOINT")
IMAGE = os.environ.get("HIVE_TEST_IMAGE", "python")
pytestmark = pytest.mark.skipif(not ENDPOINT, reason="set HIVE_TEST_ENDPOINT to run against a comb")


class Served:
    def __init__(self, **kw):
        langs = {"python": IMAGE, "bash": IMAGE,
                 "pycheck": Language("main.py", "python3 main.py", "python3 -m py_compile main.py", IMAGE)}
        app = create_app(langs, endpoint=ENDPOINT, project="sf-test", mem_mib=256, **kw)
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
        self.thread.join(180)
        assert not self.thread.is_alive()


async def up_cells():
    async with hivebox.AsyncHive(ENDPOINT, project="sf-test") as hive:
        cells = await hive.cells.list()
        return [c.id for c in cells if "sandbox-fusion" in c.labels and c.state not in ("stopping", "stopped", "failed", "expired")]


async def run(url, language, code, **kw):
    return await run_code(RunCodeRequest(language=language, code=code, **kw), endpoint=url, max_attempts=1)


async def test_calls_answer_as_sandboxfusion_does():
    with Served() as url:
        r = await run(url, "python", "import sys\nprint(sys.stdin.read().upper())", stdin="hello")
        assert (r.status, r.run_result.status, r.run_result.return_code, r.run_result.stdout) == (
            "Success", "Finished", 0, "HELLO\n")
        assert r.compile_result is None and r.executor_pod_name

        r = await run(url, "python", "raise ValueError('no')")
        assert (r.status, r.run_result.return_code) == ("Failed", 1)
        assert "ValueError: no" in r.run_result.stderr

        r = await run(url, "python", "import time\ntime.sleep(5)", run_timeout=1)
        assert (r.status, r.run_result.status, r.run_result.return_code) == ("Failed", "TimeLimitExceeded", None)

        r = await run(url, "bash", "echo $((6 * 7)); pwd")
        assert (r.status, r.run_result.stdout) == ("Success", "42\n/sandbox\n")

        data = base64.b64encode(b"3\n4\n").decode()
        code = "n = [int(x) for x in open('in/nums.txt')]\nopen('out.txt', 'w').write(str(sum(n)))"
        r = await run(url, "python", code, files={"in/nums.txt": data}, fetch_files=["out.txt", "missing.txt"])
        assert r.status == "Success" and r.files == {"out.txt": base64.b64encode(b"7").decode()}

        # The client only sends the languages it knows, so a language of this server's own is
        # posted as JSON.
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{url}/run_code", json={"language": "pycheck", "code": "print(1"}) as resp:
                r = RunCodeResponse(**await resp.json())
            assert (r.status, r.run_result) == ("Failed", None)
            assert r.compile_result.return_code != 0 and "SyntaxError" in r.compile_result.stderr
            async with s.post(f"{url}/run_code", json={"language": "pycheck", "code": "print(1)"}) as resp:
                r = RunCodeResponse(**await resp.json())
            assert (r.status, r.compile_result.return_code, r.run_result.stdout) == ("Success", 0, "1\n")

        with pytest.raises(Exception, match="has no image on this server"):
            await run(url, "cpp", "int main() {}")

        first = await run(url, "bash", "echo secret > /tmp/left")
        second = await run(url, "bash", "cat /tmp/left")
        assert second.status == "Failed" and first.executor_pod_name != second.executor_pod_name

        # verl posts the JSON itself, with memory_limit_MB.
        async with aiohttp.ClientSession() as s:
            body = {"code": "x = bytearray(400 << 20)\nprint(len(x))", "language": "python", "stdin": "",
                    "compile_timeout": 10, "run_timeout": 30, "memory_limit_MB": 128, "files": {}, "fetch_files": []}
            async with s.post(f"{url}/run_code", json=body) as resp:
                out = await resp.json()
            assert (out["status"], out["run_result"]["return_code"]) == ("Failed", -9), out
            assert "out of memory" in out["run_result"]["stderr"]
            body["memory_limit_MB"] = 1024
            async with s.post(f"{url}/run_code", json=body) as resp:
                out = await resp.json()
            assert (out["status"], out["run_result"]["stdout"]) == ("Success", f"{400 << 20}\n"), out
            async with s.post(f"{url}/run_code", data=b"{nope") as resp:
                assert resp.status == 422
            async with s.get(f"{url}/v1/ping") as resp:
                assert await resp.text() == "pong"
    assert await up_cells() == []


async def timed(url, n, at_once):
    times = []
    slots = asyncio.Semaphore(at_once)

    async def one(i):
        async with slots:
            t = time.perf_counter()
            r = await run(url, "python", f"print({i} * 2)")
            times.append(time.perf_counter() - t)
            assert r.run_result.stdout == f"{i * 2}\n"

    t = time.perf_counter()
    await asyncio.gather(*(one(i) for i in range(n)))
    took = time.perf_counter() - t
    times.sort()
    return took, times[len(times) // 2], times[int(len(times) * 0.99) - 1 if len(times) > 1 else 0]


async def test_a_pool_hides_the_cell_start():
    """The time a call takes with no pool and with cells kept ready, one at a time and eight at
    once, printed."""
    lines = []
    for pool in (0, 8):
        with Served(pool=pool) as url:
            if pool:
                await asyncio.sleep(20)
            for n, at_once in ((20, 1), (40, 8)):
                took, p50, p99 = await timed(url, n, at_once)
                lines.append(f"pool {pool}, {n} calls {at_once} at once: {took:.2f} s, "
                             f"p50 {p50 * 1000:.0f} ms p99 {p99 * 1000:.0f} ms")
    assert await up_cells() == []
    print("\n" + "\n".join(lines))
