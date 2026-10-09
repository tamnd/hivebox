"""SandboxFusion's `/run_code` API, with each call run in a fresh hivebox cell.

    python -m hivebox.sandboxfusion --lang python=python --lang cpp=gcc --pool 4 --port 8080

verl, the `sandbox-fusion` client and other code that calls SandboxFusion can point at this
server instead. Each call gets a cell of its own with no network: the code and the call's files
go into /sandbox, the language's compile command runs if it has one, then its run command with
the call's stdin, and the files asked for come back before the cell is stopped. The answer has
SandboxFusion's fields and statuses, and `executor_pod_name` is the cell id.

A language is served when it has an image, named with `--lang LANGUAGE=IMAGE`. The commands for
python, bash, cpp, go, java, nodejs and rust are built in, and `create_app` takes other
languages or other commands as `Language`s. `--pool N` keeps N cells ready for each language,
each used for one call and then stopped, so a call does not wait for a cell to start. A call
with `memory_limit_MB` gets a cell with that limit, made for it, and code that goes over it
ends with return code -9 as under SandboxFusion. The client comes from `--endpoint`, `--token`
and `--project`, or $HIVE_ENDPOINT, $HIVE_TOKEN and $HIVE_PROJECT.

There is no auth on the HTTP side, as in SandboxFusion, so the server listens on 127.0.0.1
unless told otherwise and belongs where only the trainers reach it.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import contextlib
import io
import os
import posixpath
import tarfile
import time
import uuid
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Route

from . import _errors
from ._client import AsyncHive, Cell, RunResult, Spec
from .v1 import types_pb2

__all__ = ["LANGUAGES", "Language", "create_app", "main"]

WORKDIR = "/sandbox"

# A pooled cell older than this is replaced rather than used, well before its hard TTL.
POOL_MAX_AGE = 30 * 60


@dataclass(frozen=True)
class Language:
    """How code in one language runs: the file the code goes in, the command that compiles it
    if any, and the command that runs it, both run with bash in /sandbox."""

    source: str
    run: str
    compile: str | None = None
    image: str | None = None


LANGUAGES: dict[str, Language] = {
    "python": Language("main.py", "python3 main.py"),
    "bash": Language("main.sh", "bash main.sh"),
    "cpp": Language("main.cpp", "./main", "g++ -std=c++17 -O2 -o main main.cpp"),
    "go": Language("main.go", "./main", "go build -o main main.go"),
    "java": Language("Main.java", "java Main", "javac Main.java"),
    "nodejs": Language("main.js", "node main.js"),
    "rust": Language("main.rs", "./main", "rustc -O -o main main.rs"),
}


@dataclass
class Limits:
    pool: int = 0
    mem_mib: int = 0
    max_mem_mib: int = 0
    cpu_milli: int = 0
    max_timeout_s: float = 300.0
    max_concurrency: int = 64
    labels: dict[str, str] = field(default_factory=dict)


class _Pool:
    """Cells ready for one language, each handed out once."""

    def __init__(self, hive: AsyncHive, spec: Spec, size: int, background: set[asyncio.Task[Any]]):
        self.hive, self.spec, self.size = hive, spec, size
        self.ready: asyncio.Queue[tuple[Cell, float]] = asyncio.Queue()
        self.making = 0
        self.background = background

    def fill(self) -> None:
        while self.ready.qsize() + self.making < self.size:
            self.making += 1
            _spawn(self.background, self._make())

    async def _make(self) -> None:
        try:
            cell = await self.hive.cells.create(self.spec)
            self.ready.put_nowait((cell, time.monotonic()))
        except _errors.HiveError:
            # The next call makes its own cell, and the pool tries again then.
            pass
        finally:
            self.making -= 1

    async def take(self) -> Cell:
        try:
            while True:
                cell, made = self.ready.get_nowait()
                if time.monotonic() - made < POOL_MAX_AGE:
                    return cell
                _spawn(self.background, _stop(cell))
        except asyncio.QueueEmpty:
            return await self.hive.cells.create(self.spec)
        finally:
            self.fill()

    async def drain(self) -> None:
        cells = []
        while not self.ready.empty():
            cells.append(self.ready.get_nowait()[0])
        await asyncio.gather(*(_stop(c) for c in cells))


def _spawn(tasks: set[asyncio.Task[Any]], coro) -> None:
    t = asyncio.get_running_loop().create_task(coro)
    tasks.add(t)
    t.add_done_callback(tasks.discard)


async def _stop(cell: Cell) -> None:
    with contextlib.suppress(_errors.HiveError):
        await cell.stop()


def _result(r: RunResult) -> dict[str, Any]:
    timed_out = r.timed_out
    return {
        "status": "TimeLimitExceeded" if timed_out else "Finished",
        "execution_time": r.wall,
        "return_code": None if timed_out else (-r.signal if r.signal else r.exit_code),
        "stdout": r.stdout.decode(errors="replace"),
        "stderr": r.stderr.decode(errors="replace"),
    }


async def _killed_for_memory(cell: Cell) -> bool:
    """Whether the cell was stopped for running out of memory. The OOM kill takes the whole cell,
    so the run's stream ends with an error rather than an exit code, and the comb may take a moment
    to see why."""
    for _ in range(50):
        with contextlib.suppress(_errors.HiveError):
            await cell.refresh()
            if cell.info.cause == types_pb2.CAUSE_OOM:
                return True
            if cell.info.cause != types_pb2.CAUSE_UNSPECIFIED:
                return False
        await asyncio.sleep(0.1)
    return False


def _status(*results: dict[str, Any] | None) -> str:
    done = [r for r in results if r is not None]
    if all(r["status"] == "Finished" and r["return_code"] == 0 for r in done):
        return "Success"
    return "Failed"


def _path(name: str) -> str:
    return posixpath.normpath(posixpath.join(WORKDIR, name))


def _parents(path: str) -> list[str]:
    out = []
    while (path := posixpath.dirname(path)) not in ("/", ""):
        out.append(path)
    return out


def _tar(files: Mapping[str, bytes]) -> bytes:
    buf = io.BytesIO()
    dirs = sorted({d for path in files for d in _parents(path)})
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for d in dirs:
            info = tarfile.TarInfo(d.lstrip("/"))
            info.type, info.mode = tarfile.DIRTYPE, 0o755
            tar.addfile(info)
        for path, data in files.items():
            info = tarfile.TarInfo(path.lstrip("/"))
            info.size, info.mode = len(data), 0o644
            tar.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def _timeout(v: Any, default: float, cap: float) -> float:
    t = float(v) if v is not None else default
    return min(t, cap) if t > 0 else default


class _Server:
    def __init__(self, languages: Mapping[str, Language], limits: Limits, client: tuple[str | None, str | None, str | None]):
        self.languages = {k: v for k, v in languages.items() if v.image}
        self.limits = limits
        self.client = client
        self.hive: AsyncHive | None = None
        self.pools: dict[str, _Pool] = {}
        self.background: set[asyncio.Task[Any]] = set()
        self.slots = asyncio.Semaphore(limits.max_concurrency)
        # Every cell this server makes carries it, so the ones left when it stops can be found.
        self.tag = uuid.uuid4().hex[:12]

    def spec(self, lang: Language, mem_mib: int) -> Spec:
        return Spec(image=lang.image, backend="container", network_profile="none", mem_mib=mem_mib,
                    cpu_milli=self.limits.cpu_milli, hard_ttl="1h", labels={**self.limits.labels, "sandbox-fusion": self.tag})

    @contextlib.asynccontextmanager
    async def lifespan(self, _: Starlette) -> AsyncIterator[None]:
        endpoint, token, project = self.client
        self.hive = AsyncHive(endpoint, token=token, project=project)
        for name, lang in self.languages.items():
            pool = self.pools[name] = _Pool(self.hive, self.spec(lang, self.limits.mem_mib), self.limits.pool, self.background)
            pool.fill()
        try:
            yield
        finally:
            while self.background:
                await asyncio.gather(*self.background, return_exceptions=True)
            await asyncio.gather(*(p.drain() for p in self.pools.values()))
            # A create that timed out here can still have made its cell on the comb.
            with contextlib.suppress(_errors.HiveError):
                left = await self.hive.cells.list({"sandbox-fusion": self.tag})
                await asyncio.gather(*(_stop(c) for c in left if c.state not in ("stopping", "stopped")))
            await self.hive.close()

    async def run_code(self, request: Request) -> JSONResponse:
        try:
            req = await request.json()
            code, language = req["code"], req["language"]
            if not isinstance(code, str) or not isinstance(language, str):
                raise TypeError("code and language are strings")
        except (ValueError, KeyError, TypeError) as e:
            return JSONResponse({"detail": f"a run_code call needs JSON with code and language: {e}"}, status_code=422)
        lang = self.languages.get(language)
        if lang is None:
            served = ", ".join(sorted(self.languages)) or "none"
            return JSONResponse(self._error(f"language {language!r} has no image on this server; served: {served}"))
        async with self.slots:
            try:
                return JSONResponse(await self._run(req, lang, language))
            except _errors.HiveError as e:
                return JSONResponse(self._error(str(e)))

    def _error(self, message: str) -> dict[str, Any]:
        return {"status": "SandboxError", "message": message, "compile_result": None, "run_result": None,
                "executor_pod_name": None, "files": {}}

    async def _run(self, req: dict[str, Any], lang: Language, language: str) -> dict[str, Any]:
        assert self.hive is not None
        cap = self.limits.max_timeout_s
        compile_timeout = _timeout(req.get("compile_timeout"), 10, cap)
        run_timeout = _timeout(req.get("run_timeout"), 10, cap)
        mem = int(req.get("memory_limit_MB") or -1)
        files = {_path(lang.source): req["code"].encode()}
        for name, data in (req.get("files") or {}).items():
            if data is not None:
                files[_path(name)] = base64.b64decode(data)

        if mem > 0:
            if self.limits.max_mem_mib:
                mem = min(mem, self.limits.max_mem_mib)
            cell = await self.hive.cells.create(self.spec(lang, mem))
        else:
            cell = await self.pools[language].take()
        try:
            await cell.files.upload("/", _tar(files))
            compiled = ran = None
            alive = True
            if lang.compile:
                compiled, alive = await self._step(cell, lang.compile, compile_timeout, b"")
            if compiled is None or (compiled["status"] == "Finished" and compiled["return_code"] == 0):
                ran, alive = await self._step(cell, lang.run, run_timeout, (req.get("stdin") or "").encode())
            fetched = {}
            for name in (req.get("fetch_files") or []) if alive else []:
                with contextlib.suppress(FileNotFoundError, IsADirectoryError):
                    fetched[name] = base64.b64encode(await cell.files.read(_path(name))).decode()
        finally:
            # The answer does not wait for the cell to go.
            _spawn(self.background, _stop(cell))
        return {"status": _status(compiled, ran), "message": "", "compile_result": compiled, "run_result": ran,
                "executor_pod_name": cell.id, "files": fetched}

    async def _step(self, cell: Cell, command: str, timeout: float, stdin: bytes) -> tuple[dict[str, Any], bool]:
        """A compile or run, and whether the cell is still up after it."""
        t = time.perf_counter()
        try:
            return _result(await cell.run(["bash", "-c", command], cwd=WORKDIR, stdin=stdin, timeout=timeout)), True
        except _errors.HiveError:
            if not await _killed_for_memory(cell):
                raise
        # Reported as SandboxFusion reports a run the kernel killed for its memory limit.
        return {"status": "Finished", "execution_time": time.perf_counter() - t, "return_code": -9,
                "stdout": "", "stderr": "killed: the cell ran out of memory\n"}, False


def create_app(languages: Mapping[str, str | Language], *, pool: int = 0, mem_mib: int = 0, max_mem_mib: int = 0,
               cpu_milli: int = 0, max_timeout_s: float = 300.0, max_concurrency: int = 64,
               labels: Mapping[str, str] | None = None, endpoint: str | None = None, token: str | None = None,
               project: str | None = None) -> Starlette:
    """The ASGI app. `languages` maps a language to its image, which takes the built-in commands,
    or to a `Language` with its image set."""
    langs: dict[str, Language] = {}
    for name, v in languages.items():
        if isinstance(v, Language):
            langs[name] = v
        elif name in LANGUAGES:
            langs[name] = Language(LANGUAGES[name].source, LANGUAGES[name].run, LANGUAGES[name].compile, v)
        else:
            raise ValueError(f"{name!r} has no built-in commands, so give it as a Language")
    limits = Limits(pool=pool, mem_mib=mem_mib, max_mem_mib=max_mem_mib, cpu_milli=cpu_milli,
                    max_timeout_s=max_timeout_s, max_concurrency=max_concurrency, labels=dict(labels or {}))
    server = _Server(langs, limits, (endpoint, token, project))

    async def ping(_: Request) -> PlainTextResponse:
        return PlainTextResponse("pong")

    return Starlette(routes=[Route("/run_code", server.run_code, methods=["POST"]),
                             Route("/v1/ping", ping, methods=["GET"])],
                     lifespan=server.lifespan)


def main(argv: Sequence[str] | None = None) -> None:
    import uvicorn

    p = argparse.ArgumentParser(prog="python -m hivebox.sandboxfusion", description=__doc__.split("\n\n")[0])
    p.add_argument("--lang", action="append", default=[], metavar="LANGUAGE=IMAGE", required=True,
                   help=f"a language to serve and its image; built in: {', '.join(LANGUAGES)}")
    p.add_argument("--pool", type=int, default=0, help="cells kept ready for each language")
    p.add_argument("--mem-mib", type=int, default=0, help="a cell's memory when the call does not say")
    p.add_argument("--max-mem-mib", type=int, default=0, help="the most memory_limit_MB may ask for")
    p.add_argument("--cpu-milli", type=int, default=0)
    p.add_argument("--max-timeout", type=float, default=300.0, help="the longest compile or run, in seconds")
    p.add_argument("--max-concurrency", type=int, default=64, help="calls run at once; the rest wait")
    p.add_argument("--label", action="append", default=[], metavar="K=V", help="a label put on every cell")
    p.add_argument("--endpoint", default=os.environ.get("HIVE_ENDPOINT"))
    p.add_argument("--token", default=os.environ.get("HIVE_TOKEN"))
    p.add_argument("--project", default=os.environ.get("HIVE_PROJECT"))
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8080)
    a = p.parse_args(argv)
    app = create_app(dict(kv.split("=", 1) for kv in a.lang), pool=a.pool, mem_mib=a.mem_mib,
                     max_mem_mib=a.max_mem_mib, cpu_milli=a.cpu_milli, max_timeout_s=a.max_timeout,
                     max_concurrency=a.max_concurrency, labels=dict(kv.split("=", 1) for kv in a.label),
                     endpoint=a.endpoint, token=a.token, project=a.project)
    uvicorn.run(app, host=a.host, port=a.port, log_level="warning")


if __name__ == "__main__":
    main()
