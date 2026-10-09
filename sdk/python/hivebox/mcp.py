"""An MCP server that gives an agent hivebox cells to work in.

    python -m hivebox.mcp --image python --image node --max-cells 4

An MCP client, such as Claude Code, Cursor or an agent framework, starts it over stdio and gets
tools to make a cell, run commands in a shell that keeps its directory and variables, read and
write files, fork a cell and stop it. `--http HOST:PORT` serves it over streamable HTTP at /mcp
instead.

The agent only reaches cells this server made, at most `--max-cells` of them running at once,
from the images named with `--image` when any are. Cells get the `none` network profile unless
the agent asks for one named with `--network`. When the server exits it stops every cell it
made, unless `--keep` is given. The client comes from `--endpoint`, `--token` and `--project`,
or $HIVE_ENDPOINT, $HIVE_TOKEN and $HIVE_PROJECT.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import functools
import os
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import ParamSpec, TypeVar

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field

from . import _errors
from ._client import AsyncHive, Cell, Session, Spec

__all__ = ["CellInfo", "CommandResult", "FileEntry", "Limits", "build_server", "main"]

P = ParamSpec("P")
T = TypeVar("T")

# Read whole, a file bigger than this would fill the agent's context, so it is read in parts.
READ_LIMIT = 256 * 1024


@dataclass
class Limits:
    """What the agent may do. `images` empty means any image the node has. `networks` are the
    profiles it may ask for besides `none`."""

    images: Sequence[str] = ()
    networks: Sequence[str] = ()
    max_cells: int = 8
    max_timeout_s: float = 600.0
    mem_mib: int = 0
    cpu_milli: int = 0
    keep: bool = False
    labels: dict[str, str] = field(default_factory=dict)


class CellInfo(BaseModel):
    cell_id: str
    state: str
    image: str = ""


class CommandResult(BaseModel):
    output: str = Field(description="stdout and stderr together")
    exit_code: int
    timed_out: bool = Field(description="The command ran out of time and its shell was replaced, losing its directory and variables")
    truncated: bool = Field(description="The output was cut at the cell's limit")


class FileEntry(BaseModel):
    path: str
    type: str
    size: int


class _State:
    def __init__(self, hive: AsyncHive, limits: Limits):
        self.hive = hive
        self.limits = limits
        self.tag = uuid.uuid4().hex[:12]
        self.cells: dict[str, tuple[Cell, str]] = {}
        self.sessions: dict[str, Session] = {}
        self.locks: dict[str, asyncio.Lock] = {}
        self.making = asyncio.Lock()

    def cell(self, cell_id: str) -> Cell:
        got = self.cells.get(cell_id)
        if got is None:
            raise ToolError(f"{cell_id} is not a running cell made by this server; create_cell makes one")
        return got[0]

    async def shell(self, cell: Cell) -> Session:
        s = self.sessions.get(cell.id)
        if s is None:
            s = self.sessions[cell.id] = await cell.session()
        return s

    def room(self, n: int) -> None:
        if len(self.cells) + n > self.limits.max_cells:
            raise ToolError(f"this server runs at most {self.limits.max_cells} cells and {len(self.cells)} are "
                            "running; stop_cell one first")

    async def forget(self, cell_id: str) -> None:
        self.cells.pop(cell_id, None)
        self.locks.pop(cell_id, None)
        s = self.sessions.pop(cell_id, None)
        if s is not None:
            with contextlib.suppress(_errors.HiveError):
                await s.close()


def _hive_errors(fn: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
    """hivebox's errors go to the agent as tool errors with their message, which MCP would
    otherwise hide behind a bare "Error executing tool"."""

    @functools.wraps(fn)
    async def wrapped(*args: P.args, **kwargs: P.kwargs) -> T:
        try:
            return await fn(*args, **kwargs)
        except (_errors.HiveError, FileNotFoundError) as e:
            raise ToolError(str(e)) from e

    return wrapped


def build_server(limits: Limits | None = None, *, endpoint: str | None = None, token: str | None = None,
                 project: str | None = None) -> MCPServer:
    """The MCP server, with its tools bound to one client made when it starts."""
    limits = limits or Limits()
    state: _State | None = None

    @contextlib.asynccontextmanager
    async def lifespan(_: MCPServer) -> AsyncIterator[None]:
        nonlocal state
        hive = AsyncHive(endpoint, token=token, project=project)
        state = _State(hive, limits)
        try:
            yield
        finally:
            if not limits.keep:
                await asyncio.gather(*(_stop(c) for c, _ in state.cells.values()))
            await hive.close()

    async def _stop(cell: Cell) -> None:
        with contextlib.suppress(_errors.CellNotFound):
            await cell.stop()

    def st() -> _State:
        assert state is not None, "the server has not started"
        return state

    images = ", ".join(limits.images) if limits.images else "any image the node has"
    networks = ", ".join(["none", *limits.networks])
    server = MCPServer(
        "hivebox",
        instructions=(
            "Sandboxes for running code. create_cell makes an isolated Linux machine from an image, "
            "run_command runs bash commands in it with the directory and variables kept between calls, "
            "and stop_cell ends it. Files can be read and written directly. "
            f"Images: {images}. Network profiles: {networks}. At most {limits.max_cells} cells at once."),
        lifespan=lifespan,
        # A tool call that fails is part of an agent's work, not something for the log.
        log_level="WARNING",
    )

    @server.tool(annotations=ToolAnnotations(destructive_hint=False, open_world_hint=False))
    @_hive_errors
    async def create_cell(image: str, network_profile: str = "none", mem_mib: int = 0) -> CellInfo:
        """Make a new cell, an isolated Linux machine started from an image, and return its id.
        network_profile is `none` (no network) unless another allowed profile is named. mem_mib
        0 takes the server's default."""
        s = st()
        if limits.images and image not in limits.images:
            raise ToolError(f"image {image!r} is not allowed here; use one of {', '.join(limits.images)}")
        if network_profile not in ("none", *limits.networks):
            raise ToolError(f"network profile {network_profile!r} is not allowed here; use one of {networks}")
        async with s.making:
            s.room(1)
            cell = await s.hive.cells.create(Spec(
                image=image, backend="container", network_profile=network_profile,
                mem_mib=mem_mib or limits.mem_mib, cpu_milli=limits.cpu_milli,
                labels={**limits.labels, "hivebox-mcp": s.tag}))
            s.cells[cell.id] = (cell, image)
        return CellInfo(cell_id=cell.id, state=cell.state, image=image)

    @server.tool(annotations=ToolAnnotations(read_only_hint=True))
    @_hive_errors
    async def list_cells() -> list[CellInfo]:
        """The cells this server made that are still running."""
        s = st()
        out = []
        for cell_id, (cell, image) in list(s.cells.items()):
            try:
                await cell.refresh()
            except _errors.CellNotFound:
                await s.forget(cell_id)
                continue
            if cell.state in ("stopping", "stopped", "failed", "expired"):
                await s.forget(cell_id)
                continue
            out.append(CellInfo(cell_id=cell.id, state=cell.state, image=image))
        return out

    @server.tool(annotations=ToolAnnotations(open_world_hint=False))
    @_hive_errors
    async def run_command(cell_id: str, command: str, timeout_s: float = 120.0) -> CommandResult:
        """Run a bash command in the cell's shell and return what it printed and its exit code.
        The shell keeps its working directory and exported variables from one call to the next.
        Commands must not wait for input."""
        s = st()
        cell = s.cell(cell_id)
        timeout = min(max(timeout_s, 0.1), limits.max_timeout_s)
        async with s.locks.setdefault(cell_id, asyncio.Lock()):
            r = await (await s.shell(cell)).run(command, timeout=timeout)
        return CommandResult(output=r.output.decode(errors="replace"), exit_code=r.exit_code,
                             timed_out=r.timed_out, truncated=r.truncated)

    @server.tool(annotations=ToolAnnotations(read_only_hint=True))
    @_hive_errors
    async def read_file(cell_id: str, path: str, offset: int = 0, length: int = 0) -> str:
        """Read a text file in the cell. Files over 256 KiB are read in parts with offset and
        length, in bytes."""
        cell = st().cell(cell_id)
        info = await cell.files.stat(path)
        if info.type != "file":
            raise ToolError(f"{path} is a {info.type}, not a file")
        if length <= 0:
            if info.size - offset > READ_LIMIT:
                raise ToolError(f"{path} is {info.size} bytes; read it in parts of at most {READ_LIMIT} with "
                                "offset and length")
        else:
            length = min(length, READ_LIMIT)
        return (await cell.files.read(path, offset=offset, length=length)).decode(errors="replace")

    @server.tool(annotations=ToolAnnotations(destructive_hint=True, open_world_hint=False))
    @_hive_errors
    async def write_file(cell_id: str, path: str, content: str, append: bool = False) -> FileEntry:
        """Write a text file in the cell, making its directories, and replace what was there
        unless append is set."""
        info = await st().cell(cell_id).files.write(path, content, append=append)
        return FileEntry(path=info.path or path, type=info.type, size=info.size)

    @server.tool(annotations=ToolAnnotations(read_only_hint=True))
    @_hive_errors
    async def list_files(cell_id: str, path: str = "/", depth: int = 1) -> list[FileEntry]:
        """List a directory in the cell, down to depth levels."""
        found = await st().cell(cell_id).files.list(path, depth=min(max(depth, 1), 4))
        return [FileEntry(path=f.path, type=f.type, size=f.size) for f in found]

    @server.tool(annotations=ToolAnnotations(destructive_hint=False, open_world_hint=False))
    @_hive_errors
    async def fork_cell(cell_id: str, count: int = 1) -> list[CellInfo]:
        """Make count new cells that start from the files this cell has now, to try different
        things from the same point. Running processes and the shell's state are not copied."""
        s = st()
        cell = s.cell(cell_id)
        image = s.cells[cell_id][1]
        async with s.making:
            s.room(count)
            forks = await cell.fork(count, labels={**limits.labels, "hivebox-mcp": s.tag})
            for f in forks:
                s.cells[f.id] = (f, image)
        return [CellInfo(cell_id=f.id, state=f.state, image=image) for f in forks]

    @server.tool(annotations=ToolAnnotations(destructive_hint=True, idempotent_hint=True, open_world_hint=False))
    @_hive_errors
    async def stop_cell(cell_id: str) -> str:
        """Stop a cell and throw away everything in it."""
        s = st()
        cell = s.cell(cell_id)
        await s.forget(cell_id)
        await _stop(cell)
        return f"stopped {cell_id}"

    return server


def main(argv: Sequence[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="python -m hivebox.mcp", description=__doc__.split("\n\n")[0])
    p.add_argument("--endpoint", default=os.environ.get("HIVE_ENDPOINT"))
    p.add_argument("--token", default=os.environ.get("HIVE_TOKEN"))
    p.add_argument("--project", default=os.environ.get("HIVE_PROJECT"))
    p.add_argument("--image", action="append", default=[], help="an image the agent may use; any when not given")
    p.add_argument("--network", action="append", default=[], help="a network profile the agent may ask for besides none")
    p.add_argument("--max-cells", type=int, default=8)
    p.add_argument("--max-timeout", type=float, default=600.0, help="the longest one command may run, in seconds")
    p.add_argument("--mem-mib", type=int, default=0, help="a cell's memory when the agent does not say")
    p.add_argument("--cpu-milli", type=int, default=0)
    p.add_argument("--label", action="append", default=[], metavar="K=V", help="a label put on every cell")
    p.add_argument("--keep", action="store_true", help="leave the cells running when the server exits")
    p.add_argument("--http", metavar="HOST:PORT", help="serve streamable HTTP at /mcp instead of stdio")
    a = p.parse_args(argv)
    labels = dict(kv.split("=", 1) for kv in a.label)
    limits = Limits(images=a.image, networks=a.network, max_cells=a.max_cells, max_timeout_s=a.max_timeout,
                    mem_mib=a.mem_mib, cpu_milli=a.cpu_milli, keep=a.keep, labels=labels)
    server = build_server(limits, endpoint=a.endpoint, token=a.token, project=a.project)
    if a.http:
        host, _, port = a.http.rpartition(":")
        server.run("streamable-http", host=host or "127.0.0.1", port=int(port))
    else:
        server.run()


if __name__ == "__main__":
    main()
