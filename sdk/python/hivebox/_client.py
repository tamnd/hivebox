"""The asyncio client over grpcio."""

from __future__ import annotations

import asyncio
import os
import re
from dataclasses import dataclass, field
from typing import AsyncIterator, Iterable, Mapping, Sequence

import grpc
from google.protobuf import duration_pb2

from . import _errors
from .v1 import (
    cells_pb2,
    cells_pb2_grpc,
    exec_pb2,
    exec_pb2_grpc,
    files_pb2,
    files_pb2_grpc,
    types_pb2,
    verify_pb2,
    verify_pb2_grpc,
)

DEFAULT_SOCKET = "/run/hivebox/comb.sock"
PROJECT_HEADER = "x-hive-project"
# How much of a file goes in one message of a write.
_WRITE_CHUNK = 1 << 20
_MAX_MESSAGE = 256 << 20
# Tries for a call that is safe to repeat, when the failure was hivebox's.
_TRIES = 3


def _seconds(v: float | str | None) -> duration_pb2.Duration | None:
    """Seconds as a number, or a string like 90, 1.5s, 10m or 2h."""
    if v is None:
        return None
    if isinstance(v, str):
        m = re.fullmatch(r"\s*([0-9]*\.?[0-9]+)\s*([smh]?)\s*", v)
        if not m:
            raise _errors.InvalidArgument(f"{v!r} is not a duration")
        v = float(m.group(1)) * {"": 1, "s": 1, "m": 60, "h": 3600}[m.group(2)]
    if v < 0:
        raise _errors.InvalidArgument("a duration must not be negative")
    d = duration_pb2.Duration()
    d.FromNanoseconds(int(round(v * 1e9)))
    return d


def _name(enum, value: int, prefix: str) -> str:
    return enum.Name(value).removeprefix(prefix).lower()


@dataclass
class Spec:
    """What a cell is made from and what it may use. One of `image`, `template` or `snapshot`
    is required, and everything left unset takes the node's default."""

    image: str | None = None
    template: str | None = None
    snapshot: str | None = None
    backend: str = "auto"
    cpu_milli: int = 0
    mem_mib: int = 0
    disk_gib: int = 0
    qos: str | None = None
    network_profile: str = ""
    idle_ttl: float | str | None = None
    idle_action: str | None = None
    hard_ttl: float | str | None = None
    labels: Mapping[str, str] = field(default_factory=dict)
    env: Mapping[str, str] = field(default_factory=dict)
    max_output_bytes: int = 0
    wall_time: float | str | None = None
    trusted_image: bool = False

    def to_proto(self) -> types_pb2.CellSpec:
        sources = [s for s in (self.image, self.template, self.snapshot) if s is not None]
        if len(sources) != 1:
            raise _errors.InvalidArgument("a spec needs exactly one of image, template or snapshot")
        s = types_pb2.CellSpec(
            backend=types_pb2.Backend.Value(f"BACKEND_{self.backend.upper()}"),
            resources=types_pb2.Resources(vcpu_milli=self.cpu_milli, mem_mib=self.mem_mib, disk_gib=self.disk_gib),
            network_profile=self.network_profile,
            labels=dict(self.labels),
            env=dict(self.env),
            trusted_image=self.trusted_image,
        )
        if self.image is not None:
            s.image.ref = self.image
        elif self.template is not None:
            s.template = self.template
        else:
            s.snapshot.id = self.snapshot
        if self.qos:
            s.qos = types_pb2.Qos.Value(f"QOS_{self.qos.upper()}")
        if self.idle_action:
            s.idle_action = types_pb2.IdleAction.Value(f"IDLE_ACTION_{self.idle_action.upper()}")
        for name in ("idle_ttl", "hard_ttl"):
            d = _seconds(getattr(self, name))
            if d is not None:
                getattr(s, name).CopyFrom(d)
        if self.max_output_bytes or self.wall_time is not None:
            s.limits.output_bytes = self.max_output_bytes
            d = _seconds(self.wall_time)
            if d is not None:
                s.limits.wall_time.CopyFrom(d)
        return s


@dataclass
class RunResult:
    """How a command ended. `signal` is the signal that killed it, 0 when it exited."""

    exit_code: int
    stdout: bytes
    stderr: bytes
    truncated: bool
    timed_out: bool
    wall: float
    signal: int

    @classmethod
    def _from(cls, r) -> RunResult:
        return cls(r.exit_code, r.stdout, r.stderr, r.truncated, r.timed_out, r.wall.ToNanoseconds() / 1e9, r.signal)

    @property
    def ok(self) -> bool:
        return self.exit_code == 0 and self.signal == 0 and not self.timed_out


@dataclass
class VerifyResult:
    """What a verifier made of a subject's changes. `passed` is every run exiting with 0, and
    `error` is why the check could not be done, if it could not, in which case `passed` says
    nothing about the subject. `scores` has the test counts pytest printed, the diff's size and
    how long each step took in milliseconds."""

    passed: bool
    exit_code: int
    output: bytes
    scores: dict[str, float]
    tampered: list[str]
    flaky: bool
    runs_passed: int
    error: _errors.HiveError | None

    @classmethod
    def _from(cls, r) -> VerifyResult:
        err = None
        if r.HasField("error"):
            err = _errors.make(r.error.reason, r.error.message, is_infra_error=r.error.is_infra_error,
                               errno_name=r.error.errno or None)
        return cls(r.passed, r.exit_code, r.output, dict(r.scores), list(r.tampered), r.flaky, r.runs_passed, err)

    @property
    def is_infra_error(self) -> bool:
        """The check failed because of hivebox, so a trainer should mask the sample."""
        return self.error is not None and self.error.is_infra_error


@dataclass
class SessionResult:
    """How a command in a session ended, with stdout and stderr together."""

    exit_code: int
    output: bytes
    truncated: bool
    timed_out: bool
    wall: float


@dataclass
class FileInfo:
    path: str
    type: str
    size: int
    mode: int
    modified: float
    symlink_target: str

    @classmethod
    def _from(cls, f) -> FileInfo:
        return cls(f.path, _name(files_pb2.FileType, f.type, "FILE_TYPE_"), f.size, f.mode,
                   f.modified_at.ToNanoseconds() / 1e9, f.symlink_target)


@dataclass
class BulkResult:
    """What a pause, resume or stop did. `failures` maps each cell that failed to its error."""

    matched: int
    succeeded: int
    failures: dict[str, _errors.HiveError]

    @classmethod
    def _from(cls, r) -> BulkResult:
        failures = {f.cell_id: _errors.make(f.error.reason, f.error.message, is_infra_error=f.error.is_infra_error,
                                            errno_name=f.error.errno or None) for f in r.failures}
        return cls(r.matched, r.succeeded, failures)


def _selector(selector: str | Mapping[str, str] | Cell) -> types_pb2.CellSelector:
    if isinstance(selector, Cell):
        return types_pb2.CellSelector(id=selector.id)
    if isinstance(selector, str):
        return types_pb2.CellSelector(id=selector)
    return types_pb2.CellSelector(labels=types_pb2.LabelSelector(match=dict(selector)))


class AsyncHive:
    """A connection to a comb's local API, or to a gate.

    The endpoint is `unix:/path/to/comb.sock`, `host:port` or `https://host:port` for a gate
    with TLS, or `http://host:port` for one without. It defaults to $HIVE_ENDPOINT, then to the
    comb's socket. The token defaults to $HIVE_TOKEN. Use it as an async context manager, or call
    `close`.
    """

    def __init__(self, endpoint: str | None = None, *, token: str | None = None, project: str | None = None):
        endpoint = endpoint or os.environ.get("HIVE_ENDPOINT") or f"unix:{os.environ.get('HIVE_SOCKET', DEFAULT_SOCKET)}"
        project = project or os.environ.get("HIVE_PROJECT")
        token = token or os.environ.get("HIVE_TOKEN")
        plain = endpoint.startswith(("unix:", "http://"))
        target = endpoint.removeprefix("http://").removeprefix("https://")
        options = [("grpc.max_receive_message_length", _MAX_MESSAGE), ("grpc.max_send_message_length", _MAX_MESSAGE)]
        if endpoint.startswith("unix:"):
            # grpcio sends the socket path as the authority, which the comb's HTTP/2 server
            # rejects as malformed, so every call fails with RST_STREAM.
            options.append(("grpc.default_authority", "localhost"))
        if endpoint.startswith("https://") or (token and not plain):
            creds = grpc.ssl_channel_credentials()
            if token:
                creds = grpc.composite_channel_credentials(creds, grpc.access_token_call_credentials(token))
            self._channel = grpc.aio.secure_channel(target, creds, options=options)
            self._metadata: tuple = ()
        else:
            self._channel = grpc.aio.insecure_channel(target, options=options)
            self._metadata = (("authorization", f"Bearer {token}"),) if token else ()
        if project:
            self._metadata += ((PROJECT_HEADER, project),)
        self.endpoint = endpoint
        self._cells = cells_pb2_grpc.CellsStub(self._channel)
        self._exec = exec_pb2_grpc.ExecStub(self._channel)
        self._files = files_pb2_grpc.FilesStub(self._channel)
        self._verify = verify_pb2_grpc.VerifyStub(self._channel)
        self.cells = Cells(self)

    async def close(self) -> None:
        await self._channel.close()

    async def __aenter__(self) -> AsyncHive:
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    async def verify(self, argv: Sequence[str], *, verifier: Spec, workdir: str, subject: Cell | str | None = None,
                     protected_paths: Iterable[str] = (), files: Mapping[str, bytes | str] | None = None,
                     repeats: int = 1, timeout: float | str | None = None) -> VerifyResult:
        """Checks the changes `subject` made to the git checkout at `workdir` in a fresh cell
        made from `verifier`, with no network, and runs `argv` there `repeats` times. Changes to
        `protected_paths`, globs like tests/** under `workdir`, are left out and reported in
        `tampered`. `files`, such as hidden tests, are written in after the changes, a relative
        path being under `workdir`. With no subject the image is checked as it is. `timeout` is
        for each run. A check that could not be done returns with `error` set rather than
        raising, unless the request itself was wrong."""
        if isinstance(argv, str):
            raise _errors.InvalidArgument("argv is a list, like [\"bash\", \"-c\", script]")
        subject_id = subject.id if isinstance(subject, Cell) else (subject or "")
        req = verify_pb2.VerifyRequest(
            subject_cell_id=subject_id,
            verifier=verifier.to_proto(),
            argv=list(argv),
            workdir=workdir,
            protected_paths=list(protected_paths),
            files={k: v.encode() if isinstance(v, str) else v for k, v in (files or {}).items()},
            repeats=repeats,
            timeout=_seconds(timeout),
        )
        return VerifyResult._from(await self._call(self._verify.Run, req))

    async def _call(self, method, request, *, retry: bool = False, timeout: float | None = None):
        """One unary call. With `retry`, a failure that was hivebox's is tried again, since
        repeating the call is safe."""
        for attempt in range(_TRIES if retry else 1):
            try:
                return await method(request, metadata=self._metadata, timeout=timeout)
            except grpc.aio.AioRpcError as e:
                err = _errors.from_rpc(e)
                if not (retry and err.is_infra_error) or attempt == _TRIES - 1:
                    raise err from None
            await asyncio.sleep(0.05 * 4**attempt)


class Cells:
    """The cells of the client's project."""

    def __init__(self, hive: AsyncHive):
        self._hive = hive

    async def create(self, spec: Spec, *, idempotency_key: str = "") -> Cell:
        """Makes one cell and returns once it is running."""
        [result] = await self._create(spec, 1, idempotency_key)
        if isinstance(result, Exception):
            raise result
        return result

    def create_many(self, spec: Spec, count: int, *, idempotency_key: str = "") -> CellGroup:
        """Makes `count` cells at once. Await it for the cells, or use it as an async context
        manager to have them all stopped on the way out. Any cell that failed raises the first
        failure, after the others are stopped."""
        return CellGroup(self, spec, count, idempotency_key)

    async def _create(self, spec: Spec, count: int, key: str) -> list[Cell | _errors.HiveError]:
        req = cells_pb2.CreateRequest(spec=spec.to_proto(), count=count, idempotency_key=key)
        out: list[Cell | _errors.HiveError] = [_errors.Internal("the create ended without this cell")] * max(count, 1)
        try:
            async for e in self._hive._cells.Create(req, metadata=self._hive._metadata):
                if e.HasField("cell"):
                    out[e.index] = Cell(self._hive, e.cell)
                else:
                    out[e.index] = _errors.make(e.error.reason, e.error.message, is_infra_error=e.error.is_infra_error)
        except grpc.aio.AioRpcError as e:
            raise _errors.from_rpc(e) from None
        return out

    async def get(self, cell_id: str) -> Cell:
        c = await self._hive._call(self._hive._cells.Get, cells_pb2.GetCellRequest(id=cell_id), retry=True)
        return Cell(self._hive, c)

    async def list(self, labels: Mapping[str, str] | None = None, states: Iterable[str] = ()) -> list[Cell]:
        """Every cell with all of `labels`, in any of `states` (like "running"), or in any state."""
        states = [types_pb2.CellState.Value(f"CELL_STATE_{s.upper()}") for s in states]
        selector = types_pb2.LabelSelector(match=dict(labels)) if labels else None
        out, token = [], ""
        while True:
            req = cells_pb2.ListCellsRequest(selector=selector, states=states, page_token=token)
            page = await self._hive._call(self._hive._cells.List, req, retry=True)
            out.extend(Cell(self._hive, c) for c in page.cells)
            if not page.next_page_token:
                return out
            token = page.next_page_token

    async def pause(self, selector: str | Mapping[str, str] | Cell) -> BulkResult:
        """Pauses one cell by id, or every running cell with the labels in a dict."""
        return BulkResult._from(await self._hive._call(self._hive._cells.Pause, _selector(selector), retry=True))

    async def resume(self, selector: str | Mapping[str, str] | Cell) -> BulkResult:
        return BulkResult._from(await self._hive._call(self._hive._cells.Resume, _selector(selector), retry=True))

    async def stop(self, selector: str | Mapping[str, str] | Cell) -> BulkResult:
        req = cells_pb2.StopRequest(selector=_selector(selector))
        return BulkResult._from(await self._hive._call(self._hive._cells.Stop, req, retry=True))

    async def watch(self, selector: str | Mapping[str, str] | Cell | None = None) -> AsyncIterator[tuple[str, str, str]]:
        """Yields (cell id, from state, to state) as cells change, starting with each one's
        current state with an empty from. A watch of one cell ends once the cell has ended, and
        no selector watches every cell of the project."""
        req = cells_pb2.WatchCellsRequest(selector=_selector(selector if selector is not None else {}))
        call = self._hive._cells.Watch(req, metadata=self._hive._metadata)
        try:
            async for e in call:
                # `from` is a Python keyword, so the field is only reachable by name.
                yield e.cell.id, _state(getattr(e, "from")), _state(e.cell.state)
        except grpc.aio.AioRpcError as e:
            raise _errors.from_rpc(e) from None


def _state(v: int) -> str:
    return "" if v == 0 else _name(types_pb2.CellState, v, "CELL_STATE_")


class CellGroup:
    """Cells made together by `Cells.create_many`."""

    def __init__(self, cells: Cells, spec: Spec, count: int, key: str):
        self._args = (cells, spec, count, key)
        self.cells: list[Cell] = []

    async def _make(self) -> list[Cell]:
        cells, spec, count, key = self._args
        results = await cells._create(spec, count, key)
        self.cells = [r for r in results if isinstance(r, Cell)]
        failed = [r for r in results if not isinstance(r, Cell)]
        if failed:
            await self.stop()
            raise failed[0]
        return self.cells

    def __await__(self):
        return self._make().__await__()

    async def __aenter__(self) -> CellGroup:
        await self._make()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.stop()

    async def stop(self) -> None:
        await asyncio.gather(*(c.stop() for c in self.cells), return_exceptions=True)

    def __iter__(self):
        return iter(self.cells)

    def __len__(self) -> int:
        return len(self.cells)

    def __getitem__(self, i: int) -> Cell:
        return self.cells[i]


class Cell:
    """One cell, as it was when it was fetched."""

    def __init__(self, hive: AsyncHive, info: types_pb2.Cell):
        self._hive = hive
        self.info = info
        self.files = Files(hive, info.id)

    def __repr__(self) -> str:
        return f"Cell({self.id!r}, {self.state!r})"

    @property
    def id(self) -> str:
        return self.info.id

    @property
    def state(self) -> str:
        return _state(self.info.state)

    @property
    def labels(self) -> dict[str, str]:
        return dict(self.info.labels)

    async def refresh(self) -> Cell:
        self.info = (await self._hive.cells.get(self.id)).info
        return self

    async def pause(self) -> None:
        _one(await self._hive.cells.pause(self.id))

    async def resume(self) -> None:
        _one(await self._hive.cells.resume(self.id))

    async def stop(self) -> None:
        _one(await self._hive.cells.stop(self.id))

    async def run(self, cmd: str | Sequence[str], *, timeout: float | str | None = None, stdin: bytes = b"",
                  env: Mapping[str, str] | None = None, cwd: str = "", user: str = "", max_output_bytes: int = 0) -> RunResult:
        """Runs a command and waits for it. A string runs with sh -c, a list runs as argv. A
        command that ran and failed returns its exit code, it does not raise."""
        req = exec_pb2.RunRequest(cell_id=self.id, cwd=cwd, env=dict(env or {}), stdin=stdin,
                                  timeout=_seconds(timeout), max_output_bytes=max_output_bytes, user=user)
        if isinstance(cmd, str):
            req.shell = cmd
        else:
            req.argv.extend(cmd)
        return RunResult._from(await self._hive._call(self._hive._exec.Run, req))

    async def start(self, cmd: str | Sequence[str], *, timeout: float | str | None = None,
                    env: Mapping[str, str] | None = None, cwd: str = "", user: str = "") -> Process:
        """Starts a command with its input and output streamed."""
        start = exec_pb2.ProcessStart(cell_id=self.id, cwd=cwd, env=dict(env or {}), timeout=_seconds(timeout), user=user)
        if isinstance(cmd, str):
            start.shell = cmd
        else:
            start.argv.extend(cmd)
        p = Process(self._hive)
        await p._start(start)
        return p

    async def session(self, *, cwd: str = "", env: Mapping[str, str] | None = None, user: str = "") -> Session:
        """A shell that keeps its directory and environment from one command to the next."""
        req = exec_pb2.SessionCreateRequest(cell_id=self.id, cwd=cwd, env=dict(env or {}), user=user)
        s = await self._hive._call(self._hive._exec.SessionCreate, req)
        return Session(self._hive, exec_pb2.SessionRef(cell_id=s.cell_id, id=s.id))


def _one(r: BulkResult) -> None:
    for err in r.failures.values():
        raise err


class Process:
    """A command started with `Cell.start`. Iterate it for ("stdout" | "stderr", bytes) until it
    ends, then `result` holds how it ended. Leaving it early kills the command."""

    def __init__(self, hive: AsyncHive):
        self._hive = hive
        self._input: asyncio.Queue = asyncio.Queue()
        self.pid: int | None = None
        self.result: RunResult | None = None

    async def _requests(self):
        while True:
            item = await self._input.get()
            if item is None:
                return
            yield item

    async def _start(self, start) -> None:
        self._input.put_nowait(exec_pb2.ProcessInput(start=start))
        self._call = self._hive._exec.Start(self._requests(), metadata=self._hive._metadata)

    async def write(self, data: bytes) -> None:
        await self._input.put(exec_pb2.ProcessInput(stdin=data))

    async def signal(self, n: int) -> None:
        await self._input.put(exec_pb2.ProcessInput(signal=n))

    async def close_stdin(self) -> None:
        """Closes its stdin, so it reads end of file."""
        await self._input.put(None)

    def __aiter__(self):
        return self._outputs()

    async def _outputs(self):
        try:
            async for o in self._call:
                kind = o.WhichOneof("output")
                if kind == "pid":
                    self.pid = o.pid
                elif kind == "stdout":
                    yield "stdout", o.stdout
                elif kind == "stderr":
                    yield "stderr", o.stderr
                elif kind == "exit":
                    self.result = RunResult._from(getattr(o, "exit"))
                    return
        except grpc.aio.AioRpcError as e:
            raise _errors.from_rpc(e) from None
        finally:
            self._call.cancel()

    async def wait(self) -> RunResult:
        """Reads to the end, dropping the output, and returns how it ended."""
        async for _ in self:
            pass
        if self.result is None:
            raise _errors.Internal("the output ended before the command exited")
        return self.result


class Session:
    """A shell in a cell that keeps its state between commands."""

    def __init__(self, hive: AsyncHive, ref):
        self._hive = hive
        self._ref = ref

    @property
    def id(self) -> str:
        return self._ref.id

    async def run(self, command: str, *, timeout: float | str | None = None, max_output_bytes: int = 0) -> SessionResult:
        req = exec_pb2.SessionRunRequest(session=self._ref, command=command, timeout=_seconds(timeout), max_output_bytes=max_output_bytes)
        r = await self._hive._call(self._hive._exec.SessionRun, req)
        return SessionResult(r.exit_code, r.output, r.truncated, r.timed_out, r.wall.ToNanoseconds() / 1e9)

    async def close(self) -> None:
        await self._hive._call(self._hive._exec.SessionClose, self._ref)

    async def __aenter__(self) -> Session:
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()


class Files:
    """The files of one cell. Failures raise FileError, which is also the matching OSError."""

    def __init__(self, hive: AsyncHive, cell_id: str):
        self._hive = hive
        self._id = cell_id

    async def read(self, path: str, *, offset: int = 0, length: int = 0) -> bytes:
        """Reads a file, or `length` bytes of it from `offset`."""
        return b"".join([c async for c in self.open(path, offset=offset, length=length)])

    async def read_text(self, path: str, encoding: str = "utf-8") -> str:
        return (await self.read(path)).decode(encoding)

    async def open(self, path: str, *, offset: int = 0, length: int = 0) -> AsyncIterator[bytes]:
        """Yields a file a chunk at a time, for files too big to hold."""
        req = files_pb2.ReadFileRequest(cell_id=self._id, path=path, offset=offset, length=length)
        try:
            async for chunk in self._hive._files.Read(req, metadata=self._hive._metadata):
                yield chunk.data
        except grpc.aio.AioRpcError as e:
            raise _errors.from_rpc(e) from None

    async def write(self, path: str, data: bytes | str, *, mode: int = 0, append: bool = False) -> FileInfo:
        """Writes a file, making its parent directories. Readers see the old file or the new
        one, never half of it."""
        if isinstance(data, str):
            data = data.encode()
        header = files_pb2.WriteFileHeader(cell_id=self._id, path=path, mode=mode, make_parents=True, append=append)

        def chunks():
            yield files_pb2.WriteFileChunk(header=header)
            view = memoryview(data)
            for at in range(0, len(view), _WRITE_CHUNK):
                yield files_pb2.WriteFileChunk(data=bytes(view[at:at + _WRITE_CHUNK]))

        return FileInfo._from(await self._hive._call(self._hive._files.Write, chunks()))

    async def stat(self, path: str) -> FileInfo:
        req = files_pb2.PathRequest(cell_id=self._id, path=path)
        return FileInfo._from(await self._hive._call(self._hive._files.Stat, req, retry=True))

    async def exists(self, path: str) -> bool:
        try:
            await self.stat(path)
            return True
        except FileNotFoundError:
            return False

    async def list(self, path: str, *, depth: int = 1) -> list[FileInfo]:
        """A directory's entries `depth` levels down, sorted by path."""
        req = files_pb2.ListDirRequest(cell_id=self._id, path=path, depth=depth)
        r = await self._hive._call(self._hive._files.List, req, retry=True)
        return [FileInfo._from(e) for e in r.entries]

    async def remove(self, path: str, *, recursive: bool = False) -> None:
        req = files_pb2.PathRequest(cell_id=self._id, path=path, recursive=recursive)
        await self._hive._call(self._hive._files.Remove, req)

    async def upload(self, path: str, tar: bytes) -> None:
        """Unpacks a tar archive into the directory `path`, making it if it is missing."""
        req = files_pb2.ApplyRequest(cell_id=self._id, path=path, tar=tar)
        await self._hive._call(self._hive._files.Apply, req)

    async def watch(self, path: str, *, recursive: bool = False) -> AsyncIterator[tuple[str, str]]:
        """Yields (kind, path) for each change under a directory, like ("create", "/w/a.py")."""
        req = files_pb2.WatchDirRequest(cell_id=self._id, path=path, recursive=recursive)
        try:
            async for e in self._hive._files.Watch(req, metadata=self._hive._metadata):
                yield _name(files_pb2.FsEventKind, e.kind, "FS_EVENT_KIND_"), e.path
        except grpc.aio.AioRpcError as e:
            raise _errors.from_rpc(e) from None
