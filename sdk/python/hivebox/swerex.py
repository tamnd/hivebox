"""A SWE-ReX deployment whose runtime is a hivebox cell.

SWE-agent and other harnesses built on SWE-ReX talk to a sandbox through `AbstractDeployment`
and `AbstractRuntime`. `HiveboxDeployment` makes a cell when it starts and stops it when it stops,
and `HiveboxRuntime` maps the runtime calls onto the cell: a bash session is a hivebox session,
`execute` is a run, and files go through the files API. Nothing runs in the cell but the drone,
so any image with bash works, without the swerex server installed in it.

    from hivebox.swerex import HiveboxDeployment

    deployment = HiveboxDeployment(image="swe-requests", mem_mib=2048)
    await deployment.start()
    await deployment.runtime.create_session(CreateBashSessionRequest())
    obs = await deployment.runtime.run_in_session(BashAction(command="cd /testbed && git status"))
    await deployment.stop()

What differs from SWE-ReX's own runtime: output lines end in a plain newline, as there is no
terminal, and interactive commands (`is_interactive_command`, `is_interactive_quit`) are not
supported. Syntax is checked with `bash -n` on this machine before a command is sent, as SWE-ReX
does, so a bad command raises `BashIncorrectSyntaxError` without a round trip.
"""

from __future__ import annotations

import asyncio
import io
import logging
import os
import shutil
import tarfile
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict
from swerex.deployment.abstract import AbstractDeployment
from swerex.deployment.hooks.abstract import CombinedDeploymentHook, DeploymentHook
from swerex.exceptions import (
    BashIncorrectSyntaxError,
    CommandTimeoutError,
    DeploymentNotStartedError,
    NonZeroExitCodeError,
    SessionDoesNotExistError,
    SessionExistsError,
    SwerexException,
)
from swerex.runtime.abstract import (
    AbstractRuntime,
    Action,
    BashAction,
    BashInterruptAction,
    BashObservation,
    CloseBashSessionResponse,
    CloseResponse,
    CloseSessionRequest,
    CloseSessionResponse,
    Command,
    CommandResponse,
    CreateBashSessionRequest,
    CreateBashSessionResponse,
    CreateSessionRequest,
    CreateSessionResponse,
    IsAliveResponse,
    Observation,
    ReadFileRequest,
    ReadFileResponse,
    UploadRequest,
    UploadResponse,
    WriteFileRequest,
    WriteFileResponse,
)

from . import _errors
from ._client import AsyncHive, Cell, Session, Spec

__all__ = ["HiveboxDeployment", "HiveboxDeploymentConfig", "HiveboxRuntime"]

_log = logging.getLogger("hivebox.swerex")


def _text(b: bytes) -> str:
    return b.decode(errors="backslashreplace")


async def _check_syntax(command: str) -> None:
    """Raises BashIncorrectSyntaxError when bash cannot parse `command`. Without bash on this
    machine there is no check, and the cell's bash says what is wrong instead."""
    bash = shutil.which("bash")
    if bash is None:
        return
    p = await asyncio.create_subprocess_exec(bash, "-n", stdin=asyncio.subprocess.PIPE,
                                             stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    out, err = await p.communicate(command.encode())
    if p.returncode == 0:
        return
    stdout, stderr = _text(out), _text(err)
    msg = (f"Error (exit code {p.returncode}) while checking bash command \n{command!r}\n"
           f"---- Stderr ----\n{stderr}\n---- Stdout ----\n{stdout}")
    raise BashIncorrectSyntaxError(msg, extra_info={"bash_stdout": stdout, "bash_stderr": stderr})


def _tar(source: str, name: str) -> bytes:
    """`source`, a file or a directory, as a tar archive holding it as `name`, or holding what is
    in it when `name` is "."."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        tar.add(source, arcname=name)
    return buf.getvalue()


class HiveboxRuntime(AbstractRuntime):
    """The SWE-ReX runtime API on one cell. It does not own the cell: `close` ends the sessions
    and leaves the cell running."""

    def __init__(self, cell: Cell, *, logger: logging.Logger | None = None):
        self.cell = cell
        self.logger = logger or _log
        self._sessions: dict[str, Session] = {}

    async def is_alive(self, *, timeout: float | None = None) -> IsAliveResponse:
        try:
            r = await asyncio.wait_for(self.cell.run(["true"]), timeout)
        except (asyncio.TimeoutError, _errors.HiveError) as e:
            return IsAliveResponse(is_alive=False, message=str(e) or type(e).__name__)
        return IsAliveResponse(is_alive=r.ok, message="" if r.ok else f"true exited with {r.exit_code}")

    async def create_session(self, request: CreateSessionRequest) -> CreateSessionResponse:
        if not isinstance(request, CreateBashSessionRequest):
            raise ValueError(f"unknown session type: {request!r}")
        if request.session in self._sessions:
            raise SessionExistsError(f"session {request.session} already exists")
        session = await self.cell.session()
        self._sessions[request.session] = session
        output = ""
        if request.startup_source:
            cmd = " ; ".join(f"source {path}" for path in request.startup_source)
            r = await session.run(cmd, timeout=max(request.startup_timeout, 1.0))
            output = _text(r.output)
        return CreateBashSessionResponse(output=output)

    async def run_in_session(self, action: Action) -> Observation:
        session = self._sessions.get(action.session)
        if session is None:
            raise SessionDoesNotExistError(f"session {action.session!r} does not exist")
        if isinstance(action, BashInterruptAction):
            # A command runs to its end or its timeout inside one call, so there is nothing
            # left running to interrupt.
            return BashObservation(exit_code=0)
        if action.is_interactive_command or action.is_interactive_quit:
            raise SwerexException("hivebox sessions do not run interactive commands")
        await _check_syntax(action.command)
        r = await session.run(action.command, timeout=action.timeout)
        if r.timed_out:
            raise CommandTimeoutError(f"timeout after {action.timeout} seconds while running command {action.command!r}")
        output = _text(r.output)
        if action.check == "ignore":
            return BashObservation(output=output, exit_code=None)
        if action.check == "raise" and r.exit_code != 0:
            msg = f"Command {action.command!r} failed with exit code {r.exit_code}. Here is the output:\n{output!r}"
            if action.error_msg:
                msg = f"{action.error_msg}: {msg}"
            raise NonZeroExitCodeError(msg)
        return BashObservation(output=output, exit_code=r.exit_code)

    async def close_session(self, request: CloseSessionRequest) -> CloseSessionResponse:
        session = self._sessions.pop(request.session, None)
        if session is None:
            raise SessionDoesNotExistError(f"session {request.session!r} does not exist")
        await session.close()
        return CloseBashSessionResponse()

    async def execute(self, command: Command) -> CommandResponse:
        if command.shell:
            argv: list[str] = ["bash", "-c", command.command if isinstance(command.command, str)
                               else " ".join(command.command)]
        elif isinstance(command.command, str):
            argv = [command.command]
        else:
            argv = list(command.command)
        if command.merge_output_streams:
            argv = ["sh", "-c", 'exec "$@" 2>&1', "sh", *argv]
        r = await self.cell.run(argv, timeout=command.timeout, env=command.env, cwd=command.cwd or "")
        if r.timed_out:
            raise CommandTimeoutError(f"Timeout ({command.timeout}s) exceeded while running command")
        out = CommandResponse(stdout=_text(r.stdout), stderr=_text(r.stderr), exit_code=r.exit_code)
        if command.check and r.exit_code != 0:
            msg = (f"Command {command.command!r} failed with exit code {r.exit_code}. "
                   f"Stdout:\n{out.stdout!r}\nStderr:\n{out.stderr!r}")
            if command.error_msg:
                msg = f"{command.error_msg}: {msg}"
            raise NonZeroExitCodeError(msg)
        return out

    async def read_file(self, request: ReadFileRequest) -> ReadFileResponse:
        data = await self.cell.files.read(request.path)
        return ReadFileResponse(content=data.decode(request.encoding or "utf-8", request.errors or "strict"))

    async def write_file(self, request: WriteFileRequest) -> WriteFileResponse:
        await self.cell.files.write(request.path, request.content)
        return WriteFileResponse()

    async def upload(self, request: UploadRequest) -> UploadResponse:
        """Copies `source_path` on this machine to `target_path` in the cell, a file as that file
        and a directory as that directory."""
        source, target = request.source_path, request.target_path.rstrip("/") or "/"
        if os.path.isdir(source):
            await self.cell.files.upload(target, _tar(source, "."))
        else:
            parent, name = os.path.split(target)
            await self.cell.files.upload(parent or "/", _tar(source, name))
        return UploadResponse()

    async def close(self) -> CloseResponse:
        sessions, self._sessions = list(self._sessions.values()), {}
        for s in sessions:
            try:
                await s.close()
            except _errors.HiveError as e:
                self.logger.debug("closing a session: %s", e)
        return CloseResponse()


class HiveboxDeployment(AbstractDeployment):
    """A cell made from `spec`, or from an image and the keyword arguments of `Spec`, as a SWE-ReX
    deployment. `hive` defaults to a client made from $HIVE_ENDPOINT, $HIVE_TOKEN and
    $HIVE_PROJECT, which the deployment closes when it stops."""

    def __init__(self, *, image: str | None = None, spec: Spec | None = None, hive: AsyncHive | None = None,
                 logger: logging.Logger | None = None, **spec_kw: Any):
        if (image is None) == (spec is None):
            raise ValueError("give an image or a spec, not both")
        if spec is None:
            spec_kw.setdefault("backend", "container")
            spec = Spec(image=image, **spec_kw)
        elif spec_kw:
            raise ValueError("with a spec, put the cell's settings in it")
        self.spec = spec
        self.logger = logger or _log
        self._hive = hive
        self._own_hive = hive is None
        self._hooks = CombinedDeploymentHook()
        self._cell: Cell | None = None
        self._runtime: HiveboxRuntime | None = None

    def add_hook(self, hook: DeploymentHook) -> None:
        self._hooks.add_hook(hook)

    @property
    def cell(self) -> Cell | None:
        """The cell, once started."""
        return self._cell

    async def is_alive(self, *, timeout: float | None = None) -> IsAliveResponse:
        return await self.runtime.is_alive(timeout=timeout)

    async def start(self) -> None:
        if self._runtime is not None:
            return
        if self._hive is None:
            self._hive = AsyncHive()
        self._hooks.on_custom_step("Starting hivebox cell")
        self._cell = await self._hive.cells.create(self.spec)
        self.logger.info("hivebox cell %s is running", self._cell.id)
        self._runtime = HiveboxRuntime(self._cell, logger=self.logger)

    async def stop(self) -> None:
        runtime, cell = self._runtime, self._cell
        self._runtime = self._cell = None
        if runtime is not None:
            await runtime.close()
        if cell is not None:
            try:
                await cell.stop()
            except _errors.CellNotFound:
                pass
        if self._own_hive and self._hive is not None:
            await self._hive.close()
            self._hive = None

    @property
    def runtime(self) -> HiveboxRuntime:
        if self._runtime is None:
            raise DeploymentNotStartedError()
        return self._runtime


class HiveboxDeploymentConfig(BaseModel):
    """The deployment as config, next to SWE-ReX's own `*DeploymentConfig` classes."""

    image: str
    """The image the cell is made from."""
    backend: str = "container"
    cpu_milli: int = 0
    mem_mib: int = 0
    disk_gib: int = 0
    network_profile: str = ""
    hard_ttl: float | str | None = None
    labels: dict[str, str] = {}
    env: dict[str, str] = {}

    type: Literal["hivebox"] = "hivebox"
    """Discriminator for (de)serialization/CLI. Do not change."""

    model_config = ConfigDict(extra="forbid")

    def get_deployment(self) -> HiveboxDeployment:
        return HiveboxDeployment(**self.model_dump(exclude={"type"}))
