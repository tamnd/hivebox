"""The state the tools of one trajectory share: its task, its cell, its shell and its verdict."""

from __future__ import annotations

import asyncio
import contextvars
import os
import time
from collections import OrderedDict
from typing import Any, Mapping

from .. import _errors
from .._client import AsyncHive, Cell, Session, Spec, VerifyResult

# How long a cell may outlive the trajectory's budget, for the verifier to take its diff. A
# cell nobody stops, because the worker died or no loop ran, ends at that point anyway.
GRACE_S = 900

# Set by HiveAgentLoop for the trajectory it runs. The tools' calls run in tasks made inside the
# loop's run, so they see it too.
current: contextvars.ContextVar[Trajectory | None] = contextvars.ContextVar("hivebox_trajectory", default=None)

# Trajectories found by request id, for tools used without HiveAgentLoop. Closed ones stay a
# while, so a call after submit is refused rather than making a new cell.
_by_request: OrderedDict[str, Trajectory] = OrderedDict()
_KEEP = 1 << 16

_clients: dict[tuple, AsyncHive] = {}


def client(config: Mapping[str, Any]) -> AsyncHive:
    """One client for each endpoint, token and project in each event loop."""
    endpoint = config.get("endpoint") or os.environ.get("HIVE_ENDPOINT")
    token = config.get("token") or os.environ.get("HIVE_TOKEN")
    project = config.get("project") or os.environ.get("HIVE_PROJECT")
    key = (endpoint, token, project, id(asyncio.get_running_loop()))
    if key not in _clients:
        _clients[key] = AsyncHive(endpoint, token=token, project=project)
    return _clients[key]


def task(config: Mapping[str, Any], *overrides: Mapping[str, Any] | None) -> dict[str, Any]:
    """The task from the config's `task` with each override's keys on top, one level deep for
    `cell`, `limits`, `verify` and `labels`, so a sample can set only `verify.files`."""
    out: dict[str, Any] = {k: dict(v) if isinstance(v, Mapping) else v for k, v in (config.get("task") or {}).items()}
    for o in overrides:
        for k, v in (o or {}).items():
            if isinstance(v, Mapping) and isinstance(out.get(k), dict):
                out[k] = {**out[k], **v}
            else:
                out[k] = v
    for need in ("image", "workdir"):
        if not out.get(need):
            raise _errors.InvalidArgument(f"the hivebox task has no {need}")
    return out


def spec(image: str, size: Mapping[str, Any] | None, **kw) -> Spec:
    size = size or {}
    return Spec(image=image, backend=size.get("backend", "container"), mem_mib=int(size.get("mem_mib", 1024)),
                cpu_milli=int(size.get("vcpu_milli", 1000)), **kw)


class Trajectory:
    """One trajectory's cell, made on the first tool call, its shell and its verdict."""

    def __init__(self, hive: AsyncHive, task: dict[str, Any], key: str):
        self.hive = hive
        self.task = task
        self.key = key
        self.cell: Cell | None = None
        self.result: VerifyResult | None = None
        self.error: _errors.HiveError | None = None
        self.submitted = False
        self.closed = False
        self.verify_s = 0.0
        self.history: dict[str, list[str]] = {}
        self._session: Session | None = None
        self._lock = asyncio.Lock()

    @property
    def workdir(self) -> str:
        return self.task["workdir"]

    @property
    def command_timeout(self) -> float:
        return float(self.task.get("limits", {}).get("command_timeout_s", 600))

    async def get_cell(self) -> Cell:
        async with self._lock:
            if self.closed:
                raise _errors.CellNotRunning("the trajectory's changes were submitted and its cell is stopped")
            if self.cell is None:
                limits = self.task.get("limits", {})
                labels = {**self.task.get("labels", {}), "trajectory": self.key[:256]}
                if self.task.get("task_id"):
                    labels["task"] = str(self.task["task_id"])[:256]
                s = spec(self.task["image"], self.task.get("cell"), labels=labels,
                         hard_ttl=float(limits.get("max_wall_s", 1800)) + GRACE_S)
                self.cell = await self.hive.cells.create(s)
            return self.cell

    async def shell(self, command: str, timeout: float) -> tuple[str, int, bool]:
        """Runs `command` in the trajectory's shell, which keeps its directory and variables
        between calls. A shell a timeout killed is opened again for the next command."""
        cell = await self.get_cell()
        note = ""
        for attempt in (0, 1):
            if self._session is None:
                self._session = await cell.session(cwd=self.workdir)
            try:
                r = await self._session.run(command, timeout=timeout)
                break
            except _errors.HiveError:
                self._session = None
                if attempt:
                    raise
                note = "(the shell had ended, so this ran in a new one, starting in the workdir)\n"
        if r.timed_out:
            self._session = None
        return note + r.output.decode("utf-8", "replace"), r.exit_code, r.timed_out

    async def verify(self) -> VerifyResult | None:
        """Checks the cell's changes once, however often it is asked. A trajectory that never
        made a cell is checked against the image as it is."""
        async with self._lock:
            if self.result is not None or self.error is not None:
                return self.result
            v = self.task.get("verify") or {}
            if not v.get("argv"):
                self.error = _errors.InvalidArgument("the hivebox task has no verify.argv")
                return None
            t = time.monotonic()
            try:
                self.result = await self.hive.verify(
                    v["argv"],
                    subject=self.cell,
                    verifier=spec(v.get("image") or self.task["image"], v.get("cell") or self.task.get("cell")),
                    workdir=self.workdir,
                    protected_paths=v.get("protected_paths", ()),
                    files=v.get("files"),
                    repeats=int(v.get("repeats", 1)),
                    timeout=v.get("timeout_s") or None,
                    grader=v.get("grader"),
                    task=v.get("task", ""),
                    grader_files=v.get("grader_files", ()),
                )
            except _errors.HiveError as e:
                self.error = e
            self.verify_s = time.monotonic() - t
            return self.result

    @property
    def is_infra_error(self) -> bool:
        if self.error is not None:
            return bool(self.error.is_infra_error)
        return self.result is not None and self.result.is_infra_error

    @property
    def reward(self) -> float | None:
        """1 when the tests passed, 0 when they did not or the agent changed a protected path and
        the task says that counts, and None when hivebox failed and the sample should be masked.
        With `verify.grader` in the task, the grader's reward stands in for the 1, and a sample it
        could not grade gets None too."""
        if self.is_infra_error:
            return None
        r = self.result
        if r is None or r.error is not None:
            return 0.0
        v = self.task.get("verify") or {}
        tampered = bool(r.tampered) and v.get("zero_on_tamper", True)
        if v.get("grader"):
            return None if r.reward is None else (0.0 if tampered else r.reward)
        return 1.0 if r.passed and not tampered else 0.0

    def fields(self) -> dict[str, Any]:
        """What goes in the sample's extra fields. Every sample has every key."""
        r = self.result
        err = self.error or (r.error if r else None)
        return {
            "hive_cell": self.cell.id if self.cell else "",
            "hive_passed": bool(r and r.passed and r.error is None),
            "hive_infra_error": self.is_infra_error,
            "hive_tampered": len(r.tampered) if r else 0,
            "hive_submitted": self.submitted,
            "hive_verify_s": round(self.verify_s, 3),
            "hive_error": f"{err.reason}: {err}" if err else "",
        }

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        cell, self._session, self.history = self.cell, None, {}
        if cell is not None:
            try:
                await cell.stop()
            except _errors.HiveError:
                pass  # It ends at its hard TTL anyway.


def find(config: Mapping[str, Any], kwargs: Mapping[str, Any], create_kwargs: Mapping[str, Any] | None) -> Trajectory:
    """The trajectory a tool call belongs to: the one HiveAgentLoop set, or else one found by
    the request id of verl's agent data."""
    t = current.get()
    if t is not None:
        return t
    agent_data = kwargs.get("agent_data")
    key = getattr(agent_data, "request_id", None)
    if not key:
        raise _errors.InvalidArgument("a hivebox tool needs HiveAgentLoop or verl's agent_data to know its trajectory")
    if key not in _by_request:
        _by_request[key] = Trajectory(client(config), task(config, create_kwargs), key)
        while len(_by_request) > _KEEP:
            _by_request.popitem(last=False)
    return _by_request[key]
