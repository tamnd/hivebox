"""An OpenEnv environment whose episodes run in hivebox cells.

OpenEnv serves an environment over HTTP and WebSocket, and trainers such as TRL, OpenRLHF and
NeMo-RL talk to it through `EnvClient`. `HiveboxEnv` is an environment where each episode is a
fresh cell with one bash shell: an action is a command, its observation is the output and exit
code, and an action with `submit` ends the episode with a reward. `create_hivebox_app` serves it.

    from hivebox.openenv import create_hivebox_app

    app = create_hivebox_app(image="swe-requests", mem_mib=2048, workdir="/testbed",
                             check="python -m pytest -q tests/test_fix.py", max_steps=50)
    # uvicorn module:app --port 8000

The reward is 1.0 or 0.0. With `check`, a command run in the episode's cell decides it. With
`verify`, an argv run by `Verify.Run` on the episode's changes in a fresh cell with no network,
which the agent cannot have touched, decides it. How it was decided is in the last
observation's `verdict`. A cell is stopped when its episode ends, when the next reset starts, or
when the session closes. Every WebSocket session gets its own environment, so episodes run side
by side up to the server's `max_concurrent_envs`.
"""

from __future__ import annotations

import asyncio
import threading
import uuid
from collections.abc import Callable, Coroutine, Mapping, Sequence
from typing import Any, TypeVar

from openenv.core.env_server.http_server import create_app
from openenv.core.env_server.interfaces import Environment
from openenv.core.env_server.types import Action, EnvironmentMetadata, Observation, State
from pydantic import Field

from . import _errors
from ._client import AsyncHive, Cell, Session, Spec

__all__ = ["CommandAction", "CommandObservation", "HiveboxEnv", "HiveboxState", "create_hivebox_app"]

T = TypeVar("T")


class CommandAction(Action):
    """A command for the episode's shell, which keeps its directory and variables."""

    command: str = Field(default="", description="A bash command. Empty runs nothing.")
    submit: bool = Field(default=False, description="End the episode after the command and score it.")
    timeout_s: float | None = Field(default=None, description="The longest the command may take, within the server's limit.")


class CommandObservation(Observation):
    """What the command printed, stdout and stderr together, and how it ended."""

    output: str = Field(default="", description="What the command printed")
    exit_code: int | None = Field(default=None, description="Its exit code, None when nothing ran")
    timed_out: bool = Field(default=False, description="Whether it ran out of time, which loses the shell's state")
    truncated: bool = Field(default=False, description="Whether the output was cut at the cell's limit")
    cell_id: str = Field(default="", description="The episode's cell")
    verdict: dict[str, Any] = Field(default_factory=dict, description="How the episode was scored, once it ends")


class HiveboxState(State):
    cell_id: str | None = Field(default=None, description="The episode's cell")


class _Loop:
    """One event loop on a thread of its own for every environment's hivebox calls. OpenEnv calls
    an environment from its server's loop or from executor threads, and a gRPC channel belongs
    to the loop it was made on, so all of them go here."""

    _lock = threading.Lock()
    _loop: asyncio.AbstractEventLoop | None = None

    @classmethod
    def get(cls) -> asyncio.AbstractEventLoop:
        with cls._lock:
            if cls._loop is None:
                loop = asyncio.new_event_loop()
                threading.Thread(target=loop.run_forever, name="hivebox-openenv", daemon=True).start()
                cls._loop = loop
            return cls._loop


class HiveboxEnv(Environment[CommandAction, CommandObservation, HiveboxState]):
    """Episodes in cells made from `spec`, or from an image and the keyword arguments of `Spec`.
    The client is made from `endpoint`, `token` and `project`, or $HIVE_ENDPOINT, $HIVE_TOKEN and
    $HIVE_PROJECT. `workdir` is where the shell starts and what `verify` checks. `max_steps`,
    when set, ends an episode after that many actions with a reward of 0.0 unless it submitted.
    `timeout_s` is the longest one command may take unless a step asks for less."""

    SUPPORTS_CONCURRENT_SESSIONS = True

    def __init__(self, *, image: str | None = None, spec: Spec | None = None, endpoint: str | None = None,
                 token: str | None = None, project: str | None = None, workdir: str = "", check: str | None = None,
                 verify: Sequence[str] | None = None, verifier: Spec | None = None,
                 verify_files: Mapping[str, bytes | str] | None = None, max_steps: int = 0,
                 timeout_s: float = 300.0, transform: Any = None, rubric: Any = None, **spec_kw: Any):
        super().__init__(transform=transform, rubric=rubric)
        if (image is None) == (spec is None):
            raise ValueError("give an image or a spec, not both")
        if check is not None and verify is not None:
            raise ValueError("give check or verify, not both")
        if spec is None:
            spec_kw.setdefault("backend", "container")
            spec = Spec(image=image, **spec_kw)
        elif spec_kw:
            raise ValueError("with a spec, put the cell's settings in it")
        self.spec = spec
        self.workdir = workdir
        self.check = check
        self.verify = list(verify) if verify is not None else None
        self.verifier = verifier or Spec(image=spec.image, template=spec.template, snapshot=spec.snapshot,
                                         backend=spec.backend, cpu_milli=spec.cpu_milli, mem_mib=spec.mem_mib)
        self.verify_files = dict(verify_files or {})
        self.max_steps = max_steps
        self.timeout_s = timeout_s
        self._client = (endpoint, token, project)
        self._hive: AsyncHive | None = None
        self._cell: Cell | None = None
        self._session: Session | None = None
        self._state = HiveboxState()

    def _on_loop(self, coro: Coroutine[Any, Any, T]) -> asyncio.Future[T]:
        return asyncio.wrap_future(asyncio.run_coroutine_threadsafe(coro, _Loop.get()))

    def _wait(self, coro: Coroutine[Any, Any, T]) -> T:
        return asyncio.run_coroutine_threadsafe(coro, _Loop.get()).result()

    @property
    def state(self) -> HiveboxState:
        return self._state

    @property
    def cell(self) -> Cell | None:
        """The current episode's cell."""
        return self._cell

    def get_metadata(self) -> EnvironmentMetadata:
        return EnvironmentMetadata(name="hivebox", description="Each episode is a bash shell in a fresh hivebox cell.")

    def reset(self, seed: int | None = None, episode_id: str | None = None, **kwargs: Any) -> CommandObservation:
        return self._wait(self._reset(episode_id))

    async def reset_async(self, seed: int | None = None, episode_id: str | None = None,
                          **kwargs: Any) -> CommandObservation:
        return await self._on_loop(self._reset(episode_id))

    def step(self, action: CommandAction, timeout_s: float | None = None, **kwargs: Any) -> CommandObservation:
        return self._wait(self._step(action, timeout_s))

    async def step_async(self, action: CommandAction, timeout_s: float | None = None,
                         **kwargs: Any) -> CommandObservation:
        return await self._on_loop(self._step(action, timeout_s))

    def close(self) -> None:
        self._wait(self._close())

    async def _reset(self, episode_id: str | None) -> CommandObservation:
        await self._end()
        if self._hive is None:
            endpoint, token, project = self._client
            self._hive = AsyncHive(endpoint, token=token, project=project)
        self._cell = await self._hive.cells.create(self.spec)
        self._session = await self._cell.session(cwd=self.workdir)
        self._state = HiveboxState(episode_id=episode_id or str(uuid.uuid4()), step_count=0, cell_id=self._cell.id)
        self._reset_rubric()
        return self._apply_transform(CommandObservation(cell_id=self._cell.id))

    async def _step(self, action: CommandAction, timeout_s: float | None) -> CommandObservation:
        if self._cell is None or self._session is None:
            raise RuntimeError("the episode has ended, so reset before the next step")
        self._state.step_count += 1
        obs = CommandObservation(cell_id=self._cell.id)
        if action.command:
            timeout = min(t for t in (action.timeout_s, timeout_s, self.timeout_s) if t)
            r = await self._session.run(action.command, timeout=timeout)
            obs.output = r.output.decode(errors="replace")
            obs.exit_code, obs.timed_out, obs.truncated = r.exit_code, r.timed_out, r.truncated
        if action.submit:
            obs.done = True
            obs.reward = await self._score(obs)
        elif self.max_steps and self._state.step_count >= self.max_steps:
            obs.done, obs.reward = True, 0.0
            obs.verdict["out_of_steps"] = True
        elif self.rubric is not None:
            obs.reward = await self._apply_rubric_async(action, obs)
        if obs.done:
            await self._end()
        return self._apply_transform(obs)

    async def _score(self, obs: CommandObservation) -> float | None:
        assert self._cell is not None and self._hive is not None
        if self.check is not None:
            r = await self._cell.run(["bash", "-c", self.check], cwd=self.workdir, timeout=self.timeout_s)
            obs.verdict["check_exit_code"] = r.exit_code
            return 1.0 if r.ok else 0.0
        if self.verify is not None:
            v = await self._hive.verify(self.verify, verifier=self.verifier, workdir=self.workdir, subject=self._cell,
                                        files=self.verify_files, timeout=self.timeout_s)
            obs.verdict.update(passed=v.passed, tampered=v.tampered, scores=v.scores)
            if v.error is not None:
                # hivebox could not do the check, so the sample says nothing about the agent, and
                # a trainer should mask it when infra_error is set.
                obs.verdict.update(infra_error=v.is_infra_error, error=str(v.error))
                return None
            return 1.0 if v.passed else 0.0
        return None

    async def _end(self) -> None:
        cell, session = self._cell, self._session
        self._cell = self._session = None
        if session is not None:
            try:
                await session.close()
            except _errors.HiveError:
                pass
        if cell is not None:
            try:
                await cell.stop()
            except _errors.CellNotFound:
                pass

    async def _close(self) -> None:
        try:
            await self._end()
        finally:
            if self._hive is not None:
                await self._hive.close()
                self._hive = None


def create_hivebox_app(env: Callable[[], HiveboxEnv] | None = None, *, max_concurrent_envs: int = 64,
                       **env_kw: Any):
    """The OpenEnv app for `HiveboxEnv`, made with `env` or with `HiveboxEnv(**env_kw)` for each
    session."""
    factory = env or (lambda: HiveboxEnv(**env_kw))
    return create_app(factory, CommandAction, CommandObservation, env_name="hivebox",
                      max_concurrent_envs=max_concurrent_envs)
