"""Harbor trials in hivebox cells.

Harbor runs an agent on a task inside an environment and then runs the task's tests there.
`HiveboxEnvironment` is that environment as a cell, so a Harbor job runs on a hivebox node
without Docker:

    harbor run -p tasks/hello -a oracle -e hivebox.harbor:HiveboxEnvironment \\
        --ek image=python --ek mem_mib=2048

The client comes from $HIVE_ENDPOINT, $HIVE_TOKEN and $HIVE_PROJECT, or from the `endpoint`,
`token` and `project` kwargs. Each trial gets a cell made from an image the node already has:
the `image` kwarg, or else the task's `docker_image`. hivebox does not build Dockerfiles, so a
task with only a Dockerfile needs its image imported on the node first (see `hivectl image
import`) and named with `image`.

A task with no network gets the `none` network profile, and one with the internet gets the
`network_profile` kwarg, `open` unless set. Allowlists are not supported, and Harbor turns such
tasks away before it starts them. The task's CPUs, memory and storage become the cell's limits.
"""

from __future__ import annotations

import asyncio
import io
import math
import shlex
import tarfile
import uuid
from pathlib import Path
from typing import Any

from harbor.environments.base import BaseEnvironment, ExecResult, transfer_tar_filter
from harbor.environments.capabilities import EnvironmentCapabilities, EnvironmentResourceCapabilities
from harbor.models.task.config import NetworkMode

from . import _errors
from ._client import AsyncHive, Cell, Spec

__all__ = ["HiveboxEnvironment"]


def _tar(source: Path) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        tar.add(source, arcname=".")
    return buf.getvalue()


class HiveboxEnvironment(BaseEnvironment):
    """A Harbor environment that is one hivebox cell, made when the trial starts and stopped
    when it stops."""

    def __init__(self, *args: Any, image: str | None = None, backend: str = "container",
                 network_profile: str = "open", endpoint: str | None = None, token: str | None = None,
                 project: str | None = None, mem_mib: int | str | None = None,
                 cpu_milli: int | str | None = None, labels: dict[str, str] | None = None, **kwargs: Any):
        # _validate_definition runs in the base constructor and needs the image.
        self._image = image
        super().__init__(*args, **kwargs)
        self._image = image or self.task_env_config.docker_image
        self._backend = backend
        self._public_profile = network_profile
        self._client = (endpoint, token, project)
        self._mem_mib = int(mem_mib) if mem_mib else None
        self._cpu_milli = int(cpu_milli) if cpu_milli else None
        self._labels = dict(labels or {})
        self._hive: AsyncHive | None = None
        self._cell: Cell | None = None

    @staticmethod
    def type() -> str:
        return "hivebox"

    @classmethod
    def resource_capabilities(cls) -> EnvironmentResourceCapabilities:
        return EnvironmentResourceCapabilities(cpu_limit=True, memory_limit=True)

    @property
    def capabilities(self) -> EnvironmentCapabilities:
        return EnvironmentCapabilities(disable_internet=True)

    def _validate_definition(self) -> None:
        if not (self._image or self.task_env_config.docker_image):
            raise ValueError(
                f"task {self.environment_name} has no docker_image, and hivebox does not build "
                "Dockerfiles: import the image on the node and pass it with --ek image=NAME")

    def _spec(self) -> Spec:
        cpus, mem, storage = self._effective_cpus, self._effective_memory_mb, self._effective_storage_mb
        no_net = self.network_policy.network_mode == NetworkMode.NO_NETWORK
        labels = {"harbor-task": self.environment_name[:63], **self._labels}
        return Spec(
            image=self._image,
            backend=self._backend,
            cpu_milli=self._cpu_milli or (cpus * 1000 if cpus else 0),
            mem_mib=self._mem_mib or (mem or 0),
            disk_gib=math.ceil(storage / 1024) if storage else 0,
            network_profile="none" if no_net else self._public_profile,
            labels=labels,
            env=self._startup_env(),
        )

    @property
    def cell(self) -> Cell:
        if self._cell is None:
            raise RuntimeError("the environment has not started")
        return self._cell

    async def start(self, force_build: bool) -> None:
        endpoint, token, project = self._client
        self._hive = AsyncHive(endpoint, token=token, project=project)
        try:
            self._cell = await self._hive.cells.create(self._spec())
        except BaseException:
            await self._hive.close()
            self._hive = None
            raise
        self.logger.debug(f"hivebox cell {self._cell.id} for {self.session_id}")
        # Docker makes the image's WORKDIR, and commands run there, so it has to be there. The
        # log dirs Docker would mount are made writable by anyone, as Harbor does for the rest.
        workdir, logs = self.task_env_config.workdir or "/", self._mount_targets(writable_only=True)
        script = 'mkdir -p "$0" && { [ $# -eq 0 ] || { mkdir -p "$@" && chmod 777 "$@"; }; }'
        r = await self.cell.run(["sh", "-c", script, workdir, *logs], user="root")
        if r.exit_code != 0:
            raise RuntimeError(f"making {[workdir, *logs]} in the cell failed: {r.stderr.decode(errors='replace')}")
        await self._upload_environment_dir_after_start()

    async def stop(self, delete: bool) -> None:
        cell, hive = self._cell, self._hive
        self._cell = self._hive = None
        try:
            if cell is not None:
                try:
                    await cell.stop()
                except _errors.CellNotFound:
                    pass
                except _errors.HiveError as e:
                    self.logger.error(f"stopping hivebox cell {cell.id}: {e}")
        finally:
            if hive is not None:
                await hive.close()

    async def exec(self, command: str, cwd: str | None = None, env: dict[str, str] | None = None,
                   timeout_sec: int | None = None, user: str | int | None = None) -> ExecResult:
        user = self._resolve_user(user)
        r = await self.cell.run(
            ["bash", "-c", command],
            timeout=timeout_sec or None,
            env=self._merge_env(env),
            cwd=cwd or self.task_env_config.workdir or "",
            user="" if user is None else str(user),
        )
        if r.timed_out:
            raise RuntimeError(f"Command timed out after {timeout_sec} seconds")
        return ExecResult(stdout=r.stdout.decode(errors="replace"), stderr=r.stderr.decode(errors="replace"),
                          return_code=r.exit_code)

    async def upload_file(self, source_path: Path | str, target_path: str) -> None:
        await self.cell.files.write(target_path, Path(source_path).read_bytes())

    async def upload_dir(self, source_dir: Path | str, target_dir: str) -> None:
        await self.cell.files.upload(target_dir, _tar(Path(source_dir)))

    async def download_file(self, source_path: str, target_path: Path | str) -> None:
        data = await self.cell.files.read(source_path)
        target = Path(target_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)

    async def download_dir(self, source_dir: str, target_dir: Path | str) -> None:
        # One archive, made in the cell and read as a file, so neither the output limit of a run
        # nor a call per file gets in the way of a big tree.
        archive = f"/tmp/.hivebox-harbor-{uuid.uuid4().hex}.tar"
        r = await self.cell.run(["tar", "cf", archive, "-C", source_dir, "."], user="root")
        try:
            if r.exit_code != 0:
                raise RuntimeError(f"archiving {source_dir} failed with {r.exit_code}: {r.stderr.decode(errors='replace')}")
            data = await self.cell.files.read(archive)
        finally:
            await self.cell.run(f"rm -f {shlex.quote(archive)}", user="root")
        target = Path(target_dir)
        target.mkdir(parents=True, exist_ok=True)

        def extract() -> None:
            with tarfile.open(fileobj=io.BytesIO(data)) as tar:
                tar.extractall(target, filter=transfer_tar_filter)

        await asyncio.to_thread(extract)

    async def is_dir(self, path: str, user: str | int | None = None) -> bool:
        try:
            return (await self.cell.files.stat(path)).type == "dir"
        except FileNotFoundError:
            return False

    async def is_file(self, path: str, user: str | int | None = None) -> bool:
        try:
            return (await self.cell.files.stat(path)).type == "file"
        except FileNotFoundError:
            return False
