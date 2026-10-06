"""Grading for slime's coding agent example in a hivebox verifier cell.

slime's `examples/coding_agent_rl/swe.py` grades a sample by making a fresh E2B sandbox from the
task's image, applying the agent's diff and running the task's `eval_cmd` or `f2p_script`.
`run_evaluation` does the same in one `Verify.Run` call: the diff, the pre commands and the
script go in as files, and the verifier cell has no network. It takes the same arguments and
returns the same `(reward, applied_cleanly)`, so `install(swe)` swaps it in without changing
`generate.py`. Tasks graded with swepro or the SWE-bench protocol keep slime's own grader.

A check hivebox could not do raises `HiveError`, which slime's `generate` turns into an aborted
sample that is left out of training, rather than a reward of 0 the policy would learn from.
"""

from __future__ import annotations

import asyncio
import functools
import shlex
from dataclasses import dataclass
from typing import Any, Mapping

from . import _errors
from ._client import AsyncHive, Spec, VerifyResult

# Where the files go in the verifier cell. They are outside the workdir so the tests never see them.
PATCH = "/tmp/__hive_patch__.diff"
PRE = "/tmp/__hive_pre__.sh"
F2P = "/tmp/__hive_f2p__.py"
APPLIED = "/tmp/__hive_applied__"

# The exit code and the line that mean the diff did not apply, so the tests did not run.
NOT_APPLIED = 97
NOT_APPLIED_LINE = "hivebox: the patch did not apply"

# The same ladder as slime's _apply_diff: the first that works wins.
_APPLY = (
    f"git -c safe.directory='*' apply --3way --whitespace=nowarn {PATCH}",
    f"git -c safe.directory='*' apply --whitespace=nowarn {PATCH}",
    f"patch -p1 --no-backup-if-mismatch < {PATCH}",
)


class EvalResult(tuple):
    """`(reward, applied_cleanly)` as slime's grader returns it, with hivebox's verdict in
    `verdict`, which is None when the diff did not apply."""

    def __new__(cls, reward: float, applied_cleanly: bool, verdict: VerifyResult | None = None):
        self = super().__new__(cls, (reward, applied_cleanly))
        self.verdict = verdict
        return self

    @property
    def reward(self) -> float:
        return self[0]

    @property
    def applied_cleanly(self) -> bool:
        return self[1]


@dataclass
class Grader:
    """How the verifier cell is made and how often the tests run. `hive` defaults to a client
    made from $HIVE_ENDPOINT, $HIVE_TOKEN and $HIVE_PROJECT."""

    hive: AsyncHive | None = None
    backend: str = "container"
    mem_mib: int = 2048
    cpu_milli: int = 2000
    repeats: int = 1

    def __post_init__(self):
        self._clients: dict[int, AsyncHive] = {}

    def client(self) -> AsyncHive:
        if self.hive is not None:
            return self.hive
        loop = id(asyncio.get_running_loop())
        if loop not in self._clients:
            self._clients[loop] = AsyncHive()
        return self._clients[loop]


DEFAULT = Grader()


def script(workdir: str, grading: Mapping[str, Any]) -> tuple[str, dict[str, str]]:
    """The verifier's bash script and the files it needs, for a scaleswe task's `grading`. The
    pre commands and the diff run once, so a second run of the tests sees the same tree."""
    files: dict[str, str] = {}
    lines = [f"cd {shlex.quote(workdir)}", f"if [ ! -e {APPLIED} ]; then"]
    pre = grading.get("pre_commands")
    if pre:
        body = pre.replace("\\n", "\n") if isinstance(pre, str) else "\n".join(c for c in pre if c)
        files[PRE] = "set -e\n" + body
        lines.append(f"  bash {PRE} || true")
    lines += [
        f"  if [ -s {PATCH} ] && ! {{ " + " || ".join(f"({c})" for c in _APPLY) + "; }; then",
        f"    echo {shlex.quote(NOT_APPLIED_LINE)} >&2",
        f"    exit {NOT_APPLIED}",
        "  fi",
        f"  touch {APPLIED}",
        "fi",
    ]
    if grading.get("eval_cmd"):
        lines.append(grading["eval_cmd"])
    else:
        files[F2P] = grading["f2p_script"]
        lines.append(f"python {F2P}")
    return "\n".join(lines) + "\n", files


async def run_evaluation(md: Mapping[str, Any], *, diff_text: str, timeout_sec: int, grader: Grader | None = None,
                         fallback=None) -> EvalResult:
    """Grades `diff_text` for the task `md`, the dict slime's `swe.get_metadata` makes, as
    slime's `run_evaluation` does. Tasks hivebox does not grade go to `fallback`."""
    grading = md.get("grading") or {}
    if md.get("protocol") not in (None, "scaleswe") or grading.get("swepro"):
        if fallback is None:
            raise _errors.InvalidArgument("hivebox grades scaleswe tasks with eval_cmd or f2p_script, not swepro or swebench")
        return await fallback(md, diff_text=diff_text, timeout_sec=timeout_sec)
    if not (grading.get("eval_cmd") or grading.get("f2p_script")):
        return EvalResult(0.0, True)
    g = grader or DEFAULT
    body, files = script(md["workdir"], grading)
    files[PATCH] = diff_text or ""
    r = await g.client().verify(
        ["bash", "-c", body],
        verifier=Spec(image=md["image"], backend=g.backend, mem_mib=g.mem_mib, cpu_milli=g.cpu_milli),
        workdir=md["workdir"],
        files=files,
        repeats=g.repeats,
        timeout=timeout_sec,
    )
    if r.error is not None:
        raise r.error
    if r.exit_code == NOT_APPLIED and NOT_APPLIED_LINE.encode() in r.output:
        return EvalResult(0.0, False)
    return EvalResult(1.0 if r.passed else 0.0, True, r)


def install(swe, grader: Grader | None = None) -> None:
    """Makes `swe.run_evaluation` grade in hivebox, keeping slime's own for the tasks hivebox
    does not grade. `swe` is `examples.coding_agent_rl.swe`, and `generate.py` picks the
    change up because it calls `swe.run_evaluation` by name."""
    orig = swe.run_evaluation
    swe.run_evaluation = functools.partial(run_evaluation, grader=grader, fallback=orig)
