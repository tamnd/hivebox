"""SWE-bench style datasets as hivebox tasks.

`import` turns a dataset's rows into pollen tasks, one JSON object a line, and `validate` checks
them on a node: the gold patch has to pass and the code as it was has to fail.

    python -m hivebox.datasets import swe-bench-verified --out tasks.jsonl --images images.txt
    python -m hivebox.datasets validate swe-bench-verified --only 'psf__requests-' --parallel 4

DATASET is one of swe-bench, swe-bench-lite, swe-bench-verified, swe-gym or swe-rebench, any
Hugging Face dataset id with the same columns, or a local .jsonl or .json file of rows. Rows come
from the `datasets` package when it is installed, and otherwise from the Hugging Face rows API,
which needs nothing but the standard library. $HF_TOKEN is sent when set.

Each task starts a cell from the row's image with the repo checked out at /testbed and the
problem statement as $HIVE_INSTRUCTION. The policy is the gold patch, no change at all, or an
agent command of yours. The check runs in a fresh cell with no network: it puts the test files
back as they were, applies the row's test patch, runs the repo's test command and reads the log
with the repo's parser, and passes only when every FAIL_TO_PASS and PASS_TO_PASS test passed, as
SWE-bench grades a run. The test command, install step and parser per repo and version are
SWE-bench's own, and a SWE-rebench row brings its own in `install_config`. A repo none of these
know is run with `pytest -rA`.

hivebox does not pull images. `--images` writes each image's name on the node and the registry
reference it comes from, a pair a line, to be imported first, for example with `skopeo copy
docker://REF oci:DIR` and `hive-nectar import-oci DIR`.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import nullcontext, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import _errors
from ._client import AsyncHive, Spec

__all__ = ["DATASETS", "Dataset", "Outcome", "image_ref", "load", "local_image", "main", "task", "test_spec", "validate"]

WORKDIR = "/testbed"
SWE_DIR = "/tmp/hive-swe"
GRADE_ARGV = ["bash", "-c", f'exec "$(command -v python3 || echo /opt/miniconda3/bin/python3)" {SWE_DIR}/grade.py']
PATCH_END = "HIVE_SWE_PATCH_END"
NON_TEST_EXTS = (".json", ".png", "csv", ".txt", ".md", ".jpg", ".jpeg", ".pkl", ".yml", ".yaml", ".toml")
ROWS_API = "https://datasets-server.huggingface.co/rows"


@dataclass(frozen=True)
class Dataset:
    """Where a dataset's rows are and the image a row runs in when the row does not say."""

    hf_id: str
    split: str
    image: Callable[[dict[str, Any]], str]


def _swebench_image(row: dict[str, Any]) -> str:
    return f"swebench/sweb.eval.x86_64.{row['instance_id'].lower().replace('__', '_1776_')}:latest"


def _swegym_image(row: dict[str, Any]) -> str:
    return f"xingyaoww/sweb.eval.x86_64.{row['instance_id'].lower().replace('__', '_s_')}:latest"


DATASETS = {
    "swe-bench": Dataset("princeton-nlp/SWE-bench", "test", _swebench_image),
    "swe-bench-lite": Dataset("princeton-nlp/SWE-bench_Lite", "test", _swebench_image),
    "swe-bench-verified": Dataset("princeton-nlp/SWE-bench_Verified", "test", _swebench_image),
    "swe-gym": Dataset("SWE-Gym/SWE-Gym", "train", _swegym_image),
    "swe-rebench": Dataset("nebius/SWE-rebench", "test", _swebench_image),
}


def load(source: str, split: str | None = None, *, only: str | None = None,
         limit: int | None = None) -> Iterator[dict[str, Any]]:
    """The rows of `source`, a name in DATASETS, a Hugging Face id or a local file, whose
    instance_id `only` matches, if given, up to `limit` of them."""
    pattern = re.compile(only) if only else None
    n = 0
    for row in _rows(source, split):
        if pattern and not pattern.search(row["instance_id"]):
            continue
        yield _normal(row)
        n += 1
        if limit and n >= limit:
            return


def _rows(source: str, split: str | None) -> Iterator[dict[str, Any]]:
    path = Path(source)
    if path.suffix in (".jsonl", ".json") and path.exists():
        with path.open() as f:
            if path.suffix == ".json":
                yield from json.load(f)
            else:
                yield from (json.loads(line) for line in f if line.strip())
        return
    known = DATASETS.get(source)
    hf_id = known.hf_id if known else source
    split = split or (known.split if known else "test")
    try:
        import datasets
    except ImportError:
        yield from _rows_api(hf_id, split)
        return
    yield from datasets.load_dataset(hf_id, split=split, streaming=True)


def _rows_api(hf_id: str, split: str, page: int = 100) -> Iterator[dict[str, Any]]:
    headers = {"Authorization": f"Bearer {os.environ['HF_TOKEN']}"} if os.environ.get("HF_TOKEN") else {}
    offset = 0
    while True:
        query = urllib.parse.urlencode({"dataset": hf_id, "config": "default", "split": split,
                                        "offset": offset, "length": page})
        for attempt in range(6):
            try:
                with urllib.request.urlopen(urllib.request.Request(f"{ROWS_API}?{query}", headers=headers),
                                            timeout=120) as r:
                    got = json.load(r)
                break
            except urllib.error.HTTPError as e:
                if e.code not in (429, 500, 502, 503, 504) or attempt == 5:
                    raise
            except (urllib.error.URLError, TimeoutError):
                if attempt == 5:
                    raise
            time.sleep(2 ** attempt)
        for r in got["rows"]:
            yield r["row"]
        offset += len(got["rows"])
        if not got["rows"] or offset >= got["num_rows_total"]:
            return


def _normal(row: dict[str, Any]) -> dict[str, Any]:
    row = dict(row)
    for k in ("FAIL_TO_PASS", "PASS_TO_PASS"):
        if isinstance(row.get(k), str):
            row[k] = json.loads(row[k])
        row[k] = list(row.get(k) or [])
    return row


def image_ref(row: dict[str, Any], dataset: str | None = None) -> str:
    """The registry reference of the image a row runs in."""
    if row.get("docker_image") or row.get("image_name"):
        return row.get("docker_image") or row["image_name"]
    known = DATASETS.get(dataset or "")
    return (known.image if known else _swebench_image)(row)


def local_image(ref: str) -> str:
    """The name an image has on the node: its reference without the registry, tag or digest, cut
    to the 63 characters a name may have."""
    name = ref.split("@", 1)[0]
    if ":" in name.rsplit("/", 1)[-1]:
        name = name.rsplit(":", 1)[0]
    for prefix in ("docker.io/", "index.docker.io/", "registry-1.docker.io/"):
        name = name.removeprefix(prefix)
    name = re.sub(r"[^A-Za-z0-9._/-]", "-", name)
    if len(name) > 63:
        name = f"{name[:50]}-{hashlib.sha256(ref.encode()).hexdigest()[:12]}"
    return name


def _version(v: str) -> tuple[int, ...]:
    return tuple(int(x) for x in re.findall(r"\d+", v or "")[:2])


_LOCALE = ["export LANG=en_US.UTF-8", "export LC_ALL=en_US.UTF-8", "export PYTHONIOENCODING=utf8",
           "export LANGUAGE=en_US:en"]
_PIP_E = "python -m pip install -e ."
_PARSERS = {"psf/requests": "pytest_options", "pylint-dev/pylint": "pytest_options", "pydicom/pydicom": "pytest_options",
            "astropy/astropy": "pytest_v2", "scikit-learn/scikit-learn": "pytest_v2", "sphinx-doc/sphinx": "pytest_v2",
            "matplotlib/matplotlib": "matplotlib", "mwaskom/seaborn": "seaborn", "django/django": "django",
            "sympy/sympy": "sympy"}


def _swebench_spec(repo: str, version: str) -> tuple[str, str, list[str]]:
    """SWE-bench's test command, install step and setup commands for a repo at a version."""
    v = _version(version)
    if repo == "django/django":
        cmd = "./tests/runtests.py --verbosity 2" + ("" if v == (1, 9) else " --settings=test_sqlite --parallel 1")
        if v >= (3, 0):
            setup = ["sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && locale-gen", "export LANG=en_US.UTF-8",
                     "export LANGUAGE=en_US:en", "export LC_ALL=en_US.UTF-8"] if v < (4, 0) else []
            return cmd, _PIP_E, setup
        return cmd, "python setup.py install", _LOCALE if v >= (1, 7) else []
    if repo == "astropy/astropy":
        cmd = "pytest -rA -vv -o console_output_style=classic --tb=no" if v < (3, 0) else "pytest -rA"
        return cmd, "python -m pip install -e .[test] --verbose", []
    if repo == "matplotlib/matplotlib":
        if v < (3, 0):
            return "pytest -rA", "python setup.py build; python setup.py install", []
        return "pytest -rA", "python -m pip install --no-build-isolation -e \".[dev]\"" if v >= (3, 8) else _PIP_E, []
    if repo == "mwaskom/seaborn":
        return "pytest --no-header -rA", _PIP_E if v < (0, 12) else "python -m pip install -e .[dev]", []
    if repo == "sympy/sympy":
        return "PYTHONWARNINGS='ignore::UserWarning,ignore::SyntaxWarning' bin/test -C --verbose", _PIP_E, []
    if repo == "sphinx-doc/sphinx":
        return "tox --current-env -epy39 -v --", "python -m pip install -e .[test]", []
    if repo == "scikit-learn/scikit-learn":
        return "pytest -rA", "python -m pip install -v --no-use-pep517 --no-build-isolation -e .", []
    install = {"psf/requests": "python -m pip install .", "marshmallow-code/marshmallow": "python -m pip install -e '.[dev]'",
               "pvlib/pvlib-python": "python -m pip install -e .[all]"}.get(repo, _PIP_E)
    return "pytest -rA", install, []


def _patch_files(patch: str) -> list[str]:
    return list(dict.fromkeys(re.findall(r"^diff --git a/.* b/(.*)$", patch, re.MULTILINE)))


def _directives(row: dict[str, Any]) -> list[str]:
    out = [f for f in _patch_files(row["test_patch"]) if not f.endswith(NON_TEST_EXTS)]
    if row["repo"] == "django/django":
        out = [f.removesuffix(".py").removeprefix("tests/").replace("/", ".") for f in out]
    return out


def test_spec(row: dict[str, Any]) -> dict[str, Any]:
    """What the grader needs to know about a row, written to spec.json in the verifier cell."""
    config = row.get("install_config") or {}
    if config.get("test_cmd"):
        cmd = config["test_cmd"]
        cmd = "; ".join(cmd) if isinstance(cmd, list) else cmd
        install, setup = config.get("install") or "", list(config.get("eval_commands") or [])
        parser = (config.get("log_parser") or "pytest").removeprefix("parse_log_")
        conda_env = None if config.get("no_use_env") else "testbed"
        env = dict(config.get("env_vars") or {})
    else:
        cmd, install, setup = _swebench_spec(row["repo"], str(row.get("version", "")))
        parser, conda_env, env = _PARSERS.get(row["repo"], "pytest"), "testbed", {}
    if parser not in ("pytest", "pytest_options", "pytest_v2", "matplotlib", "seaborn", "django", "sympy"):
        parser = "pytest"
    return {"instance_id": row["instance_id"], "repo": row["repo"], "base_commit": row["base_commit"],
            "workdir": WORKDIR, "conda_env": conda_env, "env": env, "eval_commands": setup, "install": install,
            "test_cmd": cmd, "directives": _directives(row), "test_files": _patch_files(row["test_patch"]),
            "parser": parser, "fail_to_pass": row["FAIL_TO_PASS"], "pass_to_pass": row["PASS_TO_PASS"]}


def _grader() -> str:
    return Path(__file__).with_name("_swe_grade.py").read_text()


def _verify_files(row: dict[str, Any]) -> dict[str, str]:
    return {f"{SWE_DIR}/grade.py": _grader(), f"{SWE_DIR}/spec.json": json.dumps(test_spec(row)),
            f"{SWE_DIR}/test.patch": row["test_patch"]}


def _gold(row: dict[str, Any]) -> str:
    patch = row["patch"]
    if PATCH_END in patch:
        raise ValueError(f"{row['instance_id']}: the patch holds {PATCH_END}")
    return f"git apply -v - <<'{PATCH_END}'\n{patch.rstrip(chr(10))}\n{PATCH_END}"


def _policy(row: dict[str, Any], agent: str) -> str:
    return {"gold": _gold, "none": lambda _: "true"}.get(agent, lambda _: agent)(row)


def task(row: dict[str, Any], *, dataset: str | None = None, image: str | None = None, agent: str = "gold",
         samples: int = 1, mem_mib: int = 4096, vcpu_milli: int = 2000, timeout_s: int = 1800) -> dict[str, Any]:
    """A row as a pollen task. `agent` is `gold` for the row's own patch, `none` for no change,
    or a shell command run in /testbed with the problem statement in $HIVE_INSTRUCTION."""
    return {
        "task_id": row["instance_id"],
        "image": image or local_image(image_ref(row, dataset)),
        "instruction": row.get("problem_statement", ""),
        "n_samples": samples,
        "workdir": WORKDIR,
        "cell": {"mem_mib": mem_mib, "vcpu_milli": vcpu_milli},
        "labels": {"dataset": dataset or "", "repo": row["repo"]},
        "policy": {"script": [[_policy(row, agent)]]},
        "verify": {"argv": GRADE_ARGV, "protected_paths": _patch_files(row["test_patch"]),
                   "files": _verify_files(row), "timeout_s": timeout_s, "zero_on_tamper": False},
    }


@dataclass
class Outcome:
    """How one row did in `validate`. The row is sound when the gold patch passed and the code as
    it was failed. Times are in seconds."""

    instance_id: str
    gold_passed: bool = False
    empty_passed: bool = False
    gold_s: float = 0.0
    empty_s: float = 0.0
    tests: str = ""
    error: str = ""

    @property
    def sound(self) -> bool:
        return not self.error and self.gold_passed and not self.empty_passed


def _missed(output: bytes) -> str:
    lines = output.decode("utf-8", "replace").splitlines()
    shown = [line.removeprefix("hive-swe: ") for line in lines if line.startswith("hive-swe: ")][:5]
    summary = next((line.strip("= ") for line in reversed(lines) if line.startswith("==== ")), "")
    return "; ".join([summary, *shown]).strip("; ")


async def _check(hive: AsyncHive, row: dict[str, Any], spec: Spec, timeout: float) -> Outcome:
    out = Outcome(row["instance_id"])
    files = _verify_files(row)
    protected = _patch_files(row["test_patch"])
    t = time.perf_counter()
    cell = await hive.cells.create(spec)
    try:
        r = await cell.run(["bash", "-c", _gold(row)], cwd=WORKDIR, timeout=600)
        if r.exit_code != 0:
            out.error = f"the gold patch did not apply: {r.stderr.decode('utf-8', 'replace')[-300:]}"
            return out
        gold = await hive.verify(GRADE_ARGV, verifier=spec, workdir=WORKDIR, subject=cell, protected_paths=protected,
                                 files=files, timeout=timeout)
    finally:
        with suppress(_errors.HiveError):
            await cell.stop()
    out.gold_s = time.perf_counter() - t
    if gold.error:
        out.error = f"gold: {gold.error}"
        return out
    out.gold_passed, out.tests = gold.passed, _missed(gold.output)
    t = time.perf_counter()
    empty = await hive.verify(GRADE_ARGV, verifier=spec, workdir=WORKDIR, protected_paths=protected, files=files,
                              timeout=timeout)
    out.empty_s = time.perf_counter() - t
    if empty.error:
        out.error = f"empty: {empty.error}"
    out.empty_passed = empty.passed
    return out


async def validate(rows: Iterable[dict[str, Any]], *, hive: AsyncHive, dataset: str | None = None,
                   image: str | None = None, parallel: int = 1, mem_mib: int = 4096, cpu_milli: int = 2000,
                   timeout: float = 1800, report: Callable[[Outcome], None] | None = None) -> list[Outcome]:
    """Runs each row's gold patch and then no change at all through its check. `report` is
    called with each outcome as it comes."""
    tag = uuid.uuid4().hex[:12]
    slots = asyncio.Semaphore(parallel)

    async def one(row: dict[str, Any]) -> Outcome:
        spec = Spec(image=image or local_image(image_ref(row, dataset)), backend="container", network_profile="none",
                    mem_mib=mem_mib, cpu_milli=cpu_milli, labels={"hive-swe": tag})
        async with slots:
            try:
                out = await _check(hive, row, spec, timeout)
            except _errors.HiveError as e:
                out = Outcome(row["instance_id"], error=str(e))
        if report:
            report(out)
        return out

    try:
        return list(await asyncio.gather(*(one(r) for r in rows)))
    finally:
        for c in await hive.cells.list({"hive-swe": tag}):
            if c.state not in ("stopping", "stopped", "failed", "expired"):
                with suppress(_errors.HiveError):
                    await c.stop()


def _line(o: Outcome) -> str:
    if o.error:
        return f"{o.instance_id}: error, {o.error}"
    gold = "passed" if o.gold_passed else f"FAILED ({o.tests})"
    empty = "PASSED, the tests do not catch the bug" if o.empty_passed else "failed"
    return f"{o.instance_id}: gold {gold} in {o.gold_s:.1f} s, unchanged {empty} in {o.empty_s:.1f} s"


def _summary(outcomes: Sequence[Outcome], took: float) -> str:
    n = len(outcomes)
    gold = [o.gold_s for o in outcomes if not o.error]
    empty = [o.empty_s for o in outcomes if not o.error]
    line = (f"{n} rows in {took:.0f} s: {sum(o.sound for o in outcomes)} sound, "
            f"{sum(o.gold_passed for o in outcomes)} gold passed, {sum(not o.empty_passed and not o.error for o in outcomes)} "
            f"unchanged failed, {sum(bool(o.error) for o in outcomes)} errors")
    if gold:
        line += (f"; gold check p50 {statistics.median(gold):.1f} s max {max(gold):.1f} s, "
                 f"unchanged p50 {statistics.median(empty):.1f} s max {max(empty):.1f} s")
    return line


def main(argv: Sequence[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m hivebox.datasets", description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("import", "validate"):
        s = sub.add_parser(name)
        s.add_argument("dataset", help=f"one of {', '.join(DATASETS)}, a Hugging Face id or a .jsonl or .json file")
        s.add_argument("--split", help="the split, test unless the dataset has another")
        s.add_argument("--only", help="a regex an instance_id has to match")
        s.add_argument("--limit", type=int, help="at most this many rows")
        s.add_argument("--image", help="one image name on the node for every row")
        s.add_argument("--mem-mib", type=int, default=4096)
        s.add_argument("--cpu-milli", type=int, default=2000)
        s.add_argument("--timeout", type=int, default=1800, help="seconds a check may take")
    imp, val = sub.choices["import"], sub.choices["validate"]
    imp.add_argument("--agent", default="gold", help="gold, none, or a shell command run in /testbed")
    imp.add_argument("--samples", type=int, default=1)
    imp.add_argument("--out", help="the task file, stdout if not given")
    imp.add_argument("--images", help="a file for each image's name on the node and its registry reference")
    val.add_argument("--parallel", type=int, default=1, help="rows checked at once")
    val.add_argument("--endpoint", default=os.environ.get("HIVE_ENDPOINT"))
    val.add_argument("--token", default=os.environ.get("HIVE_TOKEN"))
    val.add_argument("--project", default=os.environ.get("HIVE_PROJECT"))
    a = p.parse_args(argv)
    known = a.dataset if a.dataset in DATASETS else None
    rows = load(a.dataset, a.split, only=a.only, limit=a.limit)
    if a.cmd == "import":
        images: dict[str, str] = {}
        n = 0
        with open(a.out, "w") if a.out else nullcontext(sys.stdout) as f:
            for row in rows:
                t = task(row, dataset=known, image=a.image, agent=a.agent, samples=a.samples, mem_mib=a.mem_mib,
                         vcpu_milli=a.cpu_milli, timeout_s=a.timeout)
                images.setdefault(t["image"], image_ref(row, known))
                print(json.dumps(t), file=f)
                n += 1
        if a.images:
            Path(a.images).write_text("".join(f"{k} {v}\n" for k, v in images.items()))
        print(f"{n} tasks, {len(images)} images", file=sys.stderr)
        return 0

    async def run() -> list[Outcome]:
        async with AsyncHive(a.endpoint, token=a.token, project=a.project) as hive:
            return await validate(list(rows), hive=hive, dataset=known, image=a.image, parallel=a.parallel,
                                  mem_mib=a.mem_mib, cpu_milli=a.cpu_milli, timeout=a.timeout,
                                  report=lambda o: print(_line(o), flush=True))

    t = time.perf_counter()
    outcomes = asyncio.run(run())
    print(_summary(outcomes, time.perf_counter() - t))
    return 0 if all(o.sound for o in outcomes) else 1


if __name__ == "__main__":
    sys.exit(main())
