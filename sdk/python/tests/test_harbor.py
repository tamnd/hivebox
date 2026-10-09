"""hivebox.harbor under a real `harbor run`, with Harbor's oracle agent on small local tasks. Set
HIVE_TEST_ENDPOINT and HIVE_TEST_IMAGE as for test_live.py, and have harbor installed."""

import json
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pytest

pytest.importorskip("harbor")

import hivebox  # noqa: E402

ENDPOINT = os.environ.get("HIVE_TEST_ENDPOINT")
IMAGE = os.environ.get("HIVE_TEST_IMAGE", "python")
pytestmark = pytest.mark.skipif(not ENDPOINT, reason="set HIVE_TEST_ENDPOINT to run against a comb")

TEST = """#!/bin/bash
cd /app
if [ "$(cat answer.txt)" = "$(python3 -c 'print(sum(range(101)))')" ] && [ -f data/input.csv ]; then
  echo 1 > /logs/verifier/reward.txt
else
  echo 0 > /logs/verifier/reward.txt
fi
"""


def task(root: Path, name: str, solve: str) -> Path:
    t = root / name
    (t / "solution").mkdir(parents=True)
    (t / "tests").mkdir()
    (t / "environment" / "data").mkdir(parents=True)
    (t / "task.toml").write_text(
        'schema_version = "1.4"\n\n[metadata]\n\n[verifier]\ntimeout_sec = 120.0\n\n[agent]\ntimeout_sec = 120.0\n\n'
        f'[environment]\ndocker_image = "{IMAGE}"\nworkdir = "/app"\nnetwork_mode = "no-network"\nmemory_mb = 512\ncpus = 1\n')
    (t / "instruction.md").write_text("Write the sum of 1 to 100 to /app/answer.txt.\n")
    (t / "environment" / "data" / "input.csv").write_text("a,b\n1,2\n")
    (t / "solution" / "solve.sh").write_text(f"#!/bin/bash\n{solve}\n")
    (t / "tests" / "test.sh").write_text(TEST)
    return t


def harbor(path: Path, jobs: Path, *extra: str) -> list[dict]:
    env = {**os.environ, "HIVE_ENDPOINT": ENDPOINT, "HIVE_PROJECT": "harbor-test"}
    cmd = [shutil.which("harbor") or str(Path(sys.executable).parent / "harbor"), "run", "-p", str(path),
           "-a", "oracle", "-e", "hivebox.harbor:HiveboxEnvironment", "--ek", f"image={IMAGE}",
           "-o", str(jobs), "-n", "4", "--yes", *extra]
    p = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=900)
    assert p.returncode == 0, p.stdout[-3000:] + p.stderr[-3000:]
    trials = [json.loads(f.read_text()) for f in jobs.glob("*/*/result.json")]
    assert trials, p.stdout[-3000:]
    return trials


def seconds(timing: dict) -> float:
    start, end = (datetime.fromisoformat(timing[k]) for k in ("started_at", "finished_at"))
    return (end - start).total_seconds()


def test_oracle_trials_pass_and_fail_in_cells(tmp_path):
    tasks = tmp_path / "tasks"
    task(tasks, "sum-right", "python3 -c 'print(sum(range(101)))' > /app/answer.txt")
    task(tasks, "sum-wrong", "echo 5049 > /app/answer.txt")
    trials = harbor(tasks, tmp_path / "jobs")
    rewards = {t["task_name"]: t["verifier_result"]["rewards"]["reward"] for t in trials}
    assert rewards == {"sum-right": 1.0, "sum-wrong": 0.0}, [t.get("exception_info") for t in trials]
    for t in trials:
        print(f"\n{t['task_name']}: start {seconds(t['environment_setup']):.2f} s, "
              f"agent {seconds(t['agent_execution']):.2f} s, verifier {seconds(t['verifier']):.2f} s")

    async def left():
        async with hivebox.AsyncHive(ENDPOINT, project="harbor-test") as hive:
            cells = await hive.cells.list()
            return [c for c in cells if "harbor-task" in c.labels and c.state not in ("stopping", "stopped")]

    import asyncio
    assert asyncio.run(left()) == []


def test_a_task_without_an_image_is_turned_away(tmp_path):
    t = task(tmp_path / "tasks", "built", "true")
    toml = (t / "task.toml").read_text().replace(f'docker_image = "{IMAGE}"\n', "")
    (t / "task.toml").write_text(toml)
    (t / "environment" / "Dockerfile").write_text("FROM ubuntu:24.04\nWORKDIR /app\n")
    env = {**os.environ, "HIVE_ENDPOINT": ENDPOINT}
    harbor_bin = shutil.which("harbor") or str(Path(sys.executable).parent / "harbor")
    p = subprocess.run([harbor_bin, "run", "-p", str(t), "-a", "oracle", "-e", "hivebox.harbor:HiveboxEnvironment",
                        "-o", str(tmp_path / "jobs"), "--yes"], env=env, capture_output=True, text=True, timeout=300)
    out = p.stdout + p.stderr + "".join(f.read_text() for f in (tmp_path / "jobs").glob("**/*.json"))
    # Rich wraps the traceback in a box, so the words are found with the box and breaks taken out.
    assert "hivebox does not build Dockerfiles" in " ".join(out.replace("\u2502", " ").split()), out[-3000:]
