"""hivebox.slime's grader. The first tests need nothing, the rest a real comb: set
HIVE_TEST_ENDPOINT and HIVE_TEST_GIT_IMAGE as for test_live.py, and HIVE_TOKEN through a gate."""

import os
import types

import pytest

import hivebox
from hivebox import slime

ENDPOINT = os.environ.get("HIVE_TEST_ENDPOINT")
GIT_IMAGE = os.environ.get("HIVE_TEST_GIT_IMAGE")
WORKDIR = os.environ.get("HIVE_TEST_GIT_WORKDIR", "/testbed")
live = pytest.mark.skipif(not (ENDPOINT and GIT_IMAGE), reason="set HIVE_TEST_ENDPOINT and HIVE_TEST_GIT_IMAGE")

ADD_PROBE = """diff --git a/probe.py b/probe.py
new file mode 100644
--- /dev/null
+++ b/probe.py
@@ -0,0 +1 @@
+value = 2
"""
# Changes a file the checkout does not have, so no step of the ladder can apply it.
BAD = """diff --git a/missing.txt b/missing.txt
--- a/missing.txt
+++ b/missing.txt
@@ -1 +1 @@
-old
+new
"""
CHECK = "grep -qx 'value = 2' probe.py"


def grader(**kw):
    return slime.Grader(hive=hivebox.AsyncHive(ENDPOINT, project="slime-test"), mem_mib=512, cpu_milli=1000, **kw)


def md(**grading):
    return {"protocol": "scaleswe", "instance_id": "t", "image": GIT_IMAGE, "workdir": WORKDIR, "grading": grading}


def test_the_script_applies_once_and_runs_the_tests_every_time():
    body, files = slime.script("/w d", {"eval_cmd": "pytest -q", "pre_commands": ["git status", "", "true"]})
    assert body.splitlines()[0] == "cd '/w d'"
    assert body.rstrip().endswith("touch /tmp/__hive_applied__\nfi\npytest -q")
    assert files == {slime.PRE: "set -e\ngit status\ntrue"}
    body, files = slime.script("/w", {"f2p_script": "import sys\nsys.exit(0)\n", "pre_commands": "a\\nb"})
    assert body.rstrip().endswith(f"python {slime.F2P}")
    assert files == {slime.PRE: "set -e\na\nb", slime.F2P: "import sys\nsys.exit(0)\n"}


async def test_tasks_hivebox_does_not_grade_go_to_slime():
    seen = []

    async def fallback(m, *, diff_text, timeout_sec):
        seen.append((m["protocol"], diff_text, timeout_sec))
        return (1.0, True)

    for m in ({"protocol": "swebench"}, md(swepro={"run_script_path": "x"})):
        assert await slime.run_evaluation(m, diff_text="d", timeout_sec=5, fallback=fallback) == (1.0, True)
        with pytest.raises(hivebox.InvalidArgument):
            await slime.run_evaluation(m, diff_text="d", timeout_sec=5)
    assert seen == [("swebench", "d", 5), ("scaleswe", "d", 5)]
    # Nothing to grade with is a 0, as slime gives it.
    assert await slime.run_evaluation(md(), diff_text=ADD_PROBE, timeout_sec=5) == (0.0, True)

    swe = types.SimpleNamespace(run_evaluation=fallback)
    slime.install(swe)
    assert await swe.run_evaluation({"protocol": "swebench"}, diff_text="x", timeout_sec=1) == (1.0, True)


@live
async def test_eval_cmd_and_f2p_script_grade_the_diff():
    r = await slime.run_evaluation(md(eval_cmd=CHECK), diff_text=ADD_PROBE, timeout_sec=60, grader=grader())
    assert (r.reward, r.applied_cleanly) == (1.0, True) and r.verdict.passed
    reward, applied = await slime.run_evaluation(md(eval_cmd=CHECK), diff_text="", timeout_sec=60, grader=grader())
    assert (reward, applied) == (0.0, True)
    r = await slime.run_evaluation(md(eval_cmd=CHECK), diff_text=BAD, timeout_sec=60, grader=grader())
    assert (r.reward, r.applied_cleanly, r.verdict) == (0.0, False, None)

    f2p = "import sys\nsys.exit(0 if open('probe.py').read() == 'value = 2\\n' else 1)\n"
    assert await slime.run_evaluation(md(f2p_script=f2p), diff_text=ADD_PROBE, timeout_sec=60, grader=grader()) == (1.0, True)
    assert await slime.run_evaluation(md(f2p_script=f2p), diff_text="", timeout_sec=60, grader=grader()) == (0.0, True)


@live
async def test_pre_commands_run_before_the_diff_and_once_for_all_repeats():
    # The pre commands make the file the diff then changes, as a checkout of the base commit would.
    pre = ["printf 'value = 1\\n' > probe.py", "printf 'run\\n' >> runs.txt"]
    change = ADD_PROBE.replace("new file mode 100644\n--- /dev/null", "--- a/probe.py").replace("@@ -0,0 +1 @@", "@@ -1 +1 @@\n-value = 1")
    r = await slime.run_evaluation(md(eval_cmd=f"{CHECK} && test $(wc -l < runs.txt) = 1", pre_commands=pre),
                                   diff_text=change, timeout_sec=60, grader=grader(repeats=3))
    assert (r.reward, r.applied_cleanly, r.verdict.runs_passed) == (1.0, True, 3), r.verdict.output[-500:]


@live
async def test_a_missing_image_raises_so_slime_aborts_the_sample():
    with pytest.raises(hivebox.HiveError) as e:
        await slime.run_evaluation({**md(eval_cmd="true"), "image": "no-such-image"}, diff_text="", timeout_sec=60, grader=grader())
    assert isinstance(e.value, hivebox.ImageUnavailable), repr(e.value)
