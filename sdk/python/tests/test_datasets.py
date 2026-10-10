"""hivebox.datasets: rows to tasks and the grader, without a comb, and the gold patches of real
SWE-bench rows checked on a comb when HIVE_TEST_ENDPOINT and HIVE_TEST_SWE_ROWS, a .jsonl of rows
whose images the node has, are set."""

import json
import os
import shutil
import subprocess
import sys
import time

import pytest

import hivebox
from hivebox import _swe_grade as grade
from hivebox import datasets

PATCH = """diff --git a/calc.py b/calc.py
--- a/calc.py
+++ b/calc.py
@@ -1,2 +1,2 @@
 def add(a, b):
-    return a - b
+    return a + b
"""
TEST_PATCH = """diff --git a/tests/test_calc.py b/tests/test_calc.py
new file mode 100644
--- /dev/null
+++ b/tests/test_calc.py
@@ -0,0 +1,9 @@
+import calc
+
+
+def test_add():
+    assert calc.add(2, 3) == 5
+
+
+def test_zero():
+    assert calc.add(0, 0) == 0
diff --git a/tests/data.json b/tests/data.json
new file mode 100644
--- /dev/null
+++ b/tests/data.json
@@ -0,0 +1 @@
+{}
"""


def row(**kw):
    r = {"instance_id": "calc__calc-1", "repo": "calc/calc", "version": "1.0", "base_commit": "HEAD",
         "patch": PATCH, "test_patch": TEST_PATCH, "problem_statement": "add subtracts",
         "FAIL_TO_PASS": json.dumps(["tests/test_calc.py::test_add"]),
         "PASS_TO_PASS": json.dumps(["tests/test_calc.py::test_zero"])}
    r.update(kw)
    return datasets._normal(r)


def test_images_are_named_as_the_node_names_them():
    r = row(instance_id="django__django-11099", repo="django/django")
    ref = datasets.image_ref(r, "swe-bench-verified")
    assert ref == "swebench/sweb.eval.x86_64.django_1776_django-11099:latest"
    assert datasets.local_image(ref) == "swebench/sweb.eval.x86_64.django_1776_django-11099"
    assert datasets.image_ref(r, "swe-gym") == "xingyaoww/sweb.eval.x86_64.django_s_django-11099:latest"
    assert datasets.image_ref({**r, "docker_image": "swerebench/x:1"}, "swe-bench") == "swerebench/x:1"
    assert datasets.local_image("docker.io/library/python:3.12@sha256:ab") == "library/python"
    assert datasets.local_image("localhost:5000/a/b") == "localhost-5000/a/b"
    long = datasets.local_image("swebench/" + "x" * 80 + ":latest")
    assert len(long) == 63 and long.startswith("swebench/xxx")


def test_specs_follow_swebench():
    dj = datasets.test_spec(row(repo="django/django", version="3.0", test_patch=TEST_PATCH.replace(
        "tests/test_calc.py", "tests/admin_views/test_forms.py")))
    assert dj["test_cmd"] == "./tests/runtests.py --verbosity 2 --settings=test_sqlite --parallel 1"
    assert dj["directives"] == ["admin_views.test_forms"]
    assert dj["eval_commands"][0].endswith("locale-gen") and dj["parser"] == "django"
    assert datasets.test_spec(row(repo="django/django", version="1.9"))["test_cmd"] == "./tests/runtests.py --verbosity 2"
    assert datasets.test_spec(row(repo="django/django", version="4.0"))["eval_commands"] == []
    old = datasets.test_spec(row(repo="astropy/astropy", version="1.3"))
    assert "console_output_style=classic" in old["test_cmd"] and old["parser"] == "pytest_v2"
    s = datasets.test_spec(row())
    assert (s["test_cmd"], s["parser"], s["conda_env"]) == ("pytest -rA", "pytest", "testbed")
    assert s["directives"] == ["tests/test_calc.py"] and s["test_files"] == ["tests/test_calc.py", "tests/data.json"]
    rebench = datasets.test_spec(row(install_config={"test_cmd": "pytest -x", "log_parser": "parse_log_pytest_v2",
                                                      "install": "pip install -e .", "no_use_env": True,
                                                      "env_vars": {"A": "1"}}))
    assert (rebench["test_cmd"], rebench["parser"], rebench["conda_env"], rebench["env"]) == (
        "pytest -x", "pytest_v2", None, {"A": "1"})


def test_a_row_becomes_a_pollen_task():
    t = datasets.task(row(), dataset="swe-bench", agent="gold", mem_mib=2048)
    assert set(t) == {"task_id", "image", "instruction", "n_samples", "workdir", "cell", "labels", "policy", "verify"}
    assert set(t["verify"]) == {"argv", "protected_paths", "files", "timeout_s", "zero_on_tamper"}
    assert t["workdir"] == "/testbed" and t["instruction"] == "add subtracts"
    (script,), = t["policy"]["script"]
    assert script.startswith("git apply -v - <<'HIVE_SWE_PATCH_END'\n") and "+    return a + b" in script
    assert t["verify"]["protected_paths"] == ["tests/test_calc.py", "tests/data.json"]
    spec = json.loads(t["verify"]["files"]["/tmp/hive-swe/spec.json"])
    assert spec["fail_to_pass"] == ["tests/test_calc.py::test_add"]
    assert datasets.task(row(), agent="none")["policy"]["script"] == [["true"]]
    assert datasets.task(row(), agent="my-agent --go")["policy"]["script"] == [["my-agent --go"]]
    with pytest.raises(ValueError):
        datasets.task(row(patch=PATCH + "HIVE_SWE_PATCH_END\n"))


def test_rows_load_from_a_file(tmp_path):
    p = tmp_path / "rows.jsonl"
    p.write_text("".join(json.dumps(row(instance_id=f"a__b-{i}")) + "\n" for i in range(5)))
    assert [r["instance_id"] for r in datasets.load(str(p), only="-[13]$")] == ["a__b-1", "a__b-3"]
    assert len(list(datasets.load(str(p), limit=2))) == 2
    out = tmp_path / "tasks.jsonl"
    images = tmp_path / "images.txt"
    assert datasets.main(["import", str(p), "--out", str(out), "--images", str(images), "--agent", "none"]) == 0
    assert len(out.read_text().splitlines()) == 5
    assert images.read_text().splitlines()[0] == "swebench/sweb.eval.x86_64.a_1776_b-0 swebench/sweb.eval.x86_64.a_1776_b-0:latest"


def test_the_parsers_read_real_log_lines():
    pytest_log = ("PASSED tests/a.py::test_x\nFAILED tests/a.py::test_y - AssertionError: 1 - 2\n"
                  "XFAIL tests/a.py::test_z[a b]\n")
    assert grade.parse_pytest(pytest_log) == {"tests/a.py::test_x": "PASSED", "tests/a.py::test_y": "FAILED",
                                              "tests/a.py::test_z[a": "XFAIL"}
    assert grade.parse_pytest_options("PASSED t.py::test_p[/usr/lib/x.py]\n") == {"t.py::test_p[/x.py]": "PASSED"}
    assert grade.parse_pytest_v2("\x1b[32mPASSED\x1b[0m t.py::a\nt.py::b FAILED\n") == {"t.py::a": "PASSED",
                                                                                         "t.py::b": "FAILED"}
    django_log = ("test_a (x.tests.T) ... ok\ntest_b (x.tests.T) ... FAIL\ntest_c (x.tests.T)\nA docstring ... ok\n"
                  "test_d (x.tests.T) ... skipped 'no'\ntest_e (x.tests.T) ... System check identified no issues (0 silenced)\nok\n")
    assert grade.parse_django(django_log) == {"test_a (x.tests.T)": "PASSED", "test_b (x.tests.T)": "FAILED",
                                              "A docstring": "PASSED", "test_d (x.tests.T)": "SKIPPED",
                                              "test_e (x.tests.T)": "PASSED"}
    sympy_log = "test_one ok\ntest_two F\n____ sympy/core/tests/test_a.py:test_three ____\n"
    assert grade.parse_sympy(sympy_log) == {"test_one": "PASSED", "test_two": "FAILED",
                                            "sympy/core/tests/test_a.py:test_three": "FAILED"}


@pytest.mark.skipif(not shutil.which("git"), reason="needs git")
def test_the_grader_fails_the_bug_and_passes_the_fix(tmp_path):
    """grade.py run for real on a little repo, with pytest from this python standing in for the
    image's conda env."""
    repo, swe = tmp_path / "repo", tmp_path / "swe"
    repo.mkdir()
    swe.mkdir()
    (repo / "calc.py").write_text("def add(a, b):\n    return a - b\n")
    git = ["git", "-c", "user.name=t", "-c", "user.email=t@t", "-C", str(repo)]
    subprocess.run([*git, "init", "-q"], check=True)
    subprocess.run([*git, "add", "."], check=True)
    subprocess.run([*git, "commit", "-qm", "base"], check=True)
    r = row(base_commit=subprocess.run([*git, "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip())
    spec = {**datasets.test_spec(r), "workdir": str(repo), "conda_env": None,
            "test_cmd": f"{sys.executable} -m pytest -rA -p no:cacheprovider"}
    (swe / "spec.json").write_text(json.dumps(spec))
    (swe / "test.patch").write_text(TEST_PATCH)
    shutil.copy(grade.__file__, swe / "grade.py")

    def run():
        p = subprocess.run([sys.executable, str(swe / "grade.py")], capture_output=True, text=True, timeout=120)
        return p.returncode, p.stdout.splitlines()[-1]

    # A test file the agent left is put back as the base had it, here by removing it.
    (repo / "tests").mkdir()
    (repo / "tests" / "test_calc.py").write_text("def test_add():\n    pass\n")
    code, last = run()
    assert code == 1 and last.startswith("==== 1 passed, 1 failed in ")
    subprocess.run([*git, "apply", "-"], input=PATCH, text=True, check=True)
    code, last = run()
    assert code == 0 and last.startswith("==== 2 passed, 0 failed in ")


ENDPOINT = os.environ.get("HIVE_TEST_ENDPOINT")
ROWS = os.environ.get("HIVE_TEST_SWE_ROWS")


@pytest.mark.skipif(not (ENDPOINT and ROWS), reason="set HIVE_TEST_ENDPOINT and HIVE_TEST_SWE_ROWS")
async def test_gold_patches_pass_on_a_comb():
    t = time.perf_counter()
    async with hivebox.AsyncHive(ENDPOINT, project="swe-test") as hive:
        parallel = int(os.environ.get("HIVE_TEST_SWE_PARALLEL", "2"))
        outcomes = await datasets.validate(list(datasets.load(ROWS)), hive=hive, parallel=parallel,
                                           report=lambda o: print(datasets._line(o), flush=True))
        left = [c for c in await hive.cells.list() if "hive-swe" in c.labels and c.state not in ("stopping", "stopped")]
    print(datasets._summary(outcomes, time.perf_counter() - t))
    assert [o.instance_id for o in outcomes if not o.sound] == []
    assert left == []
