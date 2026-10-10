"""Grades a SWE-bench style task in the cell hivebox.verify made for it.

hivebox.datasets writes this file, spec.json and test.patch into /tmp/hive-swe of the verifier
cell. It puts the test files back as they were at the base commit, applies the test patch, runs
the repo's test command on the files the patch touches and reads the log with the repo's parser.
It exits 0 only when every FAIL_TO_PASS and PASS_TO_PASS test passed, which is how SWE-bench
grades a run. It runs on the python3 the image has, so it keeps to what python 3.6 can do.
"""

import json
import os
import re
import shlex
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
START = ">>>>> hive-swe tests start"
END = ">>>>> hive-swe tests end"
STATUSES = ("PASSED", "FAILED", "SKIPPED", "ERROR", "XFAIL")
LOG_TAIL = 48 << 10


def parse_pytest(log):
    out = {}
    for line in log.split("\n"):
        if line.startswith(STATUSES):
            if line.startswith("FAILED"):
                line = line.replace(" - ", " ")
            words = line.split()
            if len(words) > 1:
                out[words[1]] = words[0]
    return out


def parse_pytest_options(log):
    """pytest, with a parameter that is a path cut down to its last part."""
    out = {}
    for test, status in parse_pytest(log).items():
        m = re.match(r"(.*?)\[(.*)\]", test)
        if m:
            main, option = m.groups()
            if option.startswith("/") and not option.startswith("//") and "*" not in option:
                option = "/" + option.split("/")[-1]
            test = "%s[%s]" % (main, option)
        out[test] = status
    return out


def parse_pytest_v2(log):
    """pytest with colors, and with -v lines that end with the status."""
    out = {}
    controls = str.maketrans("", "", "".join(chr(c) for c in range(1, 32)))
    for line in log.split("\n"):
        line = re.sub(r"\[(\d+)m", "", line).translate(controls)
        if line.startswith(STATUSES):
            if line.startswith("FAILED"):
                line = line.replace(" - ", " ")
            words = line.split()
            if len(words) >= 2:
                out[words[1]] = words[0]
        elif line.endswith(STATUSES):
            words = line.split()
            if len(words) >= 2:
                out[words[0]] = words[1]
    return out


def parse_matplotlib(log):
    return parse_pytest(log.replace("MouseButton.LEFT", "1").replace("MouseButton.RIGHT", "3"))


def parse_seaborn(log):
    out = {}
    for line in log.split("\n"):
        words = line.split()
        if line.startswith("FAILED") and len(words) > 1:
            out[words[1]] = "FAILED"
        elif " PASSED " in line:
            if words[1] == "PASSED":
                out[words[0]] = "PASSED"
        elif line.startswith("PASSED") and len(words) > 1:
            out[words[1]] = "PASSED"
    return out


def parse_django(log):
    out = {}
    prev = None
    for line in log.split("\n"):
        line = line.strip()
        if "--version is equivalent to version" in line:
            out["--version is equivalent to version"] = "PASSED"
        if " ... " in line:
            prev = line.split(" ... ")[0]
        for suffix in (" ... ok", " ... OK", " ...  OK"):
            if line.endswith(suffix):
                if line.startswith("Applying sites.0002_alter_domain_unique...test_no_migrations"):
                    line = line.split("...", 1)[-1].strip()
                out[line.rsplit(suffix, 1)[0]] = "PASSED"
                break
        if " ... skipped" in line:
            out[line.split(" ... skipped")[0]] = "SKIPPED"
        if line.endswith(" ... FAIL"):
            out[line.split(" ... FAIL")[0]] = "FAILED"
        if line.startswith("FAIL:"):
            out[line.split()[1].strip()] = "FAILED"
        if line.endswith(" ... ERROR"):
            out[line.split(" ... ERROR")[0]] = "ERROR"
        if line.startswith("ERROR:"):
            out[line.split()[1].strip()] = "ERROR"
        if line.lstrip().startswith("ok") and prev is not None:
            out[prev] = "PASSED"
    # A test whose output came between its name and the ok.
    for pattern in (r"^(.*?)\s\.\.\.\sTesting\ against\ Django\ installed\ in\ ((?s:.*?))\ silenced\)\.\nok$",
                    r"^(.*?)\s\.\.\.\sInternal\ Server\ Error:\ \/(.*)\/\nok$",
                    r"^(.*?)\s\.\.\.\sSystem check identified no issues \(0 silenced\)\nok$"):
        for m in re.finditer(pattern, log, re.MULTILINE):
            out[m.group(1)] = "PASSED"
    return out


def parse_sympy(log):
    out = {}
    for m in re.findall(r"(_*) (.*)\.py:(.*) (_*)", log):
        out["%s.py:%s" % (m[1], m[2])] = "FAILED"
    for line in log.split("\n"):
        line = line.strip()
        if line.startswith("test_"):
            if line.endswith(" E"):
                out[line.split()[0]] = "ERROR"
            if line.endswith(" F"):
                out[line.split()[0]] = "FAILED"
            if line.endswith(" ok"):
                out[line.split()[0]] = "PASSED"
    return out


PARSERS = {
    "pytest": parse_pytest,
    "pytest_options": parse_pytest_options,
    "pytest_v2": parse_pytest_v2,
    "matplotlib": parse_matplotlib,
    "seaborn": parse_seaborn,
    "django": parse_django,
    "sympy": parse_sympy,
}


def script(spec):
    """The bash that runs the tests, as SWE-bench's eval script does, with the install allowed to
    fail since the verifier has no network."""
    q = shlex.quote
    base = spec["base_commit"]
    lines = []
    if spec.get("conda_env"):
        lines += ["source /opt/miniconda3/bin/activate", "conda activate %s" % q(spec["conda_env"])]
    lines += ["export %s=%s" % (k, q(str(v))) for k, v in sorted((spec.get("env") or {}).items())]
    lines += spec.get("eval_commands") or []
    lines += ["cd %s" % q(spec["workdir"]), "git config --global --add safe.directory %s" % q(spec["workdir"])]
    if spec.get("install"):
        lines.append("{ %s ; } > %s/install.log 2>&1 || echo 'hive-swe: the install step failed, its log is in %s/install.log'"
                     % (spec["install"], HERE, HERE))
    for f in spec["test_files"]:
        lines.append("if git cat-file -e %s 2>/dev/null; then git checkout -q %s -- %s; else rm -rf -- %s; fi"
                     % (q(base + ":" + f), q(base), q(f), q(f)))
    lines.append("git apply -v %s/test.patch || { echo 'hive-swe: the test patch did not apply'; exit 97; }" % HERE)
    lines.append("echo %s" % q(START))
    lines.append(" ".join([spec["test_cmd"]] + [q(d) for d in spec["directives"]]))
    lines.append("echo %s" % q(END))
    return "\n".join(lines) + "\n"


def grade(spec, log):
    """The tests that had to pass and did not, as (name, status or None)."""
    if START not in log:
        return None
    part = log.split(START, 1)[1].split(END, 1)[0]
    status = PARSERS[spec["parser"]](part)
    return [(t, status.get(t)) for t in spec["fail_to_pass"] + spec["pass_to_pass"]
            if status.get(t) not in ("PASSED", "XFAIL")]


def main():
    with open(os.path.join(HERE, "spec.json")) as f:
        spec = json.load(f)
    t = time.time()
    p = subprocess.Popen(["bash", "-c", script(spec)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    log = p.communicate()[0].decode("utf-8", "replace")
    took = time.time() - t
    with open(os.path.join(HERE, "test.log"), "w") as f:
        f.write(log)
    sys.stdout.write(log[-LOG_TAIL:] + "\n")
    missed = grade(spec, log)
    if missed is None:
        print("hive-swe: the tests did not start, exit %d" % p.returncode)
        sys.exit(2)
    total = len(spec["fail_to_pass"]) + len(spec["pass_to_pass"])
    for name, status in missed[:50]:
        print("hive-swe: %s %s" % (status or "MISSING", name))
    if len(missed) > 50:
        print("hive-swe: and %d more" % (len(missed) - 50))
    print("==== %d passed, %d failed in %.2fs ====" % (total - len(missed), len(missed), took))
    sys.exit(1 if missed else 0)


if __name__ == "__main__":
    main()
