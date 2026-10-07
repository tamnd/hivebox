"""The SDK against a real comb. Set HIVE_TEST_ENDPOINT to its socket, like unix:/run/hivebox/comb.sock,
and HIVE_TEST_IMAGE to an image it has with python3 in it. Through a gate, set HIVE_TOKEN too, and
HIVE_TEST_OTHER_TOKEN to a key for another project, since there the key picks the project."""

import asyncio
import errno
import io
import os
import tarfile
import time

import pytest

import hivebox

ENDPOINT = os.environ.get("HIVE_TEST_ENDPOINT")
IMAGE = os.environ.get("HIVE_TEST_IMAGE", "python")
# A label only this run uses, so cells left by an earlier run are not counted.
RUN = f"many-{os.getpid()}"
OTHER_TOKEN = os.environ.get("HIVE_TEST_OTHER_TOKEN")
pytestmark = pytest.mark.skipif(not ENDPOINT, reason="set HIVE_TEST_ENDPOINT to run against a comb")


def other_project():
    """A client in another project."""
    if os.environ.get("HIVE_TOKEN") and not OTHER_TOKEN:
        pytest.fail("through a gate, set HIVE_TEST_OTHER_TOKEN to a key for another project")
    return hivebox.AsyncHive(ENDPOINT, project="sdk-other", token=OTHER_TOKEN)


def spec(**kw):
    return hivebox.Spec(image=IMAGE, backend="container", mem_mib=256, **kw)


async def test_a_cell_runs_commands_keeps_sessions_and_files():
    async with hivebox.AsyncHive(ENDPOINT, project="sdk-test") as hive:
        cell = await hive.cells.create(spec(labels={"test": "one"}))
        try:
            assert cell.state == "running" and cell.labels == {"test": "one"}
            r = await cell.run("python3 -c 'import sys; print(6 * 7); print(\"e\", file=sys.stderr); sys.exit(3)'")
            assert (r.exit_code, r.stdout, r.stderr, r.ok) == (3, b"42\n", b"e\n", False)
            r = await cell.run(["cat"], stdin=b"x" * 100_000)
            assert r.stdout == b"x" * 100_000
            r = await cell.run("sleep 5", timeout="0.2s")
            assert r.timed_out

            async with await cell.session() as s:
                await s.run("cd /tmp && export X=42")
                assert (await s.run("echo $PWD $X")).output == b"/tmp 42\n"

            p = await cell.start("cat; echo done >&2")
            await p.write(b"hello\n")
            await p.close_stdin()
            out = [o async for o in p]
            # Two pipes, so which one is read first is not fixed.
            assert sorted(out) == [("stderr", b"done\n"), ("stdout", b"hello\n")] and p.result.exit_code == 0

            info = await cell.files.write("/tmp/w/a.txt", "hello")
            assert info.size == 5 and info.type == "file"
            assert await cell.files.read_text("/tmp/w/a.txt") == "hello"
            assert await cell.files.read("/tmp/w/a.txt", offset=1, length=3) == b"ell"
            big = os.urandom(3 << 20)
            await cell.files.write("/tmp/w/big", big)
            assert await cell.files.read("/tmp/w/big") == big
            assert [f.path for f in await cell.files.list("/tmp/w")] == ["/tmp/w/a.txt", "/tmp/w/big"]
            with pytest.raises(FileNotFoundError):
                await cell.files.read("/tmp/w/nothing")
            assert not await cell.files.exists("/tmp/w/nothing")

            buf = io.BytesIO()
            with tarfile.open(fileobj=buf, mode="w") as t:
                data = b"print('from a tar')\n"
                ti = tarfile.TarInfo("pkg/main.py")
                ti.size = len(data)
                t.addfile(ti, io.BytesIO(data))
            await cell.files.upload("/tmp/up", buf.getvalue())
            assert (await cell.run("python3 /tmp/up/pkg/main.py")).stdout == b"from a tar\n"

            with pytest.raises(OSError) as e:
                await cell.files.remove("/tmp/w")
            assert e.value.errno == errno.ENOTEMPTY
            await cell.files.remove("/tmp/w", recursive=True)

            await cell.pause()
            assert (await cell.refresh()).state == "paused"
            # A command in a paused cell resumes it.
            assert (await cell.run("echo back")).stdout == b"back\n"
        finally:
            await cell.stop()
        with pytest.raises(hivebox.CellNotRunning):
            await cell.run("true")
        with pytest.raises(hivebox.InvalidArgument):
            await hive.cells.get("c" * 26)
        # Another project does not see the cell at all.
        async with other_project() as other:
            with pytest.raises(hivebox.CellNotFound):
                await other.cells.get(cell.id)


async def test_many_cells_at_once_and_bulk_calls():
    async with hivebox.AsyncHive(ENDPOINT, project="sdk-test") as hive:
        t = time.monotonic()
        async with hive.cells.create_many(spec(labels={"test": RUN}), count=8) as group:
            made = time.monotonic() - t
            assert len(group) == 8
            rs = await asyncio.gather(*(c.run(["python3", "-c", "print(1)"]) for c in group))
            assert all(r.stdout == b"1\n" for r in rs)
            listed = await hive.cells.list({"test": RUN}, states=["running"])
            assert sorted(c.id for c in listed) == sorted(c.id for c in group)
            r = await hive.cells.pause({"test": RUN})
            assert (r.matched, r.succeeded, r.failures) == (8, 8, {})
            r = await hive.cells.resume({"test": RUN})
            assert r.succeeded == 8
        print(f"8 cells made in {made * 1000:.0f} ms")
        left = await hive.cells.list({"test": RUN}, states=["running", "paused"])
        assert left == []
        # Another project sees none of them.
        async with other_project() as other:
            assert await other.cells.list({"test": RUN}) == []


async def test_a_quarantined_cell_stays_frozen_until_it_is_stopped():
    async with hivebox.AsyncHive(ENDPOINT, project="sdk-test") as hive:
        cell = await hive.cells.create(spec(labels={"test": "quarantine"}))
        try:
            await cell.files.write("/tmp/evidence", "kept")
            q = await cell.quarantine("a live test")
            assert q.network in ("cut", "loopback", "unmanaged")
            assert (q.snapshot is None) == (q.snapshot_error is not None)
            await cell.refresh()
            assert cell.state == "paused" and cell.quarantined
            with pytest.raises(hivebox.PolicyDenied):
                await cell.resume()
            with pytest.raises(hivebox.PolicyDenied):
                await cell.run("true")
            print(f"quarantined: network {q.network}, snapshot {q.snapshot or q.snapshot_error}")
        finally:
            await cell.stop()


GIT_IMAGE = os.environ.get("HIVE_TEST_GIT_IMAGE")
GIT_WORKDIR = os.environ.get("HIVE_TEST_GIT_WORKDIR", "/testbed")


@pytest.mark.skipif(not GIT_IMAGE, reason="set HIVE_TEST_GIT_IMAGE to an image with a git checkout at HIVE_TEST_GIT_WORKDIR")
async def test_verify_checks_a_cells_changes_in_a_cell_of_its_own():
    verifier = hivebox.Spec(image=GIT_IMAGE, backend="container", mem_mib=512)
    check = ["bash", "-c", "[ \"$(cat probe.txt 2>/dev/null)\" = fixed ] && test ! -e guarded.txt && grep -q 7 hidden.txt"]
    async with hivebox.AsyncHive(ENDPOINT, project="sdk-test") as hive:
        cell = await hive.cells.create(hivebox.Spec(image=GIT_IMAGE, backend="container", mem_mib=512))
        try:
            await cell.run("echo fixed > probe.txt && echo x > guarded.txt", cwd=GIT_WORKDIR)
            r = await hive.verify(check, subject=cell, verifier=verifier, workdir=GIT_WORKDIR,
                                  protected_paths=["guarded.txt"], files={"hidden.txt": "7\n"}, repeats=2)
            assert (r.passed, r.exit_code, r.runs_passed, r.flaky, r.error) == (True, 0, 2, False, None), r.output
            assert r.tampered == ["guarded.txt"] and r.scores["diff_bytes"] > 0
        finally:
            await cell.stop()
        # With no subject the image is checked as it is, and has no probe.txt.
        r = await hive.verify(check, verifier=verifier, workdir=GIT_WORKDIR, files={"hidden.txt": "7\n"})
        assert (r.passed, r.exit_code, r.runs_passed, r.error) == (False, 1, 0, None)


# Stands in for pytest --junitxml, which the image may not have: it imports the subject's code and
# writes a report of one test.
CHECK = """import sys
import probe
ok = probe.answer() == 42
with open(sys.argv[1], "w") as f:
    f.write('<testsuite><testcase classname="check" name="answer">%s</testcase></testsuite>' % ("" if ok else "<failure/>"))
"""


@pytest.mark.skipif(not GIT_IMAGE, reason="set HIVE_TEST_GIT_IMAGE to an image with a git checkout at HIVE_TEST_GIT_WORKDIR")
async def test_verify_with_a_report_is_not_fooled_by_an_early_exit():
    verifier = hivebox.Spec(image=GIT_IMAGE, backend="container", mem_mib=512)
    check = ["python3", "check.py", "/tmp/report.xml"]
    kw = dict(verifier=verifier, workdir=GIT_WORKDIR, files={"check.py": CHECK})
    async with hivebox.AsyncHive(ENDPOINT, project="sdk-test") as hive:
        cell = await hive.cells.create(hivebox.Spec(image=GIT_IMAGE, backend="container", mem_mib=512))
        try:
            await cell.files.write(f"{GIT_WORKDIR}/probe.py", "import sys\nsys.exit(0)\n")
            r = await hive.verify(check, subject=cell, **kw)
            assert (r.passed, r.exit_code) == (True, 0), "an exit code alone is fooled"
            r = await hive.verify(check, subject=cell, report="/tmp/report.xml", must_pass=["check.py::answer"], **kw)
            assert (r.passed, r.exit_code, r.not_passed, r.scores["report"]) == (False, 0, ["check.py::answer"], 0.0)
            await cell.files.write(f"{GIT_WORKDIR}/probe.py", "def answer():\n    return 42\n")
            r = await hive.verify(check, subject=cell, report="/tmp/report.xml", must_pass=["check.py::answer"], **kw)
            assert (r.passed, r.not_passed, r.scores["tests_passed"], r.error) == (True, [], 1.0, None), r.output
        finally:
            await cell.stop()
