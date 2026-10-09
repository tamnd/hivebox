"""hivebox.swerex against a real comb, next to SWE-ReX's own LocalRuntime on this machine. Set
HIVE_TEST_ENDPOINT and HIVE_TEST_IMAGE as for test_live.py, and have swe-rex installed."""

import os
import time

import pytest

pytest.importorskip("swerex")

from swerex.exceptions import (  # noqa: E402
    BashIncorrectSyntaxError,
    CommandTimeoutError,
    DeploymentNotStartedError,
    NonZeroExitCodeError,
    SessionDoesNotExistError,
    SessionExistsError,
)
from swerex.runtime.abstract import (  # noqa: E402
    BashAction,
    BashInterruptAction,
    CloseBashSessionRequest,
    Command,
    CreateBashSessionRequest,
    ReadFileRequest,
    UploadRequest,
    WriteFileRequest,
)
from swerex.runtime.local import LocalRuntime  # noqa: E402

import hivebox  # noqa: E402
from hivebox.swerex import HiveboxDeployment, HiveboxDeploymentConfig  # noqa: E402

ENDPOINT = os.environ.get("HIVE_TEST_ENDPOINT")
IMAGE = os.environ.get("HIVE_TEST_IMAGE", "python")
pytestmark = pytest.mark.skipif(not ENDPOINT, reason="set HIVE_TEST_ENDPOINT to run against a comb")

# Commands whose output and exit code do not depend on where they run.
SAME = [
    "echo hello",
    "printf 'a\\nb\\n'; echo c >&2",
    "false",
    "(exit 7)",
    "x=4; echo $((x * x))",
    "for i in 1 2 3; do echo $i; done",
    "cat <<'EOF'\nline one\n  line two\nEOF",
    "echo a && echo b || echo c",
    "f() { echo in f $1; }; f 42",
    "python3 -c 'print(6 * 7)'",
    "echo -n no newline",
]


def deployment(**kw):
    return HiveboxDeployment(image=IMAGE, hive=hivebox.AsyncHive(ENDPOINT, project="swerex-test"), mem_mib=256, **kw)


async def started():
    d = deployment()
    await d.start()
    await d.runtime.create_session(CreateBashSessionRequest())
    return d


async def test_a_session_keeps_its_state_and_reports_exit_codes():
    d = await started()
    try:
        rt = d.runtime
        assert await d.is_alive()
        await rt.run_in_session(BashAction(command="cd /tmp && export SWEREX_X=41"))
        obs = await rt.run_in_session(BashAction(command="echo $PWD $((SWEREX_X + 1))"))
        assert (obs.output, obs.exit_code) == ("/tmp 42\n", 0)

        with pytest.raises(NonZeroExitCodeError, match="exit code 3"):
            await rt.run_in_session(BashAction(command="echo out; exit 3", error_msg="probe"))
        obs = await rt.run_in_session(BashAction(command="echo out; (exit 3)", check="silent"))
        assert (obs.output, obs.exit_code) == ("out\n", 3)
        obs = await rt.run_in_session(BashAction(command="(exit 3)", check="ignore"))
        assert obs.exit_code is None

        with pytest.raises(BashIncorrectSyntaxError):
            await rt.run_in_session(BashAction(command="if then fi"))
        with pytest.raises(CommandTimeoutError):
            await rt.run_in_session(BashAction(command="sleep 5", timeout=0.3))
        await rt.run_in_session(BashInterruptAction())
        # The shell that timed out is gone, and the next command gets a fresh one.
        obs = await rt.run_in_session(BashAction(command="echo after"))
        assert obs.output == "after\n"

        with pytest.raises(SessionExistsError):
            await rt.create_session(CreateBashSessionRequest())
        with pytest.raises(SessionDoesNotExistError):
            await rt.run_in_session(BashAction(command="true", session="other"))
        await rt.create_session(CreateBashSessionRequest(session="other"))
        obs = await rt.run_in_session(BashAction(command="echo $PWD", session="other"))
        assert obs.output != "/tmp\n"
        await rt.close_session(CloseBashSessionRequest(session="other"))
        with pytest.raises(SessionDoesNotExistError):
            await rt.close_session(CloseBashSessionRequest(session="other"))
    finally:
        await d.stop()


async def test_startup_files_are_sourced():
    d = deployment()
    await d.start()
    try:
        await d.runtime.write_file(WriteFileRequest(path="/root/.swerex-env", content="export STARTED=yes\n"))
        await d.runtime.create_session(CreateBashSessionRequest(startup_source=["/root/.swerex-env"]))
        obs = await d.runtime.run_in_session(BashAction(command="echo $STARTED"))
        assert obs.output == "yes\n"
    finally:
        await d.stop()


async def test_execute_files_and_upload(tmp_path):
    d = await started()
    try:
        rt = d.runtime
        r = await rt.execute(Command(command=["python3", "-c", "import sys; print(1); print(2, file=sys.stderr)"]))
        assert (r.stdout, r.stderr, r.exit_code) == ("1\n", "2\n", 0)
        r = await rt.execute(Command(command="echo $HOME; exit 4", shell=True))
        assert (r.stdout, r.exit_code) == ("/root\n", 4)
        r = await rt.execute(Command(command=["sh", "-c", "echo o; echo e >&2"], merge_output_streams=True))
        assert (r.stdout, r.stderr) == ("o\ne\n", "")
        r = await rt.execute(Command(command="pwd; echo $ONLY", shell=True, cwd="/tmp", env={"ONLY": "this"}))
        assert r.stdout == "/tmp\nthis\n"
        with pytest.raises(NonZeroExitCodeError):
            await rt.execute(Command(command="false", shell=True, check=True))
        with pytest.raises(CommandTimeoutError):
            await rt.execute(Command(command="sleep 5", shell=True, timeout=0.3))

        await rt.write_file(WriteFileRequest(path="/work/deep/a.txt", content="héllo\n"))
        assert (await rt.read_file(ReadFileRequest(path="/work/deep/a.txt"))).content == "héllo\n"
        with pytest.raises(FileNotFoundError):
            await rt.read_file(ReadFileRequest(path="/work/nope"))

        (tmp_path / "one.txt").write_text("one\n")
        repo = tmp_path / "repo"
        (repo / "pkg").mkdir(parents=True)
        (repo / "pkg" / "m.py").write_text("x = 1\n")
        (repo / "README").write_text("r\n")
        await rt.upload(UploadRequest(source_path=str(tmp_path / "one.txt"), target_path="/work/up/renamed.txt"))
        await rt.upload(UploadRequest(source_path=str(repo), target_path="/work/repo"))
        obs = await rt.run_in_session(BashAction(command="cat /work/up/renamed.txt; cd /work/repo && find . -type f | sort"))
        assert obs.output == "one\n./README\n./pkg/m.py\n"
    finally:
        await d.stop()


async def test_the_deployment_owns_its_cell():
    d = deployment(labels={"swerex": "own"})
    with pytest.raises(DeploymentNotStartedError):
        d.runtime
    await d.start()
    cell = d.cell
    assert cell.labels == {"swerex": "own"}
    await d.stop()
    assert (await cell.refresh()).state in ("stopping", "stopped")
    await d.stop()

    cfg = HiveboxDeploymentConfig(image=IMAGE, mem_mib=256, labels={"from": "config"})
    assert cfg.get_deployment().spec.labels == {"from": "config"}
    with pytest.raises(ValueError):
        HiveboxDeploymentConfig(image=IMAGE, nope=1)


async def test_output_matches_swerex_local_runtime():
    """The same commands give the same output and exit code here as in SWE-ReX's LocalRuntime,
    and the time each takes is printed for both."""
    local = LocalRuntime()
    await local.create_session(CreateBashSessionRequest())
    d = await started()
    try:
        for cmd in SAME:
            want = await local.run_in_session(BashAction(command=cmd, check="silent"))
            got = await d.runtime.run_in_session(BashAction(command=cmd, check="silent"))
            assert (got.output, got.exit_code) == (want.output, want.exit_code), cmd

        for name, rt in (("hivebox", d.runtime), ("swerex local", local)):
            times = []
            for i in range(200):
                t = time.perf_counter()
                await rt.run_in_session(BashAction(command=f"echo {i}"))
                times.append(time.perf_counter() - t)
            times.sort()
            print(f"\n{name}: an action took p50 {times[100] * 1000:.2f} ms p99 {times[198] * 1000:.2f} ms")
    finally:
        await local.close()
        await d.stop()
