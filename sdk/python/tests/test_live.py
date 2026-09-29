"""The SDK against a real comb. Set HIVE_TEST_ENDPOINT to its socket, like unix:/run/hivebox/comb.sock,
and HIVE_TEST_IMAGE to an image it has with python3 in it."""

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
pytestmark = pytest.mark.skipif(not ENDPOINT, reason="set HIVE_TEST_ENDPOINT to run against a comb")


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
        async with hivebox.AsyncHive(ENDPOINT, project="sdk-other") as other:
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
        async with hivebox.AsyncHive(ENDPOINT, project="sdk-other") as other:
            assert await other.cells.list({"test": RUN}) == []
