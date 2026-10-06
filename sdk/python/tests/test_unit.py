"""What can be checked without a comb."""

import grpc
import pytest
from google.protobuf import any_pb2
from google.rpc import error_details_pb2, status_pb2

import hivebox
from hivebox import _client, _errors


def test_durations_take_units():
    assert _client._seconds(None) is None
    assert _client._seconds(90).seconds == 90
    assert _client._seconds("1.5s").ToNanoseconds() == 1_500_000_000
    assert _client._seconds("10m").seconds == 600
    assert _client._seconds("2h").seconds == 7200
    for bad in ("soon", "-1", -1):
        with pytest.raises(hivebox.InvalidArgument):
            _client._seconds(bad)


def test_a_spec_needs_one_source():
    with pytest.raises(ValueError):
        hivebox.Spec().to_proto()
    with pytest.raises(ValueError):
        hivebox.Spec(image="a", template="b").to_proto()
    s = hivebox.Spec(image="python", backend="container", mem_mib=512, qos="latency", idle_ttl="10m",
                     labels={"step": "412"}, network_profile="pypi").to_proto()
    assert s.image.ref == "python" and s.resources.mem_mib == 512
    assert s.idle_ttl.seconds == 600 and not s.HasField("hard_ttl")
    assert s.labels["step"] == "412" and s.network_profile == "pypi"
    assert s.WhichOneof("source") == "image"


class FakeRpcError(grpc.aio.AioRpcError):
    def __init__(self, code, details, trailing=()):
        super().__init__(code, grpc.aio.Metadata(), grpc.aio.Metadata(*trailing), details)


def status_with(reason, **metadata):
    info = error_details_pb2.ErrorInfo(reason=reason, domain="hivebox.dev", metadata=metadata)
    detail = any_pb2.Any()
    detail.Pack(info)
    return status_pb2.Status(code=9, message="x", details=[detail]).SerializeToString()


def test_errors_are_raised_as_their_reason():
    e = _errors.from_rpc(FakeRpcError(grpc.StatusCode.NOT_FOUND, "no cell", [("grpc-status-details-bin", status_with("CELL_NOT_FOUND", is_infra_error="false"))]))
    assert isinstance(e, hivebox.CellNotFound) and not e.is_infra_error and e.message == "no cell"

    e = _errors.from_rpc(FakeRpcError(grpc.StatusCode.FAILED_PRECONDITION, "/x: no such file", [("grpc-status-details-bin", status_with("FILE_ERROR", errno="ENOENT"))]))
    assert isinstance(e, hivebox.FileError) and isinstance(e, FileNotFoundError)
    assert e.errno == 2 and e.errno_name == "ENOENT"
    e = _errors.from_rpc(FakeRpcError(grpc.StatusCode.FAILED_PRECONDITION, "odd", [("grpc-status-details-bin", status_with("FILE_ERROR", errno="EXDEV"))]))
    assert type(e) is hivebox.FileError and isinstance(e, OSError)

    # A failure from outside hivebox reads as INTERNAL, and as hivebox's when nothing answered.
    e = _errors.from_rpc(FakeRpcError(grpc.StatusCode.UNAVAILABLE, "connection refused"))
    assert isinstance(e, hivebox.Internal) and e.is_infra_error
    e = _errors.from_rpc(FakeRpcError(grpc.StatusCode.UNIMPLEMENTED, "no"))
    assert e.reason == "INTERNAL" and not e.is_infra_error


def test_a_verify_result_keeps_the_verdict_and_the_error():
    from hivebox.v1 import types_pb2, verify_pb2

    r = _client.VerifyResult._from(verify_pb2.VerifyResult(
        passed=True, exit_code=0, output=b"1 passed", scores={"tests_passed": 1.0}, tampered=["tests/a.py"],
        runs_passed=3))
    assert (r.passed, r.output, r.scores, r.tampered, r.runs_passed, r.error, r.is_infra_error) == (
        True, b"1 passed", {"tests_passed": 1.0}, ["tests/a.py"], 3, None, False)
    r = _client.VerifyResult._from(verify_pb2.VerifyResult(
        error=types_pb2.Error(reason="CAPACITY_UNAVAILABLE", message="full", is_infra_error=True)))
    assert isinstance(r.error, hivebox.CapacityUnavailable) and r.is_infra_error and not r.passed


async def test_endpoints_pick_tls_and_where_the_token_goes():
    plain = hivebox.AsyncHive("http://gate:7401", token="k", project="p")
    assert ("authorization", "Bearer k") in plain._metadata and ("x-hive-project", "p") in plain._metadata
    tls = hivebox.AsyncHive("https://gate:7401", token="k")
    assert tls._metadata == ()
    for h in (plain, tls):
        await h.close()


async def test_verify_wants_argv_as_a_list():
    hive = hivebox.AsyncHive("http://gate:7401")
    with pytest.raises(hivebox.InvalidArgument):
        await hive.verify("pytest -q", verifier=hivebox.Spec(image="i"), workdir="/w")
    await hive.close()


async def test_allowed_paths_need_scrubbing():
    hive = hivebox.AsyncHive("http://gate:7401")
    with pytest.raises(hivebox.InvalidArgument):
        await hive.snapshot("c-1", allow=["tests"])
    await hive.close()
