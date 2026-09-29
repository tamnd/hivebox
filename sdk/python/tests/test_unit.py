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
