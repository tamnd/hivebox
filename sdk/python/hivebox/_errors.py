"""The errors the SDK raises, one class per reason in spec/05_api_sdk.md section 2."""

from __future__ import annotations

import errno as _errno

import grpc
from google.rpc import error_details_pb2, status_pb2

DOMAIN = "hivebox.dev"


class HiveError(Exception):
    """A call failed. `reason` is the stable code, like CELL_NOT_FOUND."""

    reason = "INTERNAL"

    def __init__(self, message: str, *, reason: str | None = None, is_infra_error: bool | None = None, errno_name: str | None = None):
        super().__init__(message)
        self.message = message
        if reason is not None:
            self.reason = reason
        self._infra = is_infra_error
        self.errno_name = errno_name

    @property
    def is_infra_error(self) -> bool:
        """True when the failure was hivebox's and not the caller's, so a trainer can mask the sample."""
        if self._infra is not None:
            return self._infra
        return self.reason in _INFRA

    def __str__(self) -> str:
        return f"{self.reason}: {self.message}"


class QuotaExceeded(HiveError):
    reason = "QUOTA_EXCEEDED"


class CapacityUnavailable(HiveError):
    reason = "CAPACITY_UNAVAILABLE"


class CellNotFound(HiveError):
    reason = "CELL_NOT_FOUND"


class CellLost(HiveError):
    reason = "CELL_LOST"


class CellNotRunning(HiveError):
    reason = "CELL_NOT_RUNNING"


class ExecTimeout(HiveError):
    reason = "EXEC_TIMEOUT"


class OutputLimit(HiveError):
    reason = "OUTPUT_LIMIT"


class PolicyDenied(HiveError):
    reason = "POLICY_DENIED"


class InvalidArgument(HiveError, ValueError):
    reason = "INVALID_ARGUMENT"


class ImageUnavailable(HiveError):
    reason = "IMAGE_UNAVAILABLE"


class DroneUnreachable(HiveError):
    reason = "DRONE_UNREACHABLE"


class FileError(HiveError, OSError):
    """A file operation in the cell failed. It is also an OSError with `errno` set, and for the
    common causes also the builtin subclass, so `except FileNotFoundError` works."""

    reason = "FILE_ERROR"

    def __init__(self, message: str, **kw):
        HiveError.__init__(self, message, **kw)
        code = getattr(_errno, self.errno_name or "", None)
        self.errno = code
        self.strerror = message

    def __str__(self) -> str:
        return HiveError.__str__(self)


class Internal(HiveError):
    reason = "INTERNAL"


_BY_REASON = {c.reason: c for c in (
    QuotaExceeded, CapacityUnavailable, CellNotFound, CellLost, CellNotRunning, ExecTimeout,
    OutputLimit, PolicyDenied, InvalidArgument, ImageUnavailable, DroneUnreachable, FileError,
    Internal,
)}

_INFRA = {"CAPACITY_UNAVAILABLE", "CELL_LOST", "IMAGE_UNAVAILABLE", "DRONE_UNREACHABLE", "INTERNAL"}

_OS_ERRORS = {
    "ENOENT": FileNotFoundError,
    "EEXIST": FileExistsError,
    "EISDIR": IsADirectoryError,
    "ENOTDIR": NotADirectoryError,
    "EACCES": PermissionError,
    "EPERM": PermissionError,
}
_FILE_ERRORS: dict[str, type] = {}


def _file_error(errno_name: str | None) -> type:
    base = _OS_ERRORS.get(errno_name or "")
    if base is None:
        return FileError
    cls = _FILE_ERRORS.get(errno_name)
    if cls is None:
        cls = type(f"FileError{base.__name__}", (FileError, base), {})
        _FILE_ERRORS[errno_name] = cls
    return cls


def make(reason: str, message: str, *, is_infra_error: bool | None = None, errno_name: str | None = None) -> HiveError:
    """The error for `reason`, as its own class when the SDK knows it."""
    if reason == "FILE_ERROR":
        cls = _file_error(errno_name)
    else:
        cls = _BY_REASON.get(reason, HiveError)
    return cls(message, reason=reason, is_infra_error=is_infra_error, errno_name=errno_name)


def from_rpc(e: grpc.aio.AioRpcError) -> HiveError:
    """The error a failed call carries in its status details, or INTERNAL when it carries none,
    as from something that is not hivebox."""
    reason, infra, errno_name = None, None, None
    for key, value in e.trailing_metadata() or ():
        if key != "grpc-status-details-bin":
            continue
        status = status_pb2.Status.FromString(value)
        for any_ in status.details:
            info = error_details_pb2.ErrorInfo()
            if any_.Unpack(info) and info.domain == DOMAIN:
                reason = info.reason
                if "is_infra_error" in info.metadata:
                    infra = info.metadata["is_infra_error"] == "true"
                errno_name = info.metadata.get("errno") or None
    message = e.details() or ""
    if reason is None:
        code = e.code()
        # Not from hivebox: the endpoint could not be reached, or something in between failed.
        return make("INTERNAL", f"{code.name}: {message}", is_infra_error=code == grpc.StatusCode.UNAVAILABLE)
    return make(reason, message, is_infra_error=infra, errno_name=errno_name)
