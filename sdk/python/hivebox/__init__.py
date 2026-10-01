"""The Python client for hivebox.

    from hivebox import AsyncHive, Spec

    async with AsyncHive() as hive:
        cell = await hive.cells.create(Spec(image="python", mem_mib=512))
        r = await cell.run("python -c 'print(6 * 7)'")
        await cell.files.write("/work/a.txt", "hello")
        await cell.stop()

The endpoint is the comb's socket by default. The design is in spec/05_api_sdk.md.
"""

from ._client import (
    DEFAULT_SOCKET,
    AsyncHive,
    BulkResult,
    Cell,
    CellGroup,
    Cells,
    FileInfo,
    Files,
    Process,
    RunResult,
    Session,
    SessionResult,
    Spec,
)
from ._errors import (
    CapacityUnavailable,
    CellLost,
    CellNotFound,
    CellNotRunning,
    DroneUnreachable,
    ExecTimeout,
    FileError,
    HiveError,
    ImageUnavailable,
    Internal,
    InvalidArgument,
    OutputLimit,
    PolicyDenied,
    QuotaExceeded,
)

__version__ = "0.0.14"

__all__ = [
    "DEFAULT_SOCKET", "AsyncHive", "BulkResult", "Cell", "CellGroup", "Cells", "FileInfo", "Files", "Process",
    "RunResult", "Session", "SessionResult", "Spec", "CapacityUnavailable", "CellLost", "CellNotFound",
    "CellNotRunning", "DroneUnreachable", "ExecTimeout", "FileError", "HiveError", "ImageUnavailable", "Internal",
    "InvalidArgument", "OutputLimit", "PolicyDenied", "QuotaExceeded",
]
