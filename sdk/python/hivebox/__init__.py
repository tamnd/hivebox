"""The Python client for hivebox.

    from hivebox import AsyncHive, Spec

    async with AsyncHive() as hive:
        cell = await hive.cells.create(Spec(image="python", mem_mib=512))
        r = await cell.run("python -c 'print(6 * 7)'")
        await cell.files.write("/work/a.txt", "hello")
        await cell.stop()

`hive.verify` checks a cell's changes against tests in a cell of its own, as a reward for RL, and
`hive.llm` steers the node's LLM gateway and gives the token ids of the calls cells made through it.
The endpoint is the comb's socket by default. The design is in spec/05_api_sdk.md.
"""

from ._client import (
    DEFAULT_SOCKET,
    AsyncHive,
    BulkResult,
    Cell,
    CellGroup,
    Cells,
    Choice,
    FileInfo,
    Files,
    Llm,
    Process,
    Quarantined,
    RunResult,
    Session,
    SessionResult,
    Spec,
    Turn,
    VerifyResult,
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

__version__ = "0.0.37"

__all__ = [
    "DEFAULT_SOCKET", "AsyncHive", "BulkResult", "Cell", "CellGroup", "Cells", "Choice", "FileInfo", "Files", "Llm",
    "Process", "Quarantined", "RunResult", "Session", "SessionResult", "Spec", "Turn", "VerifyResult", "CapacityUnavailable",
    "CellLost", "CellNotFound", "CellNotRunning", "DroneUnreachable", "ExecTimeout", "FileError", "HiveError",
    "ImageUnavailable", "Internal", "InvalidArgument", "OutputLimit", "PolicyDenied", "QuotaExceeded",
]
