import datetime

from google.protobuf import duration_pb2 as _duration_pb2
from hivebox.v1 import types_pb2 as _types_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RunRequest(_message.Message):
    __slots__ = ("cell_id", "argv", "shell", "cwd", "env", "stdin", "timeout", "max_output_bytes", "user", "idempotency_key")
    class EnvEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    CELL_ID_FIELD_NUMBER: _ClassVar[int]
    ARGV_FIELD_NUMBER: _ClassVar[int]
    SHELL_FIELD_NUMBER: _ClassVar[int]
    CWD_FIELD_NUMBER: _ClassVar[int]
    ENV_FIELD_NUMBER: _ClassVar[int]
    STDIN_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    MAX_OUTPUT_BYTES_FIELD_NUMBER: _ClassVar[int]
    USER_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    cell_id: str
    argv: _containers.RepeatedScalarFieldContainer[str]
    shell: str
    cwd: str
    env: _containers.ScalarMap[str, str]
    stdin: bytes
    timeout: _duration_pb2.Duration
    max_output_bytes: int
    user: str
    idempotency_key: str
    def __init__(self, cell_id: _Optional[str] = ..., argv: _Optional[_Iterable[str]] = ..., shell: _Optional[str] = ..., cwd: _Optional[str] = ..., env: _Optional[_Mapping[str, str]] = ..., stdin: _Optional[bytes] = ..., timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., max_output_bytes: _Optional[int] = ..., user: _Optional[str] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class RunResult(_message.Message):
    __slots__ = ("exit_code", "stdout", "stderr", "truncated", "timed_out", "wall", "usage", "signal")
    EXIT_CODE_FIELD_NUMBER: _ClassVar[int]
    STDOUT_FIELD_NUMBER: _ClassVar[int]
    STDERR_FIELD_NUMBER: _ClassVar[int]
    TRUNCATED_FIELD_NUMBER: _ClassVar[int]
    TIMED_OUT_FIELD_NUMBER: _ClassVar[int]
    WALL_FIELD_NUMBER: _ClassVar[int]
    USAGE_FIELD_NUMBER: _ClassVar[int]
    SIGNAL_FIELD_NUMBER: _ClassVar[int]
    exit_code: int
    stdout: bytes
    stderr: bytes
    truncated: bool
    timed_out: bool
    wall: _duration_pb2.Duration
    usage: _types_pb2.ResourceUsage
    signal: int
    def __init__(self, exit_code: _Optional[int] = ..., stdout: _Optional[bytes] = ..., stderr: _Optional[bytes] = ..., truncated: _Optional[bool] = ..., timed_out: _Optional[bool] = ..., wall: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., usage: _Optional[_Union[_types_pb2.ResourceUsage, _Mapping]] = ..., signal: _Optional[int] = ...) -> None: ...

class ProcessStart(_message.Message):
    __slots__ = ("cell_id", "argv", "shell", "cwd", "env", "timeout", "user", "pty")
    class EnvEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    CELL_ID_FIELD_NUMBER: _ClassVar[int]
    ARGV_FIELD_NUMBER: _ClassVar[int]
    SHELL_FIELD_NUMBER: _ClassVar[int]
    CWD_FIELD_NUMBER: _ClassVar[int]
    ENV_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    USER_FIELD_NUMBER: _ClassVar[int]
    PTY_FIELD_NUMBER: _ClassVar[int]
    cell_id: str
    argv: _containers.RepeatedScalarFieldContainer[str]
    shell: str
    cwd: str
    env: _containers.ScalarMap[str, str]
    timeout: _duration_pb2.Duration
    user: str
    pty: PtySize
    def __init__(self, cell_id: _Optional[str] = ..., argv: _Optional[_Iterable[str]] = ..., shell: _Optional[str] = ..., cwd: _Optional[str] = ..., env: _Optional[_Mapping[str, str]] = ..., timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., user: _Optional[str] = ..., pty: _Optional[_Union[PtySize, _Mapping]] = ...) -> None: ...

class PtySize(_message.Message):
    __slots__ = ("rows", "cols")
    ROWS_FIELD_NUMBER: _ClassVar[int]
    COLS_FIELD_NUMBER: _ClassVar[int]
    rows: int
    cols: int
    def __init__(self, rows: _Optional[int] = ..., cols: _Optional[int] = ...) -> None: ...

class ProcessInput(_message.Message):
    __slots__ = ("start", "stdin", "eof", "resize", "signal")
    START_FIELD_NUMBER: _ClassVar[int]
    STDIN_FIELD_NUMBER: _ClassVar[int]
    EOF_FIELD_NUMBER: _ClassVar[int]
    RESIZE_FIELD_NUMBER: _ClassVar[int]
    SIGNAL_FIELD_NUMBER: _ClassVar[int]
    start: ProcessStart
    stdin: bytes
    eof: bool
    resize: PtySize
    signal: int
    def __init__(self, start: _Optional[_Union[ProcessStart, _Mapping]] = ..., stdin: _Optional[bytes] = ..., eof: _Optional[bool] = ..., resize: _Optional[_Union[PtySize, _Mapping]] = ..., signal: _Optional[int] = ...) -> None: ...

class ProcessOutput(_message.Message):
    __slots__ = ("pid", "stdout", "stderr", "exit")
    PID_FIELD_NUMBER: _ClassVar[int]
    STDOUT_FIELD_NUMBER: _ClassVar[int]
    STDERR_FIELD_NUMBER: _ClassVar[int]
    EXIT_FIELD_NUMBER: _ClassVar[int]
    pid: int
    stdout: bytes
    stderr: bytes
    exit: RunResult
    def __init__(self, pid: _Optional[int] = ..., stdout: _Optional[bytes] = ..., stderr: _Optional[bytes] = ..., exit: _Optional[_Union[RunResult, _Mapping]] = ...) -> None: ...

class SignalRequest(_message.Message):
    __slots__ = ("cell_id", "pid", "signal")
    CELL_ID_FIELD_NUMBER: _ClassVar[int]
    PID_FIELD_NUMBER: _ClassVar[int]
    SIGNAL_FIELD_NUMBER: _ClassVar[int]
    cell_id: str
    pid: int
    signal: int
    def __init__(self, cell_id: _Optional[str] = ..., pid: _Optional[int] = ..., signal: _Optional[int] = ...) -> None: ...

class SessionCreateRequest(_message.Message):
    __slots__ = ("cell_id", "shell", "cwd", "env", "user")
    class EnvEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    CELL_ID_FIELD_NUMBER: _ClassVar[int]
    SHELL_FIELD_NUMBER: _ClassVar[int]
    CWD_FIELD_NUMBER: _ClassVar[int]
    ENV_FIELD_NUMBER: _ClassVar[int]
    USER_FIELD_NUMBER: _ClassVar[int]
    cell_id: str
    shell: str
    cwd: str
    env: _containers.ScalarMap[str, str]
    user: str
    def __init__(self, cell_id: _Optional[str] = ..., shell: _Optional[str] = ..., cwd: _Optional[str] = ..., env: _Optional[_Mapping[str, str]] = ..., user: _Optional[str] = ...) -> None: ...

class Session(_message.Message):
    __slots__ = ("cell_id", "id")
    CELL_ID_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    cell_id: str
    id: str
    def __init__(self, cell_id: _Optional[str] = ..., id: _Optional[str] = ...) -> None: ...

class SessionRef(_message.Message):
    __slots__ = ("cell_id", "id")
    CELL_ID_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    cell_id: str
    id: str
    def __init__(self, cell_id: _Optional[str] = ..., id: _Optional[str] = ...) -> None: ...

class SessionRunRequest(_message.Message):
    __slots__ = ("session", "command", "timeout", "max_output_bytes")
    SESSION_FIELD_NUMBER: _ClassVar[int]
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    MAX_OUTPUT_BYTES_FIELD_NUMBER: _ClassVar[int]
    session: SessionRef
    command: str
    timeout: _duration_pb2.Duration
    max_output_bytes: int
    def __init__(self, session: _Optional[_Union[SessionRef, _Mapping]] = ..., command: _Optional[str] = ..., timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., max_output_bytes: _Optional[int] = ...) -> None: ...

class SessionRunResult(_message.Message):
    __slots__ = ("exit_code", "output", "truncated", "timed_out", "wall")
    EXIT_CODE_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    TRUNCATED_FIELD_NUMBER: _ClassVar[int]
    TIMED_OUT_FIELD_NUMBER: _ClassVar[int]
    WALL_FIELD_NUMBER: _ClassVar[int]
    exit_code: int
    output: bytes
    truncated: bool
    timed_out: bool
    wall: _duration_pb2.Duration
    def __init__(self, exit_code: _Optional[int] = ..., output: _Optional[bytes] = ..., truncated: _Optional[bool] = ..., timed_out: _Optional[bool] = ..., wall: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ...) -> None: ...

class SessionInput(_message.Message):
    __slots__ = ("open", "data", "resize")
    OPEN_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    RESIZE_FIELD_NUMBER: _ClassVar[int]
    open: SessionRef
    data: bytes
    resize: PtySize
    def __init__(self, open: _Optional[_Union[SessionRef, _Mapping]] = ..., data: _Optional[bytes] = ..., resize: _Optional[_Union[PtySize, _Mapping]] = ...) -> None: ...

class SessionOutput(_message.Message):
    __slots__ = ("data",)
    DATA_FIELD_NUMBER: _ClassVar[int]
    data: bytes
    def __init__(self, data: _Optional[bytes] = ...) -> None: ...
