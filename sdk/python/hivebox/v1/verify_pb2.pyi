import datetime

from google.protobuf import duration_pb2 as _duration_pb2
from hivebox.v1 import types_pb2 as _types_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class VerifyRequest(_message.Message):
    __slots__ = ("subject_cell_id", "verifier", "argv", "timeout")
    SUBJECT_CELL_ID_FIELD_NUMBER: _ClassVar[int]
    VERIFIER_FIELD_NUMBER: _ClassVar[int]
    ARGV_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    subject_cell_id: str
    verifier: _types_pb2.CellSpec
    argv: _containers.RepeatedScalarFieldContainer[str]
    timeout: _duration_pb2.Duration
    def __init__(self, subject_cell_id: _Optional[str] = ..., verifier: _Optional[_Union[_types_pb2.CellSpec, _Mapping]] = ..., argv: _Optional[_Iterable[str]] = ..., timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ...) -> None: ...

class VerifyResult(_message.Message):
    __slots__ = ("passed", "exit_code", "output", "scores", "error")
    class ScoresEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: float
        def __init__(self, key: _Optional[str] = ..., value: _Optional[float] = ...) -> None: ...
    PASSED_FIELD_NUMBER: _ClassVar[int]
    EXIT_CODE_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    SCORES_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    passed: bool
    exit_code: int
    output: bytes
    scores: _containers.ScalarMap[str, float]
    error: _types_pb2.Error
    def __init__(self, passed: _Optional[bool] = ..., exit_code: _Optional[int] = ..., output: _Optional[bytes] = ..., scores: _Optional[_Mapping[str, float]] = ..., error: _Optional[_Union[_types_pb2.Error, _Mapping]] = ...) -> None: ...
