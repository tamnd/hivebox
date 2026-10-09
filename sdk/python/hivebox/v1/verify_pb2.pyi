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
    __slots__ = ("subject_cell_id", "verifier", "argv", "timeout", "workdir", "protected_paths", "files", "repeats", "report", "must_pass", "grader", "task", "grader_files")
    class FilesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: bytes
        def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...
    SUBJECT_CELL_ID_FIELD_NUMBER: _ClassVar[int]
    VERIFIER_FIELD_NUMBER: _ClassVar[int]
    ARGV_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    WORKDIR_FIELD_NUMBER: _ClassVar[int]
    PROTECTED_PATHS_FIELD_NUMBER: _ClassVar[int]
    FILES_FIELD_NUMBER: _ClassVar[int]
    REPEATS_FIELD_NUMBER: _ClassVar[int]
    REPORT_FIELD_NUMBER: _ClassVar[int]
    MUST_PASS_FIELD_NUMBER: _ClassVar[int]
    GRADER_FIELD_NUMBER: _ClassVar[int]
    TASK_FIELD_NUMBER: _ClassVar[int]
    GRADER_FILES_FIELD_NUMBER: _ClassVar[int]
    subject_cell_id: str
    verifier: _types_pb2.CellSpec
    argv: _containers.RepeatedScalarFieldContainer[str]
    timeout: _duration_pb2.Duration
    workdir: str
    protected_paths: _containers.RepeatedScalarFieldContainer[str]
    files: _containers.ScalarMap[str, bytes]
    repeats: int
    report: str
    must_pass: _containers.RepeatedScalarFieldContainer[str]
    grader: str
    task: bytes
    grader_files: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, subject_cell_id: _Optional[str] = ..., verifier: _Optional[_Union[_types_pb2.CellSpec, _Mapping]] = ..., argv: _Optional[_Iterable[str]] = ..., timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., workdir: _Optional[str] = ..., protected_paths: _Optional[_Iterable[str]] = ..., files: _Optional[_Mapping[str, bytes]] = ..., repeats: _Optional[int] = ..., report: _Optional[str] = ..., must_pass: _Optional[_Iterable[str]] = ..., grader: _Optional[str] = ..., task: _Optional[bytes] = ..., grader_files: _Optional[_Iterable[str]] = ...) -> None: ...

class VerifyResult(_message.Message):
    __slots__ = ("passed", "exit_code", "output", "scores", "error", "tampered", "flaky", "runs_passed", "not_passed", "reward", "grade_detail", "grade_error")
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
    TAMPERED_FIELD_NUMBER: _ClassVar[int]
    FLAKY_FIELD_NUMBER: _ClassVar[int]
    RUNS_PASSED_FIELD_NUMBER: _ClassVar[int]
    NOT_PASSED_FIELD_NUMBER: _ClassVar[int]
    REWARD_FIELD_NUMBER: _ClassVar[int]
    GRADE_DETAIL_FIELD_NUMBER: _ClassVar[int]
    GRADE_ERROR_FIELD_NUMBER: _ClassVar[int]
    passed: bool
    exit_code: int
    output: bytes
    scores: _containers.ScalarMap[str, float]
    error: _types_pb2.Error
    tampered: _containers.RepeatedScalarFieldContainer[str]
    flaky: bool
    runs_passed: int
    not_passed: _containers.RepeatedScalarFieldContainer[str]
    reward: float
    grade_detail: str
    grade_error: str
    def __init__(self, passed: _Optional[bool] = ..., exit_code: _Optional[int] = ..., output: _Optional[bytes] = ..., scores: _Optional[_Mapping[str, float]] = ..., error: _Optional[_Union[_types_pb2.Error, _Mapping]] = ..., tampered: _Optional[_Iterable[str]] = ..., flaky: _Optional[bool] = ..., runs_passed: _Optional[int] = ..., not_passed: _Optional[_Iterable[str]] = ..., reward: _Optional[float] = ..., grade_detail: _Optional[str] = ..., grade_error: _Optional[str] = ...) -> None: ...
