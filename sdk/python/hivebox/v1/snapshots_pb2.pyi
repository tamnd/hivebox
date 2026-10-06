from hivebox.v1 import cells_pb2 as _cells_pb2
from hivebox.v1 import types_pb2 as _types_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SnapshotKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SNAPSHOT_KIND_UNSPECIFIED: _ClassVar[SnapshotKind]
    SNAPSHOT_KIND_DISK: _ClassVar[SnapshotKind]
    SNAPSHOT_KIND_DISK_MEM: _ClassVar[SnapshotKind]
    SNAPSHOT_KIND_PROC: _ClassVar[SnapshotKind]
SNAPSHOT_KIND_UNSPECIFIED: SnapshotKind
SNAPSHOT_KIND_DISK: SnapshotKind
SNAPSHOT_KIND_DISK_MEM: SnapshotKind
SNAPSHOT_KIND_PROC: SnapshotKind

class SnapshotRequest(_message.Message):
    __slots__ = ("cell_id", "kind", "labels", "scrub", "allow")
    class LabelsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    CELL_ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    SCRUB_FIELD_NUMBER: _ClassVar[int]
    ALLOW_FIELD_NUMBER: _ClassVar[int]
    cell_id: str
    kind: SnapshotKind
    labels: _containers.ScalarMap[str, str]
    scrub: bool
    allow: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, cell_id: _Optional[str] = ..., kind: _Optional[_Union[SnapshotKind, str]] = ..., labels: _Optional[_Mapping[str, str]] = ..., scrub: _Optional[bool] = ..., allow: _Optional[_Iterable[str]] = ...) -> None: ...

class RestoreRequest(_message.Message):
    __slots__ = ("snapshot", "count", "spec", "idempotency_key")
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    SPEC_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    snapshot: _types_pb2.SnapshotRef
    count: int
    spec: _types_pb2.CellSpec
    idempotency_key: str
    def __init__(self, snapshot: _Optional[_Union[_types_pb2.SnapshotRef, _Mapping]] = ..., count: _Optional[int] = ..., spec: _Optional[_Union[_types_pb2.CellSpec, _Mapping]] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class ForkRequest(_message.Message):
    __slots__ = ("cell_id", "count", "labels", "idempotency_key")
    class LabelsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    CELL_ID_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    cell_id: str
    count: int
    labels: _containers.ScalarMap[str, str]
    idempotency_key: str
    def __init__(self, cell_id: _Optional[str] = ..., count: _Optional[int] = ..., labels: _Optional[_Mapping[str, str]] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class CommitRequest(_message.Message):
    __slots__ = ("snapshot", "name")
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    snapshot: _types_pb2.SnapshotRef
    name: str
    def __init__(self, snapshot: _Optional[_Union[_types_pb2.SnapshotRef, _Mapping]] = ..., name: _Optional[str] = ...) -> None: ...
