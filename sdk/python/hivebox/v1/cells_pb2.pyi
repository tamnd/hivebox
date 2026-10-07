import datetime

from google.protobuf import duration_pb2 as _duration_pb2
from hivebox.v1 import types_pb2 as _types_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Placement(_message.Message):
    __slots__ = ("affinity_cell_id", "spread_labels")
    AFFINITY_CELL_ID_FIELD_NUMBER: _ClassVar[int]
    SPREAD_LABELS_FIELD_NUMBER: _ClassVar[int]
    affinity_cell_id: str
    spread_labels: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, affinity_cell_id: _Optional[str] = ..., spread_labels: _Optional[_Iterable[str]] = ...) -> None: ...

class CreateRequest(_message.Message):
    __slots__ = ("spec", "count", "idempotency_key", "placement")
    SPEC_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_FIELD_NUMBER: _ClassVar[int]
    spec: _types_pb2.CellSpec
    count: int
    idempotency_key: str
    placement: Placement
    def __init__(self, spec: _Optional[_Union[_types_pb2.CellSpec, _Mapping]] = ..., count: _Optional[int] = ..., idempotency_key: _Optional[str] = ..., placement: _Optional[_Union[Placement, _Mapping]] = ...) -> None: ...

class CreateEvent(_message.Message):
    __slots__ = ("index", "cell", "error")
    INDEX_FIELD_NUMBER: _ClassVar[int]
    CELL_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    index: int
    cell: _types_pb2.Cell
    error: _types_pb2.Error
    def __init__(self, index: _Optional[int] = ..., cell: _Optional[_Union[_types_pb2.Cell, _Mapping]] = ..., error: _Optional[_Union[_types_pb2.Error, _Mapping]] = ...) -> None: ...

class GetCellRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class ListCellsRequest(_message.Message):
    __slots__ = ("selector", "states", "page_size", "page_token")
    SELECTOR_FIELD_NUMBER: _ClassVar[int]
    STATES_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    selector: _types_pb2.LabelSelector
    states: _containers.RepeatedScalarFieldContainer[_types_pb2.CellState]
    page_size: int
    page_token: str
    def __init__(self, selector: _Optional[_Union[_types_pb2.LabelSelector, _Mapping]] = ..., states: _Optional[_Iterable[_Union[_types_pb2.CellState, str]]] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListCellsResponse(_message.Message):
    __slots__ = ("cells", "next_page_token")
    CELLS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    cells: _containers.RepeatedCompositeFieldContainer[_types_pb2.Cell]
    next_page_token: str
    def __init__(self, cells: _Optional[_Iterable[_Union[_types_pb2.Cell, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class WatchCellsRequest(_message.Message):
    __slots__ = ("selector",)
    SELECTOR_FIELD_NUMBER: _ClassVar[int]
    selector: _types_pb2.CellSelector
    def __init__(self, selector: _Optional[_Union[_types_pb2.CellSelector, _Mapping]] = ...) -> None: ...

class CellEvent(_message.Message):
    __slots__ = ("cell",)
    CELL_FIELD_NUMBER: _ClassVar[int]
    FROM_FIELD_NUMBER: _ClassVar[int]
    cell: _types_pb2.Cell
    def __init__(self, cell: _Optional[_Union[_types_pb2.Cell, _Mapping]] = ..., **kwargs) -> None: ...

class StopRequest(_message.Message):
    __slots__ = ("selector", "snapshot")
    SELECTOR_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    selector: _types_pb2.CellSelector
    snapshot: bool
    def __init__(self, selector: _Optional[_Union[_types_pb2.CellSelector, _Mapping]] = ..., snapshot: _Optional[bool] = ...) -> None: ...

class QuarantineRequest(_message.Message):
    __slots__ = ("selector", "reason")
    SELECTOR_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    selector: _types_pb2.CellSelector
    reason: str
    def __init__(self, selector: _Optional[_Union[_types_pb2.CellSelector, _Mapping]] = ..., reason: _Optional[str] = ...) -> None: ...

class QuarantineResponse(_message.Message):
    __slots__ = ("result", "cells")
    RESULT_FIELD_NUMBER: _ClassVar[int]
    CELLS_FIELD_NUMBER: _ClassVar[int]
    result: _types_pb2.BulkResult
    cells: _containers.RepeatedCompositeFieldContainer[QuarantinedCell]
    def __init__(self, result: _Optional[_Union[_types_pb2.BulkResult, _Mapping]] = ..., cells: _Optional[_Iterable[_Union[QuarantinedCell, _Mapping]]] = ...) -> None: ...

class QuarantinedCell(_message.Message):
    __slots__ = ("cell_id", "network", "snapshot_id", "snapshot_error")
    CELL_ID_FIELD_NUMBER: _ClassVar[int]
    NETWORK_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_ID_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_ERROR_FIELD_NUMBER: _ClassVar[int]
    cell_id: str
    network: str
    snapshot_id: str
    snapshot_error: _types_pb2.Error
    def __init__(self, cell_id: _Optional[str] = ..., network: _Optional[str] = ..., snapshot_id: _Optional[str] = ..., snapshot_error: _Optional[_Union[_types_pb2.Error, _Mapping]] = ...) -> None: ...

class ExtendTtlRequest(_message.Message):
    __slots__ = ("id", "hard_ttl", "idle_ttl")
    ID_FIELD_NUMBER: _ClassVar[int]
    HARD_TTL_FIELD_NUMBER: _ClassVar[int]
    IDLE_TTL_FIELD_NUMBER: _ClassVar[int]
    id: str
    hard_ttl: _duration_pb2.Duration
    idle_ttl: _duration_pb2.Duration
    def __init__(self, id: _Optional[str] = ..., hard_ttl: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., idle_ttl: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ...) -> None: ...

class UpdatePolicyRequest(_message.Message):
    __slots__ = ("id", "network_profile", "limits", "ready")
    ID_FIELD_NUMBER: _ClassVar[int]
    NETWORK_PROFILE_FIELD_NUMBER: _ClassVar[int]
    LIMITS_FIELD_NUMBER: _ClassVar[int]
    READY_FIELD_NUMBER: _ClassVar[int]
    id: str
    network_profile: str
    limits: _types_pb2.Limits
    ready: bool
    def __init__(self, id: _Optional[str] = ..., network_profile: _Optional[str] = ..., limits: _Optional[_Union[_types_pb2.Limits, _Mapping]] = ..., ready: _Optional[bool] = ...) -> None: ...

class ExposePortRequest(_message.Message):
    __slots__ = ("id", "port")
    ID_FIELD_NUMBER: _ClassVar[int]
    PORT_FIELD_NUMBER: _ClassVar[int]
    id: str
    port: int
    def __init__(self, id: _Optional[str] = ..., port: _Optional[int] = ...) -> None: ...

class PortEndpoint(_message.Message):
    __slots__ = ("host", "token")
    HOST_FIELD_NUMBER: _ClassVar[int]
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    host: str
    token: str
    def __init__(self, host: _Optional[str] = ..., token: _Optional[str] = ...) -> None: ...
