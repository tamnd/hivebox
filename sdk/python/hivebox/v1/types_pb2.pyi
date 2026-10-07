import datetime

from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Backend(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    BACKEND_UNSPECIFIED: _ClassVar[Backend]
    BACKEND_FNCALL: _ClassVar[Backend]
    BACKEND_CONTAINER: _ClassVar[Backend]
    BACKEND_MICROVM: _ClassVar[Backend]
    BACKEND_FULLVM: _ClassVar[Backend]
    BACKEND_AUTO: _ClassVar[Backend]

class Qos(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    QOS_UNSPECIFIED: _ClassVar[Qos]
    QOS_LATENCY: _ClassVar[Qos]
    QOS_STANDARD: _ClassVar[Qos]
    QOS_BEST_EFFORT: _ClassVar[Qos]

class IdleAction(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    IDLE_ACTION_UNSPECIFIED: _ClassVar[IdleAction]
    IDLE_ACTION_PAUSE: _ClassVar[IdleAction]
    IDLE_ACTION_STOP: _ClassVar[IdleAction]

class CellState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CELL_STATE_UNSPECIFIED: _ClassVar[CellState]
    CELL_STATE_PENDING: _ClassVar[CellState]
    CELL_STATE_PREPARING: _ClassVar[CellState]
    CELL_STATE_STARTING: _ClassVar[CellState]
    CELL_STATE_RUNNING: _ClassVar[CellState]
    CELL_STATE_PAUSING: _ClassVar[CellState]
    CELL_STATE_PAUSED: _ClassVar[CellState]
    CELL_STATE_STOPPING: _ClassVar[CellState]
    CELL_STATE_STOPPED: _ClassVar[CellState]
    CELL_STATE_FAILED: _ClassVar[CellState]
    CELL_STATE_EXPIRED: _ClassVar[CellState]

class Cause(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CAUSE_UNSPECIFIED: _ClassVar[Cause]
    CAUSE_REQUESTED: _ClassVar[Cause]
    CAUSE_IDLE: _ClassVar[Cause]
    CAUSE_HARD_TTL: _ClassVar[Cause]
    CAUSE_EXITED: _ClassVar[Cause]
    CAUSE_OOM: _ClassVar[Cause]
    CAUSE_POLICY: _ClassVar[Cause]
    CAUSE_START_FAILED: _ClassVar[Cause]
    CAUSE_DRONE_LOST: _ClassVar[Cause]
    CAUSE_NODE_LOST: _ClassVar[Cause]
    CAUSE_RECOVERY: _ClassVar[Cause]
BACKEND_UNSPECIFIED: Backend
BACKEND_FNCALL: Backend
BACKEND_CONTAINER: Backend
BACKEND_MICROVM: Backend
BACKEND_FULLVM: Backend
BACKEND_AUTO: Backend
QOS_UNSPECIFIED: Qos
QOS_LATENCY: Qos
QOS_STANDARD: Qos
QOS_BEST_EFFORT: Qos
IDLE_ACTION_UNSPECIFIED: IdleAction
IDLE_ACTION_PAUSE: IdleAction
IDLE_ACTION_STOP: IdleAction
CELL_STATE_UNSPECIFIED: CellState
CELL_STATE_PENDING: CellState
CELL_STATE_PREPARING: CellState
CELL_STATE_STARTING: CellState
CELL_STATE_RUNNING: CellState
CELL_STATE_PAUSING: CellState
CELL_STATE_PAUSED: CellState
CELL_STATE_STOPPING: CellState
CELL_STATE_STOPPED: CellState
CELL_STATE_FAILED: CellState
CELL_STATE_EXPIRED: CellState
CAUSE_UNSPECIFIED: Cause
CAUSE_REQUESTED: Cause
CAUSE_IDLE: Cause
CAUSE_HARD_TTL: Cause
CAUSE_EXITED: Cause
CAUSE_OOM: Cause
CAUSE_POLICY: Cause
CAUSE_START_FAILED: Cause
CAUSE_DRONE_LOST: Cause
CAUSE_NODE_LOST: Cause
CAUSE_RECOVERY: Cause

class Resources(_message.Message):
    __slots__ = ("vcpu_milli", "mem_mib", "disk_gib", "pids", "open_files")
    VCPU_MILLI_FIELD_NUMBER: _ClassVar[int]
    MEM_MIB_FIELD_NUMBER: _ClassVar[int]
    DISK_GIB_FIELD_NUMBER: _ClassVar[int]
    PIDS_FIELD_NUMBER: _ClassVar[int]
    OPEN_FILES_FIELD_NUMBER: _ClassVar[int]
    vcpu_milli: int
    mem_mib: int
    disk_gib: int
    pids: int
    open_files: int
    def __init__(self, vcpu_milli: _Optional[int] = ..., mem_mib: _Optional[int] = ..., disk_gib: _Optional[int] = ..., pids: _Optional[int] = ..., open_files: _Optional[int] = ...) -> None: ...

class Limits(_message.Message):
    __slots__ = ("output_bytes", "wall_time")
    OUTPUT_BYTES_FIELD_NUMBER: _ClassVar[int]
    WALL_TIME_FIELD_NUMBER: _ClassVar[int]
    output_bytes: int
    wall_time: _duration_pb2.Duration
    def __init__(self, output_bytes: _Optional[int] = ..., wall_time: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ...) -> None: ...

class ImageRef(_message.Message):
    __slots__ = ("ref",)
    REF_FIELD_NUMBER: _ClassVar[int]
    ref: str
    def __init__(self, ref: _Optional[str] = ...) -> None: ...

class SnapshotRef(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class CheckpointPolicy(_message.Message):
    __slots__ = ("none", "every", "on_session_idle")
    NONE_FIELD_NUMBER: _ClassVar[int]
    EVERY_FIELD_NUMBER: _ClassVar[int]
    ON_SESSION_IDLE_FIELD_NUMBER: _ClassVar[int]
    none: bool
    every: _duration_pb2.Duration
    on_session_idle: bool
    def __init__(self, none: _Optional[bool] = ..., every: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., on_session_idle: _Optional[bool] = ...) -> None: ...

class CellSpec(_message.Message):
    __slots__ = ("template", "image", "snapshot", "backend", "resources", "qos", "network_profile", "idle_ttl", "idle_action", "hard_ttl", "labels", "env", "limits", "trusted_image", "checkpoint", "burst_until_ready")
    class LabelsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    class EnvEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    TEMPLATE_FIELD_NUMBER: _ClassVar[int]
    IMAGE_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    RESOURCES_FIELD_NUMBER: _ClassVar[int]
    QOS_FIELD_NUMBER: _ClassVar[int]
    NETWORK_PROFILE_FIELD_NUMBER: _ClassVar[int]
    IDLE_TTL_FIELD_NUMBER: _ClassVar[int]
    IDLE_ACTION_FIELD_NUMBER: _ClassVar[int]
    HARD_TTL_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    ENV_FIELD_NUMBER: _ClassVar[int]
    LIMITS_FIELD_NUMBER: _ClassVar[int]
    TRUSTED_IMAGE_FIELD_NUMBER: _ClassVar[int]
    CHECKPOINT_FIELD_NUMBER: _ClassVar[int]
    BURST_UNTIL_READY_FIELD_NUMBER: _ClassVar[int]
    template: str
    image: ImageRef
    snapshot: SnapshotRef
    backend: Backend
    resources: Resources
    qos: Qos
    network_profile: str
    idle_ttl: _duration_pb2.Duration
    idle_action: IdleAction
    hard_ttl: _duration_pb2.Duration
    labels: _containers.ScalarMap[str, str]
    env: _containers.ScalarMap[str, str]
    limits: Limits
    trusted_image: bool
    checkpoint: CheckpointPolicy
    burst_until_ready: _duration_pb2.Duration
    def __init__(self, template: _Optional[str] = ..., image: _Optional[_Union[ImageRef, _Mapping]] = ..., snapshot: _Optional[_Union[SnapshotRef, _Mapping]] = ..., backend: _Optional[_Union[Backend, str]] = ..., resources: _Optional[_Union[Resources, _Mapping]] = ..., qos: _Optional[_Union[Qos, str]] = ..., network_profile: _Optional[str] = ..., idle_ttl: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., idle_action: _Optional[_Union[IdleAction, str]] = ..., hard_ttl: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., labels: _Optional[_Mapping[str, str]] = ..., env: _Optional[_Mapping[str, str]] = ..., limits: _Optional[_Union[Limits, _Mapping]] = ..., trusted_image: _Optional[bool] = ..., checkpoint: _Optional[_Union[CheckpointPolicy, _Mapping]] = ..., burst_until_ready: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ...) -> None: ...

class Cell(_message.Message):
    __slots__ = ("id", "project", "state", "cause", "backend", "spec", "node", "created_at", "state_since", "expires_at", "labels", "quarantined")
    class LabelsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    CAUSE_FIELD_NUMBER: _ClassVar[int]
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    SPEC_FIELD_NUMBER: _ClassVar[int]
    NODE_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    STATE_SINCE_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    QUARANTINED_FIELD_NUMBER: _ClassVar[int]
    id: str
    project: str
    state: CellState
    cause: Cause
    backend: Backend
    spec: CellSpec
    node: str
    created_at: _timestamp_pb2.Timestamp
    state_since: _timestamp_pb2.Timestamp
    expires_at: _timestamp_pb2.Timestamp
    labels: _containers.ScalarMap[str, str]
    quarantined: bool
    def __init__(self, id: _Optional[str] = ..., project: _Optional[str] = ..., state: _Optional[_Union[CellState, str]] = ..., cause: _Optional[_Union[Cause, str]] = ..., backend: _Optional[_Union[Backend, str]] = ..., spec: _Optional[_Union[CellSpec, _Mapping]] = ..., node: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., state_since: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., labels: _Optional[_Mapping[str, str]] = ..., quarantined: _Optional[bool] = ...) -> None: ...

class Error(_message.Message):
    __slots__ = ("reason", "message", "is_infra_error", "retryable", "errno")
    REASON_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    IS_INFRA_ERROR_FIELD_NUMBER: _ClassVar[int]
    RETRYABLE_FIELD_NUMBER: _ClassVar[int]
    ERRNO_FIELD_NUMBER: _ClassVar[int]
    reason: str
    message: str
    is_infra_error: bool
    retryable: bool
    errno: str
    def __init__(self, reason: _Optional[str] = ..., message: _Optional[str] = ..., is_infra_error: _Optional[bool] = ..., retryable: _Optional[bool] = ..., errno: _Optional[str] = ...) -> None: ...

class CellSelector(_message.Message):
    __slots__ = ("id", "labels")
    ID_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    id: str
    labels: LabelSelector
    def __init__(self, id: _Optional[str] = ..., labels: _Optional[_Union[LabelSelector, _Mapping]] = ...) -> None: ...

class LabelSelector(_message.Message):
    __slots__ = ("match",)
    class MatchEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    MATCH_FIELD_NUMBER: _ClassVar[int]
    match: _containers.ScalarMap[str, str]
    def __init__(self, match: _Optional[_Mapping[str, str]] = ...) -> None: ...

class BulkResult(_message.Message):
    __slots__ = ("matched", "succeeded", "failures")
    MATCHED_FIELD_NUMBER: _ClassVar[int]
    SUCCEEDED_FIELD_NUMBER: _ClassVar[int]
    FAILURES_FIELD_NUMBER: _ClassVar[int]
    matched: int
    succeeded: int
    failures: _containers.RepeatedCompositeFieldContainer[BulkFailure]
    def __init__(self, matched: _Optional[int] = ..., succeeded: _Optional[int] = ..., failures: _Optional[_Iterable[_Union[BulkFailure, _Mapping]]] = ...) -> None: ...

class BulkFailure(_message.Message):
    __slots__ = ("cell_id", "error")
    CELL_ID_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    cell_id: str
    error: Error
    def __init__(self, cell_id: _Optional[str] = ..., error: _Optional[_Union[Error, _Mapping]] = ...) -> None: ...

class ResourceUsage(_message.Message):
    __slots__ = ("cpu_user", "cpu_system", "peak_rss_bytes", "io_read_bytes", "io_write_bytes")
    CPU_USER_FIELD_NUMBER: _ClassVar[int]
    CPU_SYSTEM_FIELD_NUMBER: _ClassVar[int]
    PEAK_RSS_BYTES_FIELD_NUMBER: _ClassVar[int]
    IO_READ_BYTES_FIELD_NUMBER: _ClassVar[int]
    IO_WRITE_BYTES_FIELD_NUMBER: _ClassVar[int]
    cpu_user: _duration_pb2.Duration
    cpu_system: _duration_pb2.Duration
    peak_rss_bytes: int
    io_read_bytes: int
    io_write_bytes: int
    def __init__(self, cpu_user: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., cpu_system: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., peak_rss_bytes: _Optional[int] = ..., io_read_bytes: _Optional[int] = ..., io_write_bytes: _Optional[int] = ...) -> None: ...

class Empty(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
