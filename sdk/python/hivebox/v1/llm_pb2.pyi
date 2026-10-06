import datetime

from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from hivebox.v1 import types_pb2 as _types_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class LlmRoute(_message.Message):
    __slots__ = ("upstream", "api_key")
    UPSTREAM_FIELD_NUMBER: _ClassVar[int]
    API_KEY_FIELD_NUMBER: _ClassVar[int]
    upstream: str
    api_key: str
    def __init__(self, upstream: _Optional[str] = ..., api_key: _Optional[str] = ...) -> None: ...

class LlmHoldRequest(_message.Message):
    __slots__ = ("release", "retry_after", "ttl", "drain")
    RELEASE_FIELD_NUMBER: _ClassVar[int]
    RETRY_AFTER_FIELD_NUMBER: _ClassVar[int]
    TTL_FIELD_NUMBER: _ClassVar[int]
    DRAIN_FIELD_NUMBER: _ClassVar[int]
    release: bool
    retry_after: _duration_pb2.Duration
    ttl: _duration_pb2.Duration
    drain: _duration_pb2.Duration
    def __init__(self, release: _Optional[bool] = ..., retry_after: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., ttl: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., drain: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ...) -> None: ...

class LlmHoldResult(_message.Message):
    __slots__ = ("in_flight",)
    IN_FLIGHT_FIELD_NUMBER: _ClassVar[int]
    in_flight: int
    def __init__(self, in_flight: _Optional[int] = ...) -> None: ...

class LlmTurnsRequest(_message.Message):
    __slots__ = ("rollout_id", "cell_id", "take")
    ROLLOUT_ID_FIELD_NUMBER: _ClassVar[int]
    CELL_ID_FIELD_NUMBER: _ClassVar[int]
    TAKE_FIELD_NUMBER: _ClassVar[int]
    rollout_id: str
    cell_id: str
    take: bool
    def __init__(self, rollout_id: _Optional[str] = ..., cell_id: _Optional[str] = ..., take: _Optional[bool] = ...) -> None: ...

class LlmTurnsResponse(_message.Message):
    __slots__ = ("turns", "dropped", "unreached")
    TURNS_FIELD_NUMBER: _ClassVar[int]
    DROPPED_FIELD_NUMBER: _ClassVar[int]
    UNREACHED_FIELD_NUMBER: _ClassVar[int]
    turns: _containers.RepeatedCompositeFieldContainer[LlmTurn]
    dropped: int
    unreached: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, turns: _Optional[_Iterable[_Union[LlmTurn, _Mapping]]] = ..., dropped: _Optional[int] = ..., unreached: _Optional[_Iterable[int]] = ...) -> None: ...

class LlmTurn(_message.Message):
    __slots__ = ("cell_id", "rollout_id", "seq", "path", "model", "status", "stream", "started", "took", "prompt_ids", "choices", "prompt_tokens", "completion_tokens", "error")
    CELL_ID_FIELD_NUMBER: _ClassVar[int]
    ROLLOUT_ID_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    STREAM_FIELD_NUMBER: _ClassVar[int]
    STARTED_FIELD_NUMBER: _ClassVar[int]
    TOOK_FIELD_NUMBER: _ClassVar[int]
    PROMPT_IDS_FIELD_NUMBER: _ClassVar[int]
    CHOICES_FIELD_NUMBER: _ClassVar[int]
    PROMPT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    COMPLETION_TOKENS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    cell_id: str
    rollout_id: str
    seq: int
    path: str
    model: str
    status: int
    stream: bool
    started: _timestamp_pb2.Timestamp
    took: _duration_pb2.Duration
    prompt_ids: _containers.RepeatedScalarFieldContainer[int]
    choices: _containers.RepeatedCompositeFieldContainer[LlmChoice]
    prompt_tokens: int
    completion_tokens: int
    error: str
    def __init__(self, cell_id: _Optional[str] = ..., rollout_id: _Optional[str] = ..., seq: _Optional[int] = ..., path: _Optional[str] = ..., model: _Optional[str] = ..., status: _Optional[int] = ..., stream: _Optional[bool] = ..., started: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., took: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., prompt_ids: _Optional[_Iterable[int]] = ..., choices: _Optional[_Iterable[_Union[LlmChoice, _Mapping]]] = ..., prompt_tokens: _Optional[int] = ..., completion_tokens: _Optional[int] = ..., error: _Optional[str] = ...) -> None: ...

class LlmChoice(_message.Message):
    __slots__ = ("index", "output_ids", "logprobs", "finish_reason", "prompt_ids")
    INDEX_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_IDS_FIELD_NUMBER: _ClassVar[int]
    LOGPROBS_FIELD_NUMBER: _ClassVar[int]
    FINISH_REASON_FIELD_NUMBER: _ClassVar[int]
    PROMPT_IDS_FIELD_NUMBER: _ClassVar[int]
    index: int
    output_ids: _containers.RepeatedScalarFieldContainer[int]
    logprobs: _containers.RepeatedScalarFieldContainer[float]
    finish_reason: str
    prompt_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, index: _Optional[int] = ..., output_ids: _Optional[_Iterable[int]] = ..., logprobs: _Optional[_Iterable[float]] = ..., finish_reason: _Optional[str] = ..., prompt_ids: _Optional[_Iterable[int]] = ...) -> None: ...
