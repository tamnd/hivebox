import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from hivebox.v1 import types_pb2 as _types_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ImportRequest(_message.Message):
    __slots__ = ("oci_ref", "tar", "dockerfile", "name")
    OCI_REF_FIELD_NUMBER: _ClassVar[int]
    TAR_FIELD_NUMBER: _ClassVar[int]
    DOCKERFILE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    oci_ref: str
    tar: bytes
    dockerfile: str
    name: str
    def __init__(self, oci_ref: _Optional[str] = ..., tar: _Optional[bytes] = ..., dockerfile: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...

class BuildEvent(_message.Message):
    __slots__ = ("log", "done", "error")
    LOG_FIELD_NUMBER: _ClassVar[int]
    DONE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    log: str
    done: ImageManifest
    error: _types_pb2.Error
    def __init__(self, log: _Optional[str] = ..., done: _Optional[_Union[ImageManifest, _Mapping]] = ..., error: _Optional[_Union[_types_pb2.Error, _Mapping]] = ...) -> None: ...

class ImageManifest(_message.Message):
    __slots__ = ("ref", "digest", "size_bytes", "layers", "created_at")
    REF_FIELD_NUMBER: _ClassVar[int]
    DIGEST_FIELD_NUMBER: _ClassVar[int]
    SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    LAYERS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    ref: _types_pb2.ImageRef
    digest: str
    size_bytes: int
    layers: _containers.RepeatedScalarFieldContainer[str]
    created_at: _timestamp_pb2.Timestamp
    def __init__(self, ref: _Optional[_Union[_types_pb2.ImageRef, _Mapping]] = ..., digest: _Optional[str] = ..., size_bytes: _Optional[int] = ..., layers: _Optional[_Iterable[str]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ComposeRequest(_message.Message):
    __slots__ = ("base", "overlays", "name")
    BASE_FIELD_NUMBER: _ClassVar[int]
    OVERLAYS_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    base: _types_pb2.ImageRef
    overlays: _containers.RepeatedCompositeFieldContainer[_types_pb2.ImageRef]
    name: str
    def __init__(self, base: _Optional[_Union[_types_pb2.ImageRef, _Mapping]] = ..., overlays: _Optional[_Iterable[_Union[_types_pb2.ImageRef, _Mapping]]] = ..., name: _Optional[str] = ...) -> None: ...

class PrefetchRequest(_message.Message):
    __slots__ = ("images", "nodes")
    IMAGES_FIELD_NUMBER: _ClassVar[int]
    NODES_FIELD_NUMBER: _ClassVar[int]
    images: _containers.RepeatedCompositeFieldContainer[_types_pb2.ImageRef]
    nodes: int
    def __init__(self, images: _Optional[_Iterable[_Union[_types_pb2.ImageRef, _Mapping]]] = ..., nodes: _Optional[int] = ...) -> None: ...
