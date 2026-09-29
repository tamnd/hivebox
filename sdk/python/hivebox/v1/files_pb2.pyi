import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from hivebox.v1 import types_pb2 as _types_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class FileType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FILE_TYPE_UNSPECIFIED: _ClassVar[FileType]
    FILE_TYPE_FILE: _ClassVar[FileType]
    FILE_TYPE_DIR: _ClassVar[FileType]
    FILE_TYPE_SYMLINK: _ClassVar[FileType]
    FILE_TYPE_OTHER: _ClassVar[FileType]

class FsEventKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FS_EVENT_KIND_UNSPECIFIED: _ClassVar[FsEventKind]
    FS_EVENT_KIND_CREATE: _ClassVar[FsEventKind]
    FS_EVENT_KIND_WRITE: _ClassVar[FsEventKind]
    FS_EVENT_KIND_REMOVE: _ClassVar[FsEventKind]
    FS_EVENT_KIND_RENAME: _ClassVar[FsEventKind]
    FS_EVENT_KIND_CHMOD: _ClassVar[FsEventKind]
    FS_EVENT_KIND_OVERFLOW: _ClassVar[FsEventKind]

class DiffMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DIFF_MODE_UNSPECIFIED: _ClassVar[DiffMode]
    DIFF_MODE_GIT: _ClassVar[DiffMode]
    DIFF_MODE_LAYER: _ClassVar[DiffMode]
FILE_TYPE_UNSPECIFIED: FileType
FILE_TYPE_FILE: FileType
FILE_TYPE_DIR: FileType
FILE_TYPE_SYMLINK: FileType
FILE_TYPE_OTHER: FileType
FS_EVENT_KIND_UNSPECIFIED: FsEventKind
FS_EVENT_KIND_CREATE: FsEventKind
FS_EVENT_KIND_WRITE: FsEventKind
FS_EVENT_KIND_REMOVE: FsEventKind
FS_EVENT_KIND_RENAME: FsEventKind
FS_EVENT_KIND_CHMOD: FsEventKind
FS_EVENT_KIND_OVERFLOW: FsEventKind
DIFF_MODE_UNSPECIFIED: DiffMode
DIFF_MODE_GIT: DiffMode
DIFF_MODE_LAYER: DiffMode

class PathRequest(_message.Message):
    __slots__ = ("cell_id", "path", "recursive")
    CELL_ID_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    RECURSIVE_FIELD_NUMBER: _ClassVar[int]
    cell_id: str
    path: str
    recursive: bool
    def __init__(self, cell_id: _Optional[str] = ..., path: _Optional[str] = ..., recursive: _Optional[bool] = ...) -> None: ...

class ReadFileRequest(_message.Message):
    __slots__ = ("cell_id", "path", "offset", "length")
    CELL_ID_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    LENGTH_FIELD_NUMBER: _ClassVar[int]
    cell_id: str
    path: str
    offset: int
    length: int
    def __init__(self, cell_id: _Optional[str] = ..., path: _Optional[str] = ..., offset: _Optional[int] = ..., length: _Optional[int] = ...) -> None: ...

class Chunk(_message.Message):
    __slots__ = ("data",)
    DATA_FIELD_NUMBER: _ClassVar[int]
    data: bytes
    def __init__(self, data: _Optional[bytes] = ...) -> None: ...

class WriteFileHeader(_message.Message):
    __slots__ = ("cell_id", "path", "mode", "make_parents", "append")
    CELL_ID_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    MAKE_PARENTS_FIELD_NUMBER: _ClassVar[int]
    APPEND_FIELD_NUMBER: _ClassVar[int]
    cell_id: str
    path: str
    mode: int
    make_parents: bool
    append: bool
    def __init__(self, cell_id: _Optional[str] = ..., path: _Optional[str] = ..., mode: _Optional[int] = ..., make_parents: _Optional[bool] = ..., append: _Optional[bool] = ...) -> None: ...

class WriteFileChunk(_message.Message):
    __slots__ = ("header", "data")
    HEADER_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    header: WriteFileHeader
    data: bytes
    def __init__(self, header: _Optional[_Union[WriteFileHeader, _Mapping]] = ..., data: _Optional[bytes] = ...) -> None: ...

class FileInfo(_message.Message):
    __slots__ = ("path", "type", "size", "mode", "modified_at", "symlink_target")
    PATH_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    MODIFIED_AT_FIELD_NUMBER: _ClassVar[int]
    SYMLINK_TARGET_FIELD_NUMBER: _ClassVar[int]
    path: str
    type: FileType
    size: int
    mode: int
    modified_at: _timestamp_pb2.Timestamp
    symlink_target: str
    def __init__(self, path: _Optional[str] = ..., type: _Optional[_Union[FileType, str]] = ..., size: _Optional[int] = ..., mode: _Optional[int] = ..., modified_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., symlink_target: _Optional[str] = ...) -> None: ...

class ListDirRequest(_message.Message):
    __slots__ = ("cell_id", "path", "depth")
    CELL_ID_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    DEPTH_FIELD_NUMBER: _ClassVar[int]
    cell_id: str
    path: str
    depth: int
    def __init__(self, cell_id: _Optional[str] = ..., path: _Optional[str] = ..., depth: _Optional[int] = ...) -> None: ...

class ListDirResponse(_message.Message):
    __slots__ = ("entries", "truncated")
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    TRUNCATED_FIELD_NUMBER: _ClassVar[int]
    entries: _containers.RepeatedCompositeFieldContainer[FileInfo]
    truncated: bool
    def __init__(self, entries: _Optional[_Iterable[_Union[FileInfo, _Mapping]]] = ..., truncated: _Optional[bool] = ...) -> None: ...

class WatchDirRequest(_message.Message):
    __slots__ = ("cell_id", "path", "recursive")
    CELL_ID_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    RECURSIVE_FIELD_NUMBER: _ClassVar[int]
    cell_id: str
    path: str
    recursive: bool
    def __init__(self, cell_id: _Optional[str] = ..., path: _Optional[str] = ..., recursive: _Optional[bool] = ...) -> None: ...

class FsEvent(_message.Message):
    __slots__ = ("kind", "path")
    KIND_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    kind: FsEventKind
    path: str
    def __init__(self, kind: _Optional[_Union[FsEventKind, str]] = ..., path: _Optional[str] = ...) -> None: ...

class DiffRequest(_message.Message):
    __slots__ = ("cell_id", "mode", "path")
    CELL_ID_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    cell_id: str
    mode: DiffMode
    path: str
    def __init__(self, cell_id: _Optional[str] = ..., mode: _Optional[_Union[DiffMode, str]] = ..., path: _Optional[str] = ...) -> None: ...

class DiffResult(_message.Message):
    __slots__ = ("patch", "files_changed")
    PATCH_FIELD_NUMBER: _ClassVar[int]
    FILES_CHANGED_FIELD_NUMBER: _ClassVar[int]
    patch: bytes
    files_changed: int
    def __init__(self, patch: _Optional[bytes] = ..., files_changed: _Optional[int] = ...) -> None: ...

class ApplyRequest(_message.Message):
    __slots__ = ("cell_id", "path", "patch", "tar")
    CELL_ID_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    PATCH_FIELD_NUMBER: _ClassVar[int]
    TAR_FIELD_NUMBER: _ClassVar[int]
    cell_id: str
    path: str
    patch: bytes
    tar: bytes
    def __init__(self, cell_id: _Optional[str] = ..., path: _Optional[str] = ..., patch: _Optional[bytes] = ..., tar: _Optional[bytes] = ...) -> None: ...
