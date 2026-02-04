from protobuf.opencb import variant_pb2 as _variant_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Request(_message.Message):
    __slots__ = ("token", "ip", "query")
    class QueryEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    IP_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    token: str
    ip: str
    query: _containers.ScalarMap[str, str]
    def __init__(self, token: _Optional[str] = ..., ip: _Optional[str] = ..., query: _Optional[_Mapping[str, str]] = ...) -> None: ...

class Event(_message.Message):
    __slots__ = ("type", "code", "id", "name", "message")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    CODE_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    type: str
    code: int
    id: str
    name: str
    message: str
    def __init__(self, type: _Optional[str] = ..., code: _Optional[int] = ..., id: _Optional[str] = ..., name: _Optional[str] = ..., message: _Optional[str] = ...) -> None: ...

class VariantResponse(_message.Message):
    __slots__ = ("variant", "event", "count", "error", "errorFull", "stackTrace")
    VARIANT_FIELD_NUMBER: _ClassVar[int]
    EVENT_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    ERRORFULL_FIELD_NUMBER: _ClassVar[int]
    STACKTRACE_FIELD_NUMBER: _ClassVar[int]
    variant: _variant_pb2.Variant
    event: _containers.RepeatedCompositeFieldContainer[Event]
    count: int
    error: str
    errorFull: str
    stackTrace: str
    def __init__(self, variant: _Optional[_Union[_variant_pb2.Variant, _Mapping]] = ..., event: _Optional[_Iterable[_Union[Event, _Mapping]]] = ..., count: _Optional[int] = ..., error: _Optional[str] = ..., errorFull: _Optional[str] = ..., stackTrace: _Optional[str] = ...) -> None: ...

class MapResponse(_message.Message):
    __slots__ = ("values", "event", "count", "error", "errorFull", "stackTrace")
    class ValuesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    VALUES_FIELD_NUMBER: _ClassVar[int]
    EVENT_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    ERRORFULL_FIELD_NUMBER: _ClassVar[int]
    STACKTRACE_FIELD_NUMBER: _ClassVar[int]
    values: _containers.ScalarMap[str, str]
    event: _containers.RepeatedCompositeFieldContainer[Event]
    count: int
    error: str
    errorFull: str
    stackTrace: str
    def __init__(self, values: _Optional[_Mapping[str, str]] = ..., event: _Optional[_Iterable[_Union[Event, _Mapping]]] = ..., count: _Optional[int] = ..., error: _Optional[str] = ..., errorFull: _Optional[str] = ..., stackTrace: _Optional[str] = ...) -> None: ...
