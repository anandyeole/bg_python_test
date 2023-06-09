# @generated by generate_proto_mypy_stubs.py.  Do not edit!
import sys
from google.protobuf.any_pb2 import (
    Any as google___protobuf___any_pb2___Any,
)

from google.protobuf.descriptor import (
    Descriptor as google___protobuf___descriptor___Descriptor,
    FileDescriptor as google___protobuf___descriptor___FileDescriptor,
)

from google.protobuf.field_mask_pb2 import (
    FieldMask as google___protobuf___field_mask_pb2___FieldMask,
)

from google.protobuf.message import (
    Message as google___protobuf___message___Message,
)

from buildgrid._protos.google.rpc.status_pb2 import (
    Status as google___rpc___status_pb2___Status,
)

from typing import (
    Mapping as typing___Mapping,
    MutableMapping as typing___MutableMapping,
    Optional as typing___Optional,
    Text as typing___Text,
)

from typing_extensions import (
    Literal as typing_extensions___Literal,
)


builtin___bool = bool
builtin___bytes = bytes
builtin___float = float
builtin___int = int


DESCRIPTOR: google___protobuf___descriptor___FileDescriptor = ...

class Task(google___protobuf___message___Message):
    DESCRIPTOR: google___protobuf___descriptor___Descriptor = ...
    class LogsEntry(google___protobuf___message___Message):
        DESCRIPTOR: google___protobuf___descriptor___Descriptor = ...
        key: typing___Text = ...
        value: typing___Text = ...

        def __init__(self,
            *,
            key : typing___Optional[typing___Text] = None,
            value : typing___Optional[typing___Text] = None,
            ) -> None: ...
        def ClearField(self, field_name: typing_extensions___Literal[u"key",b"key",u"value",b"value"]) -> None: ...
    type___LogsEntry = LogsEntry

    name: typing___Text = ...

    @property
    def description(self) -> google___protobuf___any_pb2___Any: ...

    @property
    def logs(self) -> typing___MutableMapping[typing___Text, typing___Text]: ...

    def __init__(self,
        *,
        name : typing___Optional[typing___Text] = None,
        description : typing___Optional[google___protobuf___any_pb2___Any] = None,
        logs : typing___Optional[typing___Mapping[typing___Text, typing___Text]] = None,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions___Literal[u"description",b"description"]) -> builtin___bool: ...
    def ClearField(self, field_name: typing_extensions___Literal[u"description",b"description",u"logs",b"logs",u"name",b"name"]) -> None: ...
type___Task = Task

class TaskResult(google___protobuf___message___Message):
    DESCRIPTOR: google___protobuf___descriptor___Descriptor = ...
    name: typing___Text = ...
    complete: builtin___bool = ...

    @property
    def status(self) -> google___rpc___status_pb2___Status: ...

    @property
    def output(self) -> google___protobuf___any_pb2___Any: ...

    @property
    def meta(self) -> google___protobuf___any_pb2___Any: ...

    def __init__(self,
        *,
        name : typing___Optional[typing___Text] = None,
        complete : typing___Optional[builtin___bool] = None,
        status : typing___Optional[google___rpc___status_pb2___Status] = None,
        output : typing___Optional[google___protobuf___any_pb2___Any] = None,
        meta : typing___Optional[google___protobuf___any_pb2___Any] = None,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions___Literal[u"meta",b"meta",u"output",b"output",u"status",b"status"]) -> builtin___bool: ...
    def ClearField(self, field_name: typing_extensions___Literal[u"complete",b"complete",u"meta",b"meta",u"name",b"name",u"output",b"output",u"status",b"status"]) -> None: ...
type___TaskResult = TaskResult

class GetTaskRequest(google___protobuf___message___Message):
    DESCRIPTOR: google___protobuf___descriptor___Descriptor = ...
    name: typing___Text = ...

    def __init__(self,
        *,
        name : typing___Optional[typing___Text] = None,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions___Literal[u"name",b"name"]) -> None: ...
type___GetTaskRequest = GetTaskRequest

class UpdateTaskResultRequest(google___protobuf___message___Message):
    DESCRIPTOR: google___protobuf___descriptor___Descriptor = ...
    name: typing___Text = ...
    source: typing___Text = ...

    @property
    def result(self) -> type___TaskResult: ...

    @property
    def update_mask(self) -> google___protobuf___field_mask_pb2___FieldMask: ...

    def __init__(self,
        *,
        name : typing___Optional[typing___Text] = None,
        result : typing___Optional[type___TaskResult] = None,
        update_mask : typing___Optional[google___protobuf___field_mask_pb2___FieldMask] = None,
        source : typing___Optional[typing___Text] = None,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions___Literal[u"result",b"result",u"update_mask",b"update_mask"]) -> builtin___bool: ...
    def ClearField(self, field_name: typing_extensions___Literal[u"name",b"name",u"result",b"result",u"source",b"source",u"update_mask",b"update_mask"]) -> None: ...
type___UpdateTaskResultRequest = UpdateTaskResultRequest

class AddTaskLogRequest(google___protobuf___message___Message):
    DESCRIPTOR: google___protobuf___descriptor___Descriptor = ...
    name: typing___Text = ...
    log_id: typing___Text = ...

    def __init__(self,
        *,
        name : typing___Optional[typing___Text] = None,
        log_id : typing___Optional[typing___Text] = None,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions___Literal[u"log_id",b"log_id",u"name",b"name"]) -> None: ...
type___AddTaskLogRequest = AddTaskLogRequest

class AddTaskLogResponse(google___protobuf___message___Message):
    DESCRIPTOR: google___protobuf___descriptor___Descriptor = ...
    handle: typing___Text = ...

    def __init__(self,
        *,
        handle : typing___Optional[typing___Text] = None,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions___Literal[u"handle",b"handle"]) -> None: ...
type___AddTaskLogResponse = AddTaskLogResponse
