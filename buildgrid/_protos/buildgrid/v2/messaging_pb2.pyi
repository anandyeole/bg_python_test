"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2
import builtins
import buildgrid._protos.google.longrunning.operations_pb2
import google.protobuf.descriptor
import google.protobuf.internal.containers
import google.protobuf.message
import google.protobuf.timestamp_pb2
import typing
import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor = ...

class Job(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor = ...
    JOB_ID_FIELD_NUMBER: builtins.int
    ACTION_FIELD_NUMBER: builtins.int
    job_id: typing.Text = ...

    @property
    def action(self) -> buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2.Action: ...

    def __init__(self,
        *,
        job_id : typing.Text = ...,
        action : typing.Optional[buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2.Action] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal[u"action",b"action"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal[u"action",b"action",u"job_id",b"job_id"]) -> None: ...
global___Job = Job

class CreateOperation(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor = ...
    JOB_ID_FIELD_NUMBER: builtins.int
    OPERATION_NAME_FIELD_NUMBER: builtins.int
    ACTION_DIGEST_FIELD_NUMBER: builtins.int
    CACHEABLE_FIELD_NUMBER: builtins.int
    TIMESTAMP_FIELD_NUMBER: builtins.int
    REQUEST_METADATA_FIELD_NUMBER: builtins.int
    job_id: typing.Text = ...
    operation_name: typing.Text = ...
    cacheable: builtins.bool = ...

    @property
    def action_digest(self) -> buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2.Digest: ...

    @property
    def timestamp(self) -> google.protobuf.timestamp_pb2.Timestamp: ...

    @property
    def request_metadata(self) -> buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2.RequestMetadata: ...

    def __init__(self,
        *,
        job_id : typing.Text = ...,
        operation_name : typing.Text = ...,
        action_digest : typing.Optional[buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2.Digest] = ...,
        cacheable : builtins.bool = ...,
        timestamp : typing.Optional[google.protobuf.timestamp_pb2.Timestamp] = ...,
        request_metadata : typing.Optional[buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2.RequestMetadata] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal[u"action_digest",b"action_digest",u"request_metadata",b"request_metadata",u"timestamp",b"timestamp"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal[u"action_digest",b"action_digest",u"cacheable",b"cacheable",u"job_id",b"job_id",u"operation_name",b"operation_name",u"request_metadata",b"request_metadata",u"timestamp",b"timestamp"]) -> None: ...
global___CreateOperation = CreateOperation

class UpdateOperations(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor = ...
    JOB_ID_FIELD_NUMBER: builtins.int
    OPERATION_STATE_FIELD_NUMBER: builtins.int
    CACHEABLE_FIELD_NUMBER: builtins.int
    TIMESTAMP_FIELD_NUMBER: builtins.int
    job_id: typing.Text = ...
    cacheable: builtins.bool = ...

    @property
    def operation_state(self) -> buildgrid._protos.google.longrunning.operations_pb2.Operation: ...

    @property
    def timestamp(self) -> google.protobuf.timestamp_pb2.Timestamp: ...

    def __init__(self,
        *,
        job_id : typing.Text = ...,
        operation_state : typing.Optional[buildgrid._protos.google.longrunning.operations_pb2.Operation] = ...,
        cacheable : builtins.bool = ...,
        timestamp : typing.Optional[google.protobuf.timestamp_pb2.Timestamp] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal[u"operation_state",b"operation_state",u"timestamp",b"timestamp"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal[u"cacheable",b"cacheable",u"job_id",b"job_id",u"operation_state",b"operation_state",u"timestamp",b"timestamp"]) -> None: ...
global___UpdateOperations = UpdateOperations

class RetryableJob(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor = ...
    JOB_ID_FIELD_NUMBER: builtins.int
    ACTION_FIELD_NUMBER: builtins.int
    ROUTING_KEY_FIELD_NUMBER: builtins.int
    job_id: typing.Text = ...
    routing_key: typing.Text = ...

    @property
    def action(self) -> buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2.Action: ...

    def __init__(self,
        *,
        job_id : typing.Text = ...,
        action : typing.Optional[buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2.Action] = ...,
        routing_key : typing.Text = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal[u"action",b"action"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal[u"action",b"action",u"job_id",b"job_id",u"routing_key",b"routing_key"]) -> None: ...
global___RetryableJob = RetryableJob

class BotStatus(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor = ...
    BOT_NAME_FIELD_NUMBER: builtins.int
    ASSIGNMENTS_FIELD_NUMBER: builtins.int
    CONNECTION_TIMESTAMP_FIELD_NUMBER: builtins.int
    bot_name: typing.Text = ...

    @property
    def assignments(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___RetryableJob]: ...

    @property
    def connection_timestamp(self) -> google.protobuf.timestamp_pb2.Timestamp: ...

    def __init__(self,
        *,
        bot_name : typing.Text = ...,
        assignments : typing.Optional[typing.Iterable[global___RetryableJob]] = ...,
        connection_timestamp : typing.Optional[google.protobuf.timestamp_pb2.Timestamp] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal[u"connection_timestamp",b"connection_timestamp"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal[u"assignments",b"assignments",u"bot_name",b"bot_name",u"connection_timestamp",b"connection_timestamp"]) -> None: ...
global___BotStatus = BotStatus