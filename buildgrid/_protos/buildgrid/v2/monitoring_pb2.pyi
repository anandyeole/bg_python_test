# @generated by generate_proto_mypy_stubs.py.  Do not edit!
import sys
from google.protobuf.descriptor import (
    Descriptor as google___protobuf___descriptor___Descriptor,
    EnumDescriptor as google___protobuf___descriptor___EnumDescriptor,
    FileDescriptor as google___protobuf___descriptor___FileDescriptor,
)

from google.protobuf.duration_pb2 import (
    Duration as google___protobuf___duration_pb2___Duration,
)

from google.protobuf.internal.enum_type_wrapper import (
    _EnumTypeWrapper as google___protobuf___internal___enum_type_wrapper____EnumTypeWrapper,
)

from google.protobuf.message import (
    Message as google___protobuf___message___Message,
)

from google.protobuf.timestamp_pb2 import (
    Timestamp as google___protobuf___timestamp_pb2___Timestamp,
)

from typing import (
    Mapping as typing___Mapping,
    MutableMapping as typing___MutableMapping,
    NewType as typing___NewType,
    Optional as typing___Optional,
    Text as typing___Text,
    cast as typing___cast,
)

from typing_extensions import (
    Literal as typing_extensions___Literal,
)


builtin___bool = bool
builtin___bytes = bytes
builtin___float = float
builtin___int = int


DESCRIPTOR: google___protobuf___descriptor___FileDescriptor = ...

class BusMessage(google___protobuf___message___Message):
    DESCRIPTOR: google___protobuf___descriptor___Descriptor = ...
    sequence_number: builtin___int = ...

    @property
    def log_record(self) -> type___LogRecord: ...

    @property
    def metric_record(self) -> type___MetricRecord: ...

    def __init__(self,
        *,
        sequence_number : typing___Optional[builtin___int] = None,
        log_record : typing___Optional[type___LogRecord] = None,
        metric_record : typing___Optional[type___MetricRecord] = None,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions___Literal[u"log_record",b"log_record",u"metric_record",b"metric_record",u"record",b"record"]) -> builtin___bool: ...
    def ClearField(self, field_name: typing_extensions___Literal[u"log_record",b"log_record",u"metric_record",b"metric_record",u"record",b"record",u"sequence_number",b"sequence_number"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions___Literal[u"record",b"record"]) -> typing_extensions___Literal["log_record","metric_record"]: ...
type___BusMessage = BusMessage

class LogRecord(google___protobuf___message___Message):
    DESCRIPTOR: google___protobuf___descriptor___Descriptor = ...
    LevelValue = typing___NewType('LevelValue', builtin___int)
    type___LevelValue = LevelValue
    Level: _Level
    class _Level(google___protobuf___internal___enum_type_wrapper____EnumTypeWrapper[LogRecord.LevelValue]):
        DESCRIPTOR: google___protobuf___descriptor___EnumDescriptor = ...
        NOTSET = typing___cast(LogRecord.LevelValue, 0)
        DEBUG = typing___cast(LogRecord.LevelValue, 1)
        INFO = typing___cast(LogRecord.LevelValue, 2)
        WARNING = typing___cast(LogRecord.LevelValue, 3)
        ERROR = typing___cast(LogRecord.LevelValue, 4)
        CRITICAL = typing___cast(LogRecord.LevelValue, 5)
    NOTSET = typing___cast(LogRecord.LevelValue, 0)
    DEBUG = typing___cast(LogRecord.LevelValue, 1)
    INFO = typing___cast(LogRecord.LevelValue, 2)
    WARNING = typing___cast(LogRecord.LevelValue, 3)
    ERROR = typing___cast(LogRecord.LevelValue, 4)
    CRITICAL = typing___cast(LogRecord.LevelValue, 5)
    type___Level = Level

    class MetadataEntry(google___protobuf___message___Message):
        DESCRIPTOR: google___protobuf___descriptor___Descriptor = ...
        key: typing___Text = ...
        value: typing___Text = ...

        def __init__(self,
            *,
            key : typing___Optional[typing___Text] = None,
            value : typing___Optional[typing___Text] = None,
            ) -> None: ...
        def ClearField(self, field_name: typing_extensions___Literal[u"key",b"key",u"value",b"value"]) -> None: ...
    type___MetadataEntry = MetadataEntry

    domain: typing___Text = ...
    level: type___LogRecord.LevelValue = ...
    message: typing___Text = ...

    @property
    def creation_timestamp(self) -> google___protobuf___timestamp_pb2___Timestamp: ...

    @property
    def metadata(self) -> typing___MutableMapping[typing___Text, typing___Text]: ...

    def __init__(self,
        *,
        creation_timestamp : typing___Optional[google___protobuf___timestamp_pb2___Timestamp] = None,
        domain : typing___Optional[typing___Text] = None,
        level : typing___Optional[type___LogRecord.LevelValue] = None,
        message : typing___Optional[typing___Text] = None,
        metadata : typing___Optional[typing___Mapping[typing___Text, typing___Text]] = None,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions___Literal[u"creation_timestamp",b"creation_timestamp"]) -> builtin___bool: ...
    def ClearField(self, field_name: typing_extensions___Literal[u"creation_timestamp",b"creation_timestamp",u"domain",b"domain",u"level",b"level",u"message",b"message",u"metadata",b"metadata"]) -> None: ...
type___LogRecord = LogRecord

class MetricRecord(google___protobuf___message___Message):
    DESCRIPTOR: google___protobuf___descriptor___Descriptor = ...
    TypeValue = typing___NewType('TypeValue', builtin___int)
    type___TypeValue = TypeValue
    Type: _Type
    class _Type(google___protobuf___internal___enum_type_wrapper____EnumTypeWrapper[MetricRecord.TypeValue]):
        DESCRIPTOR: google___protobuf___descriptor___EnumDescriptor = ...
        NONE = typing___cast(MetricRecord.TypeValue, 0)
        COUNTER = typing___cast(MetricRecord.TypeValue, 1)
        TIMER = typing___cast(MetricRecord.TypeValue, 2)
        GAUGE = typing___cast(MetricRecord.TypeValue, 3)
        DISTRIBUTION = typing___cast(MetricRecord.TypeValue, 4)
    NONE = typing___cast(MetricRecord.TypeValue, 0)
    COUNTER = typing___cast(MetricRecord.TypeValue, 1)
    TIMER = typing___cast(MetricRecord.TypeValue, 2)
    GAUGE = typing___cast(MetricRecord.TypeValue, 3)
    DISTRIBUTION = typing___cast(MetricRecord.TypeValue, 4)
    type___Type = Type

    class MetadataEntry(google___protobuf___message___Message):
        DESCRIPTOR: google___protobuf___descriptor___Descriptor = ...
        key: typing___Text = ...
        value: typing___Text = ...

        def __init__(self,
            *,
            key : typing___Optional[typing___Text] = None,
            value : typing___Optional[typing___Text] = None,
            ) -> None: ...
        def ClearField(self, field_name: typing_extensions___Literal[u"key",b"key",u"value",b"value"]) -> None: ...
    type___MetadataEntry = MetadataEntry

    type: type___MetricRecord.TypeValue = ...
    name: typing___Text = ...
    count: builtin___float = ...
    value: builtin___float = ...

    @property
    def creation_timestamp(self) -> google___protobuf___timestamp_pb2___Timestamp: ...

    @property
    def duration(self) -> google___protobuf___duration_pb2___Duration: ...

    @property
    def metadata(self) -> typing___MutableMapping[typing___Text, typing___Text]: ...

    def __init__(self,
        *,
        creation_timestamp : typing___Optional[google___protobuf___timestamp_pb2___Timestamp] = None,
        type : typing___Optional[type___MetricRecord.TypeValue] = None,
        name : typing___Optional[typing___Text] = None,
        count : typing___Optional[builtin___float] = None,
        duration : typing___Optional[google___protobuf___duration_pb2___Duration] = None,
        value : typing___Optional[builtin___float] = None,
        metadata : typing___Optional[typing___Mapping[typing___Text, typing___Text]] = None,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions___Literal[u"count",b"count",u"creation_timestamp",b"creation_timestamp",u"data",b"data",u"duration",b"duration",u"value",b"value"]) -> builtin___bool: ...
    def ClearField(self, field_name: typing_extensions___Literal[u"count",b"count",u"creation_timestamp",b"creation_timestamp",u"data",b"data",u"duration",b"duration",u"metadata",b"metadata",u"name",b"name",u"type",b"type",u"value",b"value"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions___Literal[u"data",b"data"]) -> typing_extensions___Literal["count","duration","value"]: ...
type___MetricRecord = MetricRecord