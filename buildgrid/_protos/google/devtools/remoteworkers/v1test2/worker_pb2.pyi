# @generated by generate_proto_mypy_stubs.py.  Do not edit!
import sys
from google.protobuf.descriptor import (
    Descriptor as google___protobuf___descriptor___Descriptor,
    FileDescriptor as google___protobuf___descriptor___FileDescriptor,
)

from google.protobuf.internal.containers import (
    RepeatedCompositeFieldContainer as google___protobuf___internal___containers___RepeatedCompositeFieldContainer,
)

from google.protobuf.message import (
    Message as google___protobuf___message___Message,
)

from typing import (
    Iterable as typing___Iterable,
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

class Worker(google___protobuf___message___Message):
    DESCRIPTOR: google___protobuf___descriptor___Descriptor = ...
    class Property(google___protobuf___message___Message):
        DESCRIPTOR: google___protobuf___descriptor___Descriptor = ...
        key: typing___Text = ...
        value: typing___Text = ...

        def __init__(self,
            *,
            key : typing___Optional[typing___Text] = None,
            value : typing___Optional[typing___Text] = None,
            ) -> None: ...
        def ClearField(self, field_name: typing_extensions___Literal[u"key",b"key",u"value",b"value"]) -> None: ...
    type___Property = Property

    class Config(google___protobuf___message___Message):
        DESCRIPTOR: google___protobuf___descriptor___Descriptor = ...
        key: typing___Text = ...
        value: typing___Text = ...

        def __init__(self,
            *,
            key : typing___Optional[typing___Text] = None,
            value : typing___Optional[typing___Text] = None,
            ) -> None: ...
        def ClearField(self, field_name: typing_extensions___Literal[u"key",b"key",u"value",b"value"]) -> None: ...
    type___Config = Config


    @property
    def devices(self) -> google___protobuf___internal___containers___RepeatedCompositeFieldContainer[type___Device]: ...

    @property
    def properties(self) -> google___protobuf___internal___containers___RepeatedCompositeFieldContainer[type___Worker.Property]: ...

    @property
    def configs(self) -> google___protobuf___internal___containers___RepeatedCompositeFieldContainer[type___Worker.Config]: ...

    def __init__(self,
        *,
        devices : typing___Optional[typing___Iterable[type___Device]] = None,
        properties : typing___Optional[typing___Iterable[type___Worker.Property]] = None,
        configs : typing___Optional[typing___Iterable[type___Worker.Config]] = None,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions___Literal[u"configs",b"configs",u"devices",b"devices",u"properties",b"properties"]) -> None: ...
type___Worker = Worker

class Device(google___protobuf___message___Message):
    DESCRIPTOR: google___protobuf___descriptor___Descriptor = ...
    class Property(google___protobuf___message___Message):
        DESCRIPTOR: google___protobuf___descriptor___Descriptor = ...
        key: typing___Text = ...
        value: typing___Text = ...

        def __init__(self,
            *,
            key : typing___Optional[typing___Text] = None,
            value : typing___Optional[typing___Text] = None,
            ) -> None: ...
        def ClearField(self, field_name: typing_extensions___Literal[u"key",b"key",u"value",b"value"]) -> None: ...
    type___Property = Property

    handle: typing___Text = ...

    @property
    def properties(self) -> google___protobuf___internal___containers___RepeatedCompositeFieldContainer[type___Device.Property]: ...

    def __init__(self,
        *,
        handle : typing___Optional[typing___Text] = None,
        properties : typing___Optional[typing___Iterable[type___Device.Property]] = None,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions___Literal[u"handle",b"handle",u"properties",b"properties"]) -> None: ...
type___Device = Device