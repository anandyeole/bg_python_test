"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import builtins
import google.protobuf.descriptor
import google.protobuf.internal.enum_type_wrapper
import typing

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor = ...

class FieldBehavior(metaclass=_FieldBehavior):
    V = typing.NewType('V', builtins.int)

global___FieldBehavior = FieldBehavior

FIELD_BEHAVIOR_UNSPECIFIED = FieldBehavior.V(0)
OPTIONAL = FieldBehavior.V(1)
REQUIRED = FieldBehavior.V(2)
OUTPUT_ONLY = FieldBehavior.V(3)
INPUT_ONLY = FieldBehavior.V(4)
IMMUTABLE = FieldBehavior.V(5)
UNORDERED_LIST = FieldBehavior.V(6)
NON_EMPTY_DEFAULT = FieldBehavior.V(7)

class _FieldBehavior(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[FieldBehavior.V], builtins.type):
    DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor = ...
    FIELD_BEHAVIOR_UNSPECIFIED = FieldBehavior.V(0)
    OPTIONAL = FieldBehavior.V(1)
    REQUIRED = FieldBehavior.V(2)
    OUTPUT_ONLY = FieldBehavior.V(3)
    INPUT_ONLY = FieldBehavior.V(4)
    IMMUTABLE = FieldBehavior.V(5)
    UNORDERED_LIST = FieldBehavior.V(6)
    NON_EMPTY_DEFAULT = FieldBehavior.V(7)

field_behavior: google.protobuf.descriptor.FieldDescriptor = ...
