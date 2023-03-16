# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: google/devtools/build/v1/build_status.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import wrappers_pb2 as google_dot_protobuf_dot_wrappers__pb2
from google.protobuf import any_pb2 as google_dot_protobuf_dot_any__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='google/devtools/build/v1/build_status.proto',
  package='google.devtools.build.v1',
  syntax='proto3',
  serialized_options=b'\n\034com.google.devtools.build.v1B\020BuildStatusProtoP\001Z=google.golang.org/genproto/googleapis/devtools/build/v1;build\370\001\001\312\002\025Google\\Cloud\\Build\\V1',
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n+google/devtools/build/v1/build_status.proto\x12\x18google.devtools.build.v1\x1a\x1egoogle/protobuf/wrappers.proto\x1a\x19google/protobuf/any.proto\"\x9e\x03\n\x0b\x42uildStatus\x12<\n\x06result\x18\x01 \x01(\x0e\x32,.google.devtools.build.v1.BuildStatus.Result\x12\x1b\n\x13\x66inal_invocation_id\x18\x03 \x01(\t\x12\x39\n\x14\x62uild_tool_exit_code\x18\x04 \x01(\x0b\x32\x1b.google.protobuf.Int32Value\x12%\n\x07\x64\x65tails\x18\x02 \x01(\x0b\x32\x14.google.protobuf.Any\"\xd1\x01\n\x06Result\x12\x12\n\x0eUNKNOWN_STATUS\x10\x00\x12\x15\n\x11\x43OMMAND_SUCCEEDED\x10\x01\x12\x12\n\x0e\x43OMMAND_FAILED\x10\x02\x12\x0e\n\nUSER_ERROR\x10\x03\x12\x10\n\x0cSYSTEM_ERROR\x10\x04\x12\x16\n\x12RESOURCE_EXHAUSTED\x10\x05\x12 \n\x1cINVOCATION_DEADLINE_EXCEEDED\x10\x06\x12\x1d\n\x19REQUEST_DEADLINE_EXCEEDED\x10\x08\x12\r\n\tCANCELLED\x10\x07\x42\x8c\x01\n\x1c\x63om.google.devtools.build.v1B\x10\x42uildStatusProtoP\x01Z=google.golang.org/genproto/googleapis/devtools/build/v1;build\xf8\x01\x01\xca\x02\x15Google\\Cloud\\Build\\V1b\x06proto3'
  ,
  dependencies=[google_dot_protobuf_dot_wrappers__pb2.DESCRIPTOR,google_dot_protobuf_dot_any__pb2.DESCRIPTOR,])



_BUILDSTATUS_RESULT = _descriptor.EnumDescriptor(
  name='Result',
  full_name='google.devtools.build.v1.BuildStatus.Result',
  filename=None,
  file=DESCRIPTOR,
  create_key=_descriptor._internal_create_key,
  values=[
    _descriptor.EnumValueDescriptor(
      name='UNKNOWN_STATUS', index=0, number=0,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
    _descriptor.EnumValueDescriptor(
      name='COMMAND_SUCCEEDED', index=1, number=1,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
    _descriptor.EnumValueDescriptor(
      name='COMMAND_FAILED', index=2, number=2,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
    _descriptor.EnumValueDescriptor(
      name='USER_ERROR', index=3, number=3,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
    _descriptor.EnumValueDescriptor(
      name='SYSTEM_ERROR', index=4, number=4,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
    _descriptor.EnumValueDescriptor(
      name='RESOURCE_EXHAUSTED', index=5, number=5,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
    _descriptor.EnumValueDescriptor(
      name='INVOCATION_DEADLINE_EXCEEDED', index=6, number=6,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
    _descriptor.EnumValueDescriptor(
      name='REQUEST_DEADLINE_EXCEEDED', index=7, number=8,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
    _descriptor.EnumValueDescriptor(
      name='CANCELLED', index=8, number=7,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
  ],
  containing_type=None,
  serialized_options=None,
  serialized_start=338,
  serialized_end=547,
)
_sym_db.RegisterEnumDescriptor(_BUILDSTATUS_RESULT)


_BUILDSTATUS = _descriptor.Descriptor(
  name='BuildStatus',
  full_name='google.devtools.build.v1.BuildStatus',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='result', full_name='google.devtools.build.v1.BuildStatus.result', index=0,
      number=1, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='final_invocation_id', full_name='google.devtools.build.v1.BuildStatus.final_invocation_id', index=1,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='build_tool_exit_code', full_name='google.devtools.build.v1.BuildStatus.build_tool_exit_code', index=2,
      number=4, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='details', full_name='google.devtools.build.v1.BuildStatus.details', index=3,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
    _BUILDSTATUS_RESULT,
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=133,
  serialized_end=547,
)

_BUILDSTATUS.fields_by_name['result'].enum_type = _BUILDSTATUS_RESULT
_BUILDSTATUS.fields_by_name['build_tool_exit_code'].message_type = google_dot_protobuf_dot_wrappers__pb2._INT32VALUE
_BUILDSTATUS.fields_by_name['details'].message_type = google_dot_protobuf_dot_any__pb2._ANY
_BUILDSTATUS_RESULT.containing_type = _BUILDSTATUS
DESCRIPTOR.message_types_by_name['BuildStatus'] = _BUILDSTATUS
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

BuildStatus = _reflection.GeneratedProtocolMessageType('BuildStatus', (_message.Message,), {
  'DESCRIPTOR' : _BUILDSTATUS,
  '__module__' : 'google.devtools.build.v1.build_status_pb2'
  # @@protoc_insertion_point(class_scope:google.devtools.build.v1.BuildStatus)
  })
_sym_db.RegisterMessage(BuildStatus)


DESCRIPTOR._options = None
# @@protoc_insertion_point(module_scope)
