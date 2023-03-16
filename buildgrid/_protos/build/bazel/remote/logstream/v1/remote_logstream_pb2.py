# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: build/bazel/remote/logstream/v1/remote_logstream.proto

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='build/bazel/remote/logstream/v1/remote_logstream.proto',
  package='build.bazel.remote.logstream.v1',
  syntax='proto3',
  serialized_options=b'\n\037build.bazel.remote.logstream.v1B\024RemoteLogStreamProtoP\001Z\017remotelogstream\242\002\002RL\252\002\037Build.Bazel.Remote.LogStream.v1',
  serialized_pb=b'\n6build/bazel/remote/logstream/v1/remote_logstream.proto\x12\x1f\x62uild.bazel.remote.logstream.v1\"(\n\x16\x43reateLogStreamRequest\x12\x0e\n\x06parent\x18\x01 \x01(\t\"6\n\tLogStream\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x1b\n\x13write_resource_name\x18\x02 \x01(\t2\x8c\x01\n\x10LogStreamService\x12x\n\x0f\x43reateLogStream\x12\x37.build.bazel.remote.logstream.v1.CreateLogStreamRequest\x1a*.build.bazel.remote.logstream.v1.LogStream\"\x00\x42q\n\x1f\x62uild.bazel.remote.logstream.v1B\x14RemoteLogStreamProtoP\x01Z\x0fremotelogstream\xa2\x02\x02RL\xaa\x02\x1f\x42uild.Bazel.Remote.LogStream.v1b\x06proto3'
)




_CREATELOGSTREAMREQUEST = _descriptor.Descriptor(
  name='CreateLogStreamRequest',
  full_name='build.bazel.remote.logstream.v1.CreateLogStreamRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='parent', full_name='build.bazel.remote.logstream.v1.CreateLogStreamRequest.parent', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=91,
  serialized_end=131,
)


_LOGSTREAM = _descriptor.Descriptor(
  name='LogStream',
  full_name='build.bazel.remote.logstream.v1.LogStream',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='name', full_name='build.bazel.remote.logstream.v1.LogStream.name', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='write_resource_name', full_name='build.bazel.remote.logstream.v1.LogStream.write_resource_name', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=133,
  serialized_end=187,
)

DESCRIPTOR.message_types_by_name['CreateLogStreamRequest'] = _CREATELOGSTREAMREQUEST
DESCRIPTOR.message_types_by_name['LogStream'] = _LOGSTREAM
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

CreateLogStreamRequest = _reflection.GeneratedProtocolMessageType('CreateLogStreamRequest', (_message.Message,), {
  'DESCRIPTOR' : _CREATELOGSTREAMREQUEST,
  '__module__' : 'build.bazel.remote.logstream.v1.remote_logstream_pb2'
  # @@protoc_insertion_point(class_scope:build.bazel.remote.logstream.v1.CreateLogStreamRequest)
  })
_sym_db.RegisterMessage(CreateLogStreamRequest)

LogStream = _reflection.GeneratedProtocolMessageType('LogStream', (_message.Message,), {
  'DESCRIPTOR' : _LOGSTREAM,
  '__module__' : 'build.bazel.remote.logstream.v1.remote_logstream_pb2'
  # @@protoc_insertion_point(class_scope:build.bazel.remote.logstream.v1.LogStream)
  })
_sym_db.RegisterMessage(LogStream)


DESCRIPTOR._options = None

_LOGSTREAMSERVICE = _descriptor.ServiceDescriptor(
  name='LogStreamService',
  full_name='build.bazel.remote.logstream.v1.LogStreamService',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  serialized_start=190,
  serialized_end=330,
  methods=[
  _descriptor.MethodDescriptor(
    name='CreateLogStream',
    full_name='build.bazel.remote.logstream.v1.LogStreamService.CreateLogStream',
    index=0,
    containing_service=None,
    input_type=_CREATELOGSTREAMREQUEST,
    output_type=_LOGSTREAM,
    serialized_options=None,
  ),
])
_sym_db.RegisterServiceDescriptor(_LOGSTREAMSERVICE)

DESCRIPTOR.services_by_name['LogStreamService'] = _LOGSTREAMSERVICE

# @@protoc_insertion_point(module_scope)