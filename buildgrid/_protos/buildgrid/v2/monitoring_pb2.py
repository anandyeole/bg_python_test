# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: buildgrid/v2/monitoring.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import duration_pb2 as google_dot_protobuf_dot_duration__pb2
from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='buildgrid/v2/monitoring.proto',
  package='buildgrid.v2',
  syntax='proto3',
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n\x1d\x62uildgrid/v2/monitoring.proto\x12\x0c\x62uildgrid.v2\x1a\x1egoogle/protobuf/duration.proto\x1a\x1fgoogle/protobuf/timestamp.proto\"\x93\x01\n\nBusMessage\x12\x17\n\x0fsequence_number\x18\x01 \x01(\x03\x12-\n\nlog_record\x18\x02 \x01(\x0b\x32\x17.buildgrid.v2.LogRecordH\x00\x12\x33\n\rmetric_record\x18\x03 \x01(\x0b\x32\x1a.buildgrid.v2.MetricRecordH\x00\x42\x08\n\x06record\"\xcc\x02\n\tLogRecord\x12\x36\n\x12\x63reation_timestamp\x18\x01 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x0e\n\x06\x64omain\x18\x02 \x01(\t\x12,\n\x05level\x18\x03 \x01(\x0e\x32\x1d.buildgrid.v2.LogRecord.Level\x12\x0f\n\x07message\x18\x04 \x01(\t\x12\x37\n\x08metadata\x18\x05 \x03(\x0b\x32%.buildgrid.v2.LogRecord.MetadataEntry\x1a/\n\rMetadataEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"N\n\x05Level\x12\n\n\x06NOTSET\x10\x00\x12\t\n\x05\x44\x45\x42UG\x10\x01\x12\x08\n\x04INFO\x10\x02\x12\x0b\n\x07WARNING\x10\x03\x12\t\n\x05\x45RROR\x10\x04\x12\x0c\n\x08\x43RITICAL\x10\x05\"\x90\x03\n\x0cMetricRecord\x12\x36\n\x12\x63reation_timestamp\x18\x01 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12-\n\x04type\x18\x03 \x01(\x0e\x32\x1f.buildgrid.v2.MetricRecord.Type\x12\x0c\n\x04name\x18\x04 \x01(\t\x12\x0f\n\x05\x63ount\x18\x05 \x01(\x02H\x00\x12-\n\x08\x64uration\x18\x06 \x01(\x0b\x32\x19.google.protobuf.DurationH\x00\x12\x0f\n\x05value\x18\x07 \x01(\x02H\x00\x12:\n\x08metadata\x18\x08 \x03(\x0b\x32(.buildgrid.v2.MetricRecord.MetadataEntry\x1a/\n\rMetadataEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"E\n\x04Type\x12\x08\n\x04NONE\x10\x00\x12\x0b\n\x07\x43OUNTER\x10\x01\x12\t\n\x05TIMER\x10\x02\x12\t\n\x05GAUGE\x10\x03\x12\x10\n\x0c\x44ISTRIBUTION\x10\x04\x42\x06\n\x04\x64\x61tab\x06proto3'
  ,
  dependencies=[google_dot_protobuf_dot_duration__pb2.DESCRIPTOR,google_dot_protobuf_dot_timestamp__pb2.DESCRIPTOR,])



_LOGRECORD_LEVEL = _descriptor.EnumDescriptor(
  name='Level',
  full_name='buildgrid.v2.LogRecord.Level',
  filename=None,
  file=DESCRIPTOR,
  create_key=_descriptor._internal_create_key,
  values=[
    _descriptor.EnumValueDescriptor(
      name='NOTSET', index=0, number=0,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
    _descriptor.EnumValueDescriptor(
      name='DEBUG', index=1, number=1,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
    _descriptor.EnumValueDescriptor(
      name='INFO', index=2, number=2,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
    _descriptor.EnumValueDescriptor(
      name='WARNING', index=3, number=3,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
    _descriptor.EnumValueDescriptor(
      name='ERROR', index=4, number=4,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
    _descriptor.EnumValueDescriptor(
      name='CRITICAL', index=5, number=5,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
  ],
  containing_type=None,
  serialized_options=None,
  serialized_start=517,
  serialized_end=595,
)
_sym_db.RegisterEnumDescriptor(_LOGRECORD_LEVEL)

_METRICRECORD_TYPE = _descriptor.EnumDescriptor(
  name='Type',
  full_name='buildgrid.v2.MetricRecord.Type',
  filename=None,
  file=DESCRIPTOR,
  create_key=_descriptor._internal_create_key,
  values=[
    _descriptor.EnumValueDescriptor(
      name='NONE', index=0, number=0,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
    _descriptor.EnumValueDescriptor(
      name='COUNTER', index=1, number=1,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
    _descriptor.EnumValueDescriptor(
      name='TIMER', index=2, number=2,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
    _descriptor.EnumValueDescriptor(
      name='GAUGE', index=3, number=3,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
    _descriptor.EnumValueDescriptor(
      name='DISTRIBUTION', index=4, number=4,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
  ],
  containing_type=None,
  serialized_options=None,
  serialized_start=921,
  serialized_end=990,
)
_sym_db.RegisterEnumDescriptor(_METRICRECORD_TYPE)


_BUSMESSAGE = _descriptor.Descriptor(
  name='BusMessage',
  full_name='buildgrid.v2.BusMessage',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='sequence_number', full_name='buildgrid.v2.BusMessage.sequence_number', index=0,
      number=1, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='log_record', full_name='buildgrid.v2.BusMessage.log_record', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='metric_record', full_name='buildgrid.v2.BusMessage.metric_record', index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
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
    _descriptor.OneofDescriptor(
      name='record', full_name='buildgrid.v2.BusMessage.record',
      index=0, containing_type=None,
      create_key=_descriptor._internal_create_key,
    fields=[]),
  ],
  serialized_start=113,
  serialized_end=260,
)


_LOGRECORD_METADATAENTRY = _descriptor.Descriptor(
  name='MetadataEntry',
  full_name='buildgrid.v2.LogRecord.MetadataEntry',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='key', full_name='buildgrid.v2.LogRecord.MetadataEntry.key', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='value', full_name='buildgrid.v2.LogRecord.MetadataEntry.value', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=b'8\001',
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=468,
  serialized_end=515,
)

_LOGRECORD = _descriptor.Descriptor(
  name='LogRecord',
  full_name='buildgrid.v2.LogRecord',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='creation_timestamp', full_name='buildgrid.v2.LogRecord.creation_timestamp', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='domain', full_name='buildgrid.v2.LogRecord.domain', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='level', full_name='buildgrid.v2.LogRecord.level', index=2,
      number=3, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='message', full_name='buildgrid.v2.LogRecord.message', index=3,
      number=4, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='metadata', full_name='buildgrid.v2.LogRecord.metadata', index=4,
      number=5, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[_LOGRECORD_METADATAENTRY, ],
  enum_types=[
    _LOGRECORD_LEVEL,
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=263,
  serialized_end=595,
)


_METRICRECORD_METADATAENTRY = _descriptor.Descriptor(
  name='MetadataEntry',
  full_name='buildgrid.v2.MetricRecord.MetadataEntry',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='key', full_name='buildgrid.v2.MetricRecord.MetadataEntry.key', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='value', full_name='buildgrid.v2.MetricRecord.MetadataEntry.value', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=b'8\001',
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=468,
  serialized_end=515,
)

_METRICRECORD = _descriptor.Descriptor(
  name='MetricRecord',
  full_name='buildgrid.v2.MetricRecord',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='creation_timestamp', full_name='buildgrid.v2.MetricRecord.creation_timestamp', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='type', full_name='buildgrid.v2.MetricRecord.type', index=1,
      number=3, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='name', full_name='buildgrid.v2.MetricRecord.name', index=2,
      number=4, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='count', full_name='buildgrid.v2.MetricRecord.count', index=3,
      number=5, type=2, cpp_type=6, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='duration', full_name='buildgrid.v2.MetricRecord.duration', index=4,
      number=6, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='value', full_name='buildgrid.v2.MetricRecord.value', index=5,
      number=7, type=2, cpp_type=6, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='metadata', full_name='buildgrid.v2.MetricRecord.metadata', index=6,
      number=8, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[_METRICRECORD_METADATAENTRY, ],
  enum_types=[
    _METRICRECORD_TYPE,
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
    _descriptor.OneofDescriptor(
      name='data', full_name='buildgrid.v2.MetricRecord.data',
      index=0, containing_type=None,
      create_key=_descriptor._internal_create_key,
    fields=[]),
  ],
  serialized_start=598,
  serialized_end=998,
)

_BUSMESSAGE.fields_by_name['log_record'].message_type = _LOGRECORD
_BUSMESSAGE.fields_by_name['metric_record'].message_type = _METRICRECORD
_BUSMESSAGE.oneofs_by_name['record'].fields.append(
  _BUSMESSAGE.fields_by_name['log_record'])
_BUSMESSAGE.fields_by_name['log_record'].containing_oneof = _BUSMESSAGE.oneofs_by_name['record']
_BUSMESSAGE.oneofs_by_name['record'].fields.append(
  _BUSMESSAGE.fields_by_name['metric_record'])
_BUSMESSAGE.fields_by_name['metric_record'].containing_oneof = _BUSMESSAGE.oneofs_by_name['record']
_LOGRECORD_METADATAENTRY.containing_type = _LOGRECORD
_LOGRECORD.fields_by_name['creation_timestamp'].message_type = google_dot_protobuf_dot_timestamp__pb2._TIMESTAMP
_LOGRECORD.fields_by_name['level'].enum_type = _LOGRECORD_LEVEL
_LOGRECORD.fields_by_name['metadata'].message_type = _LOGRECORD_METADATAENTRY
_LOGRECORD_LEVEL.containing_type = _LOGRECORD
_METRICRECORD_METADATAENTRY.containing_type = _METRICRECORD
_METRICRECORD.fields_by_name['creation_timestamp'].message_type = google_dot_protobuf_dot_timestamp__pb2._TIMESTAMP
_METRICRECORD.fields_by_name['type'].enum_type = _METRICRECORD_TYPE
_METRICRECORD.fields_by_name['duration'].message_type = google_dot_protobuf_dot_duration__pb2._DURATION
_METRICRECORD.fields_by_name['metadata'].message_type = _METRICRECORD_METADATAENTRY
_METRICRECORD_TYPE.containing_type = _METRICRECORD
_METRICRECORD.oneofs_by_name['data'].fields.append(
  _METRICRECORD.fields_by_name['count'])
_METRICRECORD.fields_by_name['count'].containing_oneof = _METRICRECORD.oneofs_by_name['data']
_METRICRECORD.oneofs_by_name['data'].fields.append(
  _METRICRECORD.fields_by_name['duration'])
_METRICRECORD.fields_by_name['duration'].containing_oneof = _METRICRECORD.oneofs_by_name['data']
_METRICRECORD.oneofs_by_name['data'].fields.append(
  _METRICRECORD.fields_by_name['value'])
_METRICRECORD.fields_by_name['value'].containing_oneof = _METRICRECORD.oneofs_by_name['data']
DESCRIPTOR.message_types_by_name['BusMessage'] = _BUSMESSAGE
DESCRIPTOR.message_types_by_name['LogRecord'] = _LOGRECORD
DESCRIPTOR.message_types_by_name['MetricRecord'] = _METRICRECORD
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

BusMessage = _reflection.GeneratedProtocolMessageType('BusMessage', (_message.Message,), {
  'DESCRIPTOR' : _BUSMESSAGE,
  '__module__' : 'buildgrid._protos.buildgrid.v2.monitoring_pb2'
  # @@protoc_insertion_point(class_scope:buildgrid.v2.BusMessage)
  })
_sym_db.RegisterMessage(BusMessage)

LogRecord = _reflection.GeneratedProtocolMessageType('LogRecord', (_message.Message,), {

  'MetadataEntry' : _reflection.GeneratedProtocolMessageType('MetadataEntry', (_message.Message,), {
    'DESCRIPTOR' : _LOGRECORD_METADATAENTRY,
    '__module__' : 'buildgrid._protos.buildgrid.v2.monitoring_pb2'
    # @@protoc_insertion_point(class_scope:buildgrid.v2.LogRecord.MetadataEntry)
    })
  ,
  'DESCRIPTOR' : _LOGRECORD,
  '__module__' : 'buildgrid._protos.buildgrid.v2.monitoring_pb2'
  # @@protoc_insertion_point(class_scope:buildgrid.v2.LogRecord)
  })
_sym_db.RegisterMessage(LogRecord)
_sym_db.RegisterMessage(LogRecord.MetadataEntry)

MetricRecord = _reflection.GeneratedProtocolMessageType('MetricRecord', (_message.Message,), {

  'MetadataEntry' : _reflection.GeneratedProtocolMessageType('MetadataEntry', (_message.Message,), {
    'DESCRIPTOR' : _METRICRECORD_METADATAENTRY,
    '__module__' : 'buildgrid._protos.buildgrid.v2.monitoring_pb2'
    # @@protoc_insertion_point(class_scope:buildgrid.v2.MetricRecord.MetadataEntry)
    })
  ,
  'DESCRIPTOR' : _METRICRECORD,
  '__module__' : 'buildgrid._protos.buildgrid.v2.monitoring_pb2'
  # @@protoc_insertion_point(class_scope:buildgrid.v2.MetricRecord)
  })
_sym_db.RegisterMessage(MetricRecord)
_sym_db.RegisterMessage(MetricRecord.MetadataEntry)


_LOGRECORD_METADATAENTRY._options = None
_METRICRECORD_METADATAENTRY._options = None
# @@protoc_insertion_point(module_scope)
