# Copyright (C) 2018 Bloomberg LP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  <http://www.apache.org/licenses/LICENSE-2.0>
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
CAS services
==================

Implements the Content Addressable Storage API and ByteStream API.
"""

from functools import partial
import itertools
import logging
import re

import grpc

import buildgrid.server.context as context_module

from buildgrid._enums import ByteStreamResourceType
from buildgrid._exceptions import (
    InvalidArgumentError,
    NotFoundError,
    OutOfRangeError,
    PermissionDeniedError,
    RetriableError,
    StorageFullError)

from buildgrid._protos.google.bytestream import bytestream_pb2, bytestream_pb2_grpc
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2_grpc
from buildgrid.server._authentication import AuthContext, authorize
from buildgrid.server.metrics_utils import (
    DurationMetric,
    generator_method_duration_metric
)
from buildgrid.server.request_metadata_utils import printable_request_metadata
from buildgrid.server.metrics_names import (
    CAS_FIND_MISSING_BLOBS_TIME_METRIC_NAME,
    CAS_BATCH_UPDATE_BLOBS_TIME_METRIC_NAME,
    CAS_BATCH_READ_BLOBS_TIME_METRIC_NAME,
    CAS_GET_TREE_TIME_METRIC_NAME,
    CAS_BYTESTREAM_READ_TIME_METRIC_NAME,
    CAS_BYTESTREAM_WRITE_TIME_METRIC_NAME)


class ContentAddressableStorageService(remote_execution_pb2_grpc.ContentAddressableStorageServicer):

    def __init__(self, server):
        self.__logger = logging.getLogger(__name__)

        self._instances = {}

        remote_execution_pb2_grpc.add_ContentAddressableStorageServicer_to_server(self, server)

    # --- Public API ---

    def add_instance(self, name, instance):
        self._instances[name] = instance

    # --- Public API: Servicer ---

    @context_module.metadatacontext()
    @authorize(AuthContext)
    @DurationMetric(CAS_FIND_MISSING_BLOBS_TIME_METRIC_NAME)
    def FindMissingBlobs(self, request, context):
        self.__logger.info(f"FindMissingBlobs request from [{context.peer()}] "
                           f"([{printable_request_metadata(context.invocation_metadata())}])")

        try:
            instance = self._get_instance(request.instance_name)
            response = instance.find_missing_blobs(request.blob_digests)

            return response

        except InvalidArgumentError as e:
            self.__logger.info(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        except ConnectionError as e:
            self.__logger.exception(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.UNAVAILABLE)

        # Attempt to catch postgres connection failures and instruct clients to retry
        except RetriableError as e:
            self.__logger.info(f"Retriable error, client should retry in: {e.retry_info.retry_delay}")
            context.abort_with_status(e.error_status)

        except Exception as e:
            self.__logger.exception(
                f"Unexpected error in FindMissingBlobs; request=[{request}]"
            )
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)

        return remote_execution_pb2.FindMissingBlobsResponse()

    @context_module.metadatacontext()
    @authorize(AuthContext)
    @DurationMetric(CAS_BATCH_UPDATE_BLOBS_TIME_METRIC_NAME)
    def BatchUpdateBlobs(self, request, context):
        self.__logger.info(f"BatchUpdateBlobs request from [{context.peer()}] "
                           f"([{printable_request_metadata(context.invocation_metadata())}])")

        try:
            instance = self._get_instance(request.instance_name)
            response = instance.batch_update_blobs(request.requests)

            return response

        except InvalidArgumentError as e:
            self.__logger.info(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        except PermissionDeniedError as e:
            self.__logger.exception(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)

        except ConnectionError as e:
            self.__logger.exception(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.UNAVAILABLE)

        # Attempt to catch postgres connection failures and instruct clients to retry
        except RetriableError as e:
            self.__logger.info(f"Retriable error, client should retry in: {e.retry_info.retry_delay}")
            context.abort_with_status(e.error_status)

        except Exception as e:
            # Log the digests but not the data:
            printable_request = {'instance_name': request.instance_name,
                                 'digests': [r.digest for r in request.requests]}

            self.__logger.info(
                f"Unexpected error in BatchUpdateBlobs; request=[{printable_request}]"
            )
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)

        return remote_execution_pb2.BatchReadBlobsResponse()

    @context_module.metadatacontext()
    @authorize(AuthContext)
    @DurationMetric(CAS_BATCH_READ_BLOBS_TIME_METRIC_NAME)
    def BatchReadBlobs(self, request, context):
        self.__logger.info(f"BatchReadBlobs request from [{context.peer()}] "
                           f"([{printable_request_metadata(context.invocation_metadata())}])")

        try:
            instance = self._get_instance(request.instance_name)
            response = instance.batch_read_blobs(request.digests)
            return response

        except InvalidArgumentError as e:
            self.__logger.info(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        except PermissionDeniedError as e:
            self.__logger.exception(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)

        except ConnectionError as e:
            self.__logger.exception(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.UNAVAILABLE)

        # Attempt to catch postgres connection failures and instruct clients to retry
        except RetriableError as e:
            self.__logger.info(f"Retriable error, client should retry in: {e.retry_info.retry_delay}")
            context.abort_with_status(e.error_status)

        except Exception as e:
            self.__logger.exception(
                f"Unexpected error in BatchReadBlobs; request=[{request}]"
            )
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)

        return remote_execution_pb2.BatchReadBlobsResponse()

    @authorize(AuthContext)
    @DurationMetric(CAS_GET_TREE_TIME_METRIC_NAME)
    def GetTree(self, request, context):
        self.__logger.info(f"GetTree request from [{context.peer()}] "
                           f"([{printable_request_metadata(context.invocation_metadata())}])")

        try:
            instance = self._get_instance(request.instance_name)
            yield from instance.get_tree(request)

        except InvalidArgumentError as e:
            self.__logger.info(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        except ConnectionError as e:
            self.__logger.exception(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.UNAVAILABLE)

        # Attempt to catch postgres connection failures and instruct clients to retry
        except RetriableError as e:
            self.__logger.info(f"Retriable error, client should retry in: {e.retry_info.retry_delay}")
            context.abort_with_status(e.error_status)

        except Exception as e:
            self.__logger.exception(
                f"Unexpected error in GetTree; request=[{request}]"
            )
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)

        yield remote_execution_pb2.GetTreeResponse()

    # --- Private API ---

    def _get_instance(self, instance_name):
        try:
            return self._instances[instance_name]

        except KeyError:
            raise InvalidArgumentError(f"Invalid instance name: [{instance_name}]")


class ByteStreamService(bytestream_pb2_grpc.ByteStreamServicer):

    # CAS read name format: "{instance_name}/blobs/{hash}/{size}"
    CAS_READ_REGEX = '^(.*?)/?(blobs/.*/[0-9]*)$'
    # CAS write name format: "{instance_name}/uploads/{uuid}/blobs/{hash}/{size}"
    # NOTE: No `$` here since we deliberately support extra data on the end of this
    # resource name.
    CAS_WRITE_REGEX = '^(.*?)/?(uploads/.*/blobs/.*/[0-9]*)'

    # LogStream read name format: "{instance_name}/{parent}/logStreams/{name}"
    LOGSTREAM_READ_REGEX = '^(.*?)/?([^/]*/logStreams/.*)$'
    # LogStream write name format: "{instance_name}/{parent}/logStreams/{name}/{token}"
    LOGSTREAM_WRITE_REGEX = '^(.*?)/?([^/]*/logStreams/.*/.*)$'

    def __init__(self, server):
        self.__logger = logging.getLogger(__name__)

        self._instances = {}

        bytestream_pb2_grpc.add_ByteStreamServicer_to_server(self, server)

    # --- Public API ---

    def add_instance(self, name, instance):
        self._instances[name] = instance

    # --- Public API: Servicer ---

    @context_module.metadatacontext()
    @authorize(AuthContext)
    @generator_method_duration_metric(CAS_BYTESTREAM_READ_TIME_METRIC_NAME)
    def Read(self, request, context):
        self.__logger.info(f"Read request from [{context.peer()}] "
                           f"([{printable_request_metadata(context.invocation_metadata())}])")

        try:
            instance, resource, res_type = self._parse_resource_name(
                request.resource_name,
                cas_regex=self.CAS_READ_REGEX,
                logstream_regex=self.LOGSTREAM_READ_REGEX
            )
            if res_type == ByteStreamResourceType.CAS:
                blob_details = resource.split('/')
                hash_, size_bytes = blob_details[1], blob_details[2]

                yield from instance.read_cas_blob(
                    hash_, size_bytes, request.read_offset, request.read_limit)

            elif res_type == ByteStreamResourceType.LOGSTREAM:
                context.add_callback(partial(instance.disconnect_logstream_reader,
                                             resource))
                yield from instance.read_logstream(resource, context)

        except InvalidArgumentError as e:
            self.__logger.info(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            yield bytestream_pb2.ReadResponse()

        except NotFoundError as e:
            self.__logger.info(f"{request.resource_name} not found", exc_info=True)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.NOT_FOUND)
            yield bytestream_pb2.ReadResponse()

        except OutOfRangeError as e:
            self.__logger.exception(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.OUT_OF_RANGE)
            yield bytestream_pb2.ReadResponse()

        except ConnectionError as e:
            self.__logger.exception(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            yield bytestream_pb2.ReadResponse()

        except RetriableError as e:
            self.__logger.info(f"Retriable error, client should retry in: {e.retry_info.retry_delay}")
            context.abort_with_status(e.error_status)

        except Exception as e:
            self.__logger.exception(
                f"Unexpected error in ByteStreamRead; request=[{request}]"
            )
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)

    @context_module.metadatacontext()
    @authorize(AuthContext)
    @DurationMetric(CAS_BYTESTREAM_WRITE_TIME_METRIC_NAME)
    def Write(self, request_iterator, context):
        self.__logger.info(f"Write request from [{context.peer()}] "
                           f"([{printable_request_metadata(context.invocation_metadata())}])")

        request = next(request_iterator)

        try:
            instance, resource, res_type = self._parse_resource_name(
                request.resource_name,
                cas_regex=self.CAS_WRITE_REGEX,
                logstream_regex=self.LOGSTREAM_WRITE_REGEX,
            )
            if res_type == ByteStreamResourceType.CAS:
                blob_details = resource.split('/')
                _, hash_, size_bytes = blob_details[1], blob_details[3], blob_details[4]
                return instance.write_cas_blob(
                    hash_, size_bytes, itertools.chain([request], request_iterator))

            elif res_type == ByteStreamResourceType.LOGSTREAM:
                return instance.write_logstream(resource, request, request_iterator)

        except NotImplementedError as e:
            self.__logger.info(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)

        except InvalidArgumentError as e:
            self.__logger.info(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        except NotFoundError as e:
            self.__logger.exception(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.NOT_FOUND)

        except PermissionDeniedError as e:
            self.__logger.exception(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)

        except StorageFullError as e:
            self.__logger.exception(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)

        except ConnectionError as e:
            self.__logger.exception(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.UNAVAILABLE)

        except RetriableError as e:
            self.__logger.info(f"Retriable error, client should retry in: {e.retry_info.retry_delay}")
            context.abort_with_status(e.error_status)

        except Exception as e:
            # Log all the fields except `data`:
            printable_request = {'resource_name': request.resource_name,
                                 'write_offset': request.write_offset,
                                 'finish_write': request.finish_write}

            self.__logger.exception(
                f"Unexpected error in ByteStreamWrite; request=[{printable_request}]"
            )
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)

        return bytestream_pb2.WriteResponse()

    @authorize(AuthContext)
    def QueryWriteStatus(self, request, context):
        self.__logger.info(f"QueryWriteStatus request from [{context.peer()}]")

        try:
            instance, resource, res_type = self._parse_resource_name(
                request.resource_name,
                cas_regex=self.CAS_WRITE_REGEX,
                logstream_regex=self.LOGSTREAM_WRITE_REGEX,
            )
            if res_type == ByteStreamResourceType.CAS:
                context.set_code(grpc.StatusCode.UNIMPLEMENTED)
                context.set_details('Method not implemented!')
            elif res_type == ByteStreamResourceType.LOGSTREAM:
                return instance.query_logstream_status(resource, context)

        except NotImplementedError as e:
            self.__logger.info(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)

        except InvalidArgumentError as e:
            self.__logger.info(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        except NotFoundError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.NOT_FOUND)

        except PermissionDeniedError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)

        except StorageFullError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)

        except ConnectionError as e:
            self.__logger.exception(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.UNAVAILABLE)

        except RetriableError as e:
            self.__logger.info(f"Retriable error, client should retry in: {e.retry_info.retry_delay}")
            context.abort_with_status(e.error_status)

        except Exception as e:
            self.__logger.exception(
                f"Unexpected error in ByteStreamQueryWriteStatus; request=[{request}]"
            )
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)

        return bytestream_pb2.QueryWriteStatusResponse()

    # --- Private API ---

    def _parse_resource_name(self, resource_name, cas_regex='', logstream_regex=''):
        res_type = None
        cas_match = re.match(cas_regex, resource_name)
        logstream_match = re.match(logstream_regex, resource_name)
        if cas_match:
            instance_name = cas_match[1]
            resource_name = cas_match[2]
            res_type = ByteStreamResourceType.CAS

        elif logstream_match:
            instance_name = logstream_match[1]
            resource_name = logstream_match[2]
            res_type = ByteStreamResourceType.LOGSTREAM

        else:
            raise InvalidArgumentError(
                f"Invalid resource name: [{resource_name}]")

        instance = self._get_instance(instance_name)
        return instance, resource_name, res_type

    def _get_instance(self, instance_name):
        try:
            return self._instances[instance_name]

        except KeyError:
            raise InvalidArgumentError(f"Invalid instance name: [{instance_name}]")
