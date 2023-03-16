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
Storage Instances
=================
Instances of CAS and ByteStream
"""

from datetime import timedelta
import logging
from typing import List

from grpc import RpcError

from buildgrid._exceptions import (
    InvalidArgumentError,
    NotFoundError,
    OutOfRangeError,
    PermissionDeniedError,
    RetriableError
)
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2 as re_pb2
from buildgrid._protos.google.bytestream.bytestream_pb2 import (
    QueryWriteStatusResponse,
    ReadResponse,
    WriteRequest,
    WriteResponse
)
from buildgrid._protos.google.rpc import code_pb2, status_pb2
from buildgrid.server.metrics_utils import (
    Counter,
    DurationMetric,
    ExceptionCounter,
    Distribution,
    generator_method_duration_metric,
    generator_method_exception_counter
)
from buildgrid.server.metrics_names import (
    CAS_EXCEPTION_COUNT_METRIC_NAME,
    CAS_DOWNLOADED_BYTES_METRIC_NAME,
    CAS_UPLOADED_BYTES_METRIC_NAME,
    CAS_FIND_MISSING_BLOBS_NUM_REQUESTED_METRIC_NAME,
    CAS_FIND_MISSING_BLOBS_SIZE_BYTES_REQUESTED_METRIC_NAME,
    CAS_FIND_MISSING_BLOBS_NUM_MISSING_METRIC_NAME,
    CAS_FIND_MISSING_BLOBS_PERCENT_MISSING_METRIC_NAME,
    CAS_FIND_MISSING_BLOBS_SIZE_BYTES_MISSING_METRIC_NAME,
    CAS_FIND_MISSING_BLOBS_TIME_METRIC_NAME,
    CAS_BATCH_UPDATE_BLOBS_TIME_METRIC_NAME,
    CAS_BATCH_UPDATE_BLOBS_SIZE_BYTES,
    CAS_BATCH_READ_BLOBS_TIME_METRIC_NAME,
    CAS_BATCH_READ_BLOBS_SIZE_BYTES,
    CAS_GET_TREE_TIME_METRIC_NAME,
    CAS_BYTESTREAM_READ_TIME_METRIC_NAME,
    CAS_BYTESTREAM_READ_SIZE_BYTES,
    CAS_BYTESTREAM_WRITE_TIME_METRIC_NAME,
    CAS_BYTESTREAM_WRITE_SIZE_BYTES,
    LOGSTREAM_WRITE_UPLOADED_BYTES_COUNT
)
from buildgrid.settings import (
    HASH,
    HASH_LENGTH,
    MAX_REQUEST_SIZE,
    MAX_REQUEST_COUNT,
    MAX_LOGSTREAM_CHUNK_SIZE,
    STREAM_ERROR_RETRY_PERIOD
)
from buildgrid.utils import create_digest, get_unique_objects_by_attribute


def _write_empty_digest_to_storage(storage):
    if not storage:
        return
    # Some clients skip uploading a blob with size 0.
    # We pre-insert the empty blob to make sure that it is available.
    empty_digest = create_digest(b'')
    if not storage.has_blob(empty_digest):
        session = storage.begin_write(empty_digest)
        storage.commit_write(empty_digest, session)
    # This check is useful to confirm the CAS is functioning correctly
    # but also to ensure that the access timestamp on the index is
    # bumped sufficiently often so that there is less of a chance of
    # the empty blob being evicted prematurely.
    if not storage.get_blob(empty_digest):
        raise NotFoundError("Empty blob not found after writing it to"
                            " storage. Is your CAS configured correctly?")


class ContentAddressableStorageInstance:

    def __init__(self, storage, read_only=False):
        self.__logger = logging.getLogger(__name__)

        self._instance_name = None

        self.__storage = storage

        self.__read_only = read_only

    # --- Public API ---

    @property
    def instance_name(self):
        return self._instance_name

    @ExceptionCounter(CAS_EXCEPTION_COUNT_METRIC_NAME)
    def register_instance_with_server(self, instance_name, server):
        """Names and registers the CAS instance with a given server."""
        if self._instance_name is None:
            server.add_cas_instance(self, instance_name)

            self._instance_name = instance_name
            if not self.__storage.instance_name:
                self.__storage.set_instance_name(instance_name)

        else:
            raise AssertionError("Instance already registered")

    def setup_grpc(self):
        self.__storage.setup_grpc()

        # This is a check to ensure the CAS is functional, as well as make
        # sure that the empty digest is pre-populated (some tools don't
        # upload it, so we need to). It is done here since it needs to happen
        # after gRPC initialization in the case of a remote CAS backend.
        _write_empty_digest_to_storage(self.__storage)

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def hash_type(self):
        return self.__storage.hash_type()

    def max_batch_total_size_bytes(self):
        return self.__storage.max_batch_total_size_bytes()

    def symlink_absolute_path_strategy(self):
        return self.__storage.symlink_absolute_path_strategy()

    @DurationMetric(CAS_FIND_MISSING_BLOBS_TIME_METRIC_NAME, instanced=True)
    @ExceptionCounter(CAS_EXCEPTION_COUNT_METRIC_NAME)
    def find_missing_blobs(self, blob_digests):
        storage = self.__storage
        blob_digests = list(get_unique_objects_by_attribute(blob_digests, "hash"))
        missing_blobs = storage.missing_blobs(blob_digests)

        num_blobs_in_request = len(blob_digests)
        if num_blobs_in_request > 0:
            num_blobs_missing = len(missing_blobs)
            percent_missing = float((num_blobs_missing / num_blobs_in_request) * 100)

            with Distribution(CAS_FIND_MISSING_BLOBS_NUM_REQUESTED_METRIC_NAME,
                              instance_name=self._instance_name) as metric_num_requested:
                metric_num_requested.count = float(num_blobs_in_request)

            with Distribution(CAS_FIND_MISSING_BLOBS_NUM_MISSING_METRIC_NAME,
                              instance_name=self._instance_name) as metric_num_missing:
                metric_num_missing.count = float(num_blobs_missing)

            with Distribution(CAS_FIND_MISSING_BLOBS_PERCENT_MISSING_METRIC_NAME,
                              instance_name=self._instance_name) as metric_percent_missing:
                metric_percent_missing.count = percent_missing

        for digest in blob_digests:
            with Distribution(CAS_FIND_MISSING_BLOBS_SIZE_BYTES_REQUESTED_METRIC_NAME,
                              instance_name=self._instance_name) as metric_requested_blob_size:
                metric_requested_blob_size.count = float(digest.size_bytes)

        for digest in missing_blobs:
            with Distribution(CAS_FIND_MISSING_BLOBS_SIZE_BYTES_MISSING_METRIC_NAME,
                              instance_name=self._instance_name) as metric_missing_blob_size:
                metric_missing_blob_size.count = float(digest.size_bytes)

        return re_pb2.FindMissingBlobsResponse(missing_blob_digests=missing_blobs)

    @DurationMetric(CAS_BATCH_UPDATE_BLOBS_TIME_METRIC_NAME, instanced=True)
    @ExceptionCounter(CAS_EXCEPTION_COUNT_METRIC_NAME)
    def batch_update_blobs(self, requests):
        if self.__read_only:
            raise PermissionDeniedError(f"CAS instance {self._instance_name} is read-only")

        storage = self.__storage
        store = []
        for request_proto in get_unique_objects_by_attribute(requests, "digest.hash"):
            store.append((request_proto.digest, request_proto.data))

            with Distribution(CAS_BATCH_UPDATE_BLOBS_SIZE_BYTES,
                              instance_name=self._instance_name) as metric_blob_size:
                metric_blob_size.count = float(request_proto.digest.size_bytes)

        response = re_pb2.BatchUpdateBlobsResponse()
        statuses = storage.bulk_update_blobs(store)

        with Counter(metric_name=CAS_UPLOADED_BYTES_METRIC_NAME,
                     instance_name=self._instance_name) as bytes_counter:
            for (digest, _), status in zip(store, statuses):
                response_proto = response.responses.add()
                response_proto.digest.CopyFrom(digest)
                response_proto.status.CopyFrom(status)
                if response_proto.status.code == 0:
                    bytes_counter.increment(response_proto.digest.size_bytes)

        return response

    @DurationMetric(CAS_BATCH_READ_BLOBS_TIME_METRIC_NAME, instanced=True)
    @ExceptionCounter(CAS_EXCEPTION_COUNT_METRIC_NAME)
    def batch_read_blobs(self, digests):
        storage = self.__storage

        response = re_pb2.BatchReadBlobsResponse()

        max_batch_size = storage.max_batch_total_size_bytes()

        # Only process unique digests
        good_digests = []
        bad_digests = []
        requested_bytes = 0
        for digest in get_unique_objects_by_attribute(digests, "hash"):
            if len(digest.hash) != HASH_LENGTH:
                bad_digests.append(digest)
            else:
                good_digests.append(digest)
                requested_bytes += digest.size_bytes

        if requested_bytes > max_batch_size:
            raise InvalidArgumentError('Combined total size of blobs exceeds '
                                       'server limit. '
                                       f'({requested_bytes} > {max_batch_size} [byte])')

        blobs_read = {}
        if len(good_digests) > 0:
            blobs_read = storage.bulk_read_blobs(good_digests)

        with Counter(metric_name=CAS_DOWNLOADED_BYTES_METRIC_NAME,
                     instance_name=self._instance_name) as bytes_counter:
            for digest in good_digests:
                response_proto = response.responses.add()
                response_proto.digest.CopyFrom(digest)

                if digest.hash in blobs_read and blobs_read[digest.hash] is not None:
                    response_proto.data = blobs_read[digest.hash].read()
                    status_code = code_pb2.OK
                    bytes_counter.increment(digest.size_bytes)

                    with Distribution(CAS_BATCH_READ_BLOBS_SIZE_BYTES,
                                      instance_name=self._instance_name) as metric_blob_size:
                        metric_blob_size.count = float(digest.size_bytes)
                else:
                    status_code = code_pb2.NOT_FOUND

                response_proto.status.CopyFrom(status_pb2.Status(code=status_code))

        for digest in bad_digests:
            response_proto = response.responses.add()
            response_proto.digest.CopyFrom(digest)
            status_code = code_pb2.INVALID_ARGUMENT
            response_proto.status.CopyFrom(status_pb2.Status(code=status_code))

        for blob in blobs_read.values():
            blob.close()

        return response

    @DurationMetric(CAS_GET_TREE_TIME_METRIC_NAME, instanced=True)
    @ExceptionCounter(CAS_EXCEPTION_COUNT_METRIC_NAME)
    def get_tree(self, request):
        storage = self.__storage

        response = re_pb2.GetTreeResponse()
        page_size = request.page_size

        if not request.page_size:
            request.page_size = MAX_REQUEST_COUNT

        root_digest = request.root_digest
        page_size = request.page_size

        with Counter(metric_name=CAS_DOWNLOADED_BYTES_METRIC_NAME,
                     instance_name=self._instance_name) as bytes_counter:
            def __get_tree(node_digest):
                nonlocal response, page_size, request

                if not page_size:
                    page_size = request.page_size
                    yield response
                    response = re_pb2.GetTreeResponse()

                if response.ByteSize() >= (MAX_REQUEST_SIZE):
                    yield response
                    response = re_pb2.GetTreeResponse()

                directory_from_digest = storage.get_message(
                    node_digest, re_pb2.Directory)

                bytes_counter.increment(node_digest.size_bytes)

                page_size -= 1
                response.directories.extend([directory_from_digest])

                for directory in directory_from_digest.directories:
                    yield from __get_tree(directory.digest)

                yield response
                response = re_pb2.GetTreeResponse()

            yield from __get_tree(root_digest)


class ByteStreamInstance:

    BLOCK_SIZE = 1 * 1024 * 1024  # 1 MB block size

    def __init__(self, storage=None, read_only=False, stream_storage=None,
                 disable_overwrite_early_return=False):
        self._logger = logging.getLogger(__name__)

        self._instance_name = None

        self.__storage = storage
        self._stream_store = stream_storage
        self._query_activity_timeout = 30

        self.__read_only = read_only

        # If set, prevents `ByteStream.Write()` from returning without
        # reading all the client's `WriteRequests` for a digest that is
        # already in storage (i.e. not follow the REAPI-specified
        # behavior).
        self.__disable_overwrite_early_return = disable_overwrite_early_return
        # (Should only be used to work around issues with implementations
        # that treat the server half-closing its end of the gRPC stream
        # as a HTTP/2 stream error.)

    # --- Public API ---

    @property
    def instance_name(self):
        return self._instance_name

    @instance_name.setter
    def instance_name(self, instance_name):
        self._instance_name = instance_name

    def setup_grpc(self):
        if self.__storage:
            self.__storage.setup_grpc()

        # This is a check to ensure the CAS is functional, as well as make
        # sure that the empty digest is pre-populated (some tools don't
        # upload it, so we need to). It is done here since it needs to happen
        # after gRPC initialization in the case of a remote CAS backend.
        _write_empty_digest_to_storage(self.__storage)

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    @ExceptionCounter(CAS_EXCEPTION_COUNT_METRIC_NAME)
    def register_instance_with_server(self, instance_name, server):
        """Names and registers the byte-stream instance with a given server."""
        if self._instance_name is None:
            server.add_bytestream_instance(self, instance_name)

            self._instance_name = instance_name
            if self.__storage is not None and not self.__storage.instance_name:
                self.__storage.set_instance_name(instance_name)
        else:
            raise AssertionError("Instance already registered")

    def disconnect_logstream_reader(self, read_name: str):
        self._logger.info(f"Disconnecting reader from [{read_name}].")
        try:
            self._stream_store.streaming_client_left(read_name)
        except NotFoundError as e:
            self._logger.debug(f"Did not disconnect reader: {str(e)}")

    @generator_method_duration_metric(CAS_BYTESTREAM_READ_TIME_METRIC_NAME)
    @generator_method_exception_counter(CAS_EXCEPTION_COUNT_METRIC_NAME)
    def read_cas_blob(self, digest_hash, digest_size, read_offset, read_limit):
        # pylint: disable=no-else-raise
        if self.__storage is None:
            raise InvalidArgumentError(
                "ByteStream instance not configured for use with CAS.")

        if len(digest_hash) != HASH_LENGTH or not digest_size.isdigit():
            raise InvalidArgumentError(f"Invalid digest [{digest_hash}/{digest_size}]")

        digest = re_pb2.Digest(hash=digest_hash, size_bytes=int(digest_size))

        # Check the given read offset and limit.
        if read_offset < 0 or read_offset > digest.size_bytes:
            raise OutOfRangeError("Read offset out of range")

        elif read_limit == 0:
            bytes_remaining = digest.size_bytes - read_offset

        elif read_limit > 0:
            bytes_remaining = read_limit

        else:
            raise InvalidArgumentError("Negative read_limit is invalid")

        # Read the blob from storage and send its contents to the client.
        result = self.__storage.get_blob(digest)
        if result is None:
            raise NotFoundError("Blob not found")

        if read_offset > 0:
            result.seek(read_offset)

        with Distribution(metric_name=CAS_BYTESTREAM_READ_SIZE_BYTES,
                          instance_name=self._instance_name) as metric_blob_size:
            metric_blob_size.count = float(digest.size_bytes)

        with Counter(metric_name=CAS_DOWNLOADED_BYTES_METRIC_NAME,
                     instance_name=self._instance_name) as bytes_counter:
            while bytes_remaining > 0:
                block_data = result.read(min(self.BLOCK_SIZE, bytes_remaining))
                yield ReadResponse(data=block_data)
                bytes_counter.increment(len(block_data))
                bytes_remaining -= self.BLOCK_SIZE

        result.close()

    def read_logstream(self, resource_name, context):
        if self._stream_store is None:
            raise InvalidArgumentError(
                "ByteStream instance not configured for use with LogStream.")

        stream_iterator = self._stream_store.read_stream_bytes_blocking_iterator(
            resource_name, max_chunk_size=MAX_LOGSTREAM_CHUNK_SIZE, offset=0)

        self._stream_store.new_client_streaming(resource_name)

        for message in stream_iterator:
            if not context.is_active():
                break
            yield ReadResponse(data=message)

    @DurationMetric(CAS_BYTESTREAM_WRITE_TIME_METRIC_NAME, instanced=True)
    @ExceptionCounter(CAS_EXCEPTION_COUNT_METRIC_NAME)
    def write_cas_blob(self, digest_hash, digest_size, requests):
        if self.__read_only:
            raise PermissionDeniedError(
                f"ByteStream instance {self._instance_name} is read-only")

        if len(digest_hash) != HASH_LENGTH or not digest_size.isdigit():
            raise InvalidArgumentError(f"Invalid digest [{digest_hash}/{digest_size}]")

        digest = re_pb2.Digest(hash=digest_hash, size_bytes=int(digest_size))

        with Distribution(metric_name=CAS_BYTESTREAM_WRITE_SIZE_BYTES,
                          instance_name=self._instance_name) as metric_blob_size:
            metric_blob_size.count = float(digest.size_bytes)

        if self.__storage.has_blob(digest):
            # According to the REAPI specification:
            # "When attempting an upload, if another client has already
            # completed the upload (which may occur in the middle of a single
            # upload if another client uploads the same blob concurrently),
            # the request will terminate immediately [...]".
            #
            # However, half-closing the stream can be problematic with some
            # intermediaries like HAProxy.
            # (https://github.com/haproxy/haproxy/issues/1219)
            #
            # If half-closing the stream is not allowed, we read and drop
            # all the client's messages before returning, still saving
            # the cost of a write to storage.
            if self.__disable_overwrite_early_return:
                try:
                    for request in requests:
                        if request.finish_write:
                            break
                        continue
                except RpcError:
                    msg = "ByteStream client disconnected whilst streaming requests, upload cancelled."
                    self._logger.debug(msg)
                    raise RetriableError(msg, retry_period=timedelta(seconds=STREAM_ERROR_RETRY_PERIOD))

            return WriteResponse(committed_size=digest.size_bytes)

        write_session = self.__storage.begin_write(digest)

        # Start the write session and write the first request's data.
        with Counter(metric_name=CAS_UPLOADED_BYTES_METRIC_NAME,
                     instance_name=self._instance_name) as bytes_counter:
            computed_hash = HASH()

            # Handle subsequent write requests.
            try:
                for request in requests:
                    write_session.write(request.data)

                    computed_hash.update(request.data)
                    bytes_counter.increment(len(request.data))

                    if request.finish_write:
                        break
            except RpcError:
                write_session.close()
                msg = "ByteStream client disconnected whilst streaming requests, upload cancelled."
                self._logger.debug(msg)
                raise RetriableError(msg, retry_period=timedelta(seconds=STREAM_ERROR_RETRY_PERIOD))

            # Check that the data matches the provided digest.
            if bytes_counter.count != digest.size_bytes:
                raise NotImplementedError(
                    "Cannot close stream before finishing write, "
                    f"got {bytes_counter.count} bytes but expected {digest.size_bytes}")

            if computed_hash.hexdigest() != digest.hash:
                raise InvalidArgumentError("Data does not match hash")

            self.__storage.commit_write(digest, write_session)
            return WriteResponse(committed_size=int(bytes_counter.count))

    def write_logstream(self, resource_name: str, first_request: WriteRequest,
                        requests: List[WriteRequest]) -> WriteResponse:
        if self._stream_store is None:
            raise InvalidArgumentError(
                "ByteStream instance not configured for use with LogStream.")

        with Counter(metric_name=LOGSTREAM_WRITE_UPLOADED_BYTES_COUNT,
                     instance_name=self.instance_name) as bytes_counter:
            self._stream_store.append_to_stream(resource_name, first_request.data)
            bytes_counter.increment(len(first_request.data))

            for request in requests:
                self._stream_store.append_to_stream(resource_name, request.data)
                bytes_counter.increment(len(request.data))

            self._stream_store.append_to_stream(resource_name, None, mark_finished=True)
            return WriteResponse(committed_size=int(bytes_counter.count))

    def query_logstream_status(self, resource_name: str,
                               context) -> QueryWriteStatusResponse:
        if self._stream_store is None:
            raise InvalidArgumentError(
                "ByteStream instance not configured for use with LogStream.")

        while context.is_active():
            if self._stream_store.wait_for_streaming_clients(resource_name, self._query_activity_timeout):
                streamlength = self._stream_store.writeable_stream_length(resource_name)
                return QueryWriteStatusResponse(committed_size=streamlength.length, complete=streamlength.finished)

        raise NotFoundError(f"Stream [{resource_name}] didn't become writeable.")
