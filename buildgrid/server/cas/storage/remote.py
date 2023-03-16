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
RemoteStorage
==================

Forwwards storage requests to a remote storage.
"""

import io
import logging
from tempfile import NamedTemporaryFile

import buildgrid.server.context as context_module
from buildgrid._exceptions import GrpcUninitializedError, NotFoundError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2, remote_execution_pb2_grpc
from buildgrid._protos.google.rpc import code_pb2
from buildgrid._protos.google.rpc import status_pb2
from buildgrid.client.capabilities import CapabilitiesInterface
from buildgrid.client.cas import download, upload
from buildgrid.client.channel import setup_channel
from buildgrid.settings import HASH, MAX_IN_MEMORY_BLOB_SIZE_BYTES
from .storage_abc import StorageABC


class RemoteStorage(StorageABC):

    def __init__(self, remote, instance_name, channel_options=None, tls_credentials=None, retries=0, max_backoff=64,
                 request_timeout=None):
        self.__logger = logging.getLogger(__name__)

        self.remote_instance_name = instance_name
        self._remote = remote
        self._channel_options = channel_options
        if tls_credentials is None:
            tls_credentials = {}
        self._tls_credentials = tls_credentials
        self.retries = retries
        self.max_backoff = max_backoff
        self._request_timeout = request_timeout

        self._stub_cas = None
        self.channel = None

    def setup_grpc(self):
        if self.channel is None:
            self.channel, _ = setup_channel(
                self._remote,
                auth_token=None,
                client_key=self._tls_credentials.get("tls-client-key"),
                client_cert=self._tls_credentials.get("tls-client-cert"),
                server_cert=self._tls_credentials.get("tls-server-cert"),
                timeout=self._request_timeout
            )

        if self._stub_cas is None:
            self._stub_cas = remote_execution_pb2_grpc.ContentAddressableStorageStub(self.channel)

    def get_capabilities(self):
        interface = CapabilitiesInterface(self.channel)
        capabilities = interface.get_capabilities(self.remote_instance_name)
        return capabilities.cache_capabilities

    def has_blob(self, digest):
        self.__logger.debug(f"Checking for blob: [{digest}]")
        if not self.missing_blobs([digest]):
            return True
        return False

    def get_blob(self, digest):
        if self.channel is None:
            raise GrpcUninitializedError("Remote CAS backend used before gRPC initialization.")

        self.__logger.debug(f"Getting blob: [{digest}]")
        with download(self.channel,
                      instance=self.remote_instance_name,
                      retries=self.retries,
                      max_backoff=self.max_backoff) as downloader:
            if digest.size_bytes > MAX_IN_MEMORY_BLOB_SIZE_BYTES:
                # Avoid storing the large blob completely in memory.
                temp_file = NamedTemporaryFile(delete=True)  # pylint: disable=consider-using-with
                try:
                    downloader.download_file(digest, temp_file.name, queue=False)
                except NotFoundError:
                    return None
                reader = io.BufferedReader(temp_file)
                reader.seek(0)
                return reader
            else:
                blob = downloader.get_blob(digest)
                if blob is not None:
                    return io.BytesIO(blob)
                else:
                    return None

    def delete_blob(self, digest):
        """ The REAPI doesn't have a deletion method, so we can't support
        deletion for remote storage.
        """
        raise NotImplementedError(
            "Deletion is not supported for remote storage!")

    def bulk_delete(self, digests):
        """ The REAPI doesn't have a deletion method, so we can't support
        bulk deletion for remote storage.
        """
        raise NotImplementedError(
            " Bulk deletion is not supported for remote storage!")

    def begin_write(self, digest):
        if digest.size_bytes > MAX_IN_MEMORY_BLOB_SIZE_BYTES:
            # To avoid storing the whole file in memory, write incoming uploads
            # to a temporary file.
            write_session = NamedTemporaryFile(delete=True)  # pylint: disable=consider-using-with
        else:
            # But, to maximize performance, keep blobs that are small
            # enough in-memory.
            write_session = io.BytesIO()
        return write_session

    def commit_write(self, digest, write_session):
        if self.channel is None:
            raise GrpcUninitializedError("Remote CAS backend used before gRPC initialization.")

        self.__logger.debug(f"Writing blob: [{digest}]")
        with upload(self.channel,
                    instance=self.remote_instance_name,
                    retries=self.retries,
                    max_backoff=self.max_backoff) as uploader:
            uploader.put_blob(write_session, digest=digest)

    def missing_blobs(self, digests):
        if self._stub_cas is None:
            raise GrpcUninitializedError("Remote CAS backend used before gRPC initialization.")

        if len(digests) > 100:
            self.__logger.debug(
                f"Missing blobs request for: {digests[:100]} (truncated)")
        else:
            self.__logger.debug(f"Missing blobs request for: {digests}")
        request = remote_execution_pb2.FindMissingBlobsRequest(instance_name=self.remote_instance_name)

        for blob in digests:
            request_digest = request.blob_digests.add()
            request_digest.hash = blob.hash
            request_digest.size_bytes = blob.size_bytes

        response = (self._stub_cas.FindMissingBlobs(request,
                                                    metadata=context_module.metadata_list()))

        return response.missing_blob_digests

    def bulk_update_blobs(self, blobs):
        if self._stub_cas is None:
            raise GrpcUninitializedError("Remote CAS backend used before gRPC initialization.")

        sent_digests = []
        with upload(self.channel,
                    instance=self.remote_instance_name,
                    retries=self.retries,
                    max_backoff=self.max_backoff) as uploader:
            for digest, blob in blobs:
                if len(blob) != digest.size_bytes or HASH(blob).hexdigest() != digest.hash:
                    sent_digests.append(remote_execution_pb2.Digest())
                else:
                    sent_digests.append(uploader.put_blob(io.BytesIO(blob), digest=digest, queue=True))

        assert len(sent_digests) == len(blobs)

        return [status_pb2.Status(code=code_pb2.OK) if d.ByteSize() > 0
                else status_pb2.Status(code=code_pb2.UNKNOWN) for d in sent_digests]

    def bulk_read_blobs(self, digests):
        if self._stub_cas is None:
            raise GrpcUninitializedError("Remote CAS backend used before gRPC initialization.")

        self.__logger.debug(f"Bulk read blobs request for: {digests}")
        with download(self.channel,
                      instance=self.remote_instance_name,
                      retries=self.retries,
                      max_backoff=self.max_backoff) as downloader:
            results = downloader.get_available_blobs(digests)
            # Transform List of (data, digest) pairs to expected hash-blob map
            return {result[1].hash: io.BytesIO(result[0]) for result in results}
