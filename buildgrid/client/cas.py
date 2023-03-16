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
#
# pylint: disable=anomalous-backslash-in-string


from collections import namedtuple
from contextlib import contextmanager
from io import SEEK_END, BytesIO
import os
from typing import BinaryIO, Dict, List, Optional, Set, Tuple
import uuid

import grpc
from google.protobuf.message import Message

import buildgrid.server.context as context_module
from buildgrid._exceptions import NotFoundError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2, remote_execution_pb2_grpc
from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import (
    Digest,
    Directory
)
from buildgrid._protos.google.bytestream import bytestream_pb2, bytestream_pb2_grpc
from buildgrid._protos.google.rpc import code_pb2
from buildgrid.client.capabilities import CapabilitiesInterface
from buildgrid.client.retrier import GrpcRetrier
from buildgrid.settings import HASH, MAX_REQUEST_SIZE, MAX_REQUEST_COUNT, BATCH_REQUEST_SIZE_THRESHOLD
from buildgrid.utils import create_digest_from_file, merkle_tree_maker


_FileRequest = namedtuple('_FileRequest', ['digest', 'output_paths'])


class _CallCache:
    """Per remote grpc.StatusCode.UNIMPLEMENTED call cache."""
    __calls: Dict[grpc.Channel, Set[str]] = {}

    @classmethod
    def mark_unimplemented(cls, channel, name):
        if channel not in cls.__calls:
            cls.__calls[channel] = set()
        cls.__calls[channel].add(name)

    @classmethod
    def unimplemented(cls, channel, name):
        if channel not in cls.__calls:
            return False
        return name in cls.__calls[channel]


class _CasBatchRequestSizesCache:
    """Cache that stores, for each remote, the limit of bytes that can
    be transferred using batches as well as a threshold that determines
    when a file can be fetched as part of a batch request.
    """
    __cas_max_batch_transfer_size: Dict[grpc.Channel, Dict[str, int]] = {}
    __cas_batch_request_size_threshold: Dict[grpc.Channel, Dict[str, int]] = {}

    @classmethod
    def max_effective_batch_size_bytes(cls, channel: grpc.Channel, instance_name: str) -> int:
        """Returns the maximum number of bytes that can be transferred
        using batch methods for the given remote.
        """
        if channel not in cls.__cas_max_batch_transfer_size:
            cls.__cas_max_batch_transfer_size[channel] = {}

        if instance_name not in cls.__cas_max_batch_transfer_size[channel]:
            max_batch_size = cls._get_server_max_batch_total_size_bytes(channel,
                                                                        instance_name)

            cls.__cas_max_batch_transfer_size[channel][instance_name] = max_batch_size

        return cls.__cas_max_batch_transfer_size[channel][instance_name]

    @classmethod
    def batch_request_size_threshold(cls, channel: grpc.Channel, instance_name: str) -> int:
        if channel not in cls.__cas_batch_request_size_threshold:
            cls.__cas_batch_request_size_threshold[channel] = {}

        if instance_name not in cls.__cas_batch_request_size_threshold[channel]:
            # Computing the threshold:
            max_batch_size = cls.max_effective_batch_size_bytes(channel,
                                                                instance_name)
            threshold = int(BATCH_REQUEST_SIZE_THRESHOLD * max_batch_size)

            cls.__cas_batch_request_size_threshold[channel][instance_name] = threshold

        return cls.__cas_batch_request_size_threshold[channel][instance_name]

    @classmethod
    def _get_server_max_batch_total_size_bytes(cls, channel: grpc.Channel, instance_name: str) -> int:
        """Returns the maximum number of bytes that can be effectively
        transferred using batches, considering the limits imposed by
        the server's configuration and by gRPC.
        """
        try:
            capabilities_interface = CapabilitiesInterface(channel)
            server_capabilities = capabilities_interface.get_capabilities(instance_name)

            cache_capabilities = server_capabilities.cache_capabilities

            max_batch_total_size = cache_capabilities.max_batch_total_size_bytes
            # The server could set this value to 0 (no limit set).
            if max_batch_total_size:
                return min(max_batch_total_size, MAX_REQUEST_SIZE)
        except ConnectionError:
            pass

        return MAX_REQUEST_SIZE


@contextmanager
def download(channel,
             instance: str = None,
             u_uid: str = None,
             retries: int = 0,
             max_backoff: int = 64,
             should_backoff: bool = True):
    """Context manager generator for the :class:`Downloader` class."""
    downloader = Downloader(channel,
                            instance=instance,
                            retries=retries,
                            max_backoff=max_backoff,
                            should_backoff=should_backoff)
    try:
        yield downloader
    finally:
        downloader.close()


class Downloader:
    """Remote CAS files, directories and messages download helper.

    The :class:`Downloader` class comes with a generator factory function that
    can be used together with the `with` statement for context management::

        from buildgrid.client.cas import download

        with download(channel, instance='build') as downloader:
            downloader.get_message(message_digest)
    """

    def __init__(self,
                 channel: grpc.Channel,
                 instance: str = None,
                 retries: int = 0,
                 max_backoff: int = 64,
                 should_backoff: bool = True):
        """Initializes a new :class:`Downloader` instance.

        Args:
            channel (grpc.Channel): A gRPC channel to the CAS endpoint.
            instance (str, optional): the targeted instance's name.
        """
        self.channel = channel

        self.instance_name = instance

        self._grpc_retrier = GrpcRetrier(retries=retries, max_backoff=max_backoff, should_backoff=should_backoff)

        self.__bytestream_stub = bytestream_pb2_grpc.ByteStreamStub(self.channel)
        self.__cas_stub = remote_execution_pb2_grpc.ContentAddressableStorageStub(self.channel)

        self.__file_requests: Dict[str, _FileRequest] = {}
        self.__file_request_count = 0
        self.__file_request_size = 0
        self.__file_response_size = 0

    # --- Public API ---

    def get_blob(self, digest: Digest) -> Optional[str]:
        """Retrieves a blob from the remote CAS server.

        Args:
            digest (:obj:`Digest`): the blob's digest to fetch.

        Returns:
            bytearray: the fetched blob data or None if not found.
        """
        try:
            blob = self._grpc_retrier.retry(self._fetch_blob, digest)
        except NotFoundError:
            return None

        return blob

    def get_blobs(self, digests: List[Digest]) -> List[str]:
        """Retrieves a list of blobs from the remote CAS server.

        Args:
            digests (list): list of :obj:`Digest`\ s for the blobs to fetch.

        Returns:
            list: the fetched blob data list.

        Raises:
            NotFoundError: if a blob is not present in the remote CAS server.
        """
        # _fetch_blob_batch returns (data, digest) pairs.
        # We only want the data.
        return [result[0] for result in self._grpc_retrier.retry(self._fetch_blob_batch, digests)]

    def get_available_blobs(self, digests: List[Digest]) -> List[Tuple[str, Digest]]:
        """Retrieves a list of blobs from the remote CAS server.

        Skips blobs not found on the server without raising an error.

        Args:
            digests (list): list of :obj:`Digest`\ s for the blobs to fetch.

        Returns:
            list: the fetched blob data list as (data, digest) pairs
        """
        return self._grpc_retrier.retry(self._fetch_blob_batch, digests, skip_unavailable=True)

    def get_message(self, digest: Digest, message: Message) -> Message:
        """Retrieves a :obj:`Message` from the remote CAS server.

        Args:
            digest (:obj:`Digest`): the message's digest to fetch.
            message (:obj:`Message`): an empty message to fill.

        Returns:
            :obj:`Message`: `message` filled or emptied if not found.
        """
        try:
            message_blob = self._grpc_retrier.retry(self._fetch_blob, digest)
        except NotFoundError:
            message_blob = None

        if message_blob is not None:
            message.ParseFromString(message_blob)
        else:
            message.Clear()

        return message

    def get_messages(self, digests: List[Digest], messages: List[Message]) -> List[Message]:
        """Retrieves a list of :obj:`Message`\ s from the remote CAS server.

        Note:
            The `digests` and `messages` list **must** contain the same number
            of elements.

        Args:
            digests (list):  list of :obj:`Digest`\ s for the messages to fetch.
            messages (list): list of empty :obj:`Message`\ s to fill.

        Returns:
            list: the fetched and filled message list.
        """
        assert len(digests) == len(messages)

        # The individual empty messages might be of differing types, so we need
        # to set up a mapping
        digest_message_map = {digest.hash: message for (digest, message) in zip(digests, messages)}

        batch_response = self._grpc_retrier.retry(self._fetch_blob_batch, digests)

        messages = []
        for message_blob, message_digest in batch_response:
            message = digest_message_map[message_digest.hash]
            message.ParseFromString(message_blob)
            messages.append(message)

        return messages

    def download_file(self, digest: Digest, file_path: str, is_executable: bool = False, queue: bool = True):
        """Retrieves a file from the remote CAS server.

        If queuing is allowed (`queue=True`), the download request **may** be
        defer. An explicit call to :func:`~flush` can force the request to be
        send immediately (along with the rest of the queued batch).

        Args:
            digest (:obj:`Digest`): the file's digest to fetch.
            file_path (str): absolute or relative path to the local file to write.
            is_executable (bool): whether the file is executable or not.
            queue (bool, optional): whether or not the download request may be
                queued and submitted as part of a batch upload request. Defaults
                to True.

        Raises:
            NotFoundError: if `digest` is not present in the remote CAS server.
            OSError: if `file_path` does not exist or is not readable.
        """
        if not os.path.isabs(file_path):
            file_path = os.path.abspath(file_path)

        if not queue or digest.size_bytes > self._queueable_file_size_threshold():
            self._grpc_retrier.retry(self._fetch_file, digest, file_path, is_executable)
        else:
            self._queue_file(digest, file_path, is_executable=is_executable)

    def download_directory(self, digest: Digest, directory_path: str):
        """Retrieves a :obj:`Directory` from the remote CAS server.

        Args:
            digest (:obj:`Digest`): the directory's digest to fetch.
            directory_path (str): the path to download to

        Raises:
            NotFoundError: if `digest` is not present in the remote CAS server.
            FileExistsError: if `directory_path` already contains parts of their
                fetched directory's content.
        """
        if not os.path.isabs(directory_path):
            directory_path = os.path.abspath(directory_path)

        self._grpc_retrier.retry(self._fetch_directory, digest, directory_path)

    def flush(self):
        """Ensures any queued request gets sent."""
        if self.__file_requests:
            self._grpc_retrier.retry(self._fetch_file_batch, self.__file_requests)

            self.__file_requests.clear()
            self.__file_request_count = 0
            self.__file_request_size = 0
            self.__file_response_size = 0

    def close(self):
        """Closes the underlying connection stubs.

        Note:
            This will always send pending requests before closing connections,
            if any.
        """
        self.flush()

        self.__bytestream_stub = None
        self.__cas_stub = None

    # --- Private API ---

    def _fetch_blob(self, digest: Digest) -> bytearray:
        """Fetches a blob using ByteStream.Read()"""

        if self.instance_name:
            resource_name = '/'.join([self.instance_name, 'blobs',
                                      digest.hash, str(digest.size_bytes)])
        else:
            resource_name = '/'.join(['blobs', digest.hash, str(digest.size_bytes)])

        read_blob = bytearray()
        read_request = bytestream_pb2.ReadRequest()
        read_request.resource_name = resource_name
        read_request.read_offset = 0

        for read_response in (self.__bytestream_stub.Read(read_request,
                                                          metadata=context_module.metadata_list())):
            read_blob += read_response.data

        assert len(read_blob) == digest.size_bytes
        return read_blob

    def _fetch_blob_batch(self, digests: List[Digest], *, skip_unavailable=False) -> List[Tuple[str, Digest]]:
        """Fetches blobs using ContentAddressableStorage.BatchReadBlobs()
           Returns (data, digest) pairs"""
        batch_fetched = False
        read_blobs = []

        # First, try BatchReadBlobs(), if not already known not being implemented:
        if not _CallCache.unimplemented(self.channel, 'BatchReadBlobs'):
            batch_request = remote_execution_pb2.BatchReadBlobsRequest()
            batch_request.digests.extend(digests)
            if self.instance_name is not None:
                batch_request.instance_name = self.instance_name

            try:
                batch_response = (self.__cas_stub.BatchReadBlobs(batch_request,
                                                                 metadata=context_module.metadata_list()))

                for response in batch_response.responses:
                    assert response.digest in digests

                    if response.status.code == code_pb2.OK:
                        read_blobs.append((response.data, response.digest))
                    elif response.status.code == code_pb2.NOT_FOUND:
                        if not skip_unavailable:
                            raise NotFoundError('Requested blob does not exist '
                                                'on the remote.')
                    else:
                        raise ConnectionError('Error in CAS reply while fetching blob.')

                batch_fetched = True

            except grpc.RpcError as e:
                status_code = e.code()
                if status_code == grpc.StatusCode.UNIMPLEMENTED:
                    _CallCache.mark_unimplemented(self.channel, 'BatchReadBlobs')
                elif status_code == grpc.StatusCode.INVALID_ARGUMENT:
                    read_blobs.clear()
                else:
                    raise

        # Fallback to Read() if no BatchReadBlobs():
        if not batch_fetched:
            for digest in digests:
                blob = self._grpc_retrier.retry(self._fetch_blob, digest)
                read_blobs.append((blob, digest))

        return read_blobs

    def _fetch_file(self, digest: Digest, file_path: str, is_executable: bool = False):
        """Fetches a file using ByteStream.Read()"""
        if self.instance_name:
            resource_name = '/'.join([self.instance_name, 'blobs',
                                      digest.hash, str(digest.size_bytes)])
        else:
            resource_name = '/'.join(['blobs', digest.hash, str(digest.size_bytes)])

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        read_request = bytestream_pb2.ReadRequest()
        read_request.resource_name = resource_name
        read_request.read_offset = 0

        with open(file_path, 'wb') as byte_file:
            for read_response in (self.__bytestream_stub.Read(read_request,
                                                              metadata=context_module.metadata_list())):
                byte_file.write(read_response.data)

            assert byte_file.tell() == digest.size_bytes

        if is_executable:
            os.chmod(file_path, 0o755)  # rwxr-xr-x / 755

    def _queue_file(self, digest: Digest, file_path: str, is_executable: bool = False):
        """Queues a file for later batch download"""
        batch_size_limit = self._max_effective_batch_size_bytes()

        if self.__file_request_size + digest.ByteSize() > batch_size_limit:
            self.flush()
        elif self.__file_response_size + digest.size_bytes > batch_size_limit:
            self.flush()
        elif self.__file_request_count >= MAX_REQUEST_COUNT:
            self.flush()

        output_path = (file_path, is_executable)

        # When queueing a file we take into account the cases where
        # we might want to download the same digest to different paths.
        if digest.hash not in self.__file_requests:
            request = _FileRequest(digest=digest, output_paths=[output_path])
            self.__file_requests[digest.hash] = request

            self.__file_request_count += 1
            self.__file_request_size += digest.ByteSize()
            self.__file_response_size += digest.size_bytes
        else:
            # We already have that hash queued; we'll fetch the blob
            # once and write copies of it:
            self.__file_requests[digest.hash].output_paths.append(output_path)

    def _fetch_file_batch(self, requests):
        """Sends queued data using ContentAddressableStorage.BatchReadBlobs().

        Takes a dictionary (digest.hash, _FileRequest) as input.
        """
        batch_digests = [request.digest for request in requests.values()]
        batch_responses = self._fetch_blob_batch(batch_digests)

        for file_blob, file_digest in batch_responses:
            output_paths = requests[file_digest.hash].output_paths

            for file_path, is_executable in output_paths:
                os.makedirs(os.path.dirname(file_path), exist_ok=True)

                with open(file_path, 'wb') as byte_file:
                    byte_file.write(file_blob)

                if is_executable:
                    os.chmod(file_path, 0o755)  # rwxr-xr-x / 755

    def _fetch_directory(self, digest: Digest, directory_path: str):
        """Fetches a file using ByteStream.GetTree()"""
        # Better fail early if the local root path cannot be created:
        os.makedirs(directory_path, exist_ok=True)

        directories = {}
        directory_fetched = False
        # First, try GetTree() if not known to be unimplemented yet:
        if not _CallCache.unimplemented(self.channel, 'GetTree'):
            tree_request = remote_execution_pb2.GetTreeRequest()
            tree_request.root_digest.CopyFrom(digest)
            tree_request.page_size = MAX_REQUEST_COUNT
            if self.instance_name is not None:
                tree_request.instance_name = self.instance_name

            try:
                for tree_response in self.__cas_stub.GetTree(tree_request):
                    for directory in tree_response.directories:
                        directory_blob = directory.SerializeToString()
                        directory_hash = HASH(directory_blob).hexdigest()

                        directories[directory_hash] = directory

                assert digest.hash in directories

                directory = directories[digest.hash]
                self._write_directory(directory, directory_path, directories=directories)

                directory_fetched = True
            except grpc.RpcError as e:
                status_code = e.code()
                if status_code == grpc.StatusCode.UNIMPLEMENTED:
                    _CallCache.mark_unimplemented(self.channel, 'GetTree')

                else:
                    raise

        # If no GetTree(), _write_directory() will use BatchReadBlobs()
        # if available or Read() if not.
        if not directory_fetched:
            directory = remote_execution_pb2.Directory()
            directory.ParseFromString(self._grpc_retrier.retry(self._fetch_blob, digest))

            self._write_directory(directory, directory_path)

    def _write_directory(self, root_directory: Directory, root_path: str, directories: Dict[str, Directory] = None):
        """Generates a local directory structure"""
        os.makedirs(root_path, exist_ok=True)
        self._write_directory_recursively(root_directory, root_path, directories=None)

    def _write_directory_recursively(self,
                                     root_directory: Directory,
                                     root_path: str,
                                     directories: Dict[str, Directory] = None):
        """Generate local directory recursively"""
        # i) Files:
        for file_node in root_directory.files:
            file_path = os.path.join(root_path, file_node.name)

            if os.path.lexists(file_path):
                raise FileExistsError(f"'{file_path}' already exists")

            self.download_file(file_node.digest, file_path,
                               is_executable=file_node.is_executable)
        self.flush()

        # ii) Directories:
        pending_directory_digests = []
        pending_directory_paths = {}
        for directory_node in root_directory.directories:
            directory_hash = directory_node.digest.hash

            # FIXME: Guard against ../
            directory_path = os.path.join(root_path, directory_node.name)
            os.mkdir(directory_path)

            if directories and directory_node.digest.hash in directories:
                # We already have the directory; just write it:
                directory = directories[directory_hash]

                self._write_directory_recursively(directory, directory_path,
                                                  directories=directories)
            else:
                # Gather all the directories that we need to get to
                # try fetching them in a single batch request:
                if directory_hash not in pending_directory_paths:
                    pending_directory_digests.append(directory_node.digest)
                    pending_directory_paths[directory_hash] = [directory_path]
                else:
                    pending_directory_paths[directory_hash].append(directory_path)

        if pending_directory_paths:
            fetched_blobs = self._grpc_retrier.retry(self._fetch_blob_batch, pending_directory_digests)

            for (directory_blob, directory_digest) in fetched_blobs:
                directory = remote_execution_pb2.Directory()
                directory.ParseFromString(directory_blob)

                # Assuming that the server might not return the blobs in
                # the same order than they were asked for, we read
                # the hashes of the returned blobs:
                # Guarantees for the reply orderings might change in
                # the specification at some point.
                # See: github.com/bazelbuild/remote-apis/issues/52

                for directory_path in pending_directory_paths[directory_digest.hash]:
                    self._write_directory(directory, directory_path,
                                          directories=directories)

        # iii) Symlinks:
        for symlink_node in root_directory.symlinks:
            symlink_path = os.path.join(root_path, symlink_node.name)
            os.symlink(symlink_node.target, symlink_path)

    def _max_effective_batch_size_bytes(self):
        """Returns the effective maximum number of bytes that can be
        transferred using batches, considering gRPC maximum message size.
        """
        return _CasBatchRequestSizesCache.max_effective_batch_size_bytes(self.channel,
                                                                         self.instance_name)

    def _queueable_file_size_threshold(self):
        """Returns the size limit up until which files can be queued to
        be requested in a batch.
        """
        return _CasBatchRequestSizesCache.batch_request_size_threshold(self.channel,
                                                                       self.instance_name)


@contextmanager
def upload(channel: grpc.Channel,
           instance: str = None,
           u_uid: str = None,
           retries: int = 0,
           max_backoff: int = 64,
           should_backoff: bool = True):
    """Context manager generator for the :class:`Uploader` class."""
    uploader = Uploader(channel,
                        instance=instance,
                        u_uid=u_uid,
                        retries=retries,
                        max_backoff=max_backoff,
                        should_backoff=should_backoff)
    try:
        yield uploader
    finally:
        uploader.close()


class Uploader:
    """Remote CAS files, directories and messages upload helper.

    The :class:`Uploader` class comes with a generator factory function that can
    be used together with the `with` statement for context management::

        from buildgrid.client.cas import upload

        with upload(channel, instance='build') as uploader:
            uploader.upload_file('/path/to/local/file')
    """

    def __init__(self,
                 channel: grpc.Channel,
                 instance: str = None,
                 u_uid: str = None,
                 retries: int = 0,
                 max_backoff: int = 64,
                 should_backoff: bool = True):
        """Initializes a new :class:`Uploader` instance.

        Args:
            channel (grpc.Channel): A gRPC channel to the CAS endpoint.
            instance (str, optional): the targeted instance's name.
            u_uid (str, optional): a UUID for CAS transactions.
        """
        self.channel = channel

        self.instance_name = instance
        if u_uid is not None:
            self.u_uid = u_uid
        else:
            self.u_uid = str(uuid.uuid4())

        self._grpc_retrier = GrpcRetrier(retries=retries, max_backoff=max_backoff, should_backoff=should_backoff)

        self.__bytestream_stub = bytestream_pb2_grpc.ByteStreamStub(self.channel)
        self.__cas_stub = remote_execution_pb2_grpc.ContentAddressableStorageStub(self.channel)

        self.__requests: Dict[str, Tuple[bytes, Digest]] = {}
        self.__request_count = 0
        self.__request_size = 0

    # --- Public API ---

    def put_blob(
        self,
        blob: BinaryIO,
        digest: Digest = None,
        queue: bool = False,
        length: Optional[int] = None
    ) -> Digest:
        """Stores a blob into the remote CAS server.

        If queuing is allowed (`queue=True`), the upload request **may** be
        defer. An explicit call to :func:`~flush` can force the request to be
        send immediately (along with the rest of the queued batch).

        The caller should set at least one of ``digest`` or ``length`` to
        allow the uploader to skip determining the size of the blob using
        multiple seeks.

        Args:
            blob (BinaryIO): a file-like object containing the blob.
            digest (:obj:`Digest`, optional): the blob's digest.
            queue (bool, optional): whether or not the upload request may be
                queued and submitted as part of a batch upload request. Defaults
                to False.
            length (int, optional): The size of the blob, in bytes. If ``digest``
                is also set, this is ignored in favour of ``digest.size_bytes``.

        Returns:
            :obj:`Digest`: the sent blob's digest.
        """
        if digest is not None:
            length = digest.size_bytes
        elif length is None:
            # If neither the digest or the length were set, fall back to
            # seeking to the end of the object to find the length
            blob.seek(0, SEEK_END)
            length = blob.tell()
            blob.seek(0)

        if not queue or length > self._queueable_file_size_threshold():
            blob_digest = self._grpc_retrier.retry(self._send_blob, blob, digest)
        else:
            blob_digest = self._queue_blob(blob.read(), digest=digest)

        return blob_digest

    def put_message(self, message: Message, digest: Digest = None, queue: bool = False) -> Digest:
        """Stores a message into the remote CAS server.

        If queuing is allowed (`queue=True`), the upload request **may** be
        defer. An explicit call to :func:`~flush` can force the request to be
        send immediately (along with the rest of the queued batch).

        Args:
            message (:obj:`Message`): the message object.
            digest (:obj:`Digest`, optional): the message's digest.
            queue (bool, optional): whether or not the upload request may be
                queued and submitted as part of a batch upload request. Defaults
                to False.

        Returns:
            :obj:`Digest`: the sent message's digest.
        """
        message_blob = message.SerializeToString()

        if not queue or len(message_blob) > self._queueable_file_size_threshold():
            message_digest = self._grpc_retrier.retry(self._send_blob, BytesIO(message_blob), digest)
        else:
            message_digest = self._queue_blob(message_blob, digest=digest)

        return message_digest

    def upload_file(self, file_path: str, queue: bool = True) -> Digest:
        """Stores a local file into the remote CAS storage.

        If queuing is allowed (`queue=True`), the upload request **may** be
        defer. An explicit call to :func:`~flush` can force the request to be
        send immediately (allong with the rest of the queued batch).

        Args:
            file_path (str): absolute or relative path to a local file.
            queue (bool, optional): whether or not the upload request may be
                queued and submitted as part of a batch upload request. Defaults
                to True.

        Returns:
            :obj:`Digest`: The digest of the file's content.

        Raises:
            FileNotFoundError: If `file_path` does not exist.
            PermissionError: If `file_path` is not readable.
        """
        if not os.path.isabs(file_path):
            file_path = os.path.abspath(file_path)

        with open(file_path, 'rb') as file_object:
            if not queue or os.path.getsize(file_path) > self._queueable_file_size_threshold():
                file_digest = self._grpc_retrier.retry(self._send_blob, file_object)
            else:
                file_digest = self._queue_blob(file_object.read())

        return file_digest

    def upload_directory(self, directory_path: str, queue: bool = True) -> Digest:
        """Stores a local folder into the remote CAS storage.

        If queuing is allowed (`queue=True`), the upload request **may** be
        defer. An explicit call to :func:`~flush` can force the request to be
        send immediately (allong with the rest of the queued batch).

        Args:
            directory_path (str): absolute or relative path to a local folder.
            queue (bool, optional): wheter or not the upload requests may be
                queued and submitted as part of a batch upload request. Defaults
                to True.

        Returns:
            :obj:`Digest`: The digest of the top :obj:`Directory`.

        Raises:
            FileNotFoundError: If `directory_path` does not exist.
            PermissionError: If `directory_path` is not readable.
        """
        if not os.path.isabs(directory_path):
            directory_path = os.path.abspath(directory_path)

        if not queue:
            for node, blob, _ in merkle_tree_maker(directory_path):
                if node.DESCRIPTOR is remote_execution_pb2.DirectoryNode.DESCRIPTOR:
                    last_directory_node = node

                self._grpc_retrier.retry(self._send_blob, blob, node.digest)

        else:
            for node, blob, _ in merkle_tree_maker(directory_path):
                if node.DESCRIPTOR is remote_execution_pb2.DirectoryNode.DESCRIPTOR:
                    last_directory_node = node

                if node.digest.size_bytes > self._queueable_file_size_threshold():
                    self._grpc_retrier.retry(self._send_blob, blob, node.digest)
                else:
                    self._queue_blob(blob.read(), digest=node.digest)

        return last_directory_node.digest

    def upload_tree(self, directory_path: str, queue: bool = True) -> Digest:
        """Stores a local folder into the remote CAS storage as a :obj:`Tree`.

        If queuing is allowed (`queue=True`), the upload request **may** be
        defer. An explicit call to :func:`~flush` can force the request to be
        send immediately (allong with the rest of the queued batch).

        Args:
            directory_path (str): absolute or relative path to a local folder.
            queue (bool, optional): wheter or not the upload requests may be
                queued and submitted as part of a batch upload request. Defaults
                to True.

        Returns:
            :obj:`Digest`: The digest of the :obj:`Tree`.

        Raises:
            FileNotFoundError: If `directory_path` does not exist.
            PermissionError: If `directory_path` is not readable.
        """
        if not os.path.isabs(directory_path):
            directory_path = os.path.abspath(directory_path)

        directories = []

        if not queue:
            for node, blob, _ in merkle_tree_maker(directory_path):
                if node.DESCRIPTOR is remote_execution_pb2.DirectoryNode.DESCRIPTOR:
                    # TODO: Get the Directory object from merkle_tree_maker():
                    directory = remote_execution_pb2.Directory()
                    directory.ParseFromString(blob.read())
                    directories.append(directory)

                self._grpc_retrier.retry(self._send_blob, blob, node.digest)

        else:
            for node, blob, _ in merkle_tree_maker(directory_path):
                if node.DESCRIPTOR is remote_execution_pb2.DirectoryNode.DESCRIPTOR:
                    # TODO: Get the Directory object from merkle_tree_maker():
                    directory = remote_execution_pb2.Directory()
                    directory.ParseFromString(blob.read())
                    directories.append(directory)

                if node.digest.size_bytes > self._queueable_file_size_threshold():
                    self._grpc_retrier.retry(self._send_blob, blob, node.digest)
                else:
                    self._queue_blob(blob.read(), digest=node.digest)

        tree = remote_execution_pb2.Tree()
        tree.root.CopyFrom(directories[-1])
        tree.children.extend(directories[:-1])

        return self.put_message(tree, queue=queue)

    def flush(self):
        """Ensures any queued request gets sent."""
        if self.__requests:
            self._grpc_retrier.retry(self._send_blob_batch, self.__requests)

            self.__requests.clear()
            self.__request_count = 0
            self.__request_size = 0

    def close(self):
        """Closes the underlying connection stubs.

        Note:
            This will always send pending requests before closing connections,
            if any.
        """
        self.flush()

        self.__bytestream_stub = None
        self.__cas_stub = None

    # --- Private API ---

    def _send_blob(self, blob: BinaryIO, digest: Digest = None) -> Digest:
        """Sends a memory block using ByteStream.Write()"""
        blob.seek(0)
        blob_digest = Digest()
        if digest is not None:
            blob_digest.CopyFrom(digest)
        else:
            blob_digest = create_digest_from_file(blob)

        if self.instance_name:
            resource_name = '/'.join([self.instance_name, 'uploads', self.u_uid, 'blobs',
                                      blob_digest.hash, str(blob_digest.size_bytes)])
        else:
            resource_name = '/'.join(['uploads', self.u_uid, 'blobs',
                                      blob_digest.hash, str(blob_digest.size_bytes)])

        def __write_request_stream(resource, content: BinaryIO):
            offset = 0
            finished = False
            remaining = blob_digest.size_bytes - offset
            while not finished:
                chunk_size = min(remaining, MAX_REQUEST_SIZE)
                remaining -= chunk_size

                request = bytestream_pb2.WriteRequest()
                request.resource_name = resource
                request.data = content.read(chunk_size)
                request.write_offset = offset
                request.finish_write = remaining <= 0

                yield request

                offset += chunk_size
                finished = request.finish_write

        write_requests = __write_request_stream(resource_name, blob)

        write_response = (self.__bytestream_stub.Write(write_requests,
                                                       metadata=context_module.metadata_list()))

        assert write_response.committed_size == blob_digest.size_bytes

        return blob_digest

    def _queue_blob(self, blob: bytes, digest: Digest = None) -> Digest:
        """Queues a memory block for later batch upload"""
        blob_digest = Digest()
        if digest is not None:
            blob_digest.CopyFrom(digest)
        else:
            blob_digest.hash = HASH(blob).hexdigest()
            blob_digest.size_bytes = len(blob)

        # If we are here queueing a file we know that its size is
        # smaller than gRPC's message size limit.
        # We'll make a single batch request as big as the server allows.
        batch_size_limit = self._max_effective_batch_size_bytes()

        if self.__request_size + blob_digest.size_bytes > batch_size_limit:
            self.flush()
        elif self.__request_count >= MAX_REQUEST_COUNT:
            self.flush()

        self.__requests[blob_digest.hash] = (blob, blob_digest)
        self.__request_count += 1
        self.__request_size += blob_digest.size_bytes

        return blob_digest

    def _send_blob_batch(self, batch: Dict[str, Tuple[bytes, Digest]]) -> List[str]:
        """Sends queued data using ContentAddressableStorage.BatchUpdateBlobs()"""
        batch_fetched = False
        written_digests = []

        # First, try BatchUpdateBlobs(), if not already known not being implemented:
        if not _CallCache.unimplemented(self.channel, 'BatchUpdateBlobs'):
            batch_request = remote_execution_pb2.BatchUpdateBlobsRequest()
            if self.instance_name is not None:
                batch_request.instance_name = self.instance_name

            for blob, digest in batch.values():
                request = batch_request.requests.add()
                request.digest.CopyFrom(digest)
                request.data = blob

            try:
                batch_response = (self.__cas_stub.BatchUpdateBlobs(batch_request,
                                                                   metadata=context_module.metadata_list()))

                for response in batch_response.responses:
                    assert response.digest.hash in batch

                    written_digests.append(response.digest)
                    if response.status.code != code_pb2.OK:
                        response.digest.Clear()

                batch_fetched = True

            except grpc.RpcError as e:
                status_code = e.code()
                if status_code == grpc.StatusCode.UNIMPLEMENTED:
                    _CallCache.mark_unimplemented(self.channel, 'BatchUpdateBlobs')

                elif status_code == grpc.StatusCode.INVALID_ARGUMENT:
                    written_digests.clear()
                    batch_fetched = False

                else:
                    raise

        # Fallback to Write() if no BatchUpdateBlobs():
        if not batch_fetched:
            for blob, digest in batch.values():
                written_digests.append(self._send_blob(BytesIO(blob)))

        return written_digests

    def _max_effective_batch_size_bytes(self):
        """Returns the effective maximum number of bytes that can be
        transferred using batches, considering gRPC maximum message size.
        """
        return _CasBatchRequestSizesCache.max_effective_batch_size_bytes(self.channel,
                                                                         self.instance_name)

    def _queueable_file_size_threshold(self):
        """Returns the size limit up until which files can be queued to
        be requested in a batch.
        """
        return _CasBatchRequestSizesCache.batch_request_size_threshold(self.channel,
                                                                       self.instance_name)
