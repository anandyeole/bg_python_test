# Copyright (C) 2020 Bloomberg LP
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
StreamStorageABC
====================

The abstract base class for stream storage providers.
"""

import abc
from typing import Optional, Iterator, NamedTuple
from uuid import uuid4

from buildgrid._exceptions import OutOfRangeError


class StreamHandle(NamedTuple):
    name: str
    write_resource_name: str

    @staticmethod
    def _create_new(parent: str = '', prefix: str = '') -> 'StreamHandle':
        read_name = f'{parent}/logStreams/{prefix}{str(uuid4())}'
        write_name = f'{read_name}/{str(uuid4())}'
        return StreamHandle(read_name, write_name)

    @staticmethod
    def _read_name_from_write_name(write_name: str) -> str:
        return write_name[:write_name.rfind('/')]


class StreamLength(NamedTuple):
    length: int
    finished: bool


class StreamChunk(NamedTuple):
    data: bytes
    chunk_length: int
    finished: bool


class StreamStorageABC(abc.ABC):
    def create_stream(self, parent: str, prefix: str) -> StreamHandle:
        """
            Creates a stream for a specific parent with a specific prefix and a
            random UUID, and returns a StreamHandle namedtuple containing:
                name (str): The name of the string for read access
                write_resource_name (str): The secret name used only for writing
              to the stream
        """
        new_stream_handle = StreamHandle._create_new(parent, prefix)
        self._create_stream(new_stream_handle)
        return new_stream_handle

    @abc.abstractmethod
    def _create_stream(self, stream_handle: StreamHandle):
        """
            Creates a stream with a specific StreamHandle in the StreamStorage
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def stream_exists(self, stream_name: str) -> bool:
        """
            Returns True when there is a stream with the given instance/stream_name.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def stream_finished(self, stream_name: str) -> bool:
        """
            Returns True/False depending on whether the stream has been marked
          as completed.

            Raises:
                NotFoundError when a stream with the given instance/stream_name
              does not exist.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def stream_length(self, stream_name: str) -> StreamLength:
        """
            Returns a namedtuple of type StreamLength for the stream
            with the given instance/stream_name.
                length (int): The length of the stream in bytes
                finished (bool): A boolean indicating whether the stream has finished

            Raises:
                NotFoundError when a stream with the given instance/stream_name
              does not exist.
        """
        raise NotImplementedError()

    def writeable_stream_length(self, write_resource_name: str) -> StreamLength:
        """
            Returns a namedtuple of type StreamLength for the stream
            with the given instance/write_resource_name.
                length (int): The length of the stream in bytes
                finished (bool): A boolean indicating whether the stream has finished

            Raises:
                NotFoundError when a stream with the given instance/stream_name
              does not exist.
        """
        read_name = StreamHandle._read_name_from_write_name(write_resource_name)
        return self.stream_length(read_name)

    def append_to_stream(self, write_resource_name: str, message: Optional[bytes] = None, *,
                         mark_finished: Optional[bool] = False) -> None:
        """
            Appends the `message` to the stream with the passed `write_resource_name`
          as long as the stream has not been marked as finished yet and marks the stream as
          finished when the relevant arg is set to True.

            Raises:
                NotFoundError when a stream with the given instance/write_resource_name
              does not exist.
                StreamFinishedError when the stream was already marked as finished and
              no further writes are allowed.
                StorageFullError when the backing storage is full.
                WriteError when the write_resource_name is correct but
              the write failed for other reasons.
        """
        read_name = StreamHandle._read_name_from_write_name(
            write_resource_name)
        self._append_to_stream(read_name,
                               write_resource_name,
                               message,
                               mark_finished=mark_finished)

    @abc.abstractmethod
    def _append_to_stream(self, stream_name, write_resource_name: str, message: Optional[bytes] = None,
                          *, mark_finished: Optional[bool] = False) -> None:
        """
            Appends the `message` to the stream with the passed `name` and `write_resource_name`
          as long as the stream has not been marked as finished yet and marks the stream as
          finished when the relevant arg is set to True.

            Raises:
                NotFoundError when a stream with the given instance/write_resource_name
              does not exist.
                StreamFinishedError when the stream was already marked as finished and
              no further writes are allowed.
                StorageFullError when the backing storage is full.
                WriteError when the write_resource_name is correct but
              the write failed for other reasons.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def read_stream_chunk_blocking(
            self, stream_name: str, read_offset_bytes: int,
            read_limit_bytes: Optional[int] = None) -> StreamChunk:
        """
            Returns a commited chunk of the stream message or waits until it is available as follows:
                chunk = message[read_offset_bytes, min(read_limit_bytes, finished_stream_length) )

            When the `read_limit_bytes` argument is not set, this method will return the whole message
            starting from the specified offset, blocking until the stream finishes.

            In cases in which the stream has not finished and the chunk offset and size requested was
            not commited yet, this method will block and wait until the chunk is commited and/or
            the stream is finished, returning the appropriate chunk.

            Raises:
                NotFoundError when a stream with the given instance/stream_name
              does not exist.
                OutOfRangeError when a finished stream does not contain a chunk of data
              at the specified offset.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def read_stream_chunk(self, stream_name: str, read_offset_bytes: int,
                          read_limit_bytes: Optional[int] = None) -> StreamChunk:
        """
            Returns a commited chunk of the stream message or raises if it is not available as follows:
                chunk = message[read_offset_bytes, min(read_limit_bytes, finished_stream_length) )

            When the `read_limit_bytes` argument is not set, this method will return the whole message
            starting from the specified offest, raising if the stream hasn't finished.

            Raises:
                NotFoundError when a stream with the given instance/stream_name
              does not exist.
                StreamWritePendingError when the requested chunk is not fully commited.
                OutOfRangeError when a finished stream does not contain a chunk of data
              at the specified offset.
        """
        raise NotImplementedError()

    def read_stream_bytes_blocking_iterator(
            self,
            stream_name: str,
            max_chunk_size: int,
            offset: int = 0) -> Iterator[bytes]:
        """
            An iterator returning commited chunks of up to `chunk_size` until the end of the stream.
            Blocks and waits until the stream finishes.

            Can optionally start at a specific `chunk_offset`, which starts streaming data from
                (`stream_offset = chunk_offset * chunk_size`)

            Raises:
                NotFoundError when a stream with the given instance/stream_name
              does not exist.
                OutOfRangeError when a finished stream does not contain a chunk of data
              at the specified offset.
        """
        more_to_stream = True
        while more_to_stream:
            try:
                chunk = self.read_stream_chunk_blocking(
                    stream_name, offset, max_chunk_size)
                if chunk.finished:
                    more_to_stream = False
                else:
                    offset += chunk.chunk_length
                yield chunk.data
            except OutOfRangeError:
                more_to_stream = False

    def new_client_streaming(self, stream_name: str):
        """
            Inform the StreamStorage backend that a new client
            is streaming the stream `stream_name` in case
            it cares about the number of clients streaming a specific
            stream.
            This can be useful for e.g. cleaning up old streams.
            No-op by default.
            Raises:
                NotFoundError when a stream with the given instance/stream_name
              does not exist.
        """
        pass

    def streaming_client_left(self, stream_name: str):
        """
            Inform the StreamStorage backend that a client
            streaming the stream `stream_name` has left, in case
            it cares about the number of clients streaming a specific
            stream.
            This can be useful for e.g. cleaning up old streams.
            No-op by default.
            Raises:
                NotFoundError when a stream with the given instance/stream_name
              does not/no longer exist(s).
        """
        pass

    def wait_for_streaming_clients(self, write_resource_name: str, timeout: int=300) -> bool:
        """
            Returns a boolean specifying if at least one client is streaming this stream
            within the specified timeout.

            This allows the writers to only start writing when there are readers
            interested in the stream.
            Note: Accepts `write_resource_name` and calls
                  `_wait_for_streaming_clients(read_name)` on the backend.
            Raises:
                NotFoundError when a stream with the given instance/stream_name
              does not exist.
        """
        read_name = StreamHandle._read_name_from_write_name(write_resource_name)
        return self._wait_for_streaming_clients(read_name, timeout)

    @abc.abstractmethod
    def _wait_for_streaming_clients(self, stream_name: str, timeout: int) -> bool:
        """
            Returns a boolean specifying if at least one client is streaming this stream
            within the specified timeout.

            This allows the writers to only start writing when there are readers
            interested in the stream.
            Raises:
                NotFoundError when a stream with the given instance/stream_name
              does not exist.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def delete_stream(self, write_resource_name):
        raise NotImplementedError()

    def set_instance_name(self, instance_name: str) -> None:
        # This method should always get called, so there's no benefit in
        # adding an __init__ to this abstract class (therefore adding the
        # need for subclasses to call `super()`) just to define a null
        # value for this.
        # pylint: disable=attribute-defined-outside-init
        self._instance_name: Optional[str] = instance_name
