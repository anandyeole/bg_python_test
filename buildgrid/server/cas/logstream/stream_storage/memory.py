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

from collections import OrderedDict
from itertools import islice
import logging
from threading import Event, Lock
from typing import Dict, Optional

from buildgrid.settings import MAX_LOGSTREAMS_BEFORE_CLEANUP
from buildgrid._exceptions import NotFoundError, StreamAlreadyExistsError, StreamFinishedError, StreamWritePendingError
from buildgrid.server.cas.logstream.stream_storage.stream_storage_abc import StreamStorageABC, \
    StreamHandle, StreamLength, StreamChunk


class _MemoryStream():
    def __init__(self, stream_handle: StreamHandle):
        self._stream_handle = stream_handle
        self._stream_length = StreamLength(length=0, finished=False)

        self._read_ready = Event()
        self._writer_lock = Lock()
        self._streamlength_snapshot_lock = Lock()
        self._data: bytearray = bytearray()

    @property
    def finished(self):
        return self._stream_length.finished

    @property
    def streamlength(self) -> StreamLength:
        with self._streamlength_snapshot_lock:
            return StreamLength(**self._stream_length._asdict())

    @property
    def name(self) -> str:
        return self._stream_handle.name

    @property
    def write_name(self) -> str:
        return self._stream_handle.write_resource_name

    def append(self, message: Optional[bytes] = None, mark_finished: Optional[bool] = False):
        if self.streamlength.finished:
            raise StreamFinishedError(
                f'The LogStream with the parent=[{self.write_name}] '
                'has already been marked as finished, cannot append')

        # Append data and update length/finished
        with self._writer_lock:
            if message:
                self._data.extend(message)
        with self._streamlength_snapshot_lock:
            new_length = self._stream_length.length
            if message:
                new_length += len(message)
            new_finished = False
            if mark_finished:
                new_finished = True

            self._stream_length = StreamLength(new_length, new_finished)

        # Inform anyone that was waiting on data to check
        self._read_ready.set()
        self._read_ready.clear()

    def _get_chunk(self,
                   streamlength_snapshot: StreamLength,
                   offset: int,
                   end_offset: Optional[int] = None) -> Optional[StreamChunk]:
        if streamlength_snapshot.length >= offset:
            # If we have a message starting at offset `offset`, return
            # message[offset:end_offset]
            # When `end_offset` > `stream_length`, returns message up to
            # `stream_length`
            data_chunk = bytes(self._data[offset:end_offset])
            return StreamChunk(data_chunk,
                               len(data_chunk),
                               streamlength_snapshot.finished)
        elif streamlength_snapshot.finished:
            # If we the stream has finished and we didn't get enough bytes,
            # we are done!
            raise StreamFinishedError(
                f'The LogStream with the parent=[{self.write_name}] '
                'has already been marked as finished and its length is '
                f'length=[{streamlength_snapshot.length}], '
                f'cannot read message at offset=[{offset}].')
        else:
            return None

    def read_chunk(self, offset: int = 0, limit: Optional[int] = None) -> StreamChunk:
        end_offset = None
        if limit:
            end_offset = offset + limit

        streamlength_snapshot = self.streamlength
        chunk = self._get_chunk(streamlength_snapshot, offset, end_offset)

        if chunk:
            return chunk

        raise StreamWritePendingError(
            f'Only length=[{streamlength_snapshot.length}] bytes '
            f'were commited to the LogStream with the parent=[{self.write_name}] '
            f' so far, writes starting at offset=[{offset}] are still pending.'
        )

    def read_chunk_blocking(self,
                            offset: int = 0,
                            limit: Optional[int] = None) -> StreamChunk:
        max_end_offset = None
        if limit:
            max_end_offset = offset + limit

        streamlength_snapshot = self.streamlength
        give_up = False
        while not give_up:
            give_up = streamlength_snapshot.finished
            chunk = self._get_chunk(streamlength_snapshot, offset, max_end_offset)
            if chunk:
                return chunk
            if not self._read_ready.wait(timeout=600):
                break

        return StreamChunk(data=bytes(),
                           chunk_length=streamlength_snapshot.length,
                           finished=streamlength_snapshot.finished)


class MemoryStreamStorage(StreamStorageABC):
    def __init__(self, max_streams_count: int = MAX_LOGSTREAMS_BEFORE_CLEANUP):
        self._logger = logging.getLogger(__name__)
        self._streams: Dict[str, _MemoryStream] = {}
        self._streams_readers: OrderedDict[str, int] = OrderedDict()
        self._streams_readers_lock = Lock()
        self._streams_readers_changed_event = Event()
        self._max_stream_count = max_streams_count
        self._cleanup_max_batch_size = max_streams_count // 2

    def _lru_cleanup(self):
        if len(self._streams_readers) < self._max_stream_count:
            return

        self._logger.info(f"Running LRU cleanup of unused LogStreams;  "
                          f"max_stream_count=[{self._max_stream_count}],"
                          f"cleanup_max_batch_size=[{self._cleanup_max_batch_size}]")

        streams_to_remove = []
        changes = False
        with self._streams_readers_lock:
            # Figure out which streams to clean up by iterating through
            # Do not mutate dictionary during iteration (python doesn't like that)
            stream_iterator = islice(self._streams_readers.items(), 0, None)
            while len(streams_to_remove) < self._cleanup_max_batch_size and stream_iterator:
                try:
                    stream_name, reader_count = next(stream_iterator)
                except StopIteration:
                    break

                if reader_count == 0:
                    streams_to_remove.append(stream_name)

            # Now loop through the list of all streams names
            # that were marked for removal and actually clean up
            for stream_name in streams_to_remove:
                self._delete_stream(stream_name, delete_readers=True)
                changes = True

            if len(streams_to_remove) == 0:
                self._logger.info("Cleaned up n=[0] LogStreams, "
                                  "all LogStreams in use.")
            else:
                self._logger.info(f"Cleaned up n=[{len(streams_to_remove)}] LogStreams.")

        if changes:
            self._streams_readers_changed_event.set()
            self._streams_readers_changed_event.clear()

    def _create_stream(self, stream_handle: StreamHandle):
        if stream_handle.name in self._streams:
            raise StreamAlreadyExistsError(
                f'A LogStream with the parent [{stream_handle.name}] already exists, '
                'and should be used instead of creating a new stream.')
        self._lru_cleanup()
        self._streams[stream_handle.name] = _MemoryStream(stream_handle)
        self._streams_readers[stream_handle.name] = 0
        self._logger.debug(f'Stream created: {stream_handle}')

    def stream_exists(self, stream_name: str) -> bool:
        return stream_name in self._streams

    def new_client_streaming(self, stream_name: str):
        if stream_name in self._streams:
            with self._streams_readers_lock:
                self._streams_readers[stream_name] += 1
            self._streams_readers_changed_event.set()
            self._streams_readers_changed_event.clear()
        else:
            raise NotFoundError(
                f'A LogStream with the parent [{stream_name}] doesn\'t exist.')

    def streaming_client_left(self, stream_name: str):
        if stream_name in self._streams:
            with self._streams_readers_lock:
                self._streams_readers[stream_name] -= 1
            self._streams_readers_changed_event.set()
            self._streams_readers_changed_event.clear()
        else:
            raise NotFoundError(
                f'A LogStream with the parent [{stream_name}] doesn\'t exist.')

    def stream_finished(self, stream_name: str) -> bool:
        if stream_name in self._streams:
            return self._streams[stream_name].finished
        raise NotFoundError(
            f'A LogStream with the parent [{stream_name}] doesn\'t exist')

    def stream_length(self, stream_name: str) -> StreamLength:
        if stream_name in self._streams:
            return self._streams[stream_name].streamlength
        raise NotFoundError(
            f'A LogStream with the parent [{stream_name}] doesn\'t exist')

    def _append_to_stream(self, stream_name, write_resource_name: str, message: Optional[bytes] = None,
                          *, mark_finished: Optional[bool] = False) -> None:
        if stream_name not in self._streams or self._streams[
                stream_name].write_name != write_resource_name:
            raise NotFoundError(
                f'A LogStream with the parent [{write_resource_name}] doesn\'t exist'
            )
        self._streams[stream_name].append(message, mark_finished)

    def read_stream_chunk_blocking(
            self, stream_name: str, read_offset_bytes: int,
            read_limit_bytes: Optional[int] = None) -> StreamChunk:
        if stream_name not in self._streams:
            raise NotFoundError(
                f'A LogStream with the parent [{stream_name}] doesn\'t exist')
        return self._streams[stream_name].read_chunk_blocking(
            read_offset_bytes, read_limit_bytes)

    def read_stream_chunk(self, stream_name: str, read_offset_bytes: int,
                          read_limit_bytes: Optional[int] = None) -> StreamChunk:
        if stream_name not in self._streams:
            raise NotFoundError(
                f'A LogStream with the parent [{stream_name}] doesn\'t exist')
        return self._streams[stream_name].read_chunk(read_offset_bytes, read_limit_bytes)

    def _delete_stream(self, stream_name: str, delete_readers: bool = True):
        # Evict actual stream from memory
        self._logger.debug("Deleting stream: {stream_name}")
        try:
            del self._streams[stream_name]
        except KeyError:
            # already deleted
            pass
        # Evict counter
        if delete_readers:
            try:
                del self._streams_readers[stream_name]
            except KeyError:
                pass

    def _wait_for_streaming_clients(self, stream_name: str, timeout: int) -> bool:
        if stream_name in self._streams_readers:
            n_streaming = self._streams_readers.get(stream_name, 0)
            if n_streaming < 1:
                self._streams_readers_changed_event.wait(timeout=timeout)
                try:
                    n_streaming = self._streams_readers[stream_name]
                except KeyError:
                    raise NotFoundError(
                        f'The Logstream with parent [{stream_name}] was deleted')
            return bool(n_streaming)
        raise NotFoundError(
            f'A LogStream with the parent [{stream_name}] doesn\'t exist')

    def delete_stream(self, write_resource_name: str):
        raise NotImplementedError()
