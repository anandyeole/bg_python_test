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


import logging
from typing import TYPE_CHECKING, Optional

from buildgrid._protos.build.bazel.remote.logstream.v1.remote_logstream_pb2 import LogStream
from buildgrid.server.cas.logstream.stream_storage.stream_storage_abc import StreamStorageABC
from buildgrid.server.cas.logstream.stream_storage.memory import MemoryStreamStorage
if TYPE_CHECKING:
    from buildgrid.server.server import Server


class LogStreamInstance:

    def __init__(self, prefix: str, storage: Optional[StreamStorageABC]=None):
        self._logger = logging.getLogger(__name__)
        self.instance_name: Optional[str] = None
        self._prefix = prefix
        if storage is None:
            storage = MemoryStreamStorage()
        self._stream_store = storage

    def register_instance_with_server(self, instance_name: str, server: 'Server') -> None:
        if self.instance_name is None:
            server.add_logstream_instance(self, instance_name)
            self.instance_name = instance_name
        else:
            raise AssertionError("Instance already registered")

    def setup_grpc(self):
        # The logstream instance doesn't currently have any gRPC setup to do
        pass

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def create_logstream(self, parent: str) -> LogStream:
        return LogStream(**self._stream_store.create_stream(parent, self._prefix)._asdict())
