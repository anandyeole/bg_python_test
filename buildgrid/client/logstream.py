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


from contextlib import contextmanager
import logging

from grpc import RpcError

from buildgrid._protos.build.bazel.remote.logstream.v1.remote_logstream_pb2 import CreateLogStreamRequest
from buildgrid._protos.build.bazel.remote.logstream.v1.remote_logstream_pb2_grpc import LogStreamServiceStub


@contextmanager
def logstream_client(channel, instance_name):
    client = LogStreamClient(channel, instance_name)
    try:
        yield client
    finally:
        client.close()


class LogStreamClient:

    def __init__(self, channel, instance_name=''):
        self._channel = channel
        self._instance_name = instance_name
        self._logger = logging.getLogger(__name__)
        self._logstream_stub = LogStreamServiceStub(self._channel)

    def create(self, parent):
        parent = f"{self._instance_name}/{parent}"
        request = CreateLogStreamRequest(parent=parent)
        try:
            return self._logstream_stub.CreateLogStream(request)
        except RpcError as e:
            self._logger.exception(f"Error creating a LogStream: {e.details()}")
            raise ConnectionError(e.details())

    def close(self):
        self._logstream_stub = None
