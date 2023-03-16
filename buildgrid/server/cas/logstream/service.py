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

from buildgrid._exceptions import InvalidArgumentError
from buildgrid._protos.build.bazel.remote.logstream.v1 import remote_logstream_pb2_grpc
from buildgrid._protos.build.bazel.remote.logstream.v1.remote_logstream_pb2 import (
    CreateLogStreamRequest,
    LogStream
)
from buildgrid.server._authentication import AuthContext, authorize
from buildgrid.server.cas.logstream.instance import LogStreamInstance
from buildgrid.server.metrics_names import (
    LOGSTREAM_CREATE_LOG_STREAM_TIME_METRIC_NAME
)
from buildgrid.server.metrics_utils import DurationMetric


class LogStreamService(remote_logstream_pb2_grpc.LogStreamServiceServicer):

    def __init__(self, server):
        self._logger = logging.getLogger(__name__)
        self._instances = {}

        remote_logstream_pb2_grpc.add_LogStreamServiceServicer_to_server(self, server)

    def add_instance(self, name: str, instance: LogStreamInstance) -> None:
        self._instances[name] = instance

    @authorize(AuthContext)
    @DurationMetric(LOGSTREAM_CREATE_LOG_STREAM_TIME_METRIC_NAME)
    def CreateLogStream(self, request: CreateLogStreamRequest, context) -> LogStream:
        self._logger.debug(f"CreateLogStream request from [{context.peer()}]")

        instance_name = ''
        parent = request.parent
        if '/' in parent:
            instance_name, parent = request.parent.rsplit("/", 1)
        instance = self._get_instance(instance_name)

        return instance.create_logstream(parent)

    def _get_instance(self, instance_name: str):
        try:
            return self._instances[instance_name]
        except KeyError:
            raise InvalidArgumentError(f"No instance named [{instance_name}]")
