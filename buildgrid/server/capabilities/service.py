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


import logging

import grpc


from buildgrid._exceptions import InvalidArgumentError, RetriableError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2, remote_execution_pb2_grpc
from buildgrid.server._authentication import AuthContext, authorize
from buildgrid.server.request_metadata_utils import printable_request_metadata


class CapabilitiesService(remote_execution_pb2_grpc.CapabilitiesServicer):

    def __init__(self, server):
        self.__logger = logging.getLogger(__name__)

        self.__instances = {}

        remote_execution_pb2_grpc.add_CapabilitiesServicer_to_server(self, server)

    # --- Public API ---

    def add_instance(self, name, instance):
        self.__instances[name] = instance

    def add_cas_instance(self, name, instance):
        self.__instances[name].add_cas_instance(instance)

    def add_action_cache_instance(self, name, instance):
        self.__instances[name].add_action_cache_instance(instance)

    def add_execution_instance(self, name, instance):
        self.__instances[name].add_execution_instance(instance)

    # --- Public API: Servicer ---

    @authorize(AuthContext)
    def GetCapabilities(self, request, context):
        """Handles GetCapabilitiesRequest messages.

        Args:
            request (GetCapabilitiesRequest): The incoming RPC request.
            context (grpc.ServicerContext): Context for the RPC call.
        """
        self.__logger.info(f"GetCapabilities request from [{context.peer()}] "
                           f"([{printable_request_metadata(context.invocation_metadata())}])")

        try:
            instance = self._get_instance(request.instance_name)

            return instance.get_capabilities()

        except InvalidArgumentError as e:
            self.__logger.info(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        except RetriableError as e:
            self.__logger.info(f"Retriable error, client should retry in: {e.retry_info.retry_delay}")
            context.abort_with_status(e.error_status)

        except Exception as e:
            self.__logger.exception(
                f"Unexpected error in GetCapabilities; request=[{request}]"
            )
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)

        return remote_execution_pb2.ServerCapabilities()

    # --- Private API ---

    def _get_instance(self, name):
        try:
            return self.__instances[name]

        except KeyError:
            raise InvalidArgumentError(f"Instance doesn't exist on server: [{name}]")
