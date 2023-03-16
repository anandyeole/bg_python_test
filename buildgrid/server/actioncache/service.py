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
ActionCacheService
==================

Allows clients to manually query/update the action cache.
"""

import logging

import grpc

import buildgrid.server.context as context_module

from buildgrid._exceptions import InvalidArgumentError, NotFoundError, StorageFullError, RetriableError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2_grpc
from buildgrid.server._authentication import AuthContext, authorize
from buildgrid.server.metrics_names import (
    AC_GET_ACTION_RESULT_TIME_METRIC_NAME,
    AC_UPDATE_ACTION_RESULT_TIME_METRIC_NAME,
    AC_CACHE_HITS_METRIC_NAME,
    AC_CACHE_MISSES_METRIC_NAME
)
from buildgrid.server.metrics_utils import publish_counter_metric, DurationMetric
from buildgrid.server.request_metadata_utils import printable_request_metadata


class ActionCacheService(remote_execution_pb2_grpc.ActionCacheServicer):

    def __init__(self, server):
        self.__logger = logging.getLogger(__name__)

        self._instances = {}

        remote_execution_pb2_grpc.add_ActionCacheServicer_to_server(self, server)

    # --- Public API ---

    def add_instance(self, name, instance):
        self._instances[name] = instance

    # --- Public API: Servicer ---

    @context_module.metadatacontext()
    @authorize(AuthContext)
    def GetActionResult(self, request, context):
        self.__logger.info(f"GetActionResult request from [{context.peer()}] "
                           f"([{printable_request_metadata(context.invocation_metadata())}])")

        try:
            instance = self._get_instance(request.instance_name)
            with DurationMetric(AC_GET_ACTION_RESULT_TIME_METRIC_NAME,
                                request.instance_name,
                                instanced=True):
                action_result = instance.get_action_result(request.action_digest)
            publish_counter_metric(
                AC_CACHE_HITS_METRIC_NAME,
                1,
                {"instance-name": request.instance_name}
            )
            return action_result

        except InvalidArgumentError as e:
            self.__logger.info(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        except NotFoundError as e:
            self.__logger.debug(e)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            publish_counter_metric(
                AC_CACHE_MISSES_METRIC_NAME,
                1,
                {"instance-name": request.instance_name}
            )

        except ConnectionError as e:
            self.__logger.exception(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.UNAVAILABLE)

        except RetriableError as e:
            self.__logger.info(f"Retriable error, client should retry in: {e.retry_info.retry_delay}")
            context.abort_with_status(e.error_status)

        except Exception as e:
            self.__logger.exception(
                f"Unexpected error in GetActionResult; request=[{request}]"
            )
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)

        return remote_execution_pb2.ActionResult()

    @context_module.metadatacontext()
    @authorize(AuthContext)
    def UpdateActionResult(self, request, context):
        self.__logger.info(f"UpdateActionResult request from [{context.peer()}] "
                           f"([{printable_request_metadata(context.invocation_metadata())}])")

        try:
            instance = self._get_instance(request.instance_name)
            with DurationMetric(AC_UPDATE_ACTION_RESULT_TIME_METRIC_NAME,
                                request.instance_name,
                                instanced=True):
                instance.update_action_result(request.action_digest, request.action_result)
            return request.action_result

        except InvalidArgumentError as e:
            self.__logger.info(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        except NotImplementedError as e:
            self.__logger.info(e)
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)

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
            self.__logger.exception(
                f"Unexpected error in UpdateActionResult; request=[{request}]"
            )
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)

        return remote_execution_pb2.ActionResult()

    # --- Private API ---

    def _get_instance(self, instance_name):
        try:
            return self._instances[instance_name]

        except KeyError:
            raise InvalidArgumentError(f"Invalid instance name: [{instance_name}]")
