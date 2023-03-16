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
BotsService
=================

"""

import logging

import grpc

from google.protobuf import empty_pb2, timestamp_pb2


from buildgrid._enums import BotStatus
from buildgrid._exceptions import (
    InvalidArgumentError, BotSessionClosedError,
    UnknownBotSessionError, BotSessionMismatchError,
    DuplicateBotSessionError, BotSessionCancelledError,
    RetriableError
)
from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2
from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2_grpc
from buildgrid.server._authentication import AuthContext, authorize
from buildgrid.server.metrics_names import (
    BOTS_CREATE_BOT_SESSION_TIME_METRIC_NAME,
    BOTS_UPDATE_BOT_SESSION_TIME_METRIC_NAME
)
from buildgrid.server.metrics_utils import DurationMetric


class BotsService(bots_pb2_grpc.BotsServicer):

    def __init__(self, server, monitor=False):
        self.__logger = logging.getLogger(__name__)

        self._instances = {}

        bots_pb2_grpc.add_BotsServicer_to_server(self, server)

        self._is_instrumented = monitor

    # --- Public API ---

    def add_instance(self, instance_name, instance):
        """Registers a new servicer instance.

        Args:
            instance_name (str): The new instance's name.
            instance (BotsInterface): The new instance itself.
        """
        self._instances[instance_name] = instance

    def get_scheduler(self, instance_name):
        """Retrieves a reference to the scheduler for an instance.

        Args:
            instance_name (str): The name of the instance to query.

        Returns:
            Scheduler: A reference to the scheduler for `instance_name`.

        Raises:
            InvalidArgumentError: If no instance named `instance_name` exists.
        """
        instance = self._get_instance(instance_name)

        return instance.scheduler

    # --- Public API: Servicer ---

    @authorize(AuthContext)
    @DurationMetric(BOTS_CREATE_BOT_SESSION_TIME_METRIC_NAME)
    def CreateBotSession(self, request, context):
        """Handles CreateBotSessionRequest messages.

        Args:
            request (CreateBotSessionRequest): The incoming RPC request.
            context (grpc.ServicerContext): Context for the RPC call.
        """
        self.__logger.info(f"CreateBotSession request from [{context.peer()}]")

        instance_name = request.parent
        bot_id = request.bot_session.bot_id

        try:
            instance = self._get_instance(instance_name)
            bot_session = instance.create_bot_session(
                instance_name,
                request.bot_session,
                context,
                deadline=context.time_remaining()
            )
            now = timestamp_pb2.Timestamp()
            now.GetCurrentTime()

            return bot_session

        except (InvalidArgumentError, BotSessionMismatchError, UnknownBotSessionError) as e:
            self.__logger.info(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        except BotSessionClosedError as e:  # Leases re-queued, not an error
            self.__logger.debug(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.DATA_LOSS)

        except DuplicateBotSessionError as e:
            self.__logger.info(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.ABORTED)

        except BotSessionCancelledError as e:
            self.__logger.info(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.CANCELLED)

        except NotImplementedError as e:
            self.__logger.info(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)

        except RetriableError as e:
            self.__logger.info(f"Retriable error, client should retry in: {e.retry_info.retry_delay}")
            context.abort_with_status(e.error_status)

        except Exception as e:
            self.__logger.exception(
                f"Unexpected error in CreateBotSession; bot_id=[{bot_id}]: "
                f"request=[{request}]"
            )
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)

        return bots_pb2.BotSession()

    @authorize(AuthContext)
    @DurationMetric(BOTS_UPDATE_BOT_SESSION_TIME_METRIC_NAME)
    def UpdateBotSession(self, request, context):
        """Handles UpdateBotSessionRequest messages.

        Args:
            request (UpdateBotSessionRequest): The incoming RPC request.
            context (grpc.ServicerContext): Context for the RPC call.
        """
        self.__logger.debug(f"UpdateBotSession request from [{context.peer()}]")

        names = request.name.split("/")
        bot_id = request.bot_session.bot_id

        try:
            instance_name = '/'.join(names[:-1])

            instance = self._get_instance(instance_name)
            bot_session, metadata = instance.update_bot_session(
                request.name,
                request.bot_session,
                context,
                deadline=context.time_remaining())

            context.set_trailing_metadata(metadata)

            return bot_session

        except (InvalidArgumentError, BotSessionMismatchError, UnknownBotSessionError) as e:
            self.__logger.info(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        except BotSessionClosedError as e:  # Leases re-queued, not an error
            self.__logger.debug(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.DATA_LOSS)

        except DuplicateBotSessionError as e:
            self.__logger.info(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.ABORTED)

        except BotSessionCancelledError as e:
            self.__logger.info(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.CANCELLED)

        except NotImplementedError as e:
            self.__logger.info(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)

        except RetriableError as e:
            self.__logger.info(f"Retriable error, client should retry in: {e.retry_info.retry_delay}")
            context.abort_with_status(e.error_status)

        except Exception as e:
            self.__logger.exception(
                f"Unexpected error in UpdateBotSession; bot_id=[{bot_id}]: "
                f"request=[{request}]"
            )
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)

        return bots_pb2.BotSession()

    @authorize(AuthContext)
    def PostBotEventTemp(self, request, context):
        """Handles PostBotEventTempRequest messages.

        Args:
            request (PostBotEventTempRequest): The incoming RPC request.
            context (grpc.ServicerContext): Context for the RPC call.
        """
        self.__logger.info(f"PostBotEventTemp request from [{context.peer()}]")

        context.set_code(grpc.StatusCode.UNIMPLEMENTED)

        return empty_pb2.Empty()

    # --- Public API: Monitoring ---

    @property
    def is_instrumented(self):
        return self._is_instrumented

    def query_n_bots(self) -> int:
        if self.is_instrumented:
            total = 0
            for instance in self._instances.values():
                total += instance.count_bots()
            return total
        raise RuntimeError("BuildGrid instrumentation is not enabled.")

    def query_n_bots_for_instance(self, instance_name: str) -> int:
        if self.is_instrumented:
            try:
                instance = self._instances[instance_name]
                return instance.count_bots()
            except KeyError:
                raise InvalidArgumentError(f"Instance doesn't exist on server: [{instance_name}].")
        raise RuntimeError("BuildGrid instrumentation is not enabled.")

    def query_n_bots_for_status(self, bot_status: BotStatus) -> int:
        if self.is_instrumented:
            try:
                total = 0
                for instance in self._instances.values():
                    total += instance.count_bots_by_status(bot_status)
                return total
            except KeyError:
                raise InvalidArgumentError(f"Bot Status: [{bot_status}] is not monitored.")
        raise RuntimeError("BuildGrid instrumentation is not enabled.")

    # --- Private API ---

    def _get_instance(self, name):
        try:
            return self._instances[name]

        except KeyError:
            raise InvalidArgumentError(f"Instance doesn't exist on server: [{name}]")
