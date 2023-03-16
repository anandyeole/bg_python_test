# Copyright (C) 2021 Bloomberg LP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  <http://www.apache.org/licenses/LICENSE-2.0>
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License' is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
Bots service
============

The Bots service is responsible for assigning work to the various worker
(sometimes "bot") machines connected to the grid. These workers communicate
with the Bots service using the `Remote Workers API`_.

Like the other BuildGrid services, the Bots service uses the concept of
instance names to contain specific sets of workers. These names could refer
to (e.g.) a project-specific BuildGrid instance as part of a wider managed
deployment.

Workers specify an instance name as part of the request messages in the Remote
Workers API, and the gRPC servicer for the Bots service routes the requests
to the correct **instance**. The actual functionality for assigning work to
workers and handling progress updates from workers lives in the
``BotsInstance`` instances which get registered with the gRPC servicer.

.. _Remote Workers API: https://github.com/googleapis/googleapis/tree/master/google/devtools/remoteworkers/v1test2

"""


from functools import partial
import logging
from typing import Dict

import grpc

from buildgrid._exceptions import (
    InvalidArgumentError, BotSessionClosedError,
    UnknownBotSessionError, BotSessionMismatchError,
    RetriableError
)
from buildgrid._protos.google.devtools.remoteworkers.v1test2.bots_pb2 import (
    BotSession,
    CreateBotSessionRequest,
    UpdateBotSessionRequest
)
from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2_grpc
from buildgrid.server._authentication import AuthContext, authorize
from buildgrid.server.metrics_names import (
    BOTS_CREATE_BOT_SESSION_TIME_METRIC_NAME,
    BOTS_UPDATE_BOT_SESSION_TIME_METRIC_NAME
)
from buildgrid.server.metrics_utils import DurationMetric
from buildgrid.server.rabbitmq.bots.instance import BotsInstance


class BotsService(bots_pb2_grpc.BotsServicer):

    """gRPC servicer class for the Bots service.

    This is the servicer which gets attached to a gRPC server and given
    incoming requests to handle.

    Those requests are routed to the ``BotsInstances`` registered with
    this servicer using the ``parent`` field in ``CreateBotSession``
    requests and the prefix of the server-assigned bot session name in
    ``UpdateBotSession`` requests.

    The actual ``BotSession`` management is done on a per-instance basis
    in the ``BotsInstance`` class.

    """

    def __init__(self, server: grpc.Server, enable_metrics: bool=False):
        """Instantiate a new Bots service.

        Args:
            server (grpc.Server): The gRPC server to add this request
                servicer to.
            enable_metrics (bool): Whether or not to enable metrics
                publishing for this Bots service.

        """
        self._logger = logging.getLogger(__name__)
        self._metrics_enabled = enable_metrics
        self._instances: Dict[str, BotsInstance] = {}

        bots_pb2_grpc.add_BotsServicer_to_server(self, server)

    def add_instance(self, name: str, instance: BotsInstance) -> None:
        """Register a new instance with the Bots service.

        Takes a name and a ``BotsInstance``, and registers the instance
        with the Bots service using the given name. This allows the
        Bots service to handle incoming requests specifying the given
        name as an instance name.

        Args:
            name (str): The name of the instance to be registered.
            instance (BotsInstance): The instance to be registered.

        """
        self._instances[name] = instance

    def _get_instance(self, name: str) -> BotsInstance:
        try:
            return self._instances[name]
        except KeyError:
            raise InvalidArgumentError(f"Instance doesn't exist on server: [{name}]")

    def start(self) -> None:
        for instance in self._instances.values():
            instance.start()

    def stop(self) -> None:
        for instance in self._instances.values():
            instance.stop()

    @authorize(AuthContext)
    @DurationMetric(BOTS_CREATE_BOT_SESSION_TIME_METRIC_NAME)
    def CreateBotSession(
        self,
        request: CreateBotSessionRequest,
        context: grpc.ServicerContext
    ) -> BotSession:
        """Handler for CreateBotSession requests.

        This method takes a request to create a new ``BotSession`` in a
        specific instance of the Bots service. This request is handed to
        the specified instance (if it exists). The instance returns the newly
        registered ``BotSession`` or raises an error if the client-provided
        session details were unacceptable.

        Args:
            request (CreateBotSessionRequest): The gRPC request to create a
                new ``BotSession``.
            context (grpc.ServicerContext): Context for this gRPC request.

        Returns:
            A ``BotSession`` gRPC message with the server-assigned fields set.

        """
        self._logger.info(f"CreateBotSession request from [{context.peer()}]")

        instance_name = request.parent
        bot_id = request.bot_session.bot_id

        try:
            instance = self._get_instance(instance_name)
            context.add_callback(partial(self._on_disconnect_callback,
                                         instance_name, request.bot_session))

            bot_session = instance.create_bot_session(instance_name,
                                                      request.bot_session,
                                                      context.time_remaining())

            return bot_session

        except (InvalidArgumentError, BotSessionMismatchError, UnknownBotSessionError) as e:
            self._logger.info(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        except BotSessionClosedError as e:  # Leases re-queued, not an error
            self._logger.debug(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.DATA_LOSS)

        except RetriableError as e:
            self._logger.info(f"Retriable error, client should retry in: {e.retry_info.retry_delay}")
            context.abort_with_status(e.error_status)

        except Exception as e:
            self._logger.exception(
                f"Unexpected error in CreateBotSession; bot_id=[{bot_id}]: "
                f"request=[{request}]"
            )
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)

        return BotSession()

    @authorize(AuthContext)
    @DurationMetric(BOTS_UPDATE_BOT_SESSION_TIME_METRIC_NAME)
    def UpdateBotSession(
        self,
        request: UpdateBotSessionRequest,
        context: grpc.ServicerContext
    ) -> BotSession:
        """Handler for UpdateBotSession requests.

        This method takes a request to update an existing ``BotSession``. The
        server-assigned name is parsed to determine the instance the session
        belongs to, and then the bot-supplied ``BotSession`` state is given
        to that instance to handle.

        Once the instance has handled any updates in either direction, the
        (potentially modified) ``BotSession`` is returned to the connected
        bot.

        Args:
            request (UpdateBotSessionRequest): The incoming gRPC request to
                update a ``BotSession``.
            context (grpc.ServicerContext): Context for this gRPC request.

        Returns:
            ``BotSession`` containing potentially updated server-assigned
            fields.
        """
        self._logger.debug(f"UpdateBotSession request from [{context.peer()}]")

        names = request.name.split("/")
        bot_id = request.bot_session.bot_id

        try:
            instance_name = '/'.join(names[:-1])
            context.add_callback(partial(self._on_disconnect_callback,
                                         instance_name, request.bot_session))

            instance = self._get_instance(instance_name)
            bot_session, metadata = instance.update_bot_session(
                request.name,
                request.bot_session,
                time_remaining=context.time_remaining())

            context.set_trailing_metadata(metadata)

            return bot_session

        except (InvalidArgumentError, BotSessionMismatchError, UnknownBotSessionError) as e:
            self._logger.info(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        except BotSessionClosedError as e:  # Leases re-queued, not an error
            self._logger.debug(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.DATA_LOSS)

        except RetriableError as e:
            self._logger.info(f"Retriable error, client should retry in: {e.retry_info.retry_delay}")
            context.abort_with_status(e.error_status)

        except Exception as e:
            self._logger.exception(
                f"Unexpected error in UpdateBotSession; bot_id=[{bot_id}]: "
                f"request=[{request}]"
            )
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)

        return BotSession()

    def _on_disconnect_callback(self, instance_name: str, bot_session: BotSession) -> None:
        """Callback to run when an RPC connection is terminated.

        Args:
            instance_name (str): The instance name that the terminated
                connection was using.
            bot_session (BotSession): The BotSession being used in the
                terminated RPC connection.

        """
        instance = self._get_instance(instance_name)
        instance.expire_assignment_for_bot_name(bot_session.name)
