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
Bot Interface
=============

Interface to grpc
"""

import logging
import grpc

from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2, bots_pb2_grpc
from buildgrid.settings import NETWORK_TIMEOUT


class BotInterface:
    """
    Interface handles calls to the server.
    """

    def __init__(self, channel, interval, executing_interval):
        self.__logger = logging.getLogger(__name__)

        self._stub = bots_pb2_grpc.BotsStub(channel)
        self.__interval = interval
        self.__executing_interval = executing_interval

    @property
    def interval(self):
        return self.__interval

    @property
    def executing_interval(self):
        return self.__executing_interval

    def create_bot_session(self, parent, bot_session):
        """ Create bot session request
        Returns BotSession if correct else a grpc StatusCode
        """
        request = bots_pb2.CreateBotSessionRequest(parent=parent,
                                                   bot_session=bot_session)
        return self._bot_call(self._stub.CreateBotSession, request)

    def update_bot_session(self, bot_session, update_mask=None):
        """ Update bot session request
        Returns BotSession if correct else a grpc StatusCode
        """
        request = bots_pb2.UpdateBotSessionRequest(name=bot_session.name,
                                                   bot_session=bot_session,
                                                   update_mask=update_mask)
        return self._bot_call(self._stub.UpdateBotSession, request)

    def _bot_call(self, call, request):
        try:
            return call(request, timeout=self.interval + NETWORK_TIMEOUT)
        except grpc.RpcError as e:
            self.__logger.error(e)
            return e.code()
