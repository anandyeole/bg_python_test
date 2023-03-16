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
Bot Session
===========

Allows connections
"""
import asyncio
import logging
import os
import platform

import grpc

from buildgrid._enums import BotStatus, LeaseState
from buildgrid._exceptions import FailedPreconditionError
from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2
from buildgrid._protos.google.rpc import code_pb2
from .tenantmanager import TenantManager


class BotSession:
    def __init__(self, instance_name, bots_interface, hardware_interface, work,
                 context=None):
        """ Unique bot ID within the farm used to identify this bot
        Needs to be human readable.
        All prior sessions with bot_id of same ID are invalidated.
        If a bot attempts to update an invalid session, it must be rejected and
        may be put in quarantine.
        """
        self.__logger = logging.getLogger(__name__)

        self._bots_interface = bots_interface
        self._hardware_interface = hardware_interface

        self._status = BotStatus.OK.value
        self._tenant_manager = TenantManager()

        self.__instance_name = instance_name
        self.__bot_id = f'{instance_name}.{platform.node()}.{os.getpid()}'
        self.__name = None

        self._work = work
        self._context = context

        self.__connected = False

    @property
    def bot_id(self):
        return self.__bot_id

    @property
    def connected(self):
        return self.__connected

    async def run(self):
        interval = self._bots_interface.interval
        executing_interval = self._bots_interface.executing_interval
        try:
            while True:
                if not self.connected:
                    self.create_bot_session()
                else:
                    self.update_bot_session()

                if not self.connected:
                    await asyncio.sleep(interval)
                else:
                    await self._tenant_manager.wait_on_tenants(executing_interval)
        except asyncio.CancelledError:
            pass

    def create_bot_session(self):
        self.__logger.debug("Creating bot session")

        session = self._bots_interface.create_bot_session(self.__instance_name, self.get_pb2())
        if session in list(grpc.StatusCode):
            self.__connected = False
            return
        self.__connected = True
        self.__name = session.name

        self.__logger.info(f"Created bot session with name: [{self.__name}]")

        for lease in session.leases:
            self._register_lease(lease)

    def update_bot_session(self):
        self.__logger.debug(f"Updating bot session: [{self.__bot_id}]")

        session = self._bots_interface.update_bot_session(self.get_pb2())
        if session in list(grpc.StatusCode):
            self.__connected = False
            return
        self.__connected = True
        server_ids = []

        for lease in session.leases:
            server_ids.append(lease.id)

            lease_state = LeaseState(lease.state)
            if lease_state == LeaseState.PENDING:
                self._register_lease(lease)

            elif lease_state == LeaseState.CANCELLED:
                self._tenant_manager.cancel_tenancy(lease.id)

        closed_lease_ids = [x for x in self._tenant_manager.get_lease_ids() if x not in server_ids]

        for lease_id in closed_lease_ids:
            self._tenant_manager.cancel_tenancy(lease_id)
            self._tenant_manager.remove_tenant(lease_id)

    def get_pb2(self):
        return bots_pb2.BotSession(worker=self._hardware_interface.get_worker_pb2(),
                                   status=self._status,
                                   leases=self._tenant_manager.get_leases(),
                                   bot_id=self.__bot_id,
                                   name=self.__name)

    def _register_lease(self, lease):
        lease_id = lease.id
        try:
            self._tenant_manager.create_tenancy(lease)

        except KeyError as e:
            self.__logger.error(e)

        else:
            try:
                self._hardware_interface.configure_hardware(lease.requirements)

            except FailedPreconditionError as e:
                self.__logger.error(e)
                self._tenant_manager.complete_lease(lease_id, status=code_pb2.FailedPreconditionError)

            else:
                self._tenant_manager.create_work(lease_id, self._work, self._context)
