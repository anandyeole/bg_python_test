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
Execution Controller
====================

An instance of the Execution controller.

All this stuff you need to make the execution service work.

Contains scheduler, execution instance, an interface to the bots
and an operations instance.
"""


import logging

from .scheduler import Scheduler
from .bots.instance import BotsInterface
from .execution.instance import ExecutionInstance
from .operations.instance import OperationsInstance
from .._enums import ServiceName


class ExecutionController:

    def __init__(self, data_store, *, property_keys, match_properties,
                 storage=None,
                 action_cache=None, action_browser_url=None,
                 operation_stream_keepalive_timeout=None,
                 bot_session_keepalive_timeout=None,
                 permissive_bot_session=None,
                 services=ServiceName.default_services(),
                 discard_unwatched_jobs=False,
                 max_execution_timeout=None,
                 max_list_operations_page_size=None,
                 logstream_url=None,
                 logstream_credentials=None,
                 logstream_instance_name=None):

        self.__logger = logging.getLogger(__name__)
        self._services = services

        self._storage = storage
        self._action_cache = action_cache

        # Create a Scheduler if we need one. This Scheduler is shared between the
        # Execution instance and Operations instance if they're both enabled. The
        # BotsInterface creates its own Scheduler and doesn't reuse this one.
        if any(service != ServiceName.BOTS.value for service in self._services):
            scheduler = Scheduler(
                data_store,
                action_cache=action_cache,
                action_browser_url=action_browser_url,
                max_execution_timeout=max_execution_timeout,
                logstream_url=logstream_url,
                logstream_credentials=logstream_credentials,
                logstream_instance_name=logstream_instance_name
            )

        if ServiceName.EXECUTION.value in self._services:
            self._execution_instance = ExecutionInstance(
                scheduler, storage, property_keys, match_properties,
                discard_unwatched_jobs, action_cache=action_cache,
                operation_stream_keepalive_timeout=operation_stream_keepalive_timeout)
        if ServiceName.BOTS.value in self._services:
            self._bots_interface = BotsInterface(
                data_store, action_cache=action_cache,
                bot_session_keepalive_timeout=bot_session_keepalive_timeout,
                permissive_bot_session=permissive_bot_session,
                logstream_url=logstream_url,
                logstream_credentials=logstream_credentials,
                logstream_instance_name=logstream_instance_name)
        if ServiceName.OPERATIONS.value in self._services:
            self._operations_instance = OperationsInstance(
                scheduler, max_list_operations_page_size=max_list_operations_page_size)

    def register_instance_with_server(self, instance_name, server):
        if ServiceName.EXECUTION.value in self._services:
            self._execution_instance.register_instance_with_server(instance_name, server)
        if ServiceName.BOTS.value in self._services:
            self._bots_interface.register_instance_with_server(instance_name, server)
        if ServiceName.OPERATIONS.value in self._services:
            self._operations_instance.register_instance_with_server(instance_name, server)

    @property
    def execution_instance(self):
        try:
            return self._execution_instance
        except AttributeError:
            return None

    @property
    def bots_interface(self):
        try:
            return self._bots_interface
        except AttributeError:
            return None

    @property
    def operations_instance(self):
        try:
            return self._operations_instance
        except AttributeError:
            return None
