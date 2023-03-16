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
ExecutionInstance
=================
An instance of the Remote Execution Service.
"""

import logging

from buildgrid._exceptions import FailedPreconditionError, InvalidArgumentError, NotFoundError
from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import (
    Action, ActionCacheUpdateCapabilities, Command
)
from buildgrid.utils import get_hash_type


class ExecutionInstance:

    def __init__(self, scheduler, storage, property_keys, match_properties,
                 discard_unwatched_jobs: bool = False, action_cache=None,
                 operation_stream_keepalive_timeout=None):
        self.__logger = logging.getLogger(__name__)

        self._scheduler = scheduler
        self._instance_name = None
        self._discard_unwatched_jobs = discard_unwatched_jobs

        self._property_keys = property_keys
        self._match_properties = match_properties
        self._unique_keys = set(["OSFamily"])  # Those keys only allow one value to be set

        self.__storage = storage
        self._action_cache = action_cache
        self._storage_capabilities = None

        self._operation_stream_keepalive_timeout = operation_stream_keepalive_timeout

    # --- Public API ---

    @property
    def instance_name(self):
        return self._instance_name

    @property
    def discard_unwatched_jobs(self) -> bool:
        return self._discard_unwatched_jobs

    @property
    def scheduler(self):
        return self._scheduler

    def setup_grpc(self):
        self._scheduler.setup_grpc()
        self.__storage.setup_grpc()

        if self._action_cache is not None:
            self._action_cache.setup_grpc()

    def start(self) -> None:
        self._scheduler.start()

    def stop(self) -> None:
        self._scheduler.stop()

    def register_instance_with_server(self, instance_name, server):
        """Names and registers the execution instance with a given server."""
        if self._instance_name is None:
            server.add_execution_instance(self, instance_name)

            self._instance_name = instance_name
            if self._scheduler is not None:
                self._scheduler.set_instance_name(instance_name)

        else:
            raise AssertionError("Instance already registered")

    def hash_type(self):
        return get_hash_type()

    def execute(self, action_digest, skip_cache_lookup, priority=0):
        """ Sends a job for execution.
        Queues an action and creates an Operation instance to be associated with
        this action.
        """
        action = self.__storage.get_message(action_digest, Action)

        if not action:
            raise FailedPreconditionError("Could not get action from storage.")

        if action.HasField('platform'):
            platform = action.platform
        else:
            self.__logger.debug("Platform not set in Action, fetching command.")
            command = self.__storage.get_message(action.command_digest, Command)
            if not command:
                raise FailedPreconditionError(
                    "Could not get command from storage.")

            platform = command.platform

        platform_requirements = {}
        for platform_property in platform.properties:
            name = platform_property.name
            if name not in self._property_keys:
                raise FailedPreconditionError(
                    f"Unregistered platform property [{name}]. "
                    f"Known properties are: [{self._property_keys}]")

            if name in self._unique_keys and name in platform_requirements:
                raise FailedPreconditionError(
                    f"Platform property [{name}] can only be set once.")

            if name in self._match_properties:
                if name not in platform_requirements:
                    platform_requirements[name] = set()
                platform_requirements[name].add(platform_property.value)

        return self._scheduler.queue_job_action(action, action_digest,
                                                platform_requirements=platform_requirements,
                                                skip_cache_lookup=skip_cache_lookup, priority=priority)

    def register_job_peer(self, job_name, peer, request_metadata=None):
        try:
            return self._scheduler.register_job_peer(
                job_name, peer, request_metadata=request_metadata)

        except NotFoundError:
            raise InvalidArgumentError(f"Job name does not exist: [{job_name}]")

    def register_operation_peer(self, operation_name, peer):
        try:
            self._scheduler.register_job_operation_peer(operation_name, peer)

        except NotFoundError:
            raise InvalidArgumentError(f"Operation name does not exist: [{operation_name}]")

    def unregister_operation_peer(self, operation_name, peer):
        try:
            self._scheduler.unregister_job_operation_peer(operation_name, peer, self._discard_unwatched_jobs)

        except NotFoundError:
            # Operation already unregistered due to being finished/cancelled
            pass
        except TimeoutError:
            self.__logger.warning(f"Could not unregister_operation_peer for "
                                  f"operation_name=[{operation_name}], peer=[{peer}] due to"
                                  f" timeout.", exc_info=True)

    def stream_operation_updates(self, operation_name, context):
        for error, operation in self._scheduler.stream_operation_updates(operation_name, context,
                                                                         self._operation_stream_keepalive_timeout):
            if error is not None:
                error.last_response = operation
                raise error

            yield operation

            if not context.is_active() or operation.done:
                return
        return

    def get_storage_capabilities(self):
        if self._storage_capabilities is None:
            self._storage_capabilities = self.__storage.get_capabilities()
        return self._storage_capabilities

    def get_action_cache_capabilities(self):
        hash_type = None
        capabilities = None
        if self._action_cache is not None:
            capabilities = ActionCacheUpdateCapabilities()
            hash_type = self._action_cache.hash_type()
            capabilities.update_enabled = self._action_cache.allow_updates
        return hash_type, capabilities
