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

from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.build.bazel.semver import semver_pb2
from buildgrid.settings import HIGH_REAPI_VERSION, LOW_REAPI_VERSION


class CapabilitiesInstance:

    def __init__(self, cas_instance=None, action_cache_instance=None, execution_instance=None):
        self.__logger = logging.getLogger(__name__)

        self._instance_name = None

        self.__cas_instance = cas_instance
        self.__action_cache_instance = action_cache_instance
        self.__execution_instance = execution_instance

        self.__high_api_version = None
        self.__low_api_version = None

    # --- Public API ---

    @property
    def instance_name(self):
        return self._instance_name

    def register_instance_with_server(self, instance_name, server):
        """Names and registers the capabilities instance with a given server."""
        if self._instance_name is None:
            server.add_capabilities_instance(self, instance_name)

            self._instance_name = instance_name

        else:
            raise AssertionError("Instance already registered")

    def add_cas_instance(self, cas_instance):
        self.__cas_instance = cas_instance

    def add_action_cache_instance(self, action_cache_instance):
        self.__action_cache_instance = action_cache_instance

    def add_execution_instance(self, execution_instance):
        self.__execution_instance = execution_instance

    def get_capabilities(self):
        cache_capabilities = self._get_cache_capabilities()
        execution_capabilities = self._get_capabilities_execution()

        if self.__high_api_version is None:
            self.__high_api_version = self._split_semantic_version(HIGH_REAPI_VERSION)
        if self.__low_api_version is None:
            self.__low_api_version = self._split_semantic_version(LOW_REAPI_VERSION)

        server_capabilities = remote_execution_pb2.ServerCapabilities()
        server_capabilities.cache_capabilities.CopyFrom(cache_capabilities)
        server_capabilities.execution_capabilities.CopyFrom(execution_capabilities)
        server_capabilities.low_api_version.CopyFrom(self.__low_api_version)
        server_capabilities.high_api_version.CopyFrom(self.__high_api_version)

        return server_capabilities

    # --- Private API ---

    def _get_cache_capabilities(self):
        capabilities = remote_execution_pb2.CacheCapabilities()
        action_cache_update_capabilities = remote_execution_pb2.ActionCacheUpdateCapabilities()

        if self.__cas_instance:
            capabilities.digest_function.extend([self.__cas_instance.hash_type()])
            capabilities.max_batch_total_size_bytes = self.__cas_instance.max_batch_total_size_bytes()
            capabilities.symlink_absolute_path_strategy = self.__cas_instance.symlink_absolute_path_strategy()
            # TODO: execution priority #102
            # capabilities.cache_priority_capabilities =

        if self.__action_cache_instance:
            capabilities.digest_function.extend([self.__action_cache_instance.hash_type()])
            action_cache_update_capabilities.update_enabled = self.__action_cache_instance.allow_updates

        # If an endpoint doesn't have a CAS instance, but does have an Execution
        # instance, it needs to report the capabilities of the CAS used by the
        # Execution instance. See https://gitlab.com/BuildGrid/buildgrid/-/issues/174
        # for more context and discussion of this.
        #
        # There's also a convenient side-effect to this, it means separated BuildGrid
        # services can all be deployed behind a single proxy endpoint without needing
        # extra work to make GetCapabilities requests make sense.
        if self.__execution_instance and not self.__cas_instance:
            remote_cache_capabilities = self.__execution_instance.get_storage_capabilities()
            capabilities.CopyFrom(remote_cache_capabilities)

        # Similarly, if an endpoint has an Execution instance but no ActionCache
        # instance, it needs to report the capabilities of the ActionCache used
        # by the Execution instance.
        if self.__execution_instance and not self.__action_cache_instance:
            ac_hash_type, remote_action_cache_capabilities = self.__execution_instance.get_action_cache_capabilities()
            if not capabilities.digest_function and ac_hash_type is not None:
                capabilities.digest_function.extend([ac_hash_type])
            if remote_action_cache_capabilities is not None:
                action_cache_update_capabilities.CopyFrom(remote_action_cache_capabilities)

        capabilities.action_cache_update_capabilities.CopyFrom(action_cache_update_capabilities)
        return capabilities

    def _get_capabilities_execution(self):
        capabilities = remote_execution_pb2.ExecutionCapabilities()
        if self.__execution_instance:
            capabilities.exec_enabled = True
            capabilities.digest_function = self.__execution_instance.hash_type()
            # TODO: execution priority #102
            # capabilities.execution_priority =

        else:
            capabilities.exec_enabled = False

        return capabilities

    def _split_semantic_version(self, version_string):
        major_version, minor_version, patch_version = version_string.split('.')

        semantic_version = semver_pb2.SemVer()
        semantic_version.major = int(major_version)
        semantic_version.minor = int(minor_version)
        semantic_version.patch = int(patch_version)

        return semantic_version
