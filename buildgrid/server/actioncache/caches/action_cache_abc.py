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


from abc import ABC, abstractmethod
import logging

from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import (
    ActionResult,
    Digest,
    Tree
)
from buildgrid.utils import get_hash_type


class ActionCacheABC(ABC):

    def __init__(self, allow_updates=False, storage=None):
        self._logger = logging.getLogger(__name__)
        self._instance_name = None
        self._allow_updates = allow_updates
        self._storage = storage

    @property
    def instance_name(self):
        return self._instance_name

    @instance_name.setter
    def instance_name(self, instance_name):
        self._instance_name = instance_name

    @property
    def allow_updates(self):
        return self._allow_updates

    def hash_type(self):
        return get_hash_type()

    def setup_grpc(self):
        if self._storage is not None:
            self._storage.setup_grpc()

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    # NOTE: This method exists for compatibility reasons. Ideally it should never
    # be used with an up-to-date configuration.
    def register_instance_with_server(self, instance_name, server):
        """Names and registers the action-cache instance with a given server."""
        if hasattr(self, '_logger'):
            self._logger.warning(
                "Cache instances should be defined in a 'caches' list and passed "
                "to an ActionCache service, rather than defined in the 'services' "
                "list themselves.")
        if self._instance_name is None:
            server.add_action_cache_instance(self, instance_name)

            self._instance_name = instance_name

        else:
            raise AssertionError("Instance already registered")

    @abstractmethod
    def get_action_result(self, action_digest: Digest) -> ActionResult:
        raise NotImplementedError()

    @abstractmethod
    def update_action_result(self, action_digest: Digest,
                             action_result: ActionResult) -> None:
        raise NotImplementedError()

    def _action_result_blobs_still_exist(self, action_result: ActionResult) -> bool:
        """Checks CAS for ActionResult output blobs existence.

        Args:
            action_result (ActionResult): ActionResult to search referenced
            output blobs for.

        Returns:
            True if all referenced blobs are present in CAS, False otherwise.
        """
        if not self._storage:
            return True
        blobs_needed = []

        for output_file in action_result.output_files:
            blobs_needed.append(output_file.digest)

        for output_directory in action_result.output_directories:
            blobs_needed.append(output_directory.tree_digest)
            tree = self._storage.get_message(output_directory.tree_digest, Tree)
            if tree is None:
                return False

            for file_node in tree.root.files:
                blobs_needed.append(file_node.digest)

            for child in tree.children:
                for file_node in child.files:
                    blobs_needed.append(file_node.digest)

        if action_result.stdout_digest.hash and not action_result.stdout_raw:
            blobs_needed.append(action_result.stdout_digest)

        if action_result.stderr_digest.hash and not action_result.stderr_raw:
            blobs_needed.append(action_result.stderr_digest)

        missing = self._storage.missing_blobs(blobs_needed)
        if len(missing) != 0:
            self._logger.debug(f"Missing {len(missing)}/{len(blobs_needed)} blobs")
            return False
        return True
