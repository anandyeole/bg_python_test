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
Action Cache
============

Implements an in-memory action Cache
"""

import logging

from ...utils import get_hash_type


class ActionCache:

    def __init__(self, cache):
        """Initialises a new ActionCache instance.

        Args:
            cache (ActionCacheABC): The cache to use to store results.

        """
        self._logger = logging.getLogger(__name__)
        self._instance_name = None

        self._cache = cache

    # --- Public API ---

    @property
    def instance_name(self):
        return self._instance_name

    @instance_name.setter
    def instance_name(self, instance_name):
        self._instance_name = instance_name
        self._cache.instance_name = instance_name

    def setup_grpc(self):
        self._cache.setup_grpc()

    def start(self) -> None:
        self._cache.start()

    def stop(self) -> None:
        self._cache.stop()

    @property
    def allow_updates(self):
        return self._cache.allow_updates

    def hash_type(self):
        return get_hash_type()

    def register_instance_with_server(self, instance_name, server):
        """Names and registers the action-cache instance with a given server."""
        if self._instance_name is None:
            server.add_action_cache_instance(self, instance_name)

            self._instance_name = instance_name
            self._cache.instance_name = instance_name

        else:
            raise AssertionError("Instance already registered")

    def get_action_result(self, action_digest):
        """Retrieves the cached result for an Action.

        If there is no cached result found, returns None.

        Args:
            action_digest (Digest): The digest of the Action to retrieve the
                cached result of.

        """
        return self._cache.get_action_result(action_digest)

    def update_action_result(self, action_digest, action_result):
        """Stores a result for an Action in the cache.

        If the result has a non-zero exit code and `cache_failed_actions` is False
        for this cache, the result is not cached.

        Args:
            action_digest (Digest): The digest of the Action whose result is
                being cached.
            action_result (ActionResult): The result to cache for the given
                Action digest.

        """
        self._cache.update_action_result(action_digest, action_result)
