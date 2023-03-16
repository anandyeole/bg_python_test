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


import logging
import collections

from buildgrid._exceptions import NotFoundError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import (
    ActionResult,
    Digest
)
from buildgrid.server.actioncache.caches.action_cache_abc import ActionCacheABC
from buildgrid.server.cas.storage.storage_abc import StorageABC
from buildgrid.server.metrics_names import AC_UNUSABLE_CACHE_HITS_METRIC_NAME
from buildgrid.server.metrics_utils import publish_counter_metric


class LruActionCache(ActionCacheABC):

    """In-memory Action Cache implementation with LRU eviction.

    This cache has a configurable fixed size, evicting the least recently
    accessed entry when adding a new entry would exceed the fixed size. The
    cache is entirely stored in memory so its contents are lost on restart.

    This type of cache is ideal for use cases that need a simple and fast
    cache, with no requirements for longevity of the cache content. It is not
    recommended to use this type of cache in situations where you may wish to
    obtain cached results a reasonable time in the future, due to its fixed
    size.

    """

    def __init__(self, storage: StorageABC, max_cached_refs: int,
                 allow_updates: bool=True, cache_failed_actions: bool=True):
        """Initialise a new in-memory LRU Action Cache.

        Args:
            storage (StorageABC): Storage backend instance to be used.
            max_cached_refs (int): Maximum number of entries to store in the cache.
            allow_updates (bool): Whether to allow writing to the cache. If false,
                this is a read-only cache for all clients.
            cache_failed_actions (bool): Whether or not to cache Actions with
                non-zero exit codes.

        """
        ActionCacheABC.__init__(self, storage=storage)

        self._logger = logging.getLogger(__name__)
        self._cache_failed_actions = cache_failed_actions
        self._storage = storage
        self._allow_updates = allow_updates
        self._max_cached_refs = max_cached_refs
        self._digest_map = collections.OrderedDict()  # type: ignore

    def get_action_result(self, action_digest: Digest) -> ActionResult:
        """Retrieves the cached result for an Action.

        If there is no cached result found, returns None.

        Args:
            action_digest (Digest): The digest of the Action to retrieve the
                cached result of.

        """
        key = self._get_key(action_digest)
        if key in self._digest_map:
            action_result = self._storage.get_message(self._digest_map[key],
                                                      remote_execution_pb2.ActionResult)

            if action_result is not None:
                if self._action_result_blobs_still_exist(action_result):
                    self._digest_map.move_to_end(key)
                    return action_result

                publish_counter_metric(
                    AC_UNUSABLE_CACHE_HITS_METRIC_NAME,
                    1,
                    {"instance-name": self._instance_name}
                )

                if self._allow_updates:
                    self._logger.debug(f"Removing {action_digest.hash}/{action_digest.size_bytes}"
                                       "from cache due to missing blobs in CAS")
                    del self._digest_map[key]

        raise NotFoundError(f"Key not found: {key}")

    def update_action_result(self, action_digest: Digest,
                             action_result: ActionResult) -> None:
        """Stores a result for an Action in the cache.

        If the result has a non-zero exit code and `cache_failed_actions` is False
        for this cache, the result is not cached.

        Args:
            action_digest (Digest): The digest of the Action whose result is
                being cached.
            action_result (ActionResult): The result to cache for the given
                Action digest.

        """
        if self._cache_failed_actions or action_result.exit_code == 0:
            key = self._get_key(action_digest)
            if not self._allow_updates:
                raise NotImplementedError("Updating cache not allowed")

            if self._max_cached_refs == 0:
                return

            while len(self._digest_map) >= self._max_cached_refs:
                self._digest_map.popitem(last=False)

            result_digest = self._storage.put_message(action_result)
            self._digest_map[key] = result_digest

            self._logger.info(
                f"Result cached for action [{action_digest.hash}/{action_digest.size_bytes}]")

    def _get_key(self, action_digest: Digest):
        """Get a hashable cache key from a given Action digest.

        Args:
            action_digest (Digest): The digest to produce a cache key for.

        """
        return (action_digest.hash, action_digest.size_bytes)
