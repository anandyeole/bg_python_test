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

from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import (
    ActionResult,
    Digest
)
from buildgrid.server.actioncache.caches.action_cache_abc import ActionCacheABC
from buildgrid._exceptions import NotFoundError


class WithCacheActionCache(ActionCacheABC):

    """
    An ActionCache that first makes use of a storage and fallback cache

    """

    def __init__(self, cache: ActionCacheABC, fallback: ActionCacheABC,
                 allow_updates, cache_failed_actions):
        """Initialise a new Action Cache with a fallback cache.

        Args:
            cache (ActionCacheABC): Local cache backend instance to be used.
            fallback(ActionCacheABC): Fallback backend instance to be used
            allow_updates(bool): Allow updates pushed to the Action Cache.
                Defaults to ``True``.
            cache_failed_actions(bool): Whether to store failed (non-zero exit
                code) actions. Default to ``True``.
        """
        ActionCacheABC.__init__(self, allow_updates=allow_updates)

        self._logger = logging.getLogger(__name__)
        self._cache_failed_actions = cache_failed_actions

        self.instance_name = None
        self._cache = cache
        self._fallback = fallback

    def get_action_result(self, action_digest: Digest) -> ActionResult:
        """Retrieves the cached result for an Action.

        Will first attempt to retrieve result from cache and then fallback. A
        NotFoundError is raised if both cache and fallback return None.

        Args:
            action_digest (Digest): The digest of the Action to retrieve the
                cached result of.

        """
        cache_result = None
        fallback_result = None
        key = self._get_key(action_digest)
        try:
            cache_result = self._cache.get_action_result(action_digest)
        except NotFoundError:
            pass
        except Exception:
            self._logger.exception(
                f"Unexpected error in cache get_action_result; action_digest=[{action_digest}]")

        if cache_result is None:
            fallback_result = self._fallback.get_action_result(action_digest)
        else:
            return cache_result

        if fallback_result is not None:
            self._cache.update_action_result(action_digest, fallback_result)
            return fallback_result

        raise NotFoundError(f"Key not found: {key}")

    def update_action_result(self, action_digest: Digest,
                             action_result: ActionResult) -> None:
        """Stores a result for an Action in the cache.

        Will attempt to store result in cache, and then in fallback cache. If
        the result has a non-zero exit code and `cache_failed_actions` is False
        for this cache, the result is not cached.

        Args:
            action_digest (Digest): The digest of the Action whose result is
                being cached.
            action_result (ActionResult): The result to cache for the given
                Action digest.

        """
        if self._cache_failed_actions or action_result.exit_code == 0:

            if not self._allow_updates:
                raise NotImplementedError("Updating cache not allowed")

            try:
                self._cache.update_action_result(action_digest, action_result)

            except Exception:
                self._logger.warning(
                    f"Failed to cache action [{action_digest.hash}/{action_digest.size_bytes}]",
                    exc_info=True)

            self._fallback.update_action_result(action_digest, action_result)

    def _get_key(self, action_digest: Digest):
        """Get a hashable cache key from a given Action digest.

        Args:
            action_digest (Digest): The digest to produce a cache key for.

        """
        return (action_digest.hash, action_digest.size_bytes)
