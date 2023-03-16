# Copyright (C) 2019, 2020 Bloomberg LP
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
Write Once Action Cache
=======================

An ActionCache backend implementation which only allows each digest to
be written to once. Any subsequent requests to cache a result for the
same digests will not be permitted.

This can be used to wrap a different ActionCache implementation to provide
immutability of cache results.

"""


import logging

from buildgrid._exceptions import NotFoundError, UpdateNotAllowedError
from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import (
    ActionResult,
    Digest
)
from buildgrid.server.actioncache.caches.action_cache_abc import ActionCacheABC


class WriteOnceActionCache(ActionCacheABC):

    def __init__(self, action_cache: ActionCacheABC):
        ActionCacheABC.__init__(self)
        self._logger = logging.getLogger(__name__)
        self._action_cache = action_cache

    @property
    def allow_updates(self):
        return self._action_cache.allow_updates

    def get_action_result(self, action_digest: Digest) -> ActionResult:
        """Retrieves the cached ActionResult for the given Action digest.

        Args:
            action_digest (Digest): The digest of the Action to retrieve the
                cached result of.

        Returns:
            The cached ActionResult matching the given digest or raises
            NotFoundError.

        """
        return self._action_cache.get_action_result(action_digest)

    def update_action_result(self, action_digest: Digest,
                             action_result: ActionResult) -> None:
        """Stores the result for a given digest in the cache.

        If the digest already exists in the cache, then an UpdateNotAllowedError
        is raised instead of storing the result.

        Args:
            action_digest (Digest): The digest of the Action whose result is
                being cached.
            action_result (ActionResult): The result to cache for the given
                Action digest.

        """
        try:
            self._action_cache.get_action_result(action_digest)
            # This should throw NotFoundError or actually exist
            self._logger.warning("Result already cached for action "
                                 f"[{action_digest.hash}/{action_digest.size_bytes}], "
                                 "WriteOnceActionCache won't overwrite it to "
                                 f"the new action_result=[{action_result}]")

            raise UpdateNotAllowedError("Result already stored for this action digest;"
                                        "WriteOnceActionCache doesn't allow updates.")
        except NotFoundError:
            self._action_cache.update_action_result(action_digest, action_result)
