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
Reference Cache
==================

Implements an in-memory reference cache.

For a given key, it
"""

import collections
import logging

from buildgrid._exceptions import NotFoundError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2


class ReferenceCache:

    def __init__(self, storage, max_cached_refs, allow_updates=True):
        """ Initialises a new ReferenceCache instance.

        Args:
            storage (StorageABC): storage backend instance to be used.
            max_cached_refs (int): maximum number of entries to be stored.
            allow_updates (bool): allow the client to write to storage
        """
        self.__logger = logging.getLogger(__name__)

        self._instance_name = None

        self.__storage = storage

        self._allow_updates = allow_updates
        self._max_cached_refs = max_cached_refs
        self._digest_map = collections.OrderedDict()

    # --- Public API ---

    @property
    def instance_name(self):
        return self._instance_name

    def setup_grpc(self):
        self.__storage.setup_grpc()

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def register_instance_with_server(self, instance_name, server):
        """Names and registers the refs instance with a given server."""
        if self._instance_name is None:
            server.add_reference_storage_instance(self, instance_name)

            self._instance_name = instance_name

        else:
            raise AssertionError("Instance already registered")

    @property
    def allow_updates(self):
        return self._allow_updates

    def get_digest_reference(self, key):
        """Retrieves the cached Digest for the given key.

        Args:
            key: key for Digest to query.

        Returns:
            The cached Digest matching the given key or raises
            NotFoundError.
        """
        if key in self._digest_map:
            reference_result = self.__storage.get_message(self._digest_map[key],
                                                          remote_execution_pb2.Digest)

            if reference_result is not None:
                return reference_result

            del self._digest_map[key]

        raise NotFoundError(f"Key not found: {key}")

    def update_reference(self, key, result):
        """Stores the result in cache for the given key.

        If the cache size limit has been reached, the oldest cache entries will
        be dropped before insertion so that the cache size never exceeds the
        maximum numbers of entries allowed.

        Args:
            key: key to store result.
            result (Digest): result digest to store.
        """
        if not self._allow_updates:
            raise NotImplementedError("Updating cache not allowed")

        if self._max_cached_refs == 0:
            return

        while len(self._digest_map) >= self._max_cached_refs:
            self._digest_map.popitem(last=False)

        result_digest = self.__storage.put_message(result)
        self._digest_map[key] = result_digest
