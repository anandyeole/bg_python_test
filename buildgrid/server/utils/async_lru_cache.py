# Copyright (C) 2022 Bloomberg LP
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

from asyncio import Lock
from collections import OrderedDict
from datetime import datetime, timedelta
from typing import Generic, Optional, TypeVar

from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import ActionResult
from buildgrid._protos.google.longrunning.operations_pb2 import Operation


T = TypeVar('T', ActionResult, Operation, bytes)


class _CacheEntry(Generic[T]):

    def __init__(self, value: T, ttl: int):
        self._never_expire = ttl <= 0
        self._expiry_date = datetime.utcnow() + timedelta(seconds=ttl)
        self.value: T = value

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return repr(self.value)

    def is_fresh(self) -> bool:
        return self._never_expire or datetime.utcnow() < self._expiry_date


class LruCache(Generic[T]):

    """Class implementing an async-safe LRU cache with an asynchronous API.

    This class provides LRU functionality similar to the existing LruCache
    class, but providing an asynchronous API and using asynchronous locks
    internally. This allows it to be used in asyncio coroutines without
    blocking the event loop.

    This class also supports setting a TTL on cache entries. Cleanup of
    entries which have outlived their TTL is done lazily when ``LruCache.get``
    is called for the relevant key.

    """

    def __init__(self, max_length: int):
        """Initialize a new LruCache.

        Args:
            max_length (int): The maximum number of entries to store in the
                cache at any time.

        """
        self._cache: "OrderedDict[str, _CacheEntry[T]]" = OrderedDict()
        self._lock = Lock()
        self._max_length = max_length

    def max_size(self) -> int:
        """Get the maximum number of items that can be stored in the cache.

        Calling ``LruCache.set`` when there are already this many entries
        in the cache will cause the oldest entry to be dropped.

        Returns:
            int: The maximum number of items to store.

        """
        return self._max_length

    async def size(self) -> int:
        """Get the current number of items in the cache.

        Returns:
            int: The number of items currently stored.

        """
        async with self._lock:
            return len(self._cache)

    async def get(self, key: str) -> Optional[T]:
        """Get the value for a given key, or ``None``.

        This method returns the value for a given key. If the key isn't
        in the cache, or the value's TTL has expired, then this returns
        ``None`` instead. In the case of a TTL expiry, the key is removed
        from the cache to save space.

        Args:
            key (str): The key to get the corresponding value for.

        Returns:
            A value mapped to the given key, or ``None``

        """
        async with self._lock:
            entry = self._cache.get(key)
            if entry is not None:
                if entry.is_fresh():
                    self._cache.move_to_end(key)
                    return entry.value
                else:
                    del self._cache[key]
        return None

    async def set(self, key: str, value: T, ttl: int) -> None:
        """Set the value and TTL of a key.

        This method sets the value and TTL of a key. A TTL of 0
        or lower means that the key won't expire due to age. Keys
        with a TTL of 0 or lower will still be dropped when they're
        the least-recently-used key.

        Args:
            key (str): The key to update.
            value (T): The value to map to the key.
            ttl (int): A TTL (in seconds) for the key.

        """
        async with self._lock:
            while len(self._cache) >= self._max_length:
                self._cache.popitem(last=False)
            self._cache[key] = _CacheEntry(value, ttl)
