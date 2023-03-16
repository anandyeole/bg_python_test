# Copyright (C) 2021 Bloomberg LP
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

from collections import OrderedDict

from threading import Lock


class LruInMemoryCache:
    def __init__(self, capacity: int):
        """Simple in-memory LRU storage container.

        Args:
            capacity (int): the maximum number of entries that can be cached.
                After exceeding the capacity, older entries will be dropped
                to make room for new ones, following a Least Recently Used
                policy.
        """
        if capacity <= 0:
            raise ValueError("Capacity must be positive")

        self._capacity = capacity

        self._ordered_dict_lock = Lock()
        self._ordered_dict = OrderedDict()  # type: ignore

    @property
    def size(self):
        """Number of elements in the cache."""
        with self._ordered_dict_lock:
            return len(self._ordered_dict)

    @property
    def capacity(self):
        """Maximum number of elements that fit in the cache."""
        return self._capacity

    def update(self, key, value):
        """Update a cache entry."""
        with self._ordered_dict_lock:
            self._ordered_dict[key] = value
            self._ordered_dict.move_to_end(key, last=True)

            if len(self._ordered_dict) > self._capacity:
                self._ordered_dict.popitem(last=False)

    def get(self, key):
        """Get a value defined in the cache. If a value for the given key is
        not found, returns None.
        Updates the last access time of the entry.
        """
        with self._ordered_dict_lock:
            value = self._ordered_dict.get(key, None)
            if value is not None:
                self._ordered_dict.move_to_end(key, last=True)
            return value
