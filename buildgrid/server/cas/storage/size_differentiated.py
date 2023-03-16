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


"""
SizeDifferentiatedStorage
=========================

A storage provider which passes requests to other storage providers
based on the size of the blob being requested.

"""

import io
import logging
from typing import List, NamedTuple

# TODO: Use the standard library version of this when we drop support
# for Python 3.7
from typing_extensions import TypedDict

from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import Digest
from .storage_abc import StorageABC


class SizeLimitedStorageType(TypedDict):
    max_size: int
    storage: StorageABC


# NOTE: This exists separately to the TypedDict to allow attribute-based access
# at runtime, rather than relying on string-based access to dictionary keys.
class _SizeLimitedStorage(NamedTuple):
    max_size: int
    storage: StorageABC


class SizeDifferentiatedStorage(StorageABC):

    def __init__(self, storages: List[SizeLimitedStorageType], fallback: StorageABC):
        self._logger = logging.getLogger(__name__)
        self._fallback_storage = fallback
        self._storages = [_SizeLimitedStorage(**storage) for storage in storages]
        self._storages.sort(key=lambda storage: storage.max_size)

    def _storage_from_digest(self, digest: Digest) -> StorageABC:
        for storage in self._storages:
            if digest.size_bytes < storage.max_size:
                return storage.storage
        # If the blob is too big for any of the size-limited storages,
        # put it in the fallback.
        return self._fallback_storage

    def setup_grpc(self):
        for storage_tuple in self._storages:
            storage_tuple.storage.setup_grpc()

    def has_blob(self, digest: Digest) -> bool:
        self._logger.debug(f"Checking for blob: [{digest}]")
        storage = self._storage_from_digest(digest)
        return storage.has_blob(digest)

    def get_blob(self, digest: Digest) -> io.IOBase:
        self._logger.debug(f"Getting blob: [{digest}]")
        storage = self._storage_from_digest(digest)
        return storage.get_blob(digest)

    def delete_blob(self, digest: Digest):
        self._logger.debug(f"Deleting blob: [{digest}]")
        storage = self._storage_from_digest(digest)
        storage.delete_blob(digest)

    def begin_write(self, digest: Digest) -> io.IOBase:
        storage = self._storage_from_digest(digest)
        return storage.begin_write(digest)

    def commit_write(self, digest: Digest, write_session: io.IOBase):
        self._logger.debug(f"Writing blob: [{digest}]")
        storage = self._storage_from_digest(digest)
        storage.commit_write(digest, write_session)
