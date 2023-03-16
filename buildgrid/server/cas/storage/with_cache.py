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
WithCacheStorage
==================

A storage provider that first checks a cache, then tries a slower
fallback provider.

To ensure clients can reliably store blobs in CAS, only `get_blob`
calls are cached -- `has_blob` and `missing_blobs` will always query
the fallback.
"""

from concurrent.futures import ThreadPoolExecutor
import io
import logging
from typing import List

from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import Digest
from buildgrid.server.metrics_names import (
    CAS_CACHE_BULK_READ_HIT_COUNT_NAME,
    CAS_CACHE_BULK_READ_HIT_PERCENTAGE_NAME,
    CAS_CACHE_BULK_READ_MISS_COUNT_NAME,
    CAS_CACHE_GET_BLOB_HIT_COUNT_NAME,
    CAS_CACHE_GET_BLOB_MISS_COUNT_NAME
)
from buildgrid.server.metrics_utils import Distribution
from .storage_abc import StorageABC


class _OutputTee(io.BufferedIOBase):
    """A file-like object that writes data to two file-like objects.

    The files should be in blocking mode; non-blocking mode is unsupported.
    """

    def __init__(self, file_a, file_b):
        super().__init__()

        self._original_a = file_a
        if isinstance(file_a, io.BufferedIOBase):
            self._a = file_a
        else:
            self._a = io.BufferedWriter(file_a)

        self._original_b = file_b
        if isinstance(file_b, io.BufferedIOBase):
            self._b = file_b
        else:
            self._b = io.BufferedWriter(file_b)

    def close(self):
        super().close()
        if self._a:
            self._a.close()
        self._b.close()

    def flush(self):
        try:
            if self._a:
                self._a.flush()
        except Exception:
            self.__logger.warning("Failed to flush write session to cache storage", exc_info=True)
            self._a.close()
            self._a = None
        self._b.flush()

    def readable(self):
        return False

    def seekable(self):
        return False

    def write(self, b):
        try:
            if self._a:
                self._a.write(b)
        except Exception:
            self.__logger.warning("Failed to write to cache storage", exc_info=True)
            self._a.close()
            self._a = None

        return self._b.write(b)

    def writable(self):
        return True


class _CachingTee(io.RawIOBase):
    """A file-like object that wraps a 'fallback' file, and when it's
    read, writes the resulting data to a 'cache' storage provider.

    You can call seek() with this class, but doing so will cancel
    reading into the cache.

    Does not support non-blocking mode.
    """

    def __init__(self, fallback_file, digest, cache):
        super().__init__()

        self.__logger = logging.getLogger(__name__)

        self._file = fallback_file
        self._digest = digest
        self._cache = cache
        self._cache_session = cache.begin_write(digest)

    def close(self):
        super().close()
        try:
            if self._cache_session:
                self._cache_session.write(self._file.read())
                self._cache.commit_write(self._digest, self._cache_session)
        except Exception:
            self.__logger.warning("Failed to write to cache storage after reading blob", exc_info=True)
            self._cache_session = None
        self._file.close()

    def readable(self):
        return True

    def seekable(self):
        return True

    def writable(self):
        return False

    def readall(self):
        data = self._file.read()
        try:
            if self._cache_session:
                self._cache_session.write(data)
        except Exception:
            self.__logger.warning("Failed to write to cache storage after reading blob", exc_info=True)
            self._cache_session = None
        return data

    def readinto(self, b):
        bytes_read = self._file.readinto(b)
        try:
            if self._cache_session:
                self._cache_session.write(b[:bytes_read])
        except Exception:
            self.__logger.warning("Failed to write to cache storage after reading blob", exc_info=True)
            self._cache_session = None
        return bytes_read

    def seek(self, offset, whence=io.SEEK_SET):
        """ If seek() is called, cancel the cache write. """
        if whence == io.SEEK_SET and offset == self._file.tell():
            return offset
        else:
            self._cache_session = None
            return self._file.seek(offset, whence)


class WithCacheStorage(StorageABC):

    def __init__(self, cache, fallback, defer_fallback_writes=False,
                 fallback_writer_threads=20):
        self.__logger = logging.getLogger(__name__)

        self._instance_name = None
        self._cache = cache
        self._fallback = fallback
        self._defer_fallback_writes = defer_fallback_writes
        if self._defer_fallback_writes:
            # pylint: disable=consider-using-with
            self._executor = ThreadPoolExecutor(
                max_workers=fallback_writer_threads,
                thread_name_prefix="WithCacheFallbackWriter"
            )

    def __del__(self):
        # If we have a fallbackwriter, wait until all work has finished to exit safely
        if self._defer_fallback_writes:
            self.__logger.warning(
                "Shutting down WithCacheStorage with deferred writes. Finishing up deferred writes."
            )
            self._executor.shutdown(wait=True)
            self.__logger.info(
                "WithCacheFallbackWriter finished deferred writes and pool was shut down."
            )

    def setup_grpc(self):
        self._cache.setup_grpc()
        self._fallback.setup_grpc()

    def has_blob(self, digest):
        try:
            if self._defer_fallback_writes and self._cache.has_blob(digest):
                return True
        except Exception:
            self.__logger.warning(f"Failed to check existence of [{digest}] in cache storage", exc_info=True)

        return self._fallback.has_blob(digest)

    def get_blob(self, digest):
        try:
            cache_result = self._cache.get_blob(digest)
            if cache_result is not None:
                with Distribution(CAS_CACHE_GET_BLOB_HIT_COUNT_NAME) as metric_cache_hit:
                    metric_cache_hit.count = 1
                return cache_result
        except Exception:
            self.__logger.warning(f"Failed to read [{digest}] from cache storage", exc_info=True)

        fallback_result = self._fallback.get_blob(digest)
        if fallback_result is None:
            return None

        with Distribution(CAS_CACHE_GET_BLOB_MISS_COUNT_NAME) as metric_cache_miss:
            metric_cache_miss.count = 1
        return _CachingTee(fallback_result, digest, self._cache)

    def delete_blob(self, digest):
        self._fallback.delete_blob(digest)
        try:
            self._cache.delete_blob(digest)
        except Exception:
            self.__logger.warning(f"Failed to delete [{digest}] from cache storage", exc_info=True)

    def bulk_delete(self, digests: List[Digest]) -> List[str]:
        # Only report back failures from the fallback
        try:
            cache_failures = self._cache.bulk_delete(digests)
            for failure in cache_failures:
                self.__logger.warning(f"Failed to delete [{failure}] from cache storage")
        except Exception:
            self.__logger.warning("Failed to bulk delete blobs from cache storage", exc_info=True)

        return self._fallback.bulk_delete(digests)

    def begin_write(self, digest):
        return _OutputTee(self._cache.begin_write(digest), self._fallback.begin_write(digest))

    def _commit_fallback_write(self, digest, write_session):
        self._fallback.commit_write(digest, write_session._original_b)

    def commit_write(self, digest, write_session):
        write_session.flush()
        written_to_cache = False
        try:
            if write_session._a:
                self._cache.commit_write(digest, write_session._original_a)
                written_to_cache = True
        except Exception:
            self.__logger.warning(f"Failed to commit write of [{digest}] to cache storage", exc_info=True)

        if written_to_cache and self._defer_fallback_writes:
            self._executor.submit(
                self._commit_fallback_write,
                digest,
                write_session
            )
        else:
            self._fallback.commit_write(digest, write_session._original_b)

    def missing_blobs(self, digests):
        return self._fallback.missing_blobs(digests)

    def bulk_update_blobs(self, blobs):
        try:
            self._cache.bulk_update_blobs(blobs)
        except Exception:
            self.__logger.warning("Failed to bulk update blobs in cache storage", exc_info=True)

        return self._fallback.bulk_update_blobs(blobs)

    def bulk_read_blobs(self, digests):
        try:
            cache_blobs = self._cache.bulk_read_blobs(digests)
        except Exception:
            self.__logger.warning("Failed to bulk read blobs from cache storage", exc_info=True)
            cache_blobs = {}

        with Distribution(CAS_CACHE_BULK_READ_HIT_COUNT_NAME) as metric_cache_hit:
            metric_cache_hit.count = len(cache_blobs)

        uncached_digests = [digest for digest in digests if cache_blobs.get(digest.hash, None) is None]

        with Distribution(CAS_CACHE_BULK_READ_MISS_COUNT_NAME) as metric_cache_miss:
            metric_cache_miss.count = len(digests) - len(cache_blobs)

        with Distribution(CAS_CACHE_BULK_READ_HIT_PERCENTAGE_NAME) as metric_cache_percent:
            if len(digests) == 0:
                metric_cache_percent.count = 0
            else:
                metric_cache_percent.count = len(cache_blobs) / len(digests) * 100

        fallback_blobs = self._fallback.bulk_read_blobs(uncached_digests)
        cache_blobs.update(fallback_blobs)

        uncached_blobs = []
        for digest in uncached_digests:
            blob = fallback_blobs.get(digest.hash)
            if blob is not None:
                uncached_blobs.append((digest, blob.read()))
                blob.seek(0)

        try:
            self._cache.bulk_update_blobs(uncached_blobs)
        except Exception:
            self.__logger.warning("Failed to add blobs to cache storage after bulk read", exc_info=True)

        return cache_blobs

    def is_cleanup_enabled(self):
        return self._fallback.is_cleanup_enabled()

    def set_instance_name(self, instance_name):
        self._instance_name = instance_name
        self._fallback.set_instance_name(instance_name)
        self._cache.set_instance_name(instance_name)

    def get_capabilities(self):
        return self._fallback.get_capabilities()
