# Copyright (C) 2019 Bloomberg LP
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
RedisStorage
==================

A storage provider that stores data in a persistent redis store.
https://redis.io/

Redis client: redis-py
https://github.com/andymccurdy/redis-py

"""
import io
import functools
import logging

import redis

from .storage_abc import StorageABC


def redis_client_exception_wrapper(func):
    """ Wrapper from handling redis client exceptions. """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except redis.RedisError:
            logging.getLogger(__name__).exception(
                f"Redis Exception in [{func.__name__}]")
            raise RuntimeError
    return wrapper


class RedisStorage(StorageABC):
    """ Interface for communicating with a redis store. """
    @redis_client_exception_wrapper
    def __init__(self, **kwargs):
        self._logger = logging.getLogger(__name__)
        self._client = redis.Redis(**kwargs)

    @redis_client_exception_wrapper
    def has_blob(self, digest) -> bool:
        self._logger.debug(f"Checking for blob: [{digest}]")
        return bool(self._client.exists(digest.hash + '_' + str(digest.size_bytes)))

    @redis_client_exception_wrapper
    def get_blob(self, digest):
        self._logger.debug(f"Getting blob: [{digest}]")
        blob = self._client.get(digest.hash + '_' + str(digest.size_bytes))
        return None if blob is None else io.BytesIO(blob)

    @redis_client_exception_wrapper
    def delete_blob(self, digest):
        self._logger.debug(f"Deleting blob: [{digest}]")
        self._client.delete(digest.hash + '_' + str(digest.size_bytes))

    @redis_client_exception_wrapper
    def begin_write(self, digest) -> io.BytesIO:
        return io.BytesIO()

    @redis_client_exception_wrapper
    def commit_write(self, digest, write_session):
        self._logger.debug(f"Writing blob: [{digest}]")
        self._client.set(digest.hash + '_' + str(digest.size_bytes), write_session.getvalue())
        write_session.close()
