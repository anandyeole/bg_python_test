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
from typing import Optional

import dns.resolver
import redis
from redis.backoff import ExponentialBackoff
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
    TimeoutError as RedisTimeoutError
)
from redis.retry import Retry as RedisRetry
from redis.sentinel import Sentinel

from buildgrid._enums import ActionCacheEntryType
from buildgrid._exceptions import NotFoundError
from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import (
    ActionResult,
    Digest
)
from buildgrid.server.actioncache.caches.action_cache_abc import ActionCacheABC
from buildgrid.server.cas.storage.redis import redis_client_exception_wrapper
from buildgrid.server.cas.storage.storage_abc import StorageABC
from buildgrid.server.metrics_names import AC_UNUSABLE_CACHE_HITS_METRIC_NAME
from buildgrid.server.metrics_utils import publish_counter_metric


class RedisActionCache(ActionCacheABC):

    def __init__(self, storage: StorageABC, allow_updates: bool=True,
                 cache_failed_actions: bool=True,
                 entry_type: Optional[ActionCacheEntryType] = ActionCacheEntryType.ACTION_RESULT_DIGEST,
                 migrate_entries: Optional[bool] = False,
                 host: Optional[str]=None, port: Optional[int]=None,
                 password: Optional[str]=None, db: int=0,
                 dns_srv_record: Optional[str] = None,
                 sentinel_master_name: Optional[str] = None,
                 max_retries=3):
        """ Initialises a new ActionCache instance using Redis.
            Stores the `ActionResult` message as a value.

                Args:
                    storage (StorageABC): storage backend instance to be used to store ActionResults.
                    allow_updates (bool): allow the client to write to storage
                    cache_failed_actions (bool): whether to store failed actions in the Action Cache
                    entry_type (ActionCacheEntryType): whether to store ActionResults or their digests.
                    migrate_entries (bool): if set, migrate entries that contain a value with
                        a different `ActionCacheEntryType` to `entry_type` as they are accessed
                        (False by default).
                    host (str): Redis host
                    port (int): Redis port
                    password (str): Redis password
                    db (int): Redis database number
                    dns_srv_record (str): Domain name for SRV record used to
                        obtain a service's network details (host/port)
                    sentinel_master_name (str): The service name of the Redis
                        master instance, used in a Redis sentinel configuration
                """
        ActionCacheABC.__init__(self, storage=storage, allow_updates=allow_updates)

        self._logger = logging.getLogger(__name__)
        self._cache_failed_actions = cache_failed_actions
        self._entry_type = entry_type
        self._migrate_entries = migrate_entries
        self._dns_srv_record = dns_srv_record
        self._sentinel_master_name = sentinel_master_name
        self._sentinel = None
        self._retry_errors = [RedisConnectionError, RedisTimeoutError]
        self._retry_strategy = RedisRetry(ExponentialBackoff(64, 1), max_retries)

        if port is not None and host is not None:
            self._host = host
            self._port = port

        if not self._sentinel_master_name:
            if self._dns_srv_record is not None:
                redis_instances = []
                redis_instances = self._perform_dns_srv_request(self._dns_srv_record)
                if redis_instances:
                    self._host = redis_instances[0][0]
                    self._port = redis_instances[0][1]
                else:
                    raise Exception("Host/port not resolvable from DNS SRV record")

            # Setting decode_responses=False fixes the folowing mypy error:
            # Argument 1 to "FromString" of "Message" has incompatible
            # type "str"; expected "ByteString"
            self._client = redis.Redis(host=self._host, port=self._port, db=db,  # type: ignore
                                       password=password, decode_responses=False,  # type: ignore
                                       retry_on_error=self._retry_errors,
                                       retry=self._retry_strategy)  # type: ignore
        else:
            # Obtain master and replicas from Redis sentinels
            self._obtain_cache_instances()

    @redis_client_exception_wrapper
    def get_action_result(self, action_digest: Digest) -> ActionResult:
        key = self._get_key(action_digest)
        action_result = self._get_action_result(key, action_digest)
        if action_result is not None:
            if self._action_result_blobs_still_exist(action_result):
                return action_result

            publish_counter_metric(
                AC_UNUSABLE_CACHE_HITS_METRIC_NAME,
                1,
                {"instance_name": self._instance_name}
            )

            if self._allow_updates:
                self._logger.debug(f"Removing {action_digest.hash}/{action_digest.size_bytes}"
                                   "from cache due to missing blobs in CAS")
                if not self._sentinel_master_name:
                    self._client.delete(key)
                else:
                    try:
                        self._client_master.delete(key)
                    except (ConnectionError, TimeoutError):
                        try:
                            self._obtain_cache_instances()
                            self._client_master.delete(key)
                        except (ConnectionError, TimeoutError):
                            raise ConnectionError("Could not connect to Redis master instance")

        raise NotFoundError(f"Key not found: [{key}]")

    @redis_client_exception_wrapper
    def update_action_result(self, action_digest: Digest,
                             action_result: ActionResult) -> None:
        if not self._allow_updates:
            raise NotImplementedError("Updating cache not allowed")

        if self._cache_failed_actions or action_result.exit_code == 0:
            action_result_digest = self._storage.put_message(action_result)

            cache_key = self._get_key(action_digest)
            if not self._sentinel_master_name:
                if self._entry_type == ActionCacheEntryType.ACTION_RESULT_DIGEST:
                    self._client.set(cache_key, action_result_digest.SerializeToString())
                else:
                    self._client.set(cache_key, action_result.SerializeToString())
            else:
                if self._entry_type == ActionCacheEntryType.ACTION_RESULT_DIGEST:
                    try:
                        self._client_master.set(cache_key, action_result_digest.SerializeToString())
                    except (ConnectionError, TimeoutError):
                        try:
                            self._obtain_cache_instances()
                            self._client_master.set(cache_key, action_result_digest.SerializeToString())
                        except (ConnectionError, TimeoutError):
                            raise ConnectionError("Could not connect to Redis master instance")
                else:
                    try:
                        self._client_master.set(cache_key, action_result.SerializeToString())
                    except (ConnectionError, TimeoutError):
                        try:
                            self._obtain_cache_instances()
                            self._client_master.set(cache_key, action_result.SerializeToString())
                        except (ConnectionError, TimeoutError):
                            raise ConnectionError("Could not connect to Redis master instance")

            self._logger.info(
                f"Result cached for action [{action_digest.hash}/{action_digest.size_bytes}]")

    def _get_key(self, action_digest: Digest) -> str:
        return f'action-cache.{action_digest.hash}_{action_digest.size_bytes}'

    def _get_action_result(self, key: str, action_digest: Digest) -> Optional[ActionResult]:
        if not self._sentinel_master_name:
            value_in_cache = self._client.get(key)
        else:
            try:
                value_in_cache = self._client_replica.get(key)
            except (ConnectionError, TimeoutError):
                try:
                    self._obtain_cache_instances()
                    value_in_cache = self._client_replica.get(key)
                except (ConnectionError, TimeoutError):
                    raise ConnectionError("Could not connect to Redis replica instance")

        if value_in_cache is None:
            return None

        # Attempting to parse the entry as a `Digest` first:
        action_result_digest = Digest.FromString(value_in_cache)
        if len(action_result_digest.hash) == len(action_digest.hash):
            # The cache contains the `Digest` of the `ActionResult`:
            action_result = self._storage.get_message(action_result_digest,
                                                      ActionResult)

            # If configured, update the entry to contain an `ActionResult`:
            if self._entry_type == ActionCacheEntryType.ACTION_RESULT and self._migrate_entries:
                self._logger.debug(f"Converting entry for {action_digest.hash}/{action_digest.size_bytes} "
                                   "from Digest to ActionResult")
                if not self._sentinel_master_name:
                    self._client.set(key, action_result.SerializeToString())
                else:
                    try:
                        self._client_master.set(key, action_result.SerializeToString())
                    except (ConnectionError, TimeoutError):
                        try:
                            self._obtain_cache_instances()
                            self._client_master.set(key, action_result.SerializeToString())
                        except (ConnectionError, TimeoutError):
                            raise ConnectionError("Could not connect to Redis master instance")
        else:
            action_result = ActionResult.FromString(value_in_cache)

            # If configured, update the entry to contain a `Digest`:
            if self._entry_type == ActionCacheEntryType.ACTION_RESULT_DIGEST and self._migrate_entries:
                self._logger.debug(f"Converting entry for {action_digest.hash}/{action_digest.size_bytes} "
                                   "from ActionResult to Digest")
                action_result_digest = self._storage.put_message(action_result)
                if not self._sentinel_master_name:
                    self._client.set(key, action_result_digest.SerializeToString())
                else:
                    try:
                        self._client_master.set(key, action_result_digest.SerializeToString())
                    except (ConnectionError, TimeoutError):
                        try:
                            self._obtain_cache_instances()
                            self._client_master.set(key, action_result_digest.SerializeToString())
                        except (ConnectionError, TimeoutError):
                            raise ConnectionError("Could not connect to Redis master instance")

        return action_result

    def _obtain_cache_instances(self):
        self._sentinel_instances = []
        if self._dns_srv_record is not None:
            self._sentinel_instances = self._perform_dns_srv_request(self._dns_srv_record)
        else:
            self._sentinel_instances.append((self._host, str(self._port)))

        if self._sentinel_instances:
            self._client_master = self._resolve_master(
                self._sentinel_instances, self._retry_errors, self._retry_strategy)
            self._client_replica = self._resolve_replica()
        else:
            raise Exception("No Sentinel host and port resolvable")

    def _perform_dns_srv_request(self, domain_name):
        srv_list = []

        try:
            srv_records = dns.resolver.resolve(domain_name, 'SRV')
        except Exception:
            self._logger.debug("Unable to resolve DNS name")
            raise Exception

        for srv in srv_records:
            srv_list.append((str(srv.target).rstrip('.'), str(srv.port)))

        return srv_list

    def _resolve_master(self, sentinel_instances, retry_errors, retry_strategy):
        sentinel = Sentinel(
            sentinel_instances,
            socket_timeout=0.2,
            retry_on_error=retry_errors,
            retry=retry_strategy)

        discovered_master = sentinel.discover_master(self._sentinel_master_name)
        client_master = sentinel.master_for(str(self._sentinel_master_name),
                                            socket_timeout=0.2)
        self._logger.info(f"Connected to Redis master at {discovered_master}")

        if client_master is not None:
            self._sentinel = sentinel
            return client_master
        else:
            raise ConnectionError("Could not connect to any Redis master instances")

    def _resolve_replica(self):
        discovered_replica = None
        client_replica = None

        discovered_replica = self._sentinel.discover_slaves(self._sentinel_master_name)

        if discovered_replica is not None:
            client_replica = self._sentinel.slave_for(str(self._sentinel_master_name),
                                                      socket_timeout=0.2)
        if client_replica is not None:
            self._logger.info(f"Connected to Redis replica at {discovered_replica}")
            return client_replica
        else:
            raise ConnectionError("Could not connect to any Redis replica instances")
