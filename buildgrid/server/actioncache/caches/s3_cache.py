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
S3 Action Cache
==================

Implements an Action Cache using S3 to store cache entries.

"""

import io
import logging
from typing import Any, Dict, Optional

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from buildgrid._exceptions import (
    NotFoundError,
    StorageFullError
)
from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import (
    ActionResult,
    Digest,
)
from buildgrid.server.actioncache.caches.action_cache_abc import ActionCacheABC
from buildgrid.server.cas.storage.storage_abc import StorageABC
from buildgrid.server.s3 import s3utils
from buildgrid._enums import ActionCacheEntryType
from buildgrid.server.metrics_names import AC_UNUSABLE_CACHE_HITS_METRIC_NAME
from buildgrid.server.metrics_utils import publish_counter_metric


class S3ActionCache(ActionCacheABC):
    def __init__(self, storage: StorageABC, allow_updates: bool=True,
                 cache_failed_actions: bool=True,
                 entry_type: Optional[ActionCacheEntryType]=ActionCacheEntryType.ACTION_RESULT_DIGEST,
                 migrate_entries: Optional[bool]=False,
                 bucket: Optional[str]=None,
                 endpoint: Optional[str]=None, access_key: Optional[str]=None,
                 secret_key: Optional[str]=None,
                 config: Optional[BotoConfig]=BotoConfig()):
        """ Initialises a new ActionCache instance using S3 to persist the action cache.

        Args:
            storage (StorageABC): storage backend instance to be used to store ActionResults.
            allow_updates (bool): allow the client to write to storage
            cache_failed_actions (bool): whether to store failed actions in the Action Cache
            entry_type (ActionCacheEntryType): whether to store ActionResults or their digests.
            migrate_entries (bool): if set, migrate entries that contain a value with
                a different `EntryType` to `entry_type` as they are accessed
                (False by default).

            bucket (str): Name of bucket
            endpoint (str): URL of endpoint.
            access-key (str): S3-ACCESS-KEY
            secret-key (str): S3-SECRET-KEY
        """
        ActionCacheABC.__init__(self, storage=storage, allow_updates=allow_updates)
        self._logger = logging.getLogger(__name__)

        self._entry_type = entry_type
        self._migrate_entries = migrate_entries

        self._cache_failed_actions = cache_failed_actions
        assert bucket is not None
        self._bucket_template = bucket

        # Boto logs can be very verbose, restrict to WARNING
        for boto_logger_name in [
                'boto3', 'botocore',
                's3transfer', 'urllib3'
        ]:
            boto_logger = logging.getLogger(boto_logger_name)
            boto_logger.setLevel(max(boto_logger.level, logging.WARNING))

        # Only pass arguments with a value to support testing with moto_server
        client_kwargs = {}  # type: Dict[str, Any]
        if endpoint is not None:
            client_kwargs["endpoint_url"] = endpoint
        if access_key is not None:
            client_kwargs["aws_access_key_id"] = access_key
        if secret_key is not None:
            client_kwargs["aws_secret_access_key"] = secret_key
        client_kwargs["config"] = config

        self._s3cache = boto3.client('s3', **client_kwargs)

    # --- Public API ---
    @property
    def allow_updates(self) -> bool:
        return self._allow_updates

    def get_action_result(self, action_digest: Digest) -> ActionResult:
        """Retrieves the cached ActionResult for the given Action digest.

        Args:
            action_digest: The digest to get the result for

        Returns:
            The cached ActionResult matching the given key or raises
            NotFoundError.
        """
        action_result = self._get_action_result(action_digest)
        if action_result is not None:
            if self._action_result_blobs_still_exist(action_result):
                return action_result

            publish_counter_metric(
                AC_UNUSABLE_CACHE_HITS_METRIC_NAME,
                1,
                {"instance_name": self._instance_name}
            )

            if self._allow_updates:
                self._logger.debug(f"Removing {action_digest.hash}/{action_digest.size_bytes} "
                                   "from cache due to missing blobs in CAS")
                self._delete_key_from_cache(action_digest)

        raise NotFoundError(f"Key not found: {action_digest.hash}/{action_digest.size_bytes}")

    def update_action_result(self, action_digest: Digest,
                             action_result: ActionResult) -> None:
        """Stores the result in cache for the given key.

        Args:
            action_digest (Digest): digest of Action to update
            action_result (ActionResult): ActionResult to store.
        """
        if not self._allow_updates:
            raise NotImplementedError("Updating cache not allowed")

        if self._cache_failed_actions or action_result.exit_code == 0:
            action_result_digest = self._storage.put_message(action_result)

            if self._entry_type == ActionCacheEntryType.ACTION_RESULT_DIGEST:
                self._update_cache_key(action_digest, action_result_digest.SerializeToString())
            else:
                self._update_cache_key(action_digest, action_result.SerializeToString())

            self._logger.info(
                f"Result cached for action [{action_digest.hash}/{action_digest.size_bytes}]")

    # --- Private API ---
    def _get_action_result(self, digest: Digest) -> Optional[ActionResult]:
        """Get an `ActionResult` from the cache.

        If present, returns the `ActionResult` corresponding to the given digest.
        Otherwise returns None.

        When configured to do so, if the value stored in the entry in S3 contains
        a different type, it will convert it to `entry_type`.

        Args:
            digest: Action digest to get the associated ActionResult digest for

        Returns:
            The `ActionResult` or None if the digest is not present
        """
        value_in_cache = self._get_value_from_cache(digest)
        if not value_in_cache:
            return None

        # Attempting to parse the entry as a `Digest` first:
        action_result_digest = Digest.FromString(value_in_cache)
        if len(action_result_digest.hash) == len(digest.hash):
            # The cache contains the `Digest` of the `ActionResult`:
            action_result = self._storage.get_message(action_result_digest,
                                                      ActionResult)

            # If configured, update the entry to contain an `ActionResult`:
            if self._entry_type == ActionCacheEntryType.ACTION_RESULT and self._migrate_entries:
                self._logger.debug(f"Converting entry for {digest.hash}/{digest.size_bytes} "
                                   "from Digest to ActionResult")
                self._update_cache_key(digest, action_result.SerializeToString())

        else:
            action_result = ActionResult.FromString(value_in_cache)

            # If configured, update the entry to contain a `Digest`:
            if self._entry_type == ActionCacheEntryType.ACTION_RESULT_DIGEST and self._migrate_entries:
                self._logger.debug(f"Converting entry for {digest.hash}/{digest.size_bytes} "
                                   "from ActionResult to Digest")
                action_result_digest = self._storage.put_message(action_result)
                self._update_cache_key(digest, action_result_digest.SerializeToString())

        return action_result

    def _get_value_from_cache(self, digest: Digest) -> Optional[bytes]:
        """Get the bytes stored in cache for the given Digest.

            Args:
                digest: Action digest to get the associated bytes in S3

            Returns:
                bytes or None if the digest is not present
        """

        try:
            s3object = self._get_s3object(digest)
            s3object.fileobj = io.BytesIO()
            s3utils.get_object(self._s3cache, s3object)
            s3object.fileobj.seek(0)
            return s3object.fileobj.read()
        except ClientError as e:
            if e.response['Error']['Code'] not in ['404', 'NoSuchKey']:
                raise
            return None

    def _update_cache_key(self, digest: Digest, value: bytes) -> None:
        try:
            s3object = self._get_s3object(digest)
            s3object.fileobj = io.BytesIO(value)
            s3object.filesize = len(value)
            s3utils.put_object(self._s3cache, s3object)
        except ClientError as error:
            if error.response['Error']['Code'] == 'QuotaExceededException':
                raise StorageFullError("ActionCache S3 Quota Exceeded.") from error
            raise error

    def _delete_key_from_cache(self, digest: Digest) -> None:
        """Remove an entry from the ActionCache

        Args:
            digest: entry to remove from the ActionCache

        Returns:
            None
        """
        if not self._allow_updates:
            raise NotImplementedError("Updating cache not allowed")

        self._s3cache.delete_object(Bucket=self._get_bucket_name(digest), Key=self._get_key(digest))

    @staticmethod
    def _get_key(digest: Digest) -> str:
        """
        Given a `Digest`, returns the key used to store its
        corresponding entry in S3.
        """
        return f'{digest.hash}_{digest.size_bytes}'

    def _get_bucket_name(self, digest: Digest) -> str:
        """Return the formatted bucket name for a given digest.

        This formats the bucket template defined in the configuration file
        using a digest to be stored in the cache. This allows the cache
        contents to be sharded across multiple S3 buckets, allowing us to
        cache more Actions than can be stored in a single S3 bucket.

        Currently the only variable interpolated into the template is
        ``digest``, which contains the hash part of the Digest. A template
        string which includes undefined variables will result in a
        non-functional ActionCache.

        Args:
            digest (Digest): The digest to get the bucket name for.

        Returns:
            str: The bucket name corresponding to the given Digest.

        """
        try:
            return self._bucket_template.format(digest=digest.hash)
        except IndexError:
            self._logger.error(
                f"Could not generate bucket name for digest=[{digest.hash}]. "
                "This is either a misconfiguration in the BuildGrid S3 "
                "ActionCache bucket configuration, or a badly formed request."
            )
            raise

    def _get_s3object(self, digest: Digest):
        return s3utils.S3Object(self._get_bucket_name(digest), self._get_key(digest))
