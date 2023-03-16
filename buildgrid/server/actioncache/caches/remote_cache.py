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
Remote Action Cache
===================

Provides an interface to a remote Action Cache. This can be used by other
services (e.g. an Execution service) to communicate with a remote cache.

It provides the same API as any other Action Cache instance backend.

"""


import logging
from typing import Dict, Optional, Tuple

from buildgrid._exceptions import GrpcUninitializedError, NotFoundError
from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import (
    ActionResult,
    Digest
)
from buildgrid.client.actioncache import ActionCacheClient
from buildgrid.client.capabilities import CapabilitiesInterface
from buildgrid.client.channel import setup_channel
from buildgrid.server.actioncache.caches.action_cache_abc import ActionCacheABC


class RemoteActionCache(ActionCacheABC):

    def __init__(
        self,
        remote: str,
        instance_name: str,
        retries=0,
        max_backoff=64,
        request_timeout: Optional[float] = None,
        channel_options: Optional[Tuple[Tuple[str, str], ...]]=None,
        tls_credentials: Optional[Dict[str, str]]=None
    ):
        """Initialises a new RemoteActionCache instance.

        Args:
            remote (str): URL of the remote ActionCache service to open a
                channel to.
            instance_name (str): The instance name of the remote ActionCache
                service.
            channel_options (tuple): Optional tuple of channel options to set
                when opening the gRPC channel to the remote.
            tls_credentials (dict): Optional credentials to use when opening
                the gRPC channel. If unset then an insecure channel will be
                used.

        """
        ActionCacheABC.__init__(self)
        self._logger = logging.getLogger(__name__)
        self._remote_instance_name = instance_name
        self._remote = remote
        self._channel_options = channel_options
        if tls_credentials is None:
            tls_credentials = {}
        self._tls_credentials = tls_credentials
        self._channel = None
        self._allow_updates = None
        self._retries = retries
        self._max_backoff = max_backoff
        self._action_cache = None

    def setup_grpc(self):
        if self._channel is None:
            self._channel, _ = setup_channel(
                self._remote,
                auth_token=None,
                client_key=self._tls_credentials.get("tls-client-key"),
                client_cert=self._tls_credentials.get("tls-client-cert"),
                server_cert=self._tls_credentials.get("tls-server-cert")
            )
        if self._action_cache is None:
            self._action_cache = ActionCacheClient(
                self._channel, self._remote_instance_name,
                self._retries, self._max_backoff)

    @property
    def allow_updates(self) -> bool:
        if self._channel is None:
            raise GrpcUninitializedError("Remote cache used before gRPC initialization.")

        # Check if updates are allowed if we haven't already.
        # This is done the first time update_action_result is called rather
        # than on instantiation because the remote cache may not be running
        # when this object is instantiated.
        if self._allow_updates is None:
            interface = CapabilitiesInterface(self._channel)
            capabilities = interface.get_capabilities(self._remote_instance_name)
            self._allow_updates = (capabilities
                                   .cache_capabilities
                                   .action_cache_update_capabilities
                                   .update_enabled)
        return self._allow_updates

    def get_action_result(self, action_digest: Digest) -> ActionResult:
        """Retrieves the cached result for an Action.

        Queries the remote ActionCache service to retrieve the cached
        result for a given Action digest. If the remote cache doesn't
        contain a result for the Action, then ``None`` is returned.

        Args:
            action_digest (Digest): The digest of the Action to retrieve the
                cached result of.

        """
        if self._action_cache is None:
            raise GrpcUninitializedError("Remote cache used before gRPC initialization.")

        action_result = self._action_cache.get(action_digest)

        if action_result is None:
            key = self._get_key(action_digest)
            raise NotFoundError(f"Key not found: {key}")
        return action_result

    def update_action_result(self, action_digest: Digest,
                             action_result: ActionResult) -> None:
        """Stores a result for an Action in the remote cache.

        Sends an ``UpdateActionResult`` request to the remote ActionCache
        service, to store the result in the remote cache.

        If the remote cache doesn't allow updates, then this raises a
        ``NotImplementedError``.

        Args:
            action_digest (Digest): The digest of the Action whose result is
                being cached.
            action_result (ActionResult): The result to cache for the given
                Action digest.

        """
        if self._action_cache is None:
            raise GrpcUninitializedError("Remote cache used before gRPC initialization.")

        if not self.allow_updates:
            raise NotImplementedError("Updating cache not allowed")
        return self._action_cache.update(action_digest, action_result)

    def _get_key(self, action_digest):
        """Get a hashable cache key from a given Action digest.

        Args:
            action_digest (Digest): The digest to produce a cache key for.

        """
        return (action_digest.hash, action_digest.size_bytes)
