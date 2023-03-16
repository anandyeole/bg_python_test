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


from contextlib import contextmanager

import buildgrid.server.context as context_module
from buildgrid._exceptions import NotFoundError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2, remote_execution_pb2_grpc
from buildgrid.client.retrier import GrpcRetrier


@contextmanager
def query(channel,
          instance=None,
          retries: int = 0,
          max_backoff: int = 64,
          should_backoff: bool = True):
    """Context manager generator for the :class:`ActionCacheClient` class."""
    client = ActionCacheClient(channel,
                               instance=instance,
                               retries=retries,
                               max_backoff=max_backoff,
                               should_backoff=should_backoff)
    try:
        yield client
    finally:
        client.close()


class ActionCacheClient:
    """Remote ActionCache service client helper.

    The :class:`ActionCacheClient` class comes with a generator factory function
    that can be used together with the `with` statement for context management::

        from buildgrid.client.actioncache import query

        with query(channel, instance='build') as action_cache:
            digest, action_result = action_cache.get(action_digest)
    """

    def __init__(self,
                 channel,
                 instance=None,
                 retries: int = 0,
                 max_backoff: int = 64,
                 should_backoff: bool = True):
        """Initializes a new :class:`ActionCacheClient` instance.

        Args:
            channel (grpc.Channel): a gRPC channel to the ActionCache endpoint.
            instance (str, optional): the targeted instance's name.
        """
        self._grpc_retrier = (GrpcRetrier(retries=retries,
                                          max_backoff=max_backoff,
                                          should_backoff=should_backoff))
        self.channel = channel

        self.instance_name = instance

        self.__actioncache_stub = remote_execution_pb2_grpc.ActionCacheStub(self.channel)

    # --- Public API ---

    def get(self, action_digest):
        """Attempt to retrieve cached :obj:`ActionResult` for : given obj:`Action`.
        """
        try:
            return self._grpc_retrier.retry(self._get, action_digest)
        except NotFoundError:
            return None

    def update(self, action_digest, action_result):
        """Attempt to map in cache an :obj:`Action` to an :obj:`ActionResult`.
        """
        try:
            return self._grpc_retrier.retry(self._update, action_digest, action_result)
        except NotFoundError:
            return None

    def _get(self, action_digest):
        """Retrieves the cached :obj:`ActionResult` for a given :obj:`Action`.

        Args:
            action_digest (:obj:`Digest`): the action's digest to query.

        Returns:
            :obj:`ActionResult`: the cached result or None if not found.

        Raises:
            grpc.RpcError: on any network or remote service error.
        """
        request = remote_execution_pb2.GetActionResultRequest()
        if self.instance_name:
            request.instance_name = self.instance_name
        request.action_digest.CopyFrom(action_digest)

        return (self.__actioncache_stub.GetActionResult(request,
                                                        metadata=context_module.metadata_list()))

    def _update(self, action_digest, action_result):
        """Maps in cache an :obj:`Action` to an :obj:`ActionResult`.

        Args:
            action_digest (:obj:`Digest`): the action's digest to update.
            action_result (:obj:`ActionResult`): the action's result.

        Returns:
            :obj:`ActionResult`: the cached result or None on failure.

        Raises:
            grpc.RpcError: on any network or remote service error.
        """
        request = remote_execution_pb2.UpdateActionResultRequest()
        if self.instance_name:
            request.instance_name = self.instance_name
        request.action_digest.CopyFrom(action_digest)
        request.action_result.CopyFrom(action_result)

        return (self.__actioncache_stub.UpdateActionResult(request,
                                                           metadata=context_module.metadata_list()))

    def close(self):
        """Closes the underlying connection stubs."""
        self.__actioncache_stub = None
