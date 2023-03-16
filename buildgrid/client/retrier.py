# Copyright (C) 2021-2022 Bloomberg LP
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


import random
import time

import grpc

from buildgrid._exceptions import (
    CancelledError,
    FailedPreconditionError,
    InvalidArgumentError,
    NotFoundError,
    PermissionDeniedError,
    StorageFullError)


class GrpcRetrier:
    def __init__(self, retries: int, max_backoff: int = 64, should_backoff: bool = True):
        """Initializes a new :class:`GrpcRetrier`.

        Args:
            retries (int): The maximum number of attempts for each RPC call.
            max_backoff (int): The maximum time to wait between retries.
            should_backoff (bool): Whether to backoff at all. Always set this to True except in tests.
        """

        self._retries = retries
        self._max_backoff = max_backoff
        self._should_backoff = should_backoff

    def retry(self, func, *args, **kwargs):
        attempts = 0
        while True:
            try:
                return func(*args, **kwargs)
            except grpc.RpcError as e:
                status_code = e.code()

                # Retry only on UNAVAILABLE and ABORTED
                if status_code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.ABORTED):
                    attempts += 1
                    if attempts > self._retries:
                        raise ConnectionError(e.details()) from e
                    if self._should_backoff:
                        # Sleep for 2^(N-1) + jitter seconds, where N is # of attempts
                        jitter = random.uniform(0, 1)
                        time.sleep(pow(2, attempts - 1) + jitter)

                elif status_code == grpc.StatusCode.CANCELLED:
                    raise CancelledError(e.details()) from e

                elif status_code == grpc.StatusCode.INVALID_ARGUMENT:
                    raise InvalidArgumentError(e.details()) from e

                elif status_code == grpc.StatusCode.DEADLINE_EXCEEDED:
                    raise TimeoutError(e.details()) from e

                elif status_code == grpc.StatusCode.NOT_FOUND:
                    raise NotFoundError("Requested data does not exist on remote") from e

                elif status_code == grpc.StatusCode.PERMISSION_DENIED:
                    raise PermissionDeniedError(e.details()) from e

                elif status_code == grpc.StatusCode.RESOURCE_EXHAUSTED:
                    raise StorageFullError(e.details()) from e

                elif status_code == grpc.StatusCode.FAILED_PRECONDITION:
                    raise FailedPreconditionError(e.details()) from e

                elif status_code == grpc.StatusCode.UNIMPLEMENTED:
                    raise NotImplementedError(e.details()) from e

                else:
                    raise
