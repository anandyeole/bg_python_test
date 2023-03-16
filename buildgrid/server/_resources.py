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


import functools
from threading import Lock

import grpc


class ExecContext:

    __max_workers = None
    __busy_workers = None
    __busy_workers_lock = Lock()

    @classmethod
    def init(cls, max_workers):
        # Max 4/5 workers for blocking requests:
        cls.__max_workers = 4 * max_workers // 5
        cls.__busy_workers = 0

    @classmethod
    def request_worker(cls):
        if cls.__max_workers is None:
            return True
        with cls.__busy_workers_lock:
            if cls.__busy_workers >= cls.__max_workers:
                return False
            cls.__busy_workers += 1
            return True

    @classmethod
    def release_worker(cls):
        if cls.__max_workers is None:
            return
        with cls.__busy_workers_lock:
            cls.__busy_workers -= 1


def limit(exec_context):
    """RPC method decorator for execution resource control.

    This decorator is design to be used together with an :class:`ExecContext`
    execution context holder::

        @limit(ExecContext)
        def Execute(self, request, context):

    Args:
        exec_context(ExecContext): Execution context holder.
    """
    def __limit_decorator(behavior):
        """RPC resource control method decorator."""
        @functools.wraps(behavior)
        def __limit_wrapper(self, request, context):
            """RPC resource control method wrapper."""
            if not exec_context.request_worker():
                context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED,
                              "Max. number of simultaneous request reached")
                return None

            context.add_callback(__limit_callback)

            return behavior(self, request, context)

        def __limit_callback():
            exec_context.release_worker()

        return __limit_wrapper

    return __limit_decorator
