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
#
# Disable broad exception catch
# pylint: disable=broad-except


import asyncio
import logging
import threading

from functools import partial

import grpc

from buildgrid._enums import LeaseState
from buildgrid._exceptions import BotError


class Tenant:

    def __init__(self, lease):
        """Initialises a new :class:`Tenant`.

        Args:
            lease (:class:`Lease`) : A new lease.

        Raises:
            ValueError: If `lease` is not in a `PENDING` state.
        """

        if lease.state != LeaseState.PENDING.value:
            raise ValueError("Lease state not `PENDING`")

        self._lease = lease
        self.__logger = logging.getLogger(__name__)
        self.__lease_cancelled = False
        self.__tenant_completed = False

    @property
    def lease(self):
        """Returns the lease"""
        return self._lease

    @property
    def tenant_completed(self):
        """Returns `True` if the work has completed or sucessfully stopped its work."""
        return self.__tenant_completed

    @property
    def lease_cancelled(self):
        """Returns `True` if the lease has been cancelled."""
        return self.__lease_cancelled

    def cancel_lease(self):
        """Cancel the lease."""
        self.__lease_cancelled = True
        self.update_lease_state(LeaseState.CANCELLED)

    def get_lease_state(self):
        """Returns the :class:`LeaseState`."""
        return LeaseState(self._lease.state)

    def update_lease_result(self, result):
        """Update the lease result.

        Args:
            result (:class:`Any`) : The result of the lease."""
        self._lease.result.CopyFrom(result)

    def update_lease_state(self, state):
        """Update the lease state.

        Args:
            state (:class:`LeaseState`) : State of the lease.
        """
        self._lease.state = state.value

    def update_lease_status(self, status):
        """Update the lease status.

        Args:
            status (:class:`Status`) : Status of the lease.
        """
        self._lease.status.CopyFrom(status)

    async def run_work(self, work, context=None, executor=None):
        """Runs work.

        Work is run in an executor.

        Args:
            work (func) : Work to do.
            context (context) : Context for work.
            executor (:class:`ThreadPoolExecutor`) : Thread pool for work to run on.
        """
        self.__logger.debug(f"Work created. Lease_id=[{self._lease.id}]")

        loop = asyncio.get_event_loop()

        try:
            event = threading.Event()
            lease = await loop.run_in_executor(executor, partial(work, self._lease, context, event))

        except asyncio.CancelledError:
            self.__logger.error(f"Lease cancelled: lease_id=[{self._lease.id}]")
            event.set()
            self.__tenant_completed = True
            # Propagate error to task wrapper
            raise

        except grpc.RpcError as e:
            self.__logger.error(e)
            self.__tenant_completed = True
            raise

        except BotError as e:
            if e.reason:
                self.__logger.error(e.reason)
            if str(e):
                self.__logger.error(e)
            if e.detail:
                self.__logger.error(e.detail)
            self.__tenant_completed = True
            raise

        except Exception as e:
            self.__logger.error(e)
            self.__tenant_completed = True
            raise

        self.__tenant_completed = True
        self.__logger.debug(f"Work completed: lease_id=[{lease.id}]")

        return lease
