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
# Disable unwanted pylint rules
# pylint: disable=anomalous-backslash-in-string,broad-except


import asyncio
import logging
from functools import partial

import grpc

from buildgrid._enums import LeaseState
from buildgrid._protos.google.rpc import code_pb2
from buildgrid._protos.google.rpc import status_pb2

from .tenant import Tenant


class TenantManager:
    """Manages a number of :class:`Tenant`\ s.

    Creates work to do, monitors and removes leases of work.
    """

    def __init__(self):
        """Initialises an instance of the :class:`TenantManager`."""
        self.__logger = logging.getLogger(__name__)
        self._tenants = {}
        self._tasks = {}

    def create_tenancy(self, lease):
        """Create a new :class:`Tenant`.

        Args:
            lease (:class:Lease) : Lease of work to do.
        """
        lease_id = lease.id

        if lease_id not in self._tenants:
            tenant = Tenant(lease)
            self._tenants[lease_id] = tenant

        else:
            raise KeyError(f"Lease id already exists: [{lease_id}]")

    def remove_tenant(self, lease_id):
        """Attempts to remove a tenant.

        If the tenant has not been cancelled, it will cancel it. If the tenant has
        not completed, it will not remove it.

        Args:
            lease_id (string) : The lease id.
        """
        if not self._tenants[lease_id].lease_cancelled:
            self.__logger.error("Attempting to remove a lease not cancelled."
                                "Bot will attempt to cancel lease."
                                f"Lease id=[{lease_id}]")
            self.cancel_tenancy(lease_id)

        elif not self._tenants[lease_id].tenant_completed:
            self.__logger.debug("Lease cancelled but tenant not completed."
                                f"Lease id=[{lease_id}]")

        else:
            self.__logger.debug(f"Removing tenant=[{lease_id}]")
            self._tenants.pop(lease_id)
            self._tasks.pop(lease_id)

    def get_leases(self):
        """Returns a list of leases managed by this instance."""
        leases = []
        for tenant in self._tenants.values():
            leases.append(tenant.lease)

        if not leases:
            return None

        return leases

    def get_lease_ids(self):
        """Returns a list of lease ids."""
        return self._tenants.keys()

    def get_lease_state(self, lease_id):
        """Returns the lease state

        Args:
            lease_id (string) : The lease id.
        """
        return self._tenants[lease_id].get_lease_state()

    def complete_lease(self, lease_id, status, task=None):
        """Informs the :class:`TenantManager` that the lease has completed.

        If it was not cancelled, it will update with the result returned from
        the task.

        Args:
            lease_id (string) : The lease id.
            status (:class:`Status`) : The final status of the lease.
            task (asyncio.Task) : The task of work.
        """
        if status is not None:
            self._update_lease_status(lease_id, status)

        if task and not task.cancelled():
            try:
                result = task.result()

            except grpc.RpcError as e:
                self.__logger.debug(f'Job was unsuccessful, with code {e.code()}')
                self._update_lease_status(lease_id, e.code())

            except Exception as e:
                self.__logger.debug(
                    'An exception occurred during execution of the work. '
                    f'Setting status to {code_pb2.INTERNAL}')
                status = status_pb2.Status()
                status.code = code_pb2.INTERNAL
                status.message = str(e)
                self._update_lease_status(lease_id, status)

            else:
                self._update_lease_result(lease_id, result.result)

            self._update_lease_state(lease_id, LeaseState.COMPLETED)

    def create_work(self, lease_id, work, context):
        """Creates work to do.

        Will place work on an asyncio loop with a callback to `complete_lease`.

        Args:
            lease_id (string) : The lease id.
            work (func) : Work to do.
            context (context) : Context for work function.
        """
        self._update_lease_state(lease_id, LeaseState.ACTIVE)
        tenant = self._tenants[lease_id]
        task = asyncio.ensure_future(tenant.run_work(work, context))

        task.add_done_callback(partial(self.complete_lease, lease_id, None))
        self._tasks[lease_id] = task

    def cancel_tenancy(self, lease_id):
        """Cancels tenancy and any work being done.

        Args:
            lease_id (string) : The lease id.
        """
        if not self._tenants[lease_id].lease_cancelled:
            self._tenants[lease_id].cancel_lease()
            self._tasks[lease_id].cancel()

    def tenant_completed(self, lease_id):
        """Returns `True` if the work has been completed.

        Args:
            lease_id (string) : The lease id.
        """
        return self._tenants[lease_id].tenant_completed

    async def wait_on_tenants(self, timeout):
        if self._tasks:
            tasks = self._tasks.values()
            await asyncio.wait(tasks,
                               timeout=timeout,
                               return_when=asyncio.FIRST_COMPLETED)

    def _update_lease_result(self, lease_id, result):
        """Updates the lease with the result."""
        self._tenants[lease_id].update_lease_result(result)

    def _update_lease_state(self, lease_id, state):
        """Updates the lease state."""
        self._tenants[lease_id].update_lease_state(state)

    def _update_lease_status(self, lease_id, status):
        """Updates the lease status."""
        self._tenants[lease_id].update_lease_status(status)
