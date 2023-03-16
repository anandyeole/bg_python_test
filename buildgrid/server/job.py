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


from datetime import datetime
import logging
import uuid
from typing import Dict, List, Optional, Set

from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp

from buildgrid._enums import LeaseState, OperationStage
from buildgrid._exceptions import CancelledError, NotFoundError, UpdateNotAllowedError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2
from buildgrid._protos.google.longrunning import operations_pb2
from buildgrid._protos.google.rpc import code_pb2, status_pb2


class Job:
    def __init__(self, *, do_not_cache: bool,
                 action: remote_execution_pb2.Action,
                 action_digest: remote_execution_pb2.Digest,
                 platform_requirements: Optional[Dict[str, Set[str]]]=None,
                 priority: int=0,
                 name: Optional[str]=None,
                 operation_names: Optional[List[str]]=None,
                 cancelled_operation_names: Optional[List[str]]=None,
                 lease: Optional[bots_pb2.Lease]=None,
                 stage: OperationStage=OperationStage.UNKNOWN,
                 cancelled: bool=False,
                 queued_timestamp: Optional[Timestamp]=None,
                 queued_time_duration: Optional[Duration]=None,
                 worker_start_timestamp: Optional[Timestamp]=None,
                 worker_completed_timestamp: Optional[Timestamp]=None,
                 result: Optional[remote_execution_pb2.ExecuteResponse]=None,
                 worker_name: Optional[str]=None,
                 n_tries: int=0,
                 status_code: Optional[int]=None,
                 stdout_stream_name: Optional[str]=None,
                 stdout_stream_write_name: Optional[str]=None,
                 stderr_stream_name: Optional[str]=None,
                 stderr_stream_write_name: Optional[str]=None):
        self.__logger = logging.getLogger(__name__)

        self._name = name or str(uuid.uuid4())
        self._priority = priority
        self._lease = lease

        self.__execute_response = result or remote_execution_pb2.ExecuteResponse()

        self.__queued_timestamp = Timestamp()
        if queued_timestamp is not None:
            self.__queued_timestamp.CopyFrom(queued_timestamp)

        self.__queued_time_duration = Duration()
        if queued_time_duration is not None:
            self.__queued_time_duration.CopyFrom(queued_time_duration)

        self.__worker_start_timestamp = Timestamp()
        if worker_start_timestamp is not None:
            self.__worker_start_timestamp.CopyFrom(worker_start_timestamp)

        self.__worker_completed_timestamp = Timestamp()
        if worker_completed_timestamp is not None:
            self.__worker_completed_timestamp.CopyFrom(worker_completed_timestamp)

        self._action = action
        self._action_digest = action_digest

        # Keep two lists of operations: all and cancelled
        # The fields (other than cancelled) will be populated with the job
        # details when an operation is accessed through `get_operation(operation_name)`
        if operation_names is None:
            operation_names = []
        if cancelled_operation_names is None:
            cancelled_operation_names = []
        self.__operations_all = set(operation_names)
        self.__operations_cancelled = set(cancelled_operation_names)
        self.__lease_cancelled = cancelled
        self.__job_cancelled = cancelled

        self.__operation_metadata = remote_execution_pb2.ExecuteOperationMetadata()
        self.__operation_metadata.action_digest.CopyFrom(action_digest)
        self.__operation_metadata.stage = stage.value
        if stdout_stream_name is not None:
            self.__operation_metadata.stdout_stream_name = stdout_stream_name
        self._stdout_stream_write_name = stdout_stream_write_name
        if stderr_stream_name is not None:
            self.__operation_metadata.stderr_stream_name = stderr_stream_name
        self._stderr_stream_write_name = stderr_stream_write_name

        self._do_not_cache = do_not_cache
        self._n_tries = n_tries
        self._status_code = status_code

        self._platform_requirements = platform_requirements \
            if platform_requirements else {}

        self.worker_name = worker_name

    def __str__(self):
        return (f"Job: name=[{self.name}], action_digest="
                f"[{self.action_digest.hash}/{self.action_digest.size_bytes}]")

    def __lt__(self, other):
        try:
            return self.priority < other.priority
        except AttributeError:
            return NotImplemented

    def __le__(self, other):
        try:
            return self.priority <= other.priority
        except AttributeError:
            return NotImplemented

    def __eq__(self, other):
        if isinstance(other, Job):
            return self.name == other.name
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __gt__(self, other):
        try:
            return self.priority > other.priority
        except AttributeError:
            return NotImplemented

    def __ge__(self, other):
        try:
            return self.priority >= other.priority
        except AttributeError:
            return NotImplemented

    # --- Public API ---

    @property
    def name(self):
        return self._name

    @property
    def cancelled(self):
        return self.__job_cancelled

    @property
    def priority(self):
        return self._priority

    def set_priority(self, new_priority, *, data_store):
        self._priority = new_priority
        data_store.update_job(self.name, {'priority': new_priority})

    @property
    def done(self):
        return self.operation_stage == OperationStage.COMPLETED

    # --- Public API: REAPI ---

    @property
    def platform_requirements(self):
        return self._platform_requirements

    @property
    def do_not_cache(self):
        return self._do_not_cache

    @property
    def action_digest(self):
        return self._action_digest

    @property
    def action(self):
        return self._action

    @property
    def operation_stage(self):
        return OperationStage(self.__operation_metadata.stage)

    @property
    def operation_metadata(self):
        return self.get_metadata(writeable_streams=False)

    @property
    def action_result(self):
        if self.__execute_response is not None:
            return self.__execute_response.result
        else:
            return None

    @property
    def execute_response(self):
        return self.__execute_response

    @execute_response.setter
    def execute_response(self, response):
        self.__execute_response = response

    @property
    def holds_cached_result(self):
        if self.__execute_response is not None:
            return self.__execute_response.cached_result
        else:
            return False

    @property
    def queued_timestamp(self) -> Timestamp:
        return self.__queued_timestamp

    @property
    def queued_timestamp_as_datetime(self) -> Optional[datetime]:
        if self.__queued_timestamp.ByteSize():
            return self.__queued_timestamp.ToDatetime()
        return None

    @property
    def queued_time_duration(self):
        return self.__queued_time_duration

    @property
    def worker_start_timestamp(self) -> Timestamp:
        return self.__worker_start_timestamp

    @property
    def worker_start_timestamp_as_datetime(self) -> Optional[datetime]:
        if self.__worker_start_timestamp.ByteSize():
            return self.__worker_start_timestamp.ToDatetime()
        return None

    @property
    def worker_completed_timestamp(self) -> Timestamp:
        return self.__worker_completed_timestamp

    @property
    def worker_completed_timestamp_as_datetime(self) -> Optional[datetime]:
        if self.__worker_completed_timestamp.ByteSize():
            return self.__worker_completed_timestamp.ToDatetime()
        return None

    def mark_worker_started(self):
        self.__worker_start_timestamp.GetCurrentTime()

    def get_metadata(self, writeable_streams=False):
        operation_metadata = remote_execution_pb2.ExecuteOperationMetadata()
        operation_metadata.CopyFrom(self.__operation_metadata)
        if writeable_streams and self._stdout_stream_write_name:
            operation_metadata.stdout_stream_name = self._stdout_stream_write_name
        if writeable_streams and self._stderr_stream_write_name:
            operation_metadata.stderr_stream_name = self._stderr_stream_write_name
        if self.__job_cancelled:
            operation_metadata.stage = OperationStage.COMPLETED.value
        return operation_metadata

    def set_action_url(self, url):
        """Generates a CAS browser URL for the job's action."""
        if url.for_message('action', self.action_digest):
            self.__execute_response.message = url.generate()

    def set_cached_result(self, action_result, data_store):
        """Allows specifying an action result from the action cache for the job.

        Note:
            This won't trigger any :class:`Operation` stage transition.

        Args:
            action_result (ActionResult): The result from cache.
        """
        self.__execute_response.result.CopyFrom(action_result)
        self.__execute_response.cached_result = True
        data_store.store_response(self)

    def n_peers(self, watch_spec):
        if watch_spec is None:
            return 0
        return len(watch_spec.peers)

    def n_peers_for_operation(self, operation_name, watch_spec):
        if watch_spec is None:
            return 0
        return len(watch_spec.peers_for_operation(operation_name))

    def register_new_operation(self, *, data_store, request_metadata=None):
        """Subscribes to a new job's :class:`Operation` stage changes.

        Returns:
            str: The name of the subscribed :class:`Operation`.
        """
        new_operation_name = str(uuid.uuid4())
        self.__operations_all.add(new_operation_name)

        data_store.create_operation(
            operation_name=new_operation_name,
            job_name=self._name,
            request_metadata=request_metadata)
        self.__logger.debug(f"Operation created for job [{self._name}]: [{new_operation_name}]")

        return new_operation_name

    def get_all_operations(self) -> List[operations_pb2.Operation]:
        """Gets all :class:`Operation` objects related to a job.

        Returns:
            list: A list of :class:`Operation` objects.
        """
        return [
            self.get_operation(operation_name) for operation_name in self.__operations_all
        ]

    def get_operation(self, operation_name: str) -> operations_pb2.Operation:
        """Returns a copy of the the job's :class:`Operation`
            with all the fields populated from the job fields

        Args:
            operation_name (str): the operation's name.

        Raises:
            NotFoundError: If no operation with `operation_name` exists.
        """
        self.__logger.debug(f"get_operation({operation_name})")
        if operation_name not in self.__operations_all:
            raise NotFoundError(f"Operation does not exist for job: operation_name=[{operation_name}], "
                                f"job_name=[{self._name}]")

        cancelled = operation_name in self.__operations_cancelled

        # Create new proto and populate fields
        operation_proto = operations_pb2.Operation()
        operation_proto.name = operation_name
        operation_proto.done = self.done or cancelled

        # Pack the metadata from the job
        operation_metadata = remote_execution_pb2.ExecuteOperationMetadata()
        operation_metadata.CopyFrom(self.operation_metadata)
        # Set the stage to COMPLETED if the operation is cancelled
        if operation_name in self.__operations_cancelled:
            operation_metadata.stage = OperationStage.COMPLETED.value
        operation_proto.metadata.Pack(operation_metadata)

        # Set the status code/execute response as appropriate
        if cancelled:
            operation_proto.error.CopyFrom(status_pb2.Status(code=code_pb2.CANCELLED))
        elif self._status_code is not None and self._status_code != code_pb2.OK:
            # If there was an error, populate the error field.
            operation_proto.error.CopyFrom(status_pb2.Status(code=self._status_code))
        else:
            # Otherwise, pack the response.
            execute_response = self.execute_response
            operation_proto.response.Pack(execute_response)

        return operation_proto

    def update_operation_stage(self, stage, *, data_store):
        """Operates a stage transition for the job's :class:`Operation`.

        Args:
            stage (OperationStage): the operation stage to transition to.
        """
        if stage.value == self.__operation_metadata.stage:
            return

        changes = {}

        self.__operation_metadata.stage = stage.value
        changes["stage"] = stage.value

        self.__logger.debug(
            f"Stage changed for job [{self._name}]: [{stage.name}] (operation)")

        if self.__operation_metadata.stage == OperationStage.QUEUED.value:
            if self.__queued_timestamp.ByteSize() == 0:
                self.__queued_timestamp.GetCurrentTime()
                changes["queued_timestamp"] = self.queued_timestamp_as_datetime
            self._n_tries += 1
            changes["n_tries"] = self._n_tries

        elif self.__operation_metadata.stage == OperationStage.EXECUTING.value:
            queue_in, queue_out = self.queued_timestamp_as_datetime, datetime.utcnow()
            if queue_in:
                self.__queued_time_duration.FromTimedelta(queue_out - queue_in)
                changes["queued_time_duration"] = self.__queued_time_duration.seconds
            else:
                self.__logger.warning("Tried to calculate `queued_time_duration` "
                                      "but initial queue time wasn't set.")

        data_store.update_job(self.name, changes)

    def cancel_all_operations(self, *, data_store):
        for operation_name in self.__operations_all:
            self.cancel_operation(operation_name, data_store=data_store)

    def cancel_operation(self, operation_name, *, data_store):
        """Triggers a job's :class:`Operation` cancellation.

        This may cancel any job's :class:`Lease` that may have been issued.

        Args:
            operation_name (str): the operation's name.

        Raises:
            NotFoundError: If no operation with `operation_name` exists.
        """
        # NOTE: This assumes that operations weren't created in a sql backend
        # from a different buildgrid
        if operation_name not in self.__operations_all:
            raise NotFoundError(f"Operation name does not exist: [{operation_name}]")

        if operation_name in self.__operations_cancelled:
            self.__logger.debug(
                f"Tried to cancel operation for job [{self._name}]: [{operation_name}] but it was already cancelled.")
        else:
            # Mark operation as cancelled and update in data_store
            self.__operations_cancelled.add(operation_name)
            data_store.update_operation(operation_name, {"cancelled": True})

            self.__logger.debug(
                f"Operation cancelled for job [{self._name}]: [{operation_name}]")

            self._mark_job_as_cancelled_if_all_operations_cancelled(data_store=data_store)

    def _mark_job_as_cancelled_if_all_operations_cancelled(self, *, data_store):
        # If all the operations are cancelled, then mark the job as cancelled too

        # NOTE: In the case of multiple buildgrid instances we may want to
        # check with the data_store first, or, handle that there as there may be more
        # operations created from different buildgrid instances

        self.__job_cancelled = self.__operations_all.issubset(self.__operations_cancelled)

        if self.__job_cancelled:
            self.__operation_metadata.stage = OperationStage.COMPLETED.value
            self.__worker_completed_timestamp.GetCurrentTime()
            changes = {
                "stage": OperationStage.COMPLETED.value,
                "cancelled": True,
                "worker_completed_timestamp": self.worker_completed_timestamp_as_datetime
            }

            data_store.update_job(self.name, changes)
            if self._lease is not None:
                self.cancel_lease(data_store=data_store)

    # --- Public API: RWAPI ---

    @property
    def lease(self):
        return self._lease

    @property
    def lease_state(self):
        if self._lease is not None:
            return LeaseState(self._lease.state)
        else:
            return None

    @property
    def lease_cancelled(self):
        return self.__lease_cancelled

    @property
    def n_tries(self):
        return self._n_tries

    @property
    def status_code(self):
        return self._status_code

    def create_lease(self, worker_name, bot_id=None, *, data_store):
        """Emits a new :class:`Lease` for the job.

        Only one :class:`Lease` can be emitted for a given job. This method
        should only be used once, any further calls are ignored.

        Args:
            worker_name (string): The name of the worker this lease is for.
            bot_id (string): The name of the corresponding bot for this job's worker.
        """
        if self._lease is not None:
            return self._lease
        elif self.__job_cancelled:
            return None

        self._lease = bots_pb2.Lease()
        self._lease.id = self._name

        if self.action is not None:
            self._lease.payload.Pack(self.action)
        else:
            self._lease.payload.Pack(self.action_digest)

        self._lease.state = LeaseState.UNSPECIFIED.value

        if bot_id is None:
            bot_id = "UNKNOWN"
        self.__logger.debug(
            f"Lease created for job [{self._name}]: [{self._lease.id}] (assigned to bot [{bot_id}])")

        self.update_lease_state(LeaseState.PENDING, skip_lease_persistence=True, data_store=data_store)

        self.worker_name = worker_name

        return self._lease

    def update_lease_state(self, state, status=None, result=None,
                           action_cache=None, *, data_store,
                           skip_lease_persistence=False, skip_notify=False):
        """Operates a state transition for the job's current :class:`Lease`.

        Args:
            state (LeaseState): the lease state to transition to.
            status (google.rpc.Status, optional): the lease execution status,
                only required if `state` is `COMPLETED`.
            result (google.protobuf.Any, optional): the lease execution result,
                only required if `state` is `COMPLETED`.
            action_cache (ActionCache) : The ActionCache object,
                only needed to store results
            skip_lease_persistence (bool) : Whether to skip storing the lease
                in the datastore for now
            skip_notify (bool) : Whether to skip notifying of job changes
        """
        if self._lease is None:
            msg = f"Called update_lease_state on job=[{self._name}] but job has no lease"
            self.__logger.debug(msg)
            raise NotFoundError(msg)

        if state.value == self._lease.state:
            return

        job_changes = {}
        lease_changes = {}

        self._lease.state = state.value
        lease_changes["state"] = state.value

        self.__logger.debug(f"State changed for job [{self._name}]: [{state.name}] (lease)")

        if self._lease.state == LeaseState.PENDING.value:
            self.__worker_start_timestamp.Clear()
            self.__worker_completed_timestamp.Clear()
            job_changes["worker_start_timestamp"] = self.worker_start_timestamp_as_datetime
            job_changes["worker_completed_timestamp"] = self.worker_completed_timestamp_as_datetime

            self._lease.status.Clear()
            self._lease.result.Clear()
            lease_changes["status"] = self._lease.status.code

        elif self._lease.state == LeaseState.COMPLETED.value:
            self.__worker_completed_timestamp.GetCurrentTime()
            job_changes["worker_completed_timestamp"] = self.worker_completed_timestamp_as_datetime

            action_result = remote_execution_pb2.ActionResult()

            # TODO: Make a distinction between build and bot failures!
            if status.code != code_pb2.OK:
                self._do_not_cache = True
                job_changes["do_not_cache"] = True

            self._status_code = status.code
            lease_changes["status"] = status.code

            if result is not None and result.Is(action_result.DESCRIPTOR):
                result.Unpack(action_result)

            action_metadata = action_result.execution_metadata
            action_metadata.queued_timestamp.CopyFrom(self.__queued_timestamp)
            action_metadata.worker_start_timestamp.CopyFrom(self.__worker_start_timestamp)
            action_metadata.worker_completed_timestamp.CopyFrom(self.__worker_completed_timestamp)

            self.__execute_response.result.CopyFrom(action_result)
            self.__execute_response.cached_result = False
            self.__execute_response.status.CopyFrom(status)

            response_changes = data_store.store_response(self, commit_changes=False)
            if response_changes:
                job_changes.update(response_changes)

            if (action_cache is not None and
                    action_cache.allow_updates and not self.do_not_cache):
                try:
                    action_cache.update_action_result(self.action_digest, self.action_result)
                    self.__logger.debug(f"Stored action result=[{self.action_result}] for "
                                        f"action_digest=[{self.action_digest}] in ActionCache")
                except UpdateNotAllowedError:
                    # The configuration doesn't allow updating the old result
                    self.__logger.exception(
                        "ActionCache is not configured to allow updates, ActionResult for action_digest="
                        f"[{self.action_digest.hash}/{self.action_digest.size_bytes}] wasn't updated.")
                except Exception:
                    self.__logger.exception("Unable to update ActionCache for action "
                                            f"[{self.action_digest.hash}/{self.action_digest.size_bytes}], "
                                            "results will not be stored in the ActionCache")

        data_store.update_job(self.name, job_changes, skip_notify=skip_notify)
        if not skip_lease_persistence:
            data_store.update_lease(self.name, lease_changes)

    def cancel_lease(self, *, data_store):
        """Triggers a job's :class:`Lease` cancellation.

        Note:
            This will not cancel the job's :class:`Operation`.
        """
        self.__lease_cancelled = True

        self.__logger.debug(f"Lease cancelled for job [{self._name}]: [{self._lease.id}]")

        if self._lease is not None:
            self.update_lease_state(LeaseState.CANCELLED, data_store=data_store)

    def delete_lease(self):
        """Discard the job's :class:`Lease`.

        Note:
            This will not cancel the job's :class:`Operation`.
        """
        if self._lease is not None:
            self.__worker_start_timestamp.Clear()
            self.__worker_completed_timestamp.Clear()

            self.__logger.debug(f"Lease deleted for job [{self._name}]: [{self._lease.id}]")

            self._lease = None

    def set_stdout_stream(self, logstream, *, data_store):
        self.__operation_metadata.stdout_stream_name = logstream.name
        self._stdout_stream_write_name = logstream.write_resource_name

        data_store.update_job(self._name, {
            "stdout_stream_name": logstream.name,
            "stdout_stream_write_name": logstream.write_resource_name
        })

    def set_stderr_stream(self, logstream, *, data_store):
        self.__operation_metadata.stderr_stream_name = logstream.name
        self._stderr_stream_write_name = logstream.write_resource_name

        data_store.update_job(self._name, {
            "stderr_stream_name": logstream.name,
            "stderr_stream_write_name": logstream.write_resource_name
        })

    # --- Public API: Monitoring ---

    def query_queue_time(self):
        return self.__queued_time_duration.ToTimedelta()

    def query_n_retries(self):
        return self._n_tries - 1 if self._n_tries > 0 else 0

    # --- Private API ---

    def get_operation_update(self, operation_name):
        """Get an operation update message tuple.

        The returned tuple is of the form

            (error, operation)

        """
        error = None
        if operation_name in self.__operations_cancelled:
            error = CancelledError("Operation has been cancelled")
        operation = self.get_operation(operation_name)

        return (error, operation)
