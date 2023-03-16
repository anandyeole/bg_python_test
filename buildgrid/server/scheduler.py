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


"""
Scheduler
=========
Schedules jobs.
"""

from datetime import timedelta
import logging
from threading import Lock
from typing import Dict, Iterator, List, Optional, Set, Tuple

from grpc import ServicerContext

from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import Action, Digest, RequestMetadata
from buildgrid._protos.google.devtools.remoteworkers.v1test2.bots_pb2 import Lease
from buildgrid._protos.google.longrunning import operations_pb2
from buildgrid._protos.google.rpc import code_pb2, status_pb2
from buildgrid._enums import LeaseState, OperationStage
from buildgrid._exceptions import NotFoundError
from buildgrid.client.channel import setup_channel
from buildgrid.server.actioncache.caches.action_cache_abc import ActionCacheABC
from buildgrid.server.job import Job
from buildgrid.server.metrics_names import (
    QUEUED_TIME_METRIC_NAME,
    WORKER_HANDLING_TIME_METRIC_NAME,
    INPUTS_FETCHING_TIME_METRIC_NAME,
    OUTPUTS_UPLOADING_TIME_METRIC_NAME,
    EXECUTION_TIME_METRIC_NAME,
    TOTAL_HANDLING_TIME_METRIC_NAME,
    SCHEDULER_CANCEL_OPERATION_TIME_METRIC_NAME,
    SCHEDULER_QUEUE_ACTION_TIME_METRIC_NAME,
    SCHEDULER_UPDATE_LEASE_TIME_METRIC_NAME
)
from buildgrid.server.persistence.interface import DataStoreInterface
from buildgrid.utils import acquire_lock_or_timeout
from buildgrid.server.metrics_utils import (
    DurationMetric,
    publish_timer_metric
)
from buildgrid.server.operations.filtering import OperationFilter


class Scheduler:

    MAX_N_TRIES = 5
    RETRYABLE_STATUS_CODES = (code_pb2.INTERNAL, code_pb2.UNAVAILABLE)

    def __init__(
        self,
        data_store: DataStoreInterface,
        action_cache: Optional[ActionCacheABC] = None,
        action_browser_url: Optional[str] = None,
        monitor: bool = False,
        max_execution_timeout: Optional[int] = None,
        logstream_url: Optional[str] = None,
        logstream_credentials: Optional[Dict[str, str]] = None,
        logstream_instance_name: Optional[str] = None,
        enable_job_watcher: bool = True
    ) -> None:
        self.__logger = logging.getLogger(__name__)

        self._instance_name: Optional[str] = None
        self._max_execution_timeout = max_execution_timeout

        self.__queue_time_average: Optional[Tuple[int, timedelta]] = None
        self.__retries_count = 0

        self._action_cache = action_cache
        self._action_browser_url = action_browser_url

        self._logstream_instance_name = logstream_instance_name
        self._logstream_url = logstream_url
        if logstream_credentials is None:
            logstream_credentials = {}
        self._logstream_credentials = logstream_credentials
        self._logstream_channel = None

        self.__operation_lock = Lock()  # Lock protecting deletion, addition and updating of jobs

        self.data_store = data_store
        self._enable_job_watcher = enable_job_watcher
        if self._action_browser_url:
            self.data_store.set_action_browser_url(self._action_browser_url)

        self._is_instrumented = False
        if monitor:
            self.activate_monitoring()

    # --- Public API ---

    @property
    def instance_name(self) -> Optional[str]:
        return self._instance_name

    def set_instance_name(self, instance_name: str) -> None:
        if not self._instance_name:
            self._instance_name = instance_name
            self.data_store.set_instance_name(instance_name)

    def setup_grpc(self) -> None:
        self.data_store.setup_grpc()

        if self._action_cache is not None:
            self._action_cache.setup_grpc()

        if self._logstream_channel is None and self._logstream_url is not None:
            self._logstream_channel, _ = setup_channel(
                self._logstream_url,
                auth_token=None,
                client_key=self._logstream_credentials.get("tls-client-key"),
                client_cert=self._logstream_credentials.get("tls-client-cert"),
                server_cert=self._logstream_credentials.get("tls-server-cert")
            )

    def start(self) -> None:
        self.data_store.start(start_job_watcher=self._enable_job_watcher)

    def stop(self) -> None:
        self.data_store.stop()

    # --- Public API: REAPI ---

    def register_job_peer(
        self,
        job_name: str,
        peer: str,
        request_metadata: Optional[RequestMetadata] = None
    ) -> str:
        """Subscribes to the job's :class:`Operation` stage changes.

        Args:
            job_name (str): name of the job to subscribe to.
            peer (str): a unique string identifying the client.

        Returns:
            str: The name of the subscribed :class:`Operation`.

        Raises:
            NotFoundError: If no job with `job_name` exists.
            TimeoutError: If the operation lock cannot be acquired within a short period of time.
        """
        with acquire_lock_or_timeout(self.__operation_lock):
            job = self.data_store.get_job_by_name(job_name, max_execution_timeout=self._max_execution_timeout)

            if job is None:
                raise NotFoundError(f"Job name does not exist: [{job_name}]")

            operation_name = job.register_new_operation(
                data_store=self.data_store, request_metadata=request_metadata)

            self.data_store.watch_job(job, operation_name, peer)

        return operation_name

    def register_job_operation_peer(self, operation_name: str, peer: str) -> None:
        """Subscribes to an existing the job's :class:`Operation` stage changes.

        Args:
            operation_name (str): name of the operation to subscribe to.
            peer (str): a unique string identifying the client.

        Returns:
            str: The name of the subscribed :class:`Operation`.

        Raises:
            NotFoundError: If no operation with `operation_name` exists.
            TimeoutError: If the operation lock cannot be acquired within a short period of time.
        """
        with acquire_lock_or_timeout(self.__operation_lock):
            job = self.data_store.get_job_by_operation(operation_name,
                                                       max_execution_timeout=self._max_execution_timeout)

            if job is None:
                raise NotFoundError(f"Operation name does not exist: [{operation_name}]")

            self.data_store.watch_job(job, operation_name, peer)

    def stream_operation_updates(
        self,
        operation_name: str,
        context: ServicerContext,
        keepalive_timeout: Optional[int]=None
    ) -> Iterator[Tuple[Optional[Exception], operations_pb2.Operation]]:
        yield from self.data_store.stream_operation_updates(operation_name, context, keepalive_timeout)

    def unregister_job_operation_peer(
        self,
        operation_name: str,
        peer: str,
        discard_unwatched_jobs: bool=False
    ) -> None:
        """Unsubscribes to one of the job's :class:`Operation` stage change.

        Args:
            operation_name (str): name of the operation to unsubscribe from.
            peer (str): a unique string identifying the client.
            discard_unwatched_jobs (bool): don't remove operation when client rpc is terminated.

        Raises:
            NotFoundError: If no operation with `operation_name` exists.
            TimeoutError: If the operation lock cannot be acquired within a short period of time.
        """
        with acquire_lock_or_timeout(self.__operation_lock):
            job = self.data_store.get_job_by_operation(operation_name)

            if job is None:
                raise NotFoundError(f"Operation name does not exist: [{operation_name}]")

            self.data_store.stop_watching_operation(job, operation_name, peer)

            if not job.n_peers_for_operation(operation_name, self.data_store.watched_jobs.get(job.name)):
                if discard_unwatched_jobs:
                    self.__logger.info(f"No peers watching the operation, removing: {operation_name}")
                    self.data_store.delete_operation(operation_name)

            if not job.n_peers(self.data_store.watched_jobs.get(job.name)) and job.done and not job.lease:
                self.data_store.delete_job(job.name)

    @DurationMetric(SCHEDULER_QUEUE_ACTION_TIME_METRIC_NAME, instanced=True)
    def queue_job_action(
        self,
        action: Action,
        action_digest: Digest,
        platform_requirements: Optional[Dict[str, Set]] = None,
        priority: int = 0,
        skip_cache_lookup: bool = False
    ) -> str:
        """Inserts a newly created job into the execution queue.

        Warning:
            Priority is handle like a POSIX ``nice`` values: a higher value
            means a low priority, 0 being default priority.

        Args:
            action (Action): the given action to queue for execution.
            action_digest (Digest): the digest of the given action.
            platform_requirements (dict(set)): platform attributes that a worker
                must satisfy in order to be assigned the job. (Each key can
                have multiple values.)
            priority (int): the execution job's priority.
            skip_cache_lookup (bool): whether or not to look for pre-computed
                result for the given action.

        Returns:
            str: the newly created job's name.
        """
        if platform_requirements is None:
            platform_requirements = {}

        job = self.data_store.get_job_by_action(action_digest,
                                                max_execution_timeout=self._max_execution_timeout)

        if job is not None and not action.do_not_cache:
            # If existing job has been cancelled or isn't
            # cacheable, create a new one.
            if not job.cancelled and not job.do_not_cache:
                # Reschedule if priority is now greater:
                if priority < job.priority:
                    job.set_priority(priority, data_store=self.data_store)

                    if job.operation_stage == OperationStage.QUEUED:
                        self.data_store.queue_job(job.name)

                    self.__logger.debug(
                        f"Job deduplicated for action [{action_digest.hash[:8]}]: [{job.name}] "
                        f"with new priority: [{priority}]")
                else:
                    self.__logger.debug(
                        f"Job deduplicated for action [{action_digest.hash[:8]}]: [{job.name}]")

                return job.name

        job = Job(do_not_cache=action.do_not_cache,
                  action=action, action_digest=action_digest,
                  platform_requirements=platform_requirements,
                  priority=priority)
        self.data_store.create_job(job)

        self.__logger.debug(
            f"Job created for action [{action_digest.hash[:8]}]: "
            f"[{job.name} requiring: {job.platform_requirements}, priority: {priority}]")

        operation_stage = None

        if self._action_cache is not None and not skip_cache_lookup:
            try:
                action_result = self._action_cache.get_action_result(job.action_digest)

                self.__logger.debug(
                    f"Job cache hit for action [{action_digest.hash[:8]}]: [{job.name}]")

                operation_stage = OperationStage.COMPLETED
                job.set_cached_result(action_result, self.data_store)

            except NotFoundError:
                operation_stage = OperationStage.QUEUED
                self.data_store.queue_job(job.name)
            except Exception:
                self.__logger.exception("Checking ActionCache for action "
                                        f"[{action_digest.hash}/{action_digest.size_bytes}] "
                                        "failed.")
                operation_stage = OperationStage.QUEUED
                self.data_store.queue_job(job.name)

        else:
            operation_stage = OperationStage.QUEUED
            self.data_store.queue_job(job.name)

        self._update_job_operation_stage(job.name, operation_stage)

        return job.name

    def get_job_operation(self, operation_name: str) -> operations_pb2.Operation:
        """Retrieves a job's :class:`Operation` by name.

        Args:
            operation_name (str): name of the operation to query.

        Raises:
            NotFoundError: If no operation with `operation_name` exists.
        """
        job = self.data_store.get_job_by_operation(operation_name,
                                                   max_execution_timeout=self._max_execution_timeout)

        if job is None:
            raise NotFoundError(f"Operation name does not exist: [{operation_name}]")

        return job.get_operation(operation_name)

    @DurationMetric(SCHEDULER_CANCEL_OPERATION_TIME_METRIC_NAME, instanced=True)
    def cancel_job_operation(self, operation_name: str) -> None:
        """"Cancels a job's :class:`Operation` by name.

        Args:
            operation_name (str): name of the operation to cancel.

        Raises:
            NotFoundError: If no operation with `operation_name` exists.
        """
        job = self.data_store.get_job_by_operation(operation_name)

        if job is None:
            raise NotFoundError(f"Operation name does not exist: [{operation_name}]")

        job.cancel_operation(operation_name, data_store=self.data_store)

    def list_operations(
        self,
        operation_filters: Optional[List[OperationFilter]]=None,
        page_size: int=None,
        page_token: str=None
    ) -> Tuple[List[operations_pb2.Operation], str]:
        if operation_filters is None:
            operation_filters = []
        operations, next_token = self.data_store.list_operations(
            operation_filters,
            page_size,
            page_token,
            max_execution_timeout=self._max_execution_timeout)
        return operations, next_token

    # --- Public API: RWAPI ---

    @DurationMetric(SCHEDULER_UPDATE_LEASE_TIME_METRIC_NAME, instanced=True)
    def update_job_lease_state(self, job_name: str, lease: Lease) -> None:
        """Requests a state transition for a job's current :class:Lease.

        Note:
            This may trigger a job's :class:`Operation` stage transition.

        Args:
            job_name (str): name of the job to update lease state from.
            lease (Lease): the lease holding the new state.

        Raises:
            NotFoundError: If no job with `job_name` exists.
        """
        job = self.data_store.get_job_by_name(job_name)

        if job is None:
            raise NotFoundError(f"Job name does not exist: [{job_name}]")

        lease_state = LeaseState(lease.state)

        operation_stage = OperationStage.UNKNOWN
        if lease_state == LeaseState.PENDING:
            job.update_lease_state(LeaseState.PENDING, data_store=self.data_store)
            operation_stage = OperationStage.QUEUED

        elif lease_state == LeaseState.ACTIVE:
            job.update_lease_state(LeaseState.ACTIVE, data_store=self.data_store)
            operation_stage = OperationStage.EXECUTING

        elif lease_state == LeaseState.COMPLETED:
            # Check the lease status to determine if the job should be retried
            lease_status = lease.status.code
            if lease_status in self.RETRYABLE_STATUS_CODES and job.n_tries < self.MAX_N_TRIES:
                self.__logger.info(f"Job {job_name} completed with a non-OK retryable status code "
                                   f"{lease_status}, retrying")
                self.retry_job_lease(job_name)
            else:
                # Update lease state to COMPLETED and store result in CAS
                # Also store mapping in ActionResult, if job is cacheable
                job.update_lease_state(LeaseState.COMPLETED,
                                       status=lease.status, result=lease.result,
                                       action_cache=self._action_cache,
                                       data_store=self.data_store,
                                       skip_notify=True)
                try:
                    self.delete_job_lease(job_name)
                except NotFoundError:
                    # Job already deleted
                    pass
                except TimeoutError:
                    self.__logger.warning(f"Could not delete job lease_id=[{lease.id}] due to timeout.",
                                          exc_info=True)

                operation_stage = OperationStage.COMPLETED

        self._update_job_operation_stage(job_name, operation_stage)

    def retry_job_lease(self, job_name: str) -> None:
        """Re-queues a job on lease execution failure.

        Note:
            This may trigger a job's :class:`Operation` stage transition.

        Args:
            job_name (str): name of the job to retry the lease from.

        Raises:
            NotFoundError: If no job with `job_name` exists.
        """
        job = self.data_store.get_job_by_name(job_name)

        if job is None:
            raise NotFoundError(f"Job name does not exist: [{job_name}]")

        updated_operation_stage = None
        if job.n_tries >= self.MAX_N_TRIES:
            updated_operation_stage = OperationStage.COMPLETED
            status = status_pb2.Status(code=code_pb2.ABORTED,
                                       message=f"Job was retried {job.n_tries} unsuccessfully. Aborting.")
            job.update_lease_state(LeaseState.COMPLETED, status=status, data_store=self.data_store)

        elif not job.cancelled:
            if job.done:
                self.__logger.info(f"Attempted to re-queue job name=[{job_name}] "
                                   f"but it was already completed.")
                return

            updated_operation_stage = OperationStage.QUEUED
            self.data_store.queue_job(job.name)

            job.update_lease_state(LeaseState.PENDING, data_store=self.data_store)

            if self._is_instrumented:
                self.__retries_count += 1

        if updated_operation_stage:
            self._update_job_operation_stage(job_name, updated_operation_stage)

    def get_job_lease(self, job_name: str) -> Lease:
        """Returns the lease associated to job, if any have been emitted yet.

        Args:
            job_name (str): name of the job to query the lease from.

        Raises:
            NotFoundError: If no job with `job_name` exists.
        """
        job = self.data_store.get_job_by_name(job_name)

        if job is None:
            raise NotFoundError(f"Job name does not exist: [{job_name}]")

        return job.lease

    def delete_job_lease(self, job_name: str) -> None:
        """Discards the lease associated with a job.

        Args:
            job_name (str): name of the job to delete the lease from.

        Raises:
            NotFoundError: If no job with `job_name` exists.
            TimeoutError: If the operation lock cannot be acquired within a short period of time.
        """
        with acquire_lock_or_timeout(self.__operation_lock):
            job = self.data_store.get_job_by_name(job_name)

            if job is None:
                raise NotFoundError(f"Job name does not exist: [{job_name}]")

            job.delete_lease()

            if not job.n_peers(self.data_store.watched_jobs.get(job.name)) and job.done:
                self.data_store.delete_job(job.name)

    def get_operation_request_metadata(self, operation_name: str) -> Dict[str, str]:
        return self.data_store.get_operation_request_metadata_by_name(operation_name)

    def get_metadata_for_leases(
        self,
        leases: List[Lease],
        writeable_streams: bool=False
    ) -> List[Tuple[str, str]]:
        """Return a list of Job metadata for a given list of leases.

        Args:
            leases (list): List of leases to get Job metadata for.

        Returns:
            List of tuples of the form
            ``('executeoperationmetadata-bin': serialized_metadata)``.

        """
        metadata = []
        for lease in leases:
            job = self.data_store.get_job_by_name(lease.id)
            if job is not None:
                job_metadata = job.get_metadata(writeable_streams=True)
                metadata.append(
                    ('executeoperationmetadata-bin', job_metadata.SerializeToString()))

        return metadata

    # --- Public API: Monitoring ---

    @property
    def is_instrumented(self) -> bool:
        return self._is_instrumented

    def activate_monitoring(self) -> None:
        """Activated jobs monitoring."""
        if self._is_instrumented:
            return

        self.__queue_time_average = 0, timedelta()
        self.__retries_count = 0

        self._is_instrumented = True

        self.data_store.activate_monitoring()

    def deactivate_monitoring(self) -> None:
        """Deactivated jobs monitoring."""
        if not self._is_instrumented:
            return

        self._is_instrumented = False

        self.__queue_time_average = None
        self.__retries_count = 0

        self.data_store.deactivate_monitoring()

    def get_metrics(self) -> Dict[str, Dict[int, int]]:
        return self.data_store.get_metrics()

    def query_n_retries(self) -> int:
        return self.__retries_count

    def query_am_queue_time(self) -> timedelta:
        if self.__queue_time_average is not None:
            return self.__queue_time_average[1]
        return timedelta()

    # --- Private API ---

    def _update_job_operation_stage(self, job_name: str, operation_stage: OperationStage) -> None:
        """Requests a stage transition for the job's :class:Operations.

        Args:
            job_name (str): name of the job to query.
            operation_stage (OperationStage): the stage to transition to.

        Raises:
            TimeoutError: If the operation lock cannot be acquired within a short period of time.
        """
        with acquire_lock_or_timeout(self.__operation_lock):
            job = self.data_store.get_job_by_name(job_name)
            if job is None:
                return

            if operation_stage == OperationStage.CACHE_CHECK:
                job.update_operation_stage(OperationStage.CACHE_CHECK,
                                           data_store=self.data_store)

            elif operation_stage == OperationStage.QUEUED:
                job.update_operation_stage(OperationStage.QUEUED,
                                           data_store=self.data_store)

            elif operation_stage == OperationStage.EXECUTING:
                job.update_operation_stage(OperationStage.EXECUTING,
                                           data_store=self.data_store)

            elif operation_stage == OperationStage.COMPLETED:
                job.update_operation_stage(OperationStage.COMPLETED,
                                           data_store=self.data_store)

                if self._is_instrumented:
                    average_order, average_time = self.__queue_time_average  # type: ignore

                    average_order += 1
                    if average_order <= 1:
                        average_time = job.query_queue_time()
                    else:
                        queue_time = job.query_queue_time()
                        average_time = average_time + ((queue_time - average_time) / average_order)

                    self.__queue_time_average = average_order, average_time

                    if not job.holds_cached_result:
                        execution_metadata = job.action_result.execution_metadata
                        context_metadata = {'instance-name': self.instance_name} if self.instance_name else None

                        queued = execution_metadata.queued_timestamp.ToDatetime()
                        worker_start = execution_metadata.worker_start_timestamp.ToDatetime()
                        worker_completed = execution_metadata.worker_completed_timestamp.ToDatetime()
                        fetch_start = execution_metadata.input_fetch_start_timestamp.ToDatetime()
                        fetch_completed = execution_metadata.input_fetch_completed_timestamp.ToDatetime()
                        execution_start = execution_metadata.execution_start_timestamp.ToDatetime()
                        execution_completed = execution_metadata.execution_completed_timestamp.ToDatetime()
                        upload_start = execution_metadata.output_upload_start_timestamp.ToDatetime()
                        upload_completed = execution_metadata.output_upload_completed_timestamp.ToDatetime()

                        # Emit build inputs fetching time record:
                        input_fetch_time = fetch_completed - fetch_start
                        publish_timer_metric(
                            INPUTS_FETCHING_TIME_METRIC_NAME,
                            input_fetch_time,
                            metadata=context_metadata)

                        # Emit build execution time record:
                        execution_time = execution_completed - execution_start
                        publish_timer_metric(
                            EXECUTION_TIME_METRIC_NAME, execution_time,
                            metadata=context_metadata)

                        # Emit build outputs uploading time record:
                        output_upload_time = upload_completed - upload_start
                        publish_timer_metric(
                            OUTPUTS_UPLOADING_TIME_METRIC_NAME, output_upload_time,
                            metadata=context_metadata)

                        # Emit total queued time record:
                        # This calculates the queue time based purely on
                        # values set in the ActionResult's ExecutedActionMetadata,
                        # which may be ever so slightly different than what
                        # the job object's queued_time is.
                        total_queued_time = worker_start - queued
                        publish_timer_metric(
                            QUEUED_TIME_METRIC_NAME, total_queued_time,
                            metadata=context_metadata)

                        # Emit total time spent in worker
                        total_worker_time = worker_completed - worker_start
                        publish_timer_metric(
                            WORKER_HANDLING_TIME_METRIC_NAME, total_worker_time,
                            metadata=context_metadata)

                        # Emit total build handling time record:
                        total_handling_time = worker_completed - queued
                        publish_timer_metric(
                            TOTAL_HANDLING_TIME_METRIC_NAME, total_handling_time,
                            metadata=context_metadata)
