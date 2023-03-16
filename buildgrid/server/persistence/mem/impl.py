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


import bisect
import logging
from multiprocessing import Queue
from queue import Empty
from threading import Condition, Lock, Thread
from typing import Dict, Callable, List, Tuple
from datetime import datetime

from buildgrid._protos.google.longrunning import operations_pb2
from buildgrid._enums import LeaseState, MetricCategories, OperationStage
from buildgrid._exceptions import InvalidArgumentError
from buildgrid.utils import JobState, BrowserURL, convert_values_to_sorted_lists, hash_from_dict
from buildgrid.server.job import Job
from buildgrid.server.metrics_names import (
    BOTS_ASSIGN_JOB_LEASES_TIME_METRIC_NAME,
    DATA_STORE_CHECK_FOR_UPDATE_TIME_METRIC_NAME,
    DATA_STORE_CREATE_JOB_TIME_METRIC_NAME,
    DATA_STORE_CREATE_LEASE_TIME_METRIC_NAME,
    DATA_STORE_CREATE_OPERATION_TIME_METRIC_NAME,
    DATA_STORE_GET_JOB_BY_DIGEST_TIME_METRIC_NAME,
    DATA_STORE_GET_JOB_BY_NAME_TIME_METRIC_NAME,
    DATA_STORE_GET_JOB_BY_OPERATION_TIME_METRIC_NAME,
    DATA_STORE_LIST_OPERATIONS_TIME_METRIC_NAME,
    DATA_STORE_QUEUE_JOB_TIME_METRIC_NAME,
    DATA_STORE_UPDATE_JOB_TIME_METRIC_NAME,
    DATA_STORE_UPDATE_LEASE_TIME_METRIC_NAME,
    DATA_STORE_UPDATE_OPERATION_TIME_METRIC_NAME
)
from buildgrid.server.metrics_utils import DurationMetric
from buildgrid.server.operations.filtering import OperationFilter, DEFAULT_OPERATION_FILTERS
from buildgrid.server.persistence.interface import DataStoreInterface


class MemoryDataStore(DataStoreInterface):

    def __init__(self, storage):
        super().__init__(storage)
        self.logger = logging.getLogger(__file__)
        self.logger.info("Creating in-memory scheduler")

        self.queue = []
        self.queue_lock = Lock()
        self.queue_condition = Condition(lock=self.queue_lock)

        self.jobs_by_action = {}
        self.jobs_by_operation = {}
        self.jobs_by_name = {}

        self.operations_by_stage = {}
        self.leases_by_state = {}
        self.is_instrumented = False

        self.update_event_queue = Queue()
        self.watcher = Thread(name="JobWatcher", target=self.wait_for_job_updates, daemon=True)
        self.watcher_keep_running = False

    def __repr__(self):
        return "In-memory data store interface"

    def activate_monitoring(self):
        if self.is_instrumented:
            return

        self.operations_by_stage = {
            stage: set() for stage in OperationStage
        }
        self.leases_by_state = {
            state: set() for state in LeaseState
        }
        self.is_instrumented = True

    def deactivate_monitoring(self):
        if not self.is_instrumented:
            return

        self.operations_by_stage = {}
        self.leases_by_state = {}
        self.is_instrumented = False

    def start(self, *, start_job_watcher: bool = True) -> None:
        if start_job_watcher and not self.watcher_keep_running:
            self.watcher_keep_running = True
            self.watcher.start()

    def stop(self) -> None:
        if self.watcher_keep_running:
            self.watcher_keep_running = False
            self.watcher.join()

    def _check_job_timeout(self, job_internal, *, max_execution_timeout=None):
        """ Do a lazy check of maximum allowed job timeouts when clients try to retrieve
            an existing job.
            Cancel the job and related operations/leases, if we detect they have
            exceeded timeouts on access.

            Returns the `buildgrid.server.Job` object, possibly updated with `cancelled=True`.
        """
        if job_internal and max_execution_timeout and job_internal.worker_start_timestamp_as_datetime:
            if job_internal.operation_stage == OperationStage.EXECUTING:
                executing_duration = datetime.utcnow() - job_internal.worker_start_timestamp_as_datetime
                if executing_duration.total_seconds() >= max_execution_timeout:
                    self.logger.warning(f"Job=[{job_internal}] has been executing for "
                                        f"executing_duration=[{executing_duration}]. "
                                        f"max_execution_timeout=[{max_execution_timeout}] "
                                        "Cancelling.")
                    job_internal.cancel_all_operations(data_store=self)
                    self.logger.info(f"Job=[{job_internal}] has been cancelled.")
        return job_internal

    @DurationMetric(DATA_STORE_GET_JOB_BY_NAME_TIME_METRIC_NAME, instanced=True)
    def get_job_by_name(self, name, *, max_execution_timeout=None):
        job = self.jobs_by_name.get(name)
        return self._check_job_timeout(job, max_execution_timeout=max_execution_timeout)

    @DurationMetric(DATA_STORE_GET_JOB_BY_DIGEST_TIME_METRIC_NAME, instanced=True)
    def get_job_by_action(self, action_digest, *, max_execution_timeout=None):
        job = self.jobs_by_action.get(action_digest.hash)
        return self._check_job_timeout(job, max_execution_timeout=max_execution_timeout)

    @DurationMetric(DATA_STORE_GET_JOB_BY_OPERATION_TIME_METRIC_NAME, instanced=True)
    def get_job_by_operation(self, operation_name, *, max_execution_timeout=None):
        job = self.jobs_by_operation.get(operation_name)
        return self._check_job_timeout(job, max_execution_timeout=max_execution_timeout)

    def get_all_jobs(self):
        return [job for job in self.jobs_by_name.values()
                if job.operation_stage != OperationStage.COMPLETED]

    def get_jobs_by_stage(self, operation_stage):
        return [job for job in self.jobs_by_name.values()
                if job.operation_stage == operation_stage]

    def _get_job_count_by_stage(self):
        results = []
        for stage in OperationStage:
            results.append((stage, len(self.get_jobs_by_stage(stage))))
        return results

    @DurationMetric(DATA_STORE_CREATE_JOB_TIME_METRIC_NAME, instanced=True)
    def create_job(self, job):
        self.jobs_by_action[job.action_digest.hash] = job
        self.jobs_by_name[job.name] = job
        if self._action_browser_url is not None:
            job.set_action_url(BrowserURL(self._action_browser_url, self._instance_name))

    @DurationMetric(DATA_STORE_QUEUE_JOB_TIME_METRIC_NAME, instanced=True)
    def queue_job(self, job_name):
        job = self.jobs_by_name[job_name]
        with self.queue_condition:
            if job.operation_stage != OperationStage.QUEUED:
                bisect.insort(self.queue, job)
                self.logger.info(f"Job queued: [{job.name}]")
                # Wake all waiters as not all waiters may have the required capabilities
                self.queue_condition.notify_all()
            else:
                self.logger.info(f"Job already queued: [{job.name}]")
                self.queue.sort()

    @DurationMetric(DATA_STORE_UPDATE_JOB_TIME_METRIC_NAME, instanced=True)
    def update_job(self, job_name, changes, skip_notify=False):
        # With this implementation, there's no need to actually make
        # changes to the stored job, since its a reference to the
        # in-memory job that caused this method to be called.
        self.update_event_queue.put((job_name, changes, skip_notify))

    def delete_job(self, job_name):
        job = self.jobs_by_name[job_name]

        del self.jobs_by_action[job.action_digest.hash]
        del self.jobs_by_name[job.name]

        self.logger.info(f"Job deleted: [{job.name}]")

        if self.is_instrumented:
            for stage in OperationStage:
                self.operations_by_stage[stage].discard(job.name)

            for state in LeaseState:
                self.leases_by_state[state].discard(job.name)

    def wait_for_job_updates(self):
        self.logger.info("Starting job watcher thread")
        while self.watcher_keep_running:
            try:
                job_name, changes, skip_notify = self.update_event_queue.get(timeout=1)
            except (EOFError, Empty):
                continue
            with DurationMetric(DATA_STORE_CHECK_FOR_UPDATE_TIME_METRIC_NAME):
                with self.watched_jobs_lock:
                    if (all(field not in changes for field in ("cancelled", "stage")) or
                            job_name not in self.watched_jobs):
                        # If the stage or cancellation state haven't changed, we don't
                        # need to do anything with this event. Similarly, if we aren't
                        # watching this job, we can ignore the event.
                        continue
                    job = self.get_job_by_name(job_name)
                    spec = self.watched_jobs[job_name]
                    new_state = JobState(job)
                    if spec.last_state != new_state and not skip_notify:
                        spec.last_state = new_state
                        if not skip_notify:
                            spec.event.notify_change()

    def store_response(self, job, commit_changes):
        # The job is always in memory in this implementation, so there's
        # no need to write anything to the CAS, since the job stays in
        # memory as long as we need it
        pass

    def get_operations_by_stage(self, operation_stage):
        return self.operations_by_stage.get(operation_stage, set())

    def _get_operation_count_by_stage(self):
        results = []
        for stage in OperationStage:
            results.append((stage, len(self.get_operations_by_stage(stage))))
        return results

    @DurationMetric(DATA_STORE_LIST_OPERATIONS_TIME_METRIC_NAME, instanced=True)
    def list_operations(self,
                        operation_filters: List[OperationFilter]=None,
                        page_size: int=None,
                        page_token: str=None,
                        max_execution_timeout: int=None) -> Tuple[List[operations_pb2.Operation], str]:

        if operation_filters and operation_filters != DEFAULT_OPERATION_FILTERS:
            raise InvalidArgumentError("Filtering is not supported with the in-memory scheduler.")

        if page_token:
            raise InvalidArgumentError("page_token is not supported in the in-memory scheduler.")

        # Run through all the jobs and see if any of are
        # exceeding the execution timeout; mark those as cancelled
        for job in self.jobs_by_name.values():
            self._check_job_timeout(job, max_execution_timeout=max_execution_timeout)

        # Return all operations
        return [
            operation for job in self.jobs_by_name.values() for operation in job.get_all_operations()
        ], ""

    @DurationMetric(DATA_STORE_CREATE_OPERATION_TIME_METRIC_NAME, instanced=True)
    def create_operation(self, operation_name, job_name, request_metadata=None):
        job = self.jobs_by_name[job_name]
        self.jobs_by_operation[operation_name] = job
        if self.is_instrumented:
            self.operations_by_stage[job.operation_stage].add(job_name)

    @DurationMetric(DATA_STORE_UPDATE_OPERATION_TIME_METRIC_NAME, instanced=True)
    def update_operation(self, operation_name, changes):
        if self.is_instrumented:
            job = self.jobs_by_operation[operation_name]
            self.operations_by_stage[job.operation_stage].add(job.name)
            other_stages = [member for member in OperationStage if member != job.operation_stage]
            for stage in other_stages:
                self.operations_by_stage[stage].discard(job.name)

    def delete_operation(self, operation_name):
        del self.jobs_by_operation[operation_name]

    def get_leases_by_state(self, lease_state):
        return self.leases_by_state.get(lease_state, set())

    def _get_lease_count_by_state(self):
        results = []
        for state in LeaseState:
            results.append((state, len(self.get_leases_by_state(state))))
        return results

    @DurationMetric(DATA_STORE_CREATE_LEASE_TIME_METRIC_NAME, instanced=True)
    def create_lease(self, lease):
        if self.is_instrumented:
            self.leases_by_state[LeaseState(lease.state)].add(lease.id)

    @DurationMetric(DATA_STORE_UPDATE_LEASE_TIME_METRIC_NAME, instanced=True)
    def update_lease(self, job_name, changes):
        if self.is_instrumented:
            job = self.jobs_by_name[job_name]
            state = LeaseState(job.lease.state)
            self.leases_by_state[state].add(job.lease.id)
            other_states = [member for member in LeaseState if member != state]
            for state in other_states:
                self.leases_by_state[state].discard(job.lease.id)

    def load_unfinished_jobs(self):
        return []

    def get_operation_request_metadata_by_name(self, operation_name):
        return None

    @DurationMetric(BOTS_ASSIGN_JOB_LEASES_TIME_METRIC_NAME, instanced=True)
    def assign_n_leases(
        self,
        *,
        capability_hash: str,
        lease_count: int,
        assignment_callback: Callable[[List[Job]], Dict[str, Job]]
    ) -> None:
        jobs = []
        for index, job in enumerate(self.queue):
            if job.cancelled:
                self.logger.debug(f"Dropping cancelled job: [{job.name}] from queue")
                del self.queue[index]
                continue

            if self._worker_is_capable(capability_hash, job):
                jobs.append(job)

            if len(jobs) >= lease_count:
                break

        assigned_jobs = assignment_callback(jobs)
        for job in assigned_jobs.values():
            self.queue.remove(job)

    def _worker_is_capable(self, worker_capabilities_hash: str, job: Job) -> bool:
        """Returns whether the worker is suitable to run the job."""
        # TODO: Replace this with the logic defined in the Platform msg. standard.

        job_requirements = job.platform_requirements
        normalized_requirements = convert_values_to_sorted_lists(job_requirements)
        # Serialize the requirements
        platform_requirements_hash = hash_from_dict(normalized_requirements)

        return platform_requirements_hash == worker_capabilities_hash

    def get_metrics(self):
        metrics = {}
        metrics[MetricCategories.JOBS.value] = {
            stage.value: count for stage, count in self._get_job_count_by_stage()
        }
        metrics[MetricCategories.LEASES.value] = {
            state.value: count for state, count in self._get_lease_count_by_state()
        }

        return metrics
