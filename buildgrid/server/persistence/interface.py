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


from abc import ABC, abstractmethod
import logging
from threading import Lock
from typing import Any, Callable, Dict, Generator, List, Mapping, Optional, Set, Tuple

from grpc import RpcContext

from buildgrid._enums import JobEventType, OperationStage
from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import Digest
from buildgrid._protos.google.devtools.remoteworkers.v1test2.bots_pb2 import Lease
from buildgrid._protos.google.longrunning.operations_pb2 import Operation
from buildgrid.server.job import Job
from buildgrid.server.operations.filtering import OperationFilter
from buildgrid.utils import JobWatchSpec


Message = Tuple[Optional[Exception], Operation]


class DataStoreInterface(ABC):  # pragma: no cover

    """Abstract class defining an interface to a data store for the scheduler.

    The ``DataStoreInterface`` defines the interface used by the scheduler to
    manage storage of its internal state. It also provides some of the
    infrastructure for streaming messages triggered by changes in job and
    operation state, which can be used (via the
    :class:`buildgrid.server.scheduler.Scheduler` itself) to stream progress
    updates back to clients.

    Implementations of the interface are required to implement all the abstract
    methods in this class. However, there is no requirement about the internal
    details of those implementations; it may be beneficial for certain
    implementations to make some of these methods a noop for example.

    Implementations must also implement some way to set the events that are
    used in ``stream_operations_updates``, which live in the ``watched_jobs``
    dictionary.

    """

    def __init__(self, storage):
        self.logger = logging.getLogger(__file__)
        self.max_get_timeout = 0
        self.storage = storage
        self.watched_jobs = {}
        self.watched_jobs_lock = Lock()
        self._action_browser_url = None
        self._instance_name = None

    def setup_grpc(self):
        self.storage.setup_grpc()

    def start(self, *, start_job_watcher: bool = True) -> None:
        pass

    def stop(self) -> None:
        pass

    # Class Properties To Set

    def set_instance_name(self, instance_name):
        self._instance_name = instance_name

    def set_action_browser_url(self, url):
        self._action_browser_url = url

    # Monitoring/metrics methods

    @abstractmethod
    def activate_monitoring(self) -> None:
        """Enable the monitoring features of the data store."""
        raise NotImplementedError()

    @abstractmethod
    def deactivate_monitoring(self) -> None:
        """Disable the monitoring features of the data store.

        This method also performs any necessary cleanup of stored metrics.

        """
        raise NotImplementedError()

    @abstractmethod
    def get_metrics(self) -> Dict[str, Dict[int, int]]:
        """Return a dictionary of metrics for jobs, operations, and leases.

        The returned dictionary is keyed by :class:`buildgrid._enums.MetricCategories`
        values, and the values are dictionaries of counts per operation stage
        (or lease state, in the case of leases).

        """
        raise NotImplementedError()

    # Job API

    @abstractmethod
    def create_job(self, job: Job) -> None:
        """Add a new job to the data store.

        NOTE: This method just stores the job in the data store. In order to
        enqueue the job to make it available for scheduling execution, the
        ``queue_job`` method should also be called.

        Args:
            job (buildgrid.server.job.Job): The job to be stored.

        """
        raise NotImplementedError()

    @abstractmethod
    def queue_job(self, job_name: str) -> None:
        """Add an existing job to the queue of jobs waiting to be assigned.

        This method adds a job with the given name to the queue of jobs. If
        the job is already in the queue, then this method ensures that the
        position of the job in the queue is correct.

        """
        raise NotImplementedError()

    @abstractmethod
    def store_response(self, job: Job, commit_changes: bool) -> None:
        """Store the job's ExecuteResponse in the data store.

        This method stores the response message for the job in the data
        store, in order to allow it to be retrieved when getting jobs
        in the future.

        This is separate from ``update_job`` as implementations will
        likely need to always have a special case for handling
        persistence of the response message.

        Args:
            job (buildgrid.server.job.Job): The job to store the response
                message of.

        """
        raise NotImplementedError()

    @abstractmethod
    def get_job_by_action(self, action_digest: Digest, *,
                          max_execution_timeout: Optional[int]=None) -> Optional[Job]:
        """Return the job corresponding to an Action digest.

        This method looks for a job object corresponding to the given
        Action digest in the data store. If a job is found it is returned,
        otherwise None is returned.

        Args:
            action_digest (Digest): The digest of the Action to find the
                corresponding job for.
            max_execution_timeout (int, Optional): The max execution timeout.

        Returns:
            buildgrid.server.job.Job or None:
                The job with the given Action digest, if it exists.
                Otherwise None.

        """
        raise NotImplementedError()

    @abstractmethod
    def get_job_by_name(self, name: str, *,
                        max_execution_timeout: Optional[int]=None) -> Optional[Job]:
        """Return the job with the given name.

        This method looks for a job with the specified name in the data
        store. If there is a matching Job it is returned, otherwise this
        returns None.

        Args:
            name (str): The name of the job to return.
            max_execution_timeout (int, Optional): The max execution timeout.

        Returns:
            buildgrid.server.job.Job or None:
                The job with the given name, if it exists. Otherwise None.

        """
        raise NotImplementedError()

    @abstractmethod
    def get_job_by_operation(self, operation_name: str, *,
                             max_execution_timeout: Optional[int]=None) -> Optional[Job]:
        """Return the Job for a given Operation.

        This method takes an Operation name, and returns the Job which
        corresponds to that Operation. If the Operation isn't found,
        or if the data store doesn't contain a corresponding job, this
        returns None.

        Args:
            operation (str): Name of the Operation whose corresponding
                Job is to be returned.
            max_execution_timeout (int, Optional): The max execution timeout.

        Returns:
            buildgrid.server.job.Job or None:
                The job related to the given operation, if it exists.
                Otherwise None.

        """
        raise NotImplementedError()

    @abstractmethod
    def get_all_jobs(self) -> List[Job]:
        """Return a list of all jobs in the data store.

        This method returns a list of all incomplete jobs in the data
        store.

        Returns:
            list: List of all incomplete jobs in the data store.

        """
        raise NotImplementedError()

    @abstractmethod
    def get_jobs_by_stage(self, operation_stage: OperationStage) -> List[Job]:
        """Return a list of jobs in the given stage.

        This method returns a list of all jobs in a specific operation stage.

        Args:
            operation_stage (OperationStage): The stage that the returned list
                of jobs should all be in.

        Returns:
            list: List of all jobs in the specified operation stage.

        """
        raise NotImplementedError()

    @abstractmethod
    def update_job(self, job_name: str, changes: Mapping[str, Any], skip_notify: bool) -> None:
        """Update a job in the data store.

        This method takes a job name and a dictionary of changes to apply to
        the job in the data store, and updates the job with those changes.
        The dictionary should be keyed by the attribute names which need to
        be updated, with the values being the new values for the attributes.

        Args:
            job_name (str): The name of the job that is being updated.
            changes: (dict): The dictionary of changes
            skip_notify: (bool): Whether notifying about job changes should be skipped

        """
        raise NotImplementedError()

    @abstractmethod
    def delete_job(self, job_name: str) -> None:
        """Delete a job from the data store.

        This method removes a job from the data store.

        Args:
            job_name (str): The name of the job to be removed.

        """
        raise NotImplementedError()

    def watch_job(self, job: Job, operation_name: str, peer: str) -> None:
        """Start watching a job and operation for changes.

        If the given job is already being watched, then this method finds (or adds)
        the operation in the job's entry in ``watched_jobs``, and adds the peer to
        the list of peers for that operation.

        Otherwise, it creates a whole new entry in ``watched_jobs`` for the given
        job, operation, and peer.

        This method runs in a thread spawned by gRPC handling a connected peer.

        Args:
            job (buildgrid.server.job.Job): The job to watch.
            operation_name (str): The name of the specific operation to
                watch.
            peer (str): The peer that is requesting to watch the job.

        """
        with self.watched_jobs_lock:
            spec = self.watched_jobs.get(job.name)
            if spec is None:
                self.watched_jobs[job.name] = spec = JobWatchSpec(job)
            spec.add_peer(operation_name, peer)
        self.logger.debug(
            f"Registered peer [{peer}] to watch operation [{operation_name}] of job [{job.name}]")

    def stream_operation_updates(self, operation_name: str,
                                 context: RpcContext,
                                 keepalive_timeout: Optional[int]=None) -> Generator[Message, None, None]:
        """Stream update messages for a given operation.

        This is a generator which yields tuples of the form

        .. code-block ::

            (error, operation)

        where ``error`` is None unless the job is cancelled, in which case
        ``error`` is a :class:`buildgrid._exceptions.CancelledError`.

        This method runs in a thread spawned by gRPC handling a connected
        peer, and should spend most of its time blocked waiting on an event
        which is set by either the thread which watches the data store for
        job updates or the main thread handling the gRPC termination
        callback.

        Iteration finishes either when the provided gRPC context becomes
        inactive, or when the job owning the operation being watched is
        deleted from the data store.

        Args:
            operation_name (str): The name of the operation to stream
                updates for.
            context (grpc.ServicerContext): The RPC context for the peer
                that is requesting a stream of events.
            keepalive_timeout (int): The maximum time to wait before sending
                the current status.

        """
        # Send an initial update as soon as we start watching, to provide the
        # peer with the initial state of the operation. This is done outside
        # the loop to simplify the logic for handling events without sending
        # unnecessary messages to peers.
        job = self.get_job_by_operation(operation_name)
        if job is None:
            return
        message = job.get_operation_update(operation_name)
        yield message

        self.logger.debug("Waiting for events")

        # Wait for events whilst the context is active. Events are set by the
        # thread which is watching the state data store for job updates.
        with self.watched_jobs_lock:
            watched_job = self.watched_jobs.get(job.name)
            if watched_job is None:
                self.logger.error(f"Unable to find job with name: [{job.name}] in watched_jobs dictionary.")
                return
            event = watched_job.event
        last_event = None
        while context.is_active():
            last_event, event_type = event.wait(last_event, timeout=keepalive_timeout)
            if event_type is None:
                self.logger.debug(f"Keepalive timeout for operation [{operation_name}]")
            else:
                self.logger.debug(
                    f"Received event #{last_event} for operation [{operation_name}] "
                    f"with type [{event_type}].")
            # A `JobEventType.STOP` event means that a peer watching this job
            # has disconnected and its termination callback has executed on
            # the thread gRPC creates for callbacks. In this case we don't
            # want to send a message, so we use `continue` to evaluate whether
            # or not to continue iteration.
            if event_type == JobEventType.STOP:
                continue

            job = self.get_job_by_operation(operation_name)
            if job is None:
                self.logger.debug(
                    f"Job for operation [{operation_name}] has gone away, stopped streaming updates.")
                return
            message = job.get_operation_update(operation_name)
            yield message

        self.logger.debug("Context became inactive, stopped streaming updates.")

    def stop_watching_operation(self, job: Job, operation_name: str, peer: str) -> None:
        """Remove the given peer from the list of peers watching the given job.

        If the given job is being watched, this method triggers a
        ``JobEventType.STOP`` for it to cause the waiting threads to check
        whether their context is still active. It then removes the given peer
        from the list of peers watching the given operation name. If this
        leaves no peers then the entire entry for the operation in the tracked
        job is removed.

        If this process leaves the job with no operations being watched, the
        job itself is removed from the `watched_jobs` dictionary, and it will
        no longer be checked for updates.

        This runs in the main thread as part of the RPC termination callback
        for ``Execute`` and ``WaitExecution`` requests.

        Args:
            job (buildgrid.server.job.Job): The job to stop watching.
            operation_name (str): The name of the specific operation to
                stop watching.
            peer (str): The peer that is requesting to stop watching the job.

        """
        with self.watched_jobs_lock:
            spec = self.watched_jobs.get(job.name)
            if spec is None:
                self.logger.debug(
                    f"Peer [{peer}] attempted to stop watching job [{job.name}] and operation "
                    f"[{operation_name}], but no record of that job being watched was found.")
                return
            spec.event.notify_stop()
            spec.remove_peer(operation_name, peer)
            if not spec.peers:
                self.logger.debug(
                    f"No peers remain watching job [{job.name}], removing it from the "
                    "dictionary of jobs being watched.")
                self.watched_jobs.pop(job.name)

    # Operation API

    @abstractmethod
    def create_operation(self, operation_name: str, job_name: str) -> None:
        """Add a new operation to the data store.

        Args:
            operation_name (str): The name of the Operation to create in the
                data store.
            job_name (str): The name of the Job representing the execution of
                this operation.

        """
        raise NotImplementedError()

    # NOTE: This method is badly named, the current implementations return a
    # set of *job* names rather than *operation* names.
    # TODO: Fix or remove this.
    @abstractmethod
    def get_operations_by_stage(self, operation_stage: OperationStage) -> Set[str]:
        """Return a set of Job names in a specific operation stage.

        Find the operations in a given stage and return a set containing the
        names of the Jobs related to those operations.

        Args:
            operation_stage (OperationStage): The stage that the operations
                should be in.

        Returns:
            set: Set of all job names with operations in the specified state.

        """
        raise NotImplementedError()

    @abstractmethod
    def list_operations(self,
                        operation_filters: List[OperationFilter],
                        page_size: int=None,
                        page_token: str=None,
                        max_execution_timeout: int=None) -> Tuple[List[Operation], str]:
        """Return all operations matching the filter.

        Returns:
            list: A page of matching operations in the data store.
            str: If nonempty, a token to be submitted by the requester for the next page of results.
        """
        raise NotImplementedError()

    @abstractmethod
    def update_operation(self, operation_name: str, changes: Mapping[str, Any]) -> None:
        """Update an operation in the data store.

        This method takes an operation name and a dictionary of changes to
        apply to the operation in the data store, and updates the operation
        with those changes. The dictionary should be keyed by the attribute
        names which need to be updated, with the values being the new values
        for the attributes.

        Args:
            operation_name (str): The name of the operation that is being updated.
            changes: (dict): The dictionary of changes to be applied.

        """
        raise NotImplementedError()

    @abstractmethod
    def delete_operation(self, operation_name: str) -> None:
        """Delete a operation from the data store.

        This method removes a operation from the data store.

        Args:
            operation_name (str): The name of the operation to be removed.

        """
        raise NotImplementedError()

    # Lease API

    @abstractmethod
    def create_lease(self, lease: Lease) -> None:
        """Add a new lease to the data store.

        Args:
            lease (Lease): The Lease protobuf object representing the lease
                to be added to the data store.

        """
        raise NotImplementedError()

    @abstractmethod
    def get_leases_by_state(self, lease_state) -> Set[str]:
        """Return the set of IDs of leases in a given state.

        Args:
            lease_state (LeaseState): The state that the leases should
                be in.

        Returns:
            set: Set of strings containing IDs of leases in the given state.

        """
        raise NotImplementedError()

    @abstractmethod
    def update_lease(self, job_name: str, changes: Mapping[str, Any]) -> None:
        """Update a lease in the data store.

        This method takes a job name and a dictionary of changes to
        apply to the lease for that job in the data store, and updates the
        lease with those changes. The dictionary should be keyed by the
        attribute names which need to be updated, with the values being the
        new values for the attributes.

        The job name is used as leases have no unique identifier; instead
        there should always be at most one active lease for the job. It is
        the responsibility of data store implementations to ensure this.

        Args:
            job_name (str): The name of the job whose lease is being updated.
            changes: (dict): The dictionary of changes to be applied.

        """
        raise NotImplementedError()

    # NOTE: We use a callback to create the leases in this method so that the
    # data store doesn't need to know anything about the bots, or other lease
    # metadata.
    @abstractmethod
    def assign_n_leases(
        self,
        *,
        capability_hash: str,
        lease_count: int,
        assignment_callback: Callable[[List[Job]], Dict[str, Job]]
    ) -> None:
        """Attempt to assign Leases for several Jobs.

        This method selects ``lease_count`` Jobs from the data store with
        platform properties matching the worker capabilities given by
        ``capability_hash``.

        The given ``assignment_callback`` function is called with this
        list of Jobs, and is responsible for actually passing the Jobs to
        workers and arranging for execution.

        The ``assignment_callback`` function must return a dictionary
        mapping Job names to Job objects. This dictionary must contain
        all the Jobs which were successfully given to workers, and is
        used by the data store to remove assigned Jobs from the queue.

        Args:
            capability_hash (str): The hash of the worker capabilities to
                use when selecting Jobs. This is matched to the hash of the
                Job's platform properties, and so should be generated using
                :func:`buildgrid.utils.hash_from_dict` for consistency.
            lease_count (int): How many Leases we want to create. This
                specifies the maximum length of the list of Jobs passed
                to ``assignment_callback``.
            assignment_callback (callable): Function which takes a list of
                Jobs to be assigned to workers, and returns a dictionary
                of ``Job.name -> Job`` containing the Jobs which were
                successfully assigned.

        """
        raise NotImplementedError()

    @abstractmethod
    def get_operation_request_metadata_by_name(self, operation_name):
        """Return a dictionary containing metadata information that was
        sent by a client as part of a ``remote_execution_pb2.RequestMetadata``
        message.

        It contains the following keys:
        ``{'tool-name', 'tool-version', 'invocation-id', 'correlated-invocations-id'}``.
        """
        raise NotImplementedError()
