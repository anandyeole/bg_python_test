# Copyright (C) 2022 Bloomberg LP
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


from datetime import datetime, timedelta
from itertools import chain, combinations
import logging
from threading import Event, Lock, RLock, Thread
from time import sleep
from typing import Callable, Dict, Iterable, List, Optional, Set

from grpc import ServicerContext

from buildgrid._exceptions import DuplicateBotSessionError
from buildgrid._protos.google.devtools.remoteworkers.v1test2.bots_pb2 import BotSession, Lease
from buildgrid.server.persistence.interface import DataStoreInterface
from buildgrid.server.job import Job
from buildgrid.settings import MAX_WORKER_TTL
from buildgrid.utils import convert_values_to_sorted_lists, flatten_capabilities, hash_from_dict


class Worker:

    """Class representing a connected worker which is ready to accept work.

    This class is used to communicate assignment of a Lease to a worker by the
    JobAssigner thread to the gRPC handler thread responsible for a given
    CreateBotSession or UpdateBotSession request.

    """

    def __init__(self, bot_session: BotSession, ttl: int, context: ServicerContext):
        """Initialize a new Worker object.

        Args:
            bot_session (BotSession): The ``BotSession`` message recieved in the
                gRPC request which led to the creation of this ``Worker``.
            ttl (int): The maximum duration that this ``Worker`` is available to
                accept new work for, in seconds.
            context (ServicerContext): The gRPC context for the gRPC request which
                led to the creation of this ``Worker``.

        """
        self._bot_session = bot_session
        self._context = context
        self._entry_time = datetime.utcnow()
        self._event = Event()
        self._lease_lock = Lock()
        self._lease = None
        self._ttl = min(ttl, MAX_WORKER_TTL)
        self._cancelled = False

    @property
    def capabilities(self) -> Dict[str, Set[str]]:
        worker_capabilities: Dict[str, Set[str]] = {}
        if self._bot_session.worker.devices:
            # According to the spec:
            #   "The first device in the worker is the "primary device" -
            #   that is, the device running a bot and which is
            #   responsible for actually executing commands."
            primary_device = self._bot_session.worker.devices[0]

            for device_property in primary_device.properties:
                if device_property.key not in worker_capabilities:
                    worker_capabilities[device_property.key] = set()
                worker_capabilities[device_property.key].add(device_property.value)
        return worker_capabilities

    @property
    def expired(self) -> bool:
        return datetime.utcnow() > self._entry_time + timedelta(seconds=self._ttl) or not self._context.is_active()

    @property
    def connected(self) -> bool:
        return self._context.is_active()

    @property
    def cancelled(self) -> bool:
        return self._cancelled

    def maybe_assign_lease(self, job: Job, *, data_store: DataStoreInterface) -> bool:
        """Attempt to assign a Lease for the given Job using this Worker.

        This method attempts to assign the given Job to the Worker. If the
        Worker doesn't yet have any Leases, and isn't expired, then this should
        be successful. If the Job already has a Lease then this method updates
        the Lease to use this worker's name, otherwise it creates a new Lease
        for the Job. If the Job is successfully assigned to the Worker, this
        method returns ``True``.

        If the worker already has a Lease, or has expired (either by the TTL
        being exceeded or the gRPC context becoming inactive) then this method
        will return ``False`` and no changes will be made to the Job's Lease.

        Args:
            job (Job): The Job to attempt to assign.
            data_store (DataStoreInterface): The data store to use when
                updating the Job.

        Returns:
            bool, indicating whether or not a Lease for the Job was assigned
                to the Worker.

        """
        # Immediately fail to assign a new Lease if the worker either has a Lease
        # already, or has expired.
        with self._lease_lock:
            if self._lease is not None or self.expired:
                return False

            # Assign the given Job's Lease to this worker. If the Job doesn't already
            # have a Lease then we'll create a new one.
            #
            # NOTE: When `data_store` is an `SQLDataStore`, this call won't actually
            # store the new Lease in the database. This is because we're in the middle
            # of an existing database session here and want the `Job` class to be
            # mostly unaware of this.
            #
            # Responsibility for persisting this Lease lies with the gRPC handler thread
            # waiting for work to be assigned to this worker.
            if not job.lease:
                self._lease = job.create_lease(
                    self._bot_session.name,
                    self._bot_session.bot_id,
                    data_store=data_store
                )
            else:
                job.worker_name = self._bot_session.name
                self._lease = job.lease

            if self._lease is not None:
                job.mark_worker_started()

            # Wake up the worker's gRPC handler thread now that work has been assigned.
            self._event.set()

            return True

    def wait_for_work(self) -> Optional[Lease]:
        """Method to block until either a Lease is assigned or the TTL expires.

        This method waits for a successful call to ``Worker.maybe_assign_lease``
        from the JobAssigner thread. At most it will wait for the duration specified
        in the TTL when the Worker was instantiated.

        If a Lease was assigned within the TTL then the related BotSession will
        be updated to include the new Lease.

        This method returns the Lease that was added, or None if no Lease was
        assigned within the TTL.

        Returns:
            Lease or None: The Lease that was assigned, if any.

        """
        # Wait to be woken by `Worker.maybe_assign_lease` or by expiry of the
        # TTL given when this object was instantiated.
        self._event.wait(timeout=self._ttl)

        # If work was assigned, add the lease to the bot session for this worker.
        with self._lease_lock:
            if self._lease is not None:
                self._bot_session.leases.append(self._lease)
            return self._lease

    def cancel_wait_for_work(self) -> None:
        """Method to mark worker as cancelled and interrupt wait_for_work"""
        with self._lease_lock:
            self._cancelled = True
            self._event.set()


class JobAssigner:

    """Class to manage assigning work to groups of workers

    This class runs a background thread which regularly fetches jobs from
    the configured data store and assigns them to workers. Workers are
    tracked by capability hash, with job assignment being done for each
    group of workers with the same hash.

    Workers may be in more than one of these buckets, since the capability
    hashes are based on the powerset of the capabilities defined in a
    ``BotSession``. In this case, the worker will only be assigned one
    job, and removed from other buckets once the assignment has happened.

    """

    # TODO: This class covers the job assignment behaviour which previously
    # was implemented as part of the shared ``Scheduler`` class. This old
    # class should lose its job assignment functionality.

    def __init__(self, data_store: DataStoreInterface):
        """Initialize a new JobAssigner

        This sets up a new JobAssigner to fetch jobs from the given data
        store. ``JobAssigner.start`` must still be called before the
        object is ready to actually assign jobs to workers.

        Args:
            data_store (DataStoreInterface): The data store to fetch jobs
                from when assigning jobs to workers.

        """
        self._buckets_lock = RLock()
        self._buckets: Dict[str, List[str]] = {}
        self._capabilities_cache: Dict[str, List[str]] = {}
        self._data_store = data_store
        self._job_assignment_thread = Thread(
            target=self._job_assignment_target,
            name="JobAssignmentThread"
        )
        self._logger = logging.getLogger(__name__)
        self._run_job_assignment_thread = False
        self._worker_map: Dict[str, Worker] = {}

    def start(self) -> None:
        """Start the JobAssignmentThread

        This starts the thread which monitors the buckets of connected workers
        and periodically attempts to assign work to them. Once this method has
        been called the JobAssigner is ready to assign jobs to workers.

        """
        if not self._run_job_assignment_thread:
            self._run_job_assignment_thread = True
            self._job_assignment_thread.start()

    def stop(self) -> None:
        """Stop the JobAssignmentThread

        This stops the thread responsible for assigning jobs to workers. This
        method only returns once the thread is fully stopped.

        """
        if self._run_job_assignment_thread:
            self._run_job_assignment_thread = False
            self._job_assignment_thread.join()

    def register_worker(self, bot_session: BotSession, ttl: int, context: ServicerContext) -> Worker:
        """Register a new worker with the JobAssigner

        This method takes a ``BotSession`` along with a TTL for the related
        connection, and creates a ``Worker`` abstraction for that
        ``BotSession``.

        The created ``Worker`` is registered in the various buckets which
        correspond to its capabilities, and then returned. The caller should
        use ``Worker.wait_for_work`` to await assignment of a job to the
        worker.

        Args:
            bot_session (BotSession): The BotSession for the newly-connected
                Worker being registered.
            ttl (int): The expected time-to-live for the gRPC connection.
                This should be lower than the gRPC request timeout.
            context (ServicerContext): The gRPC context for this Worker's
                gRPC connection. This is used to check that the connection
                is still alive before assigning work.

        Returns:
            ``Worker``, representing the registered worker.

        """
        with self._buckets_lock:
            worker = Worker(bot_session, ttl, context)
            # If there's already a worker with the same bot name, check
            # if the context is still active. If not we can remove/replace
            # it.
            #
            # This can happen if a request from a bot is still being processed
            # on the BuildGrid side but the actual bot cancels the original one
            # and sent another.
            if bot_session.name in self._worker_map:
                current_worker = self._worker_map[bot_session.name]
                if current_worker.connected:
                    raise DuplicateBotSessionError(
                        f"An active RPC for work is in-progress for BotName=[{bot_session.name}]")

                current_worker.cancel_wait_for_work()
                self.remove_worker(bot_session.name)

            self._worker_map[bot_session.name] = worker

            worker_hashes = self._get_partial_capabilities_hashes(worker.capabilities)
            for key in worker_hashes:
                if key not in self._buckets:
                    self._buckets[key] = []
                self._buckets[key].append(bot_session.name)

        return worker

    def remove_worker(self, worker_name: str, worker: Optional[Worker] = None) -> None:
        """Remove a previously registered worker

        This method takes the name of a ``Worker`` previously returned
        from ``JobAssigner.register_worker``, and removes it from the job
        assignment buckets.

        If the worker given is already not in the buckets for some reason,
        then this method does nothing.

        Args:
            worker_name (str): The name of the worker whose registration
                shoud be removed.
            worker (Worker): Optional worker to be removed, used to verify
                that the worker registered under worker_name is the actual
                worker to remove

        """
        with self._buckets_lock:
            if worker_name in self._worker_map:
                if worker and self._worker_map[worker_name] != worker:
                    # The registered worker is not the same as the worker
                    # given to remove
                    return
            for bucket in self._buckets.values():
                try:
                    bucket.remove(worker_name)
                except ValueError:
                    # If the worker is already removed, we don't really care.
                    pass
            if worker_name in self._worker_map:
                del self._worker_map[worker_name]

    def _get_job_assignment_callback_for_bucket(
        self,
        bucket: str
    ) -> Callable[[List[Job]], Dict[str, Job]]:
        """Create a function to assign a list of jobs to a bucket of workers

        This function takes a bucket name (ie. a hash of a specific group of
        platform capabilities), and returns a function which can assign a list
        of jobs to the workers in the named bucket.

        The created function will return a mapping containing the jobs which
        were successfully assigned, keyed on the job name.

        Args:
            bucket (str): The name of the bucket we need to assign jobs to.

        Returns:
            function taking a list of jobs to assign to workers in the
            given bucket, returning a mapping of name: job for the jobs
            which were successfully assigned.
        """
        def _callback(jobs: List[Job]) -> Dict[str, Job]:
            assigned_jobs = {}
            assigned_workers = []

            # Assign jobs to the workers in the given bucket, workers
            # who have been waiting longest get given jobs first.
            with self._buckets_lock:
                for worker_name in self._buckets[bucket]:
                    if not jobs:
                        break

                    worker = self._worker_map[worker_name]
                    job = jobs[0]
                    assigned = worker.maybe_assign_lease(job, data_store=self._data_store)
                    if assigned:
                        assigned_jobs[job.name] = job
                        assigned_workers.append(worker_name)
                        jobs = jobs[1:]

                # Remove the workers that now have jobs from all buckets
                for name in assigned_workers:
                    self.remove_worker(name)

            # Let the caller handle any jobs that we couldn't assign
            return assigned_jobs
        return _callback

    def _assign_jobs_to_buckets(self) -> None:
        """Assign jobs to the currently connected workers

        This method iterates over the buckets of currently connected workers,
        and requests a number of job assignments from the data store to cover
        the number of workers in each bucket. Empty buckets are skipped.

        """
        for key, bucket in self._buckets.items():
            worker_count = len(bucket)
            if worker_count > 0:
                self._data_store.assign_n_leases(
                    capability_hash=key,
                    lease_count=len(bucket),
                    assignment_callback=self._get_job_assignment_callback_for_bucket(key)
                )

    def _job_assignment_target(self) -> None:
        """JobAssignmentThread implementation

        This method is the entry point of the JobAssignmentThread. This
        thread repeatedly iterates over the buckets of connected workers,
        and attempts to assign work to them.

        """
        while self._run_job_assignment_thread:
            try:
                with self._buckets_lock:
                    self._assign_jobs_to_buckets()

                    # Clean up empty buckets
                    self._buckets = {
                        key: bucket
                        for key, bucket in self._buckets.items()
                        if len(bucket) > 0
                    }
                sleep(1.0)
            except Exception:
                self._logger.debug("Error in job assignment thread", exc_info=True)

    def _get_partial_capabilities(self, capabilities: Dict[str, List[str]]) -> Iterable[Dict[str, List[str]]]:
        """ Given a capabilities dictionary with all values as lists,
        yield all partial capabilities dictionaries. """
        CAPABILITIES_WARNING_THRESHOLD = 10

        caps_flat = flatten_capabilities(capabilities)

        if len(caps_flat) > CAPABILITIES_WARNING_THRESHOLD:
            self._logger.warning(
                "A worker with a large capabilities dictionary has been connected. "
                f"Processing its capabilities may take a while. Capabilities: {capabilities}")

        # Using the itertools powerset recipe, construct the powerset of the tuples
        capabilities_powerset = chain.from_iterable(combinations(caps_flat, r) for r in range(len(caps_flat) + 1))
        for partial_capability_tuples in capabilities_powerset:
            partial_dict: Dict[str, List[str]] = {}

            for tup in partial_capability_tuples:
                partial_dict.setdefault(tup[0], []).append(tup[1])
            yield partial_dict

    def _get_partial_capabilities_hashes(self, capabilities: Dict[str, Set[str]]) -> List[str]:
        """ Given a list of configurations, obtain each partial configuration
        for each configuration, obtain the hash of each partial configuration,
        compile these into a list, and return the result. """
        # Convert requirements values to sorted lists to make them json-serializable
        normalized_capabilities = convert_values_to_sorted_lists(capabilities)

        # Check to see if we've cached this value
        capabilities_digest = hash_from_dict(normalized_capabilities)
        try:
            return self._capabilities_cache[capabilities_digest]
        except KeyError:
            # On cache miss, expand the capabilities into each possible partial capabilities dictionary
            capabilities_list = []
            for partial_capability in self._get_partial_capabilities(normalized_capabilities):
                capabilities_list.append(hash_from_dict(partial_capability))

            self._capabilities_cache[capabilities_digest] = capabilities_list
            return capabilities_list
