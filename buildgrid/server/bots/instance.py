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
BotsInterface
=================

Instance of the Remote Workers interface.
"""
import asyncio
from collections import OrderedDict
from datetime import datetime, timedelta
import logging
from threading import Lock
from time import time
from typing import Dict, Optional, Set, Tuple
import uuid

from grpc import ServicerContext

from buildgrid._enums import BotStatus, LeaseState, OperationStage
from buildgrid._exceptions import (
    InvalidArgumentError, NotFoundError, BotSessionClosedError,
    UnknownBotSessionError, BotSessionMismatchError, DuplicateBotSessionError,
    BotSessionCancelledError
)
from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2
from buildgrid.client.channel import setup_channel
from buildgrid.client.logstream import logstream_client
from buildgrid.server.bots.job_assigner import JobAssigner
from buildgrid.server.job import Job
from buildgrid.server.metrics_names import (
    BOTS_CREATE_BOT_SESSION_TIME_METRIC_NAME,
    BOTS_UPDATE_BOT_SESSION_TIME_METRIC_NAME
)
from buildgrid.server.metrics_utils import DurationMetric
from buildgrid.server.persistence.interface import DataStoreInterface
from buildgrid.server.scheduler import Scheduler
from buildgrid.settings import NETWORK_TIMEOUT


class BotsInterface:

    def __init__(self, data_store: DataStoreInterface, *, action_cache=None,
                 bot_session_keepalive_timeout: Optional[int]=None,
                 permissive_bot_session: Optional[bool]=None,
                 logstream_url: Optional[str]=None,
                 logstream_credentials=None,
                 logstream_instance_name: Optional[str]=None):
        if logstream_credentials is None:
            logstream_credentials = {}
        self.__logger = logging.getLogger(__name__)

        self._scheduler = Scheduler(data_store, action_cache,
                                    logstream_url=logstream_url,
                                    logstream_credentials=logstream_credentials,
                                    logstream_instance_name=logstream_instance_name,
                                    enable_job_watcher=False)
        self._job_assigner = JobAssigner(data_store)
        self._data_store = data_store
        self._instance_name = None
        self._logstream_channel = None
        self._logstream_url = logstream_url
        self._logstream_credentials = logstream_credentials
        self._logstream_instance_name = logstream_instance_name

        # Mapping of bot_session.name -> bot_session.id
        self._bot_ids: Dict[str, str] = {}

        # Mapping of bot_session.status -> set(bot_session.id, ...)
        self._bot_name_by_status: Dict[BotStatus, Set] = {bot_status: set() for bot_status in BotStatus}
        self._bot_name_by_status_lock = Lock()

        self._assigned_leases: Dict[str, Set[str]] = {}
        self._assigned_leases_lock = Lock()

        self._bot_session_keepalive_timeout = bot_session_keepalive_timeout
        bot_session_reaper_started = self._setup_bot_session_reaper_loop()

        self._permissive_bot_session = permissive_bot_session
        if bot_session_reaper_started and permissive_bot_session:
            self.__logger.warning(
                "Both BotSession reaper and Permissive BotSession mode are enabled."
                "If the DNS configuration is not resulting in 'sticky sessions' with "
                "bots talking to the same BuildGrid process (unless unhealthy), "
                "the BotSession reaper from other processes may cancel and re-queue "
                "ongoing leases. Please refer to the documentation for more information."
            )

        # Ordered mapping of bot_session_name: string -> last_expire_time_we_assigned: datetime
        #   NOTE: This works because the bot_session_keepalive_timeout is the same for all bots
        # and thus always increases with time (e.g. inserting at the end keeps them sorted because
        # of this property, otherwise we may have had to insert 'in the middle')
        self._ordered_expire_times_by_botsession: Dict[str, datetime] = OrderedDict()
        # The "minimum" expire_time we have coming up
        self._next_expire_time = None
        #   The Event to set when we learn about a new expire time that is at a different point in the
        # future than what we knew (e.g. whenever we reset the value of self._next_expire_time)
        #   This is mostly useful when we end up with a `next_expire_time` closer to the future than we
        # initially thought (e.g. tracking the first BotSession expiry since all BotSessions are assigned
        # the same keepalive_timeout).
        # NOTE: asyncio.Event() is NOT thread-safe.
        #   However, here we .set() it from the ThreadPool threads handling RPC requests
        # and only clearing it from the asyncio event loop which the `reaper_loop`.
        self._deadline_event = asyncio.Event()

        #   Remembering the last n evicted_bot_sessions so that we can present the appropriate
        # messages if they ever get back. (See additional notes in `_close_bot_session`).
        self._remember_last_n_evicted_bot_sessions = 1000
        #   Maps bot_session_name: string to (eviction_time: datetime, reason: string), with a maximum size
        # of approx `_remeber_last_n_evicted_bot_sessions`.
        self._evicted_bot_sessions: Dict[str, Tuple[datetime, str]] = OrderedDict()

    # --- Public API ---

    @property
    def instance_name(self):
        return self._instance_name

    @property
    def scheduler(self):
        return self._scheduler

    def setup_grpc(self):
        self._scheduler.setup_grpc()

        if self._logstream_channel is None and self._logstream_url is not None:
            self._logstream_channel, _ = setup_channel(
                self._logstream_url,
                auth_token=None,
                client_key=self._logstream_credentials.get("tls-client-key"),
                client_cert=self._logstream_credentials.get("tls-client-cert"),
                server_cert=self._logstream_credentials.get("tls-server-cert")
            )

    def start(self) -> None:
        self._scheduler.start()
        self._job_assigner.start()

    def stop(self) -> None:
        self._scheduler.stop()
        self._job_assigner.stop()

    def register_instance_with_server(self, instance_name, server):
        """Names and registers the bots interface with a given server."""
        if self._instance_name is None:
            server.add_bots_interface(self, instance_name)

            self._instance_name = instance_name
            if self._scheduler is not None:
                self._scheduler.set_instance_name(instance_name)

        else:
            raise AssertionError("Instance already registered")

    @DurationMetric(BOTS_CREATE_BOT_SESSION_TIME_METRIC_NAME, instanced=True)
    def create_bot_session(self, parent, bot_session, context, deadline=None):
        """ Creates a new bot session. Server should assign a unique
        name to the session. If a bot with the same bot id tries to
        register with the service, the old one should be closed along
        with all its jobs.
        """
        if not bot_session.bot_id:
            raise InvalidArgumentError("Bot's id must be set by client.")

        try:
            self._check_bot_ids(bot_session.bot_id)
        except DuplicateBotSessionError:
            pass

        # Bot session name, selected by the server
        name = f"{parent}/{str(uuid.uuid4())}"
        bot_session.name = name

        self._track_bot_session(name, bot_session.bot_id)

        self._request_leases(bot_session, context, deadline=deadline, name=name)
        self._assign_deadline_for_botsession(bot_session, name)

        self.__logger.info(
            f"Opened BotSession name=[{bot_session.name}] for bot_id=[{bot_session.bot_id}].")
        leases = ",".join(lease.id[:8] for lease in bot_session.leases)
        self.__logger.debug(f"Leases assigned to newly opened BotSession name=[{bot_session.name}] "
                            f"for bot_id=[{bot_session.bot_id}]: [{leases}].")

        self._update_status_count_for_bot_session(bot_session)
        return bot_session

    def _track_bot_session(self, bot_session_name, bot_id, leases=()):
        self.__logger.debug(
            f"Now tracking BotSession name=[{bot_session_name}] "
            f"for bot_id=[{bot_id}] with leases=[{leases}]")

        self._bot_ids[bot_session_name] = bot_id
        # We want to keep a copy of lease ids we have assigned
        leases_set = set(leases)
        with self._assigned_leases_lock:
            self._assigned_leases[bot_session_name] = leases_set

    @DurationMetric(BOTS_UPDATE_BOT_SESSION_TIME_METRIC_NAME, instanced=True)
    def update_bot_session(self, name, bot_session, context, deadline=None):
        """ Client updates the server. Any changes in state to the Lease should be
        registered server side. Assigns available leases with work.
        """
        try:
            self._check_bot_ids(bot_session.bot_id, name)
        except (UnknownBotSessionError, BotSessionClosedError):
            if self._permissive_bot_session:
                # If in permissive mode, implicitly reopen botsession
                self.__logger.info(
                    f"BotSession with bot_name=[{bot_session.name}] and bot_id=[{bot_session.bot_id}] "
                    f"is now talking to this server instance. Confirming lease accuracy..."
                )

                lease_ids = [lease.id for lease in bot_session.leases]
                self._track_bot_session(name, bot_session.bot_id, lease_ids)

                self.__logger.info(
                    f"Successfully relocated BotSession with bot_name=[{bot_session.name}] "
                    f"and bot_id=[{bot_session.bot_id}] with leases=[{lease_ids}].")
            else:
                # Default behavior is to raise those exceptions
                # and handle them accordingly at a higher level
                self.__logger.warning(
                    f"Unknown bot with bot_name=[{bot_session.name}] and "
                    f"bot_id=[{bot_session.bot_id}]; permissive BotSession mode disabled.")
                raise

        self._check_assigned_leases(bot_session)

        # Stop tracking the prior deadline since we have heard back
        # by the deadline we had announced, now we're going to prepare
        # a new BotSession for the bot and once done assign a new deadline.
        self._untrack_deadline_for_botsession(bot_session.name)

        # Go over all the leases the bot sent us in this UpdateBotSession request
        lease_removed = False
        for lease in list(bot_session.leases):
            # See if we need to tell the bot about a lease update (e.g. execution-service side cancellation)
            # and whether we need to update our datastore (e.g. bot finished the lease)
            lease_to_send_to_bot, update_datastore_from_lease = self._check_lease_state(lease)

            if update_datastore_from_lease:
                try:
                    self._scheduler.update_job_lease_state(lease.id, lease)
                except NotFoundError:
                    # Lease no longer exists or is already completed, be sure it gets removed from the bot
                    self.__logger.debug("Lease Id not found when updating job lease state, removing from bot.")
                    lease_to_send_to_bot = None

            if lease_to_send_to_bot:
                # Replace old lease with updated lease (e.g. communicating cancellation)
                # only if there are changes (otherwise the previous lease/state is kept)
                if lease_to_send_to_bot != lease:
                    bot_session.leases.remove(lease)
                    bot_session.leases.append(lease_to_send_to_bot)
            else:
                # We want to remove the lease from this bot and update our records accordingly
                # For example the bot has completed the lease (and results were stored above), or,
                # the bot had to cancel
                with self._assigned_leases_lock:
                    if name in self._assigned_leases:
                        try:
                            self._assigned_leases[name].remove(lease.id)
                            self.__logger.debug(f"Removed lease id=[{lease.id}] from bot=[{name}]")
                        except KeyError:
                            self.__logger.info(f"Lease id=[{lease.id}] already removed from bot=[{name}]")
                bot_session.leases.remove(lease)
                lease_removed = True

        # Don't request new leases if a lease was removed.
        #
        # This mitigates situations where the scheduler is updated with the new
        # state of the lease, but a fault thereafter causes the worker to retry
        # the old UpdateBotSession call
        if not lease_removed:
            self._request_leases(bot_session, context, deadline=deadline, name=name)

        metadata = self._scheduler.get_metadata_for_leases(
            bot_session.leases, writeable_streams=True)

        # Assign a new deadline to the BotSession
        self._assign_deadline_for_botsession(bot_session, name)

        leases = ",".join(lease.id[:8] for lease in bot_session.leases)
        self.__logger.debug(f"Sending BotSession update for name=[{bot_session.name}], "
                            f"bot_id=[{bot_session.bot_id}]: leases=[{leases}].")

        self._update_status_count_for_bot_session(bot_session)
        return bot_session, metadata

    def count_bots(self) -> int:
        return len(self._bot_ids)

    def count_bots_by_status(self, status: BotStatus) -> int:
        return len(self._bot_name_by_status[status])

    # --- Private API ---
    def _create_log_stream(self, job: Job) -> None:
        if not self._logstream_channel:
            return

        parent_base = f"{job.action_digest.hash}_{job.action_digest.size_bytes}_{time()}"
        stdout_parent = f"{parent_base}_stdout"
        stderr_parent = f"{parent_base}_stderr"
        with logstream_client(self._logstream_channel,
                              self._logstream_instance_name) as ls_client:
            stdout_stream = ls_client.create(stdout_parent)
            stderr_stream = ls_client.create(stderr_parent)
        job.set_stdout_stream(stdout_stream, data_store=self._data_store)
        job.set_stderr_stream(stderr_stream, data_store=self._data_store)

    def _start_executing_job(self, lease: bots_pb2.Lease) -> None:
        job = self._data_store.get_job_by_name(lease.id)
        if job is not None:
            job.update_operation_stage(
                OperationStage.EXECUTING,
                data_store=self._data_store
            )
            self._create_log_stream(job)

    def _request_leases(
        self,
        bot_session: bots_pb2.BotSession,
        context: ServicerContext,
        deadline: Optional[int]=None,
        name: Optional[str]=None
    ) -> None:
        # Only send one lease at a time currently.
        if bot_session.status == BotStatus.OK.value and not bot_session.leases:
            # If a response is needed immediately, we set a reasonably short
            # deadline to allow us to hopefully still get work assigned.
            if deadline is None:
                deadline = self._data_store.max_get_timeout + 5

            ttl = deadline - NETWORK_TIMEOUT
            if ttl < 0:
                self.__logger.info(
                    f"BotSession name=[{name}] expires in less time than "
                    f"NETWORK_TIMEOUT=[{NETWORK_TIMEOUT}], no leases will be assigned")
                return

            worker = self._job_assigner.register_worker(bot_session, ttl, context)
            lease = worker.wait_for_work()

            if worker.cancelled:
                raise BotSessionCancelledError(
                    f"Request cancelled while waiting for work for BotSession name=[{name}]")

            if lease:
                self._start_executing_job(lease)
                with self._assigned_leases_lock:
                    if bot_session.name in self._assigned_leases:
                        self._assigned_leases[bot_session.name].add(lease.id)
                    else:
                        # The BotSession may no longer exist, make sure the leases are re-queued!
                        self._scheduler.retry_job_lease(lease.id)
                        self._job_assigner.remove_worker(bot_session.name, worker)
                        self.__logger.info(f'BotSession name=[{name}] closed while trying to assign lease. '
                                           f'Re-queued lease=[{lease}].')
                        raise BotSessionClosedError('BotSession closed while assigning lease. Re-queued lease.')
            else:
                self._job_assigner.remove_worker(bot_session.name, worker)

    # Returns a Tuple[Optional[Lease], bool]:
    #   (lease_to_send_to_bot, update_datastore_from_lease)
    def _check_lease_state(self, lease: bots_pb2.Lease) -> Tuple[Optional[bots_pb2.Lease], bool]:
        lease_state = LeaseState(lease.state)

        try:
            current_lease = self._scheduler.get_job_lease(lease.id)
            # get_job_lease will only return active leases in sql
            # data-store, so handle if no lease was returned
            if current_lease is None:
                return (None, False)
        except NotFoundError:
            # Job does not exist, remove lease from bot
            return (None, False)

        # If the lease was marked cancelled on the buildgrid side
        # inform the bot (update lease, no need to update bgd datastore)
        if current_lease.state == LeaseState.CANCELLED.value:
            lease.state = LeaseState.CANCELLED.value
            return (lease, False)

        # If the lease is already completed on the buildgrid side
        # remove it from the bot
        if current_lease.state == LeaseState.COMPLETED.value:
            return (None, False)

        if lease_state == LeaseState.COMPLETED:
            return (None, True)

        return (lease, True)

    def _get_bot_id_from_bot_name_or_raise(self, name):
        """ Returns the bot_id corresponding to the passed `name`.
            Raises BotSessionClosedError if the botsession was recently closed.
            Raises UnknownBotSessionError if there is no such known BotSession.
        """
        bot_id = self._bot_ids.get(name)
        if bot_id is None:
            eviction_record = self._evicted_bot_sessions.get(name)
            if eviction_record:
                raise BotSessionClosedError(f'Server has recently evicted the BotSession name=[{name}] at '
                                            f'timestamp=[{eviction_record[0]}], reason=[{eviction_record[1]}]')
            raise UnknownBotSessionError('Unknown BotSession. BuildGrid has not seen a '
                                         f'BotSession with name=[{name}] recently.')
        return bot_id

    def _check_bot_ids(self, bot_id, name=None):
        """ Checks whether the ID and the name of the bot match,
        otherwise closes the bot sessions with that name or ID
        """
        if name is not None:
            _bot_id = self._get_bot_id_from_bot_name_or_raise(name)
            if _bot_id != bot_id:
                self._close_bot_session(name, reason="bot_id mismatch between worker and bgd")
                raise BotSessionMismatchError(
                    f'Mismatch between client supplied client_bot_id=[{bot_id}] and '
                    f'buildgrid record of bgd_bot_id=[{_bot_id}] for BotSession with name=[{name}].')
        else:
            for _name, _bot_id in self._bot_ids.items():
                if bot_id == _bot_id:
                    self._close_bot_session(_name,
                                            reason="Bot with same ID trying to create a new BotSession")
                    raise DuplicateBotSessionError(
                        f'Bot ID bot_id=[{bot_id}] already registered and given bgd_bot_name=[{_name}].')

    def _assign_deadline_for_botsession(self, bot_session, bot_session_name):
        """ Assigns a deadline to the BotSession if bgd was configured to do so
        """
        # Specify bot keepalive expiry time if timeout is set
        if self._bot_session_keepalive_timeout:
            # Calculate expire time
            expire_time_python = datetime.utcnow() + timedelta(seconds=self._bot_session_keepalive_timeout)

            # Set it in the bot_session
            bot_session.expire_time.FromDatetime(expire_time_python)

            # Keep track internally for the botsession reaper
            self._track_deadline_for_bot_session(bot_session_name, expire_time_python)

    def _untrack_deadline_for_botsession(self, bot_session_name):
        """ Un-assigns the session reaper tracked deadline of the BotSession
        if bgd was configured to do so
        """
        # Specify bot keepalive expiry time if timeout is set
        if self._bot_session_keepalive_timeout:
            self._track_deadline_for_bot_session(bot_session_name, None)

    def _track_deadline_for_bot_session(self, bot_session_name, new_deadline):
        """ Updates the data structures keeping track of the last deadline
        we had assigned to this BotSession by name.
        When `new_deadline` is set to None, the deadline is unassigned.
        """
        # Keep track of the next expire time to inform the watcher
        updated_next_expire_time = False

        if new_deadline:
            # Since we're re-setting the update time for this bot, make sure to move it
            # to the end of the OrderedDict (if it was already tracked in the OrderedDict)
            try:
                self._ordered_expire_times_by_botsession.move_to_end(bot_session_name)
            except KeyError:
                pass

            self._ordered_expire_times_by_botsession[bot_session_name] = new_deadline
            updated_next_expire_time = True
        else:
            try:
                if self._ordered_expire_times_by_botsession.pop(bot_session_name):
                    updated_next_expire_time = True
            except KeyError:
                self.__logger.debug("Tried to un-assign deadline for bot_session_name="
                                    f"[{bot_session_name}] but it had no deadline to begin with.")
                pass

        # Make the botsession reaper thread look at the current new_deadline
        # (if it's nearer in the future) compared to the previously known `next_expire_time`.
        if updated_next_expire_time:
            if self._update_next_expire_time(compare_to=new_deadline):
                self._deadline_event.set()

    def _check_assigned_leases(self, bot_session):
        """Makes sure that all the leases we knew of that were assigned to the bot
        are there, and automatically retries leases the bot may have dropped.
        """
        session_lease_ids = []

        for lease in bot_session.leases:
            session_lease_ids.append(lease.id)

        with self._assigned_leases_lock:
            if bot_session.name in self._assigned_leases:
                # In order to be able to remove leases while iterating
                # we need to get the values as a list and iterate over that
                # (python3 doesn't allow modifying containers during iteration)
                for lease_id in list(self._assigned_leases[bot_session.name]):
                    if lease_id not in session_lease_ids:
                        self.__logger.warning(f"Assigned lease id=[{lease_id}], "
                                              f"not found on bot with name=[{bot_session.name}] and "
                                              f"id=[{bot_session.bot_id}]. Retrying job. "
                                              "Did the bot crash and restart?")
                        # Un-assign job from this botsession and let the scheduler handle it
                        try:
                            self._scheduler.retry_job_lease(lease_id)
                        except NotFoundError:
                            pass
                        try:
                            self._assigned_leases[bot_session.name].remove(lease_id)
                        except KeyError:
                            pass
            else:
                raise BotSessionClosedError(f"BotSession name=[{bot_session.name}] for bot_id="
                                            f"[{bot_session.bot_id}] closed while checking leases.")

    def _truncate_eviction_history(self):
        # Make sure we're only keeping the last N evicted sessions
        # NOTE: there could be some rare race conditions when the length of the OrderedDict is
        # only 1 below the limit; Multiple threads could check the size simultaneously before
        # they get to add their items in the OrderedDict, resulting in a size bigger than initially intented
        # (with a very unlikely upper bound of:
        #   O(n) = `remember_last_n_evicted_bot_sessions`
        #             + min(number_of_threads, number_of_concurrent_threads_cpu_can_handle)).
        #   The size being only 1 below the limit could also happen when the OrderedDict contains
        # exactly `n` items and a thread trying to insert sees the limit has been reached and makes
        # just enough space to add its own item.
        #   The cost of locking vs using a bit more memory for a few more items in-memory is high, thus
        # we opt for the unlikely event of the OrderedDict growing a bit more and
        # make the next thread which tries to to insert an item, clean up `while len > n`.
        while len(self._evicted_bot_sessions) > self._remember_last_n_evicted_bot_sessions:
            self._evicted_bot_sessions.popitem(last=False)

    def _close_bot_session(self, name, *, reason):
        """ Before removing the session, close any leases and
        requeue with high priority.
        """
        # If we had assigned an expire_time for this botsession, make sure to
        # clean up, regardless of the reason we end up closing this BotSession
        self._untrack_deadline_for_botsession(name)

        retried_leases = 0
        total_leases = 0
        with self._assigned_leases_lock:
            if name in self._assigned_leases:
                total_leases = len(self._assigned_leases[name])
                for lease_id in self._assigned_leases[name]:
                    try:
                        self._scheduler.retry_job_lease(lease_id)
                    except NotFoundError:
                        pass
                    else:
                        retried_leases += 1
                self._assigned_leases.pop(name)

        self._job_assigner.remove_worker(name)

        self._truncate_eviction_history()
        # Record this eviction
        self._evicted_bot_sessions[name] = (datetime.utcnow(), reason)

        try:
            bot_id = self._get_bot_id_from_bot_name_or_raise(name)
            self._bot_ids.pop(name)
        except (BotSessionMismatchError, DuplicateBotSessionError) as e:
            self.__logger.warning('Unable to identify `bot_id` associated with BotSession '
                                  f'while closing the BotSession name=[{name}]: {e}')
            bot_id = 'unknown'

        self._clear_status_count_for_bot_name(name)

        self.__logger.info(f'Closed BotSession bot_id=[{bot_id}], name=[{name}], reason=[{reason}] '
                           f'and sucessfully requeued [{retried_leases}]/[{total_leases}] leases.')

    def _update_next_expire_time(self, compare_to=None):
        """
             If we don't have any more bot_session deadlines, clear out this variable
         to avoid busy-waiting. Otherwise, populate it with the next known expiry time
         either from the queue or by comparing to the optional argument `compare_to`.
             This method returns True/False indicating whether the `next_expire_time`
        was updated.
        """
        if compare_to:
            # If we pass in a time earlier than the already known `next_expire_time`
            # or this is the first expire time we know of... set it to `compare_to`

            # NOTE: We could end up in a race condition here, where threads could
            # update the `_next_expire_time` to their own value of `compare_to`
            # if at the time they checked that their time was "earlier" than the
            # shared `_next_expire_time`.
            #   For the purpose this is used, this is an OK behavior since:
            #     1. If this method is called around the same time on different threads,
            #      the expiry time should be very close (`delta`).
            #     2. We may end up waiting for an additional `delta` time to expire the first
            #      session in the OrderedDict, and then rapidly close all the subsequent
            #      sessions with expire_time < now.
            #   This approach allows for potentially "lazy session expiry" (after an additional minimal `delta`),
            # giving priority to all the other work buildgrid needs to do, instead of using the overhead of
            # locking this and blocking up multiple threads to update this with each rpc.
            # TL;DR Approximation of the `next_expire_time` here is good enough for this purpose.
            if not self._next_expire_time or compare_to < self._next_expire_time:
                self._next_expire_time = compare_to
                return True
        else:
            _, next_expire_time_in_queue = self._get_next_botsession_expiry()
            # It is likely that the expire time we knew of is no longer in the OrderedDict
            # (e.g. we assigned a new one to that BotSession), thus this could be either
            # before or after the previously known `next_expire_time`
            if self._next_expire_time != next_expire_time_in_queue:
                self._next_expire_time = next_expire_time_in_queue
                return True

        return False

    def _next_expire_time_occurs_in(self):
        if self._next_expire_time:
            next_expire_time = (self._next_expire_time - datetime.utcnow()).total_seconds()
            # Check if this is in the future (> 0, negative values means expiry happened already!)
            if next_expire_time > 0:
                # Pad this with 0.1 second so that the expiry actually happens when we try to reap
                return round(next_expire_time + 0.1, 3)
            return 0

        return None

    def _get_next_botsession_expiry(self):
        botsession_name = None
        expire_time = None
        # We want to `peek` the first entry of the OrderedDict here
        # We do this by:
        #     1. Popping the first item (if any)
        #     2. Inserting that key-value pair again (goes to the end with the OrderedDict)
        #     3. Moving that newly re-inserted entry to the beginning (to preserve the order)
        #   This should work exactly as a `peek` since we only pop the first item in the asyncio event loop,
        # and we know that all other items we add in this OrderedDict must be >= the current first in
        # terms of expiry (Thus re-adding it and moving it to first should still maintain the sorted order).
        try:
            botsession_name, expire_time = self._ordered_expire_times_by_botsession.popitem(last=False)
        except KeyError:
            pass  # OrderedDict is empty, no BotSessions to check at this instant
        else:
            self._ordered_expire_times_by_botsession[botsession_name] = expire_time
            self._ordered_expire_times_by_botsession.move_to_end(botsession_name, last=False)

        return (botsession_name, expire_time)

    def _reap_expired_sessions(self):
        self.__logger.debug("Checking for expired BotSessions to reap...")
        next_botsession_name_to_expire, next_botsession_expire_time = self._get_next_botsession_expiry()
        while next_botsession_expire_time and next_botsession_expire_time <= datetime.utcnow():
            # This is the last deadline we have communicated with this bot...
            # It has expired.
            # If there is no bot_id -> bot_name mapping anymore, Bot may have opened a new BotSession
            bot_id = self._bot_ids.get(next_botsession_name_to_expire)

            self.__logger.warning(
                f"BotSession name=[{next_botsession_name_to_expire}] for bot_id=[{bot_id}] "
                f"with deadline=[{next_botsession_expire_time}] has expired.")
            try:
                self._close_bot_session(next_botsession_name_to_expire, reason="expired")
            except BotSessionClosedError:
                self.__logger.warning(
                    f"Expired BotSession name=[{next_botsession_name_to_expire}] "
                    f"for bot_id=[{self._bot_ids.get(next_botsession_name_to_expire)}] "
                    f"with deadline=[{next_botsession_expire_time}] was already closed.")
                pass

            next_botsession_name_to_expire, next_botsession_expire_time = self._get_next_botsession_expiry()

        self._update_next_expire_time()

    async def _reap_expired_sessions_loop(self):
        try:
            self.__logger.info(
                "Starting BotSession reaper, bot_session_keepalive_timeout="
                f"[{self._bot_session_keepalive_timeout}].")
            while True:
                try:
                    # for <= 0, assume something expired already
                    expires_in = self._next_expire_time_occurs_in()
                    if expires_in:
                        self.__logger.debug(
                            f"Waiting for an event indicating earlier expiry or wait=[{expires_in}] "
                            "for the next BotSession to expire.")
                    else:
                        self.__logger.debug("No more BotSessions to watch for expiry, waiting for new BotSessions.")
                    await asyncio.wait_for(self._deadline_event.wait(), timeout=expires_in)
                    self._deadline_event.clear()
                except asyncio.TimeoutError:
                    pass

                self._reap_expired_sessions()
        except asyncio.CancelledError:
            self.__logger.info("Cancelled reaper task.")
            pass
        except Exception as exception:
            self.__logger.exception(exception)
            raise

    def _setup_bot_session_reaper_loop(self):
        if self._bot_session_keepalive_timeout:
            if self._bot_session_keepalive_timeout <= 0:
                raise InvalidArgumentError(
                    f"[bot_session_keepalive_timeout] set to [{self._bot_session_keepalive_timeout}], "
                    "must be > 0, in seconds")

            # Add the expired session reaper in the event loop
            main_loop = asyncio.get_event_loop()
            main_loop.create_task(self._reap_expired_sessions_loop())
            return True
        return False

    def _clear_status_count_for_bot_name(self, bot_name: str) -> None:
        with self._bot_name_by_status_lock:
            for status in self._bot_name_by_status:
                self._bot_name_by_status[status].discard(bot_name)

    def _update_status_count_for_bot_session(self, bot_session: bots_pb2.BotSession) -> None:
        bot_name = bot_session.name
        bot_status = BotStatus(bot_session.status)

        self._clear_status_count_for_bot_name(bot_name)
        with self._bot_name_by_status_lock:
            try:
                self._bot_name_by_status[bot_status].add(bot_name)
            except KeyError:
                # We are not tracking this bot status, this will be a no-op
                pass
