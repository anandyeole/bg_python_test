# Copyright (C) 2021 Bloomberg LP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  <http://www.apache.org/licenses/LICENSE-2.0>
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License' is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
Bots instance
=============

The Bots service is responsible for assigning work to the various worker
(sometimes "bot") machines connected to the grid. These workers communicate
with the Bots service using the `Remote Workers API`_.

Like the other BuildGrid services, the Bots service uses the concept of
instance names to contain specific sets of workers. These names could refer
to (e.g.) a project-specific BuildGrid instance as part of a wider managed
deployment.

These instance names are mapped to instances of the ``BotsInstance`` class,
which implements the actual functionality of the Bots service. It's
responsible for selecting work and assigning it to a connected capable
worker, as well as constructing state updates based on messages from the
workers and publishing those updates for the other grid services to consume.

The ``BotsInstance`` is also responsible for keeping track of work that
should be cancelled, and making sure that any workers it finds doing that
work are informed of the cancellation.

.. _Remote Workers API: https://github.com/googleapis/googleapis/tree/master/google/devtools/remoteworkers/v1test2

"""


from datetime import datetime, timedelta
import logging
from queue import Queue
from threading import Event, Lock, TIMEOUT_MAX
from typing import Callable, Dict, Optional, Set, TYPE_CHECKING, Tuple
import uuid

from google.protobuf.any_pb2 import Any
from google.protobuf.timestamp_pb2 import Timestamp
import grpc
import pika  # type: ignore

from buildgrid._enums import LeaseState, OperationStage
from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import (
    Action, ActionResult, Digest, ExecuteOperationMetadata, ExecuteResponse)
from buildgrid._protos.buildgrid.v2.messaging_pb2 import BotStatus, Job, RetryableJob, UpdateOperations
from buildgrid._protos.google.devtools.remoteworkers.v1test2.bots_pb2 import BotSession, Lease
from buildgrid._protos.google.longrunning.operations_pb2 import Operation
from buildgrid._types import GrpcTrailingMetadata
from buildgrid.client.channel import setup_channel
from buildgrid.server.utils.lru_inmemory_cache import LruInMemoryCache
from buildgrid.server.rabbitmq._enums import Exchanges
from buildgrid.server.rabbitmq.pika_consumer import (RetryingPikaConsumer, QueueBinding)
from buildgrid.server.rabbitmq.pika_publisher import RetryingPikaPublisher
from buildgrid.server.rabbitmq.utils import MessageSpec
from buildgrid.settings import NETWORK_TIMEOUT
from buildgrid.utils import combinations_with_unique_keys, create_digest, flatten_capabilities


if TYPE_CHECKING:
    from buildgrid._app.settings.rmq_parser import RabbitMqConnection
    from buildgrid.server.rabbitmq.server import RMQServer

_state_to_stage = {
    LeaseState.UNSPECIFIED: OperationStage.UNKNOWN,
    LeaseState.PENDING: OperationStage.EXECUTING,
    LeaseState.ACTIVE: OperationStage.EXECUTING,
    LeaseState.COMPLETED: OperationStage.COMPLETED,
    LeaseState.CANCELLED: OperationStage.COMPLETED
}


class _JobAssignmentRequest:

    """Class to orchestrate a worker requesting a Job.

    Used to coordinate passing a Job from a PikaConsumer callback to a
    gRPC thread trying to assign work to a BotSession.

    """

    def __init__(self, ttl: float=0):
        """Instantiate a new _JobAssignmentRequest.

        Args:
            ttl (float): The time-to-live of this assignment request. The
                request will expire ``ttl`` seconds after instantiation.
                If this TTL is set to 0 then the request will never expire.
        """
        self._event = Event()
        self._expired = False
        self._init_time = datetime.utcnow()
        self._job: Optional[Job] = None
        self._lock = Lock()
        ttl = min(TIMEOUT_MAX, ttl)
        self._ttl = timedelta(seconds=ttl)

    def _is_expired(self) -> bool:
        """Return whether or not this assignment request has expired.

        An assignment request has expired if a number of seconds equivalent to
        the request's TTL have passed since it was instantiated.

        If the request's TTL is set to ``0``, then the request never expires.

        Returns:
            ``True`` if the assignment request has expired,
            ``False`` otherwise.

        """
        ttl_expired = False
        if self._ttl.seconds > 0:
            ttl_expired = datetime.utcnow() > self._init_time + self._ttl
        return ttl_expired or self._expired

    def expire(self) -> None:
        """Explicitly expire this assignment request, regardless of TTL.

        This function marks the assignment request as expired, preventing
        assignment of work even within the TTL defined at construction time.

        """
        self._expired = True

    def assign_job(self, job: Job) -> bool:
        """Attempt to notify a worker that it has been assigned the given Job.

        This method attempts to set ``self._job`` to the given Job and then
        set the internal event to wake up any threads that have called
        ``self.wait_for_assignment`` and provide the assigned Job to them.

        If this method has already been called (ie. a Job has already been
        assigned to the worker) or the assignment request has expired then
        nothing happens.

        This method returns True if it was able to assign the Job, and False
        otherwise.

        Args:
            job (Job): The Job message to assign to the worker.

        Returns:
            ``True`` if ``job`` was successfully assigned,
            ``False`` otherwise.

        """
        with self._lock:
            if self._job is None and not self._is_expired():
                self._job = job
                self._event.set()
                return True
            return False

    def wait_for_assignment(self, timeout: Optional[float]=None) -> Optional[Job]:
        """Wait for a Job to be assigned, and return the assigned Job.

        Args:
            timeout (float): The number of seconds to wait for a Job
                assignment to occur. Capped to ``threading.TIMEOUT_MAX``.

        Returns:
            The ``Job`` message if one was assigned before the timeout.
            ``None`` otherwise.

        """
        if timeout is not None:
            timeout = min(TIMEOUT_MAX, timeout)
        self._event.wait(timeout=timeout)
        return self._job


class BotsInstance:

    """An instance of the Bots service.

    This class handles all the BotSession management specified in the Remote
    Workers API, getting work from the queues and handing it to capable workers,
    and relaying updates on the state of Jobs from workers back to the other
    BuildGrid services.

    """

    def __init__(
        self,
        rabbitmq: "RabbitMqConnection",
        platform_queues: Dict[str, Set[str]],
        logstream_url: Optional[str]=None,
        logstream_credentials: Optional[grpc.ChannelCredentials]=None,
        logstream_instance_name: Optional[str]=None,
        max_publish_attempts: int=0,
        max_connection_attempts: int=0,
        max_cancellation_cache_capacity: int=100000
    ):
        """Instantiate a new BotsInstance.

        Args:
            rabbitmq (RabbitMQConnection): The RabbitMQ connection information
                that this BotsInstance should use.
            platform_queues (dict): Mapping of platform properties
                (represented by a semicolon-separated string) to a
                list of RabbitMQ queues that can contain work meant
                to be executed on such a platform.
            logstream_channel (grpc.Channel): The gRPC channel to use to create
                a new LogStream for streaming stdout and stderr for a Job.
            logstream_instance_name (str): The instance name of the remote
                LogStream service.
            max_publish_attempts (int): The maximum number of times to attempt
                to publish a RabbitMQ message before giving up.
            max_connection_attempts (int): The maximum number of times to attempt
                to reestablish a Consumer connection
            max_cancellation_cache_capacity (int): The maximum number of jobID cache entries to store

        """
        self._active_queues: Set[str] = set()
        self._active_queues_lock = Lock()
        self._logger = logging.getLogger(__name__)
        self._logstream_url = logstream_url
        self._logstream_credentials = logstream_credentials
        self._logstream_instance_name = logstream_instance_name
        self._logstream_channel = None
        self._rabbitmq_connection_info = rabbitmq
        self._platform_queues = platform_queues
        self._cancellation_cache = LruInMemoryCache(max_cancellation_cache_capacity)
        self._stopped = False
        self._worker_map: Dict[str, "Queue[_JobAssignmentRequest]"] = {}
        self._bot_name_to_assignment_request: Dict[str, _JobAssignmentRequest] = {}

        self._instance_name = None

        params = pika.ConnectionParameters(
            self._rabbitmq_connection_info.address,
            self._rabbitmq_connection_info.port,
            self._rabbitmq_connection_info.virtual_host
        )
        if self._rabbitmq_connection_info.credentials:
            params.credentials = self._rabbitmq_connection_info.credentials
        self._publisher = RetryingPikaPublisher(
            params,
            thread_name="BotsInstancePublisher",
            max_publish_attempts=max_publish_attempts,
            exchanges={
                Exchanges.BOT_STATUS.value.name: Exchanges.BOT_STATUS.value.type
            }
        )

        # Set up a rabbitmq consumer for cancellation messages
        self._cancellation_queue_name = f"cancellation-queue-{uuid.uuid4()}"
        exchange_dict = {
            Exchanges.JOB_CANCELLATION.value.name: Exchanges.JOB_CANCELLATION.value.type,
            Exchanges.JOBS.value.name: Exchanges.JOBS.value.type
        }
        self._bindings = set([
            QueueBinding(
                queue=self._cancellation_queue_name,
                exchange=Exchanges.JOB_CANCELLATION.value.name,
                routing_key='',
                auto_delete_queue=True
            )
        ])
        for platform, queues in self._platform_queues.items():
            for queue in queues:
                self._bindings.add(QueueBinding(
                    queue=queue,
                    exchange=Exchanges.JOBS.value.name,
                    routing_key=platform,
                    auto_delete_queue=False
                ))
                if queue not in self._worker_map:
                    self._worker_map[queue] = Queue()

        self._consumer = RetryingPikaConsumer(params,
                                              exchanges=exchange_dict,
                                              bindings=self._bindings,
                                              max_connection_attempts=max_connection_attempts,
                                              retry_delay_base=1)

        self._consumer.subscribe(self._cancellation_queue_name, self._cancel_msg_callback)
        self._logger.info(f"Subscribed to cancellation queue [{self._cancellation_queue_name}]")

    def register_instance_with_server(
        self,
        instance_name: str,
        server: 'RMQServer'
    ) -> None:
        """Names and registers the bots interface with a given server.

        Args:
            instance_name (str): The instance name to set for this instance when
                registering it with the server.
            server (RMQServer): The server to register this instance with.

        """
        if self._instance_name is None:
            server.add_bots_instance(self, instance_name)
            self._instance_name = instance_name
        else:
            raise AssertionError("Instance already registered")

    def setup_grpc(self):
        if self._logstream_channel is None and self._logstream_url is not None:
            self._logstream_channel, _ = setup_channel(
                self._logstream_url,
                auth_token=None,
                client_key=self._logstream_credentials.get("tls-client-key"),
                client_cert=self._logstream_credentials.get("tls-client-cert"),
                server_cert=self._logstream_credentials.get("tls-server-cert")
            )

    def _publish_bot_status(self, bot_session: BotSession) -> None:
        """Send a BotStatus message to the RabbitMQ publisher thread.

        This method generates a BotStatus message to publish on the BotStatus
        exchange. This message informs the services that consume it that the
        given ``BotSession`` has been used in a Create/UpdateBotSession request
        and is therefore still active.

        This message gets put on the BotsInstance's internal queue that the
        publisher thread uses to get messages to publish to RabbitMQ.

        Args:
            bot_session (BotSession): The BotSession to send a status update for.

        """
        assignments = []
        for lease in bot_session.leases:
            action = Action()
            lease.payload.Unpack(action)

            # TODO: Set routing_key here, this only needs to be set on the
            # first update for a Job, so can maybe be special-cased at job assignment
            # time rather than needing to duplicate any of the Execution service
            # config for wildcard properties
            assignments.append(RetryableJob(job_id=lease.id, action=action))  # type: ignore

        now = Timestamp()
        now.GetCurrentTime()
        bot_status = BotStatus(
            bot_name=bot_session.name,
            assignments=assignments,
            connection_timestamp=now
        )
        spec = MessageSpec(
            exchange=Exchanges.BOT_STATUS.value,
            payload=bot_status.SerializeToString()
        )
        self._publisher.send(spec)

    def _publish_operation_status(self, operation: Operation,
                                  operation_name: str,
                                  cacheable: bool,
                                  stage: OperationStage) -> None:
        """Send an UpdateOperations message to the RabbitMQ publisher
        thread.

        This method generates an UpdateOperations message to publish on
        the Operations exchange. This will allow Execution services to
        relay relevant updates to clients, ActionCaches to store the
        results of completed work, and Operations services to persist
        the change of state.

        This message gets put on the BotsInstance's internal queue that the
        publisher thread uses to get messages to publish to RabbitMQ.

        Args:
            operation (google.longrunning.operations_pb2): The
                Operation to send a status update for.
            cacheable (bool): whether the job can be cached.
            stage (OperationStage): operation's new stage.


        """
        update_operations = UpdateOperations(job_id=operation_name,
                                             operation_state=operation,
                                             cacheable=cacheable)  # type: ignore
        any_wrapper = Any()
        any_wrapper.Pack(update_operations)

        if self._instance_name is None:
            raise AssertionError("Instance is unnamed")

        if self._instance_name:
            routing_key = f"{stage.value}.{self._instance_name}"
        else:
            routing_key = f"{stage.value}"

        spec = MessageSpec(
            exchange=Exchanges.OPERATION_UPDATES.value,
            payload=any_wrapper.SerializeToString(),
            routing_key=routing_key
        )
        self._logger.debug(f"Publishing update for operation '{operation_name}' "
                           f"(stage={stage}, cacheable={cacheable}, "
                           f"routing key='{routing_key}'")
        self._publisher.send(spec)

    def start(self):
        """Prepare the BotsInstance for handling incoming requests.

        This starts up the background threads needed for publishing/consuming
        RabbitMQ messages.

        """
        # If for some reason it wasn't already done, set up any gRPC objects.
        # `setup_grpc` is idempotent so its safe to do this more than once.
        self.setup_grpc()
        self._publisher.start()

    def stop(self):
        """"Shutdown the BotsInstance cleanly.

        This sets flags to tell the background threads to stop running, and waits
        for them to finish.

        """
        self._stopped = True

        self._publisher.stop()
        self._consumer.stop()

    def __del__(self):
        if not self._stopped:
            self.stop()

    def _make_assignment_callback_for_queue(
        self,
        queue_name: str
    ) -> Callable:
        """Return a callback to handle an incoming Job message.

        This callback is intended to handle Job messages consumed from the
        platform queues. It gets the list of workers available for the
        given queue, and attempts to assign the Job message being handled
        to one of them.

        If the Job can't be assigned to any of the workers (e.g. there are
        no workers in the list for this queue, or all the workers already
        had work assigned by a different callback), then the Job message
        is NACKed and we unsubscribe from the queue.

        Args:
            queue_name (str): The name of the queue that this callback is
                for. This is needed so that the callback can determine
                which workers can execute the Job.

        Returns:
            A function which handles assigning a Job message to a worker.

        """
        def _assignment_callback(body: bytes, delivery_tag: str) -> None:
            job = Job()
            job.ParseFromString(body)
            assignment_queue = self._worker_map.get(queue_name, Queue())
            assigned = False
            assignment_request = assignment_queue.get_nowait()
            while assignment_request is not None and not assigned:
                assigned = assignment_request.assign_job(job)
                if assigned:
                    self._consumer.ack_message(delivery_tag)
                    return
                assignment_request = assignment_queue.get_nowait()

            # There are no workers left for this platform so we can't handle
            # this message (or subsequent messages) anymore.
            self._consumer.nack_message(delivery_tag)
            with self._active_queues_lock:
                # NOTE: This call can block for a while in situations where
                # RabbitMQ connectivity is bad. However, it needs to be inside
                # this lock to avoid a race condition between unsubscribe/subscribe
                # and/or discard/add calls.
                self._consumer.unsubscribe(queue_name)
                self._active_queues.discard(queue_name)

        return _assignment_callback

    def _get_queues_for_botsession(self, bot_session: BotSession) -> Set[str]:
        """Return the set of queue names that are usable by this BotSession.

        Args:
            bot_session (BotSession): The BotSession whose capabilities should
                be matched to platform queues.

        Returns:
            Set containing the queues which this BotSession is capable of
                consuming work from.

        """
        queues: Set[str] = set()
        capabilities: Dict[str, Set[str]] = {}
        if bot_session.worker.devices:
            primary_device = bot_session.worker.devices[0]
            for prop in primary_device.properties:
                if prop.key not in capabilities:
                    capabilities[prop.key] = set()
                capabilities[prop.key].add(prop.value)

        flattened_capabilities = flatten_capabilities(capabilities)
        for combination in combinations_with_unique_keys(flattened_capabilities, len(capabilities)):
            platforms = [f'{key}={value}' for key, value in sorted(combination, key=lambda i: i[0])]
            platform_string = ';'.join(platforms)
            queues.update(self._platform_queues.get(platform_string, []))

        return queues

    def _ensure_consuming_from_queues(self, queues: Set[str]) -> None:
        """Ensure that the consumer is subscribed to the given queues.

        This method checks that the consumer is subscribed to the given set
        of queues, and creates a callback and subscribes to any queues in
        the set that the consumer isn't subscribed to yet.

        Args:
            queues (set): The queues that the consumer should be subscribed to.

        """
        for queue in queues:
            with self._active_queues_lock:
                if queue not in self._active_queues:
                    callback = self._make_assignment_callback_for_queue(queue)
                    self._active_queues.add(queue)
                    # NOTE: This call can block for a while in situations where
                    # RabbitMQ connectivity is bad. However, it needs to be inside
                    # this lock to avoid a race condition between
                    # unsubscribe/subscribe and/or discard/add calls.
                    self._consumer.subscribe(queue, callback)

    def _assign_work_to_bot_session(
        self,
        bot_session: BotSession,
        timeout: Optional[float]=None
    ) -> None:
        """Attempt to add a new Lease to the given BotSession.

        This method creates a request to assign work to the provided
        BotSession, and puts that request into the internal data structure
        which maps platform/Job queues to work requests.

        This method also ensures that the BotsInstance is consuming from all
        of the queues that can be serviced by the given BotSession, before
        waiting for up to ``timeout`` seconds for work to be assigned to the
        request.

        If the request for work gets given a Job, then a Lease is created
        for that Job and appended to the list of Leases in the BotSession.

        Args:
            bot_session (BotSession): The BotSession to attempt to assign
                work to. This BotSession's ``leases`` attribute will be
                modified in-place if work is assigned.
            timeout (float): Maximum number of seconds to wait for a Job
                to be assigned to the BotSession.

        """
        # Parse the worker config in the BotSession to determine the set of
        # queues that we can consume Jobs from for this worker.
        queues = self._get_queues_for_botsession(bot_session)

        # Add the assignment Event wrapper to _worker_map for all the queues
        # served by this BotSession.
        assignment_ttl = 0.0
        if timeout is not None:
            assignment_ttl = timeout
        assignment = _JobAssignmentRequest(ttl=assignment_ttl)
        self._bot_name_to_assignment_request[bot_session.name] = assignment
        for queue in queues:
            self._worker_map[queue].put(assignment)

        # Register callbacks for any queues we aren't already consuming
        # from. This needs to be done **after** we've put the
        # _JobAssignmentRequest in the _worker_map for the queue, so that
        # the on_message callback can actually assign work.
        self._ensure_consuming_from_queues(queues)

        # Get the Job that has been assigned, waiting for up to ``timeout``
        # seconds for the assignment to happen. If we get a Job, create a
        # Lease to represent it in a way the worker can understand, and add
        # it to the BotSession.
        job = assignment.wait_for_assignment(timeout)
        if job is not None:
            lease = Lease(
                id=job.job_id,
                state=LeaseState.PENDING.value
            )
            lease.payload.Pack(job.action)
            bot_session.leases.append(lease)

    def expire_assignment_for_bot_name(self, bot_name: str):
        """Expire any pending Job request for the given bot name.

        This should be called when a worker disconnects for whatever reason,
        to ensure that we don't accidentally assign a Job to a worker who
        isn't around to accept the work.

        Args:
            bot_name (str): The server-assigned BotSession name that
                needs its assignment request expiring.

        """
        assignment = self._bot_name_to_assignment_request.get(bot_name)
        if assignment is not None:
            assignment.expire()

    def create_bot_session(
        self,
        parent: str,
        bot_session: BotSession,
        time_remaining: float
    ) -> BotSession:
        """Start a new BotSession with this instance.

        This method takes a ``BotSession`` and sets the initial server-assigned
        fields, notably the server-assigned BotSession name. It also attempts to
        assign a Job to the worker in the process.

        Args:
            parent (str): The parent part of the server-assigned name. This
                should be the instance name of this ``BotsInstance``.
            bot_session (BotSession): The initial state of the ``BotSession``
                to be created.

        """
        name = f"{parent}/{str(uuid.uuid4())}"
        bot_session.name = name

        # TODO:
        #   1. If work was assigned, publish an UpdateOperations message

        assignment_timeout = time_remaining - NETWORK_TIMEOUT
        self._assign_work_to_bot_session(bot_session, timeout=assignment_timeout)
        self._publish_bot_status(bot_session)

        return bot_session

    def update_bot_session(
        self,
        name: str,
        bot_session: BotSession,
        time_remaining: float
    ) -> Tuple[BotSession, GrpcTrailingMetadata]:
        """Update an existing BotSession.

        This method serves a few purposes. It both handles updates that the
        worker has made to the BotSession, as well as updating the relevant
        server-assigned fields to inform the worker of changes. These changes
        are things like the Job being cancelled, or new work being assigned
        to a worker which has spare capacity.

        This method is also used to decide worker health, with a message
        being published whenever this method is called to announce that a
        specific BotSession has been seen.

        Args:
            name (str): The name of the BotSession being updated.
            bot_session (BotSession): The BotSession which needs to be
                updated and have any worker-assigned updates handled.
            time_remaining (float): How long is left to handle this
                BotSession before the gRPC connection times out. This
                is used to decide how long to wait when looking for new
                work to assign.

        """

        # TODO:
        #   1. Publish UpdateOperations message with new job state (if any)
        #   2. Attempt to assign new work if the session has no jobs left
        #   2(b). Publish UpdateOperations message if work was assigned

        # Lookup the lease.id in our jobID cache and if found, set the state
        # of the lease to CANCELLED
        for lease in bot_session.leases:
            if self._cancellation_cache.get(lease.id) is not None:
                self._logger.debug(f"Found job={lease.id} in cancellation cache, setting state to CANCELLED")
                lease.state = LeaseState.CANCELLED.value

        # If there are no leases assigned to the worker, try to give it some
        # more work.
        if len(bot_session.leases) == 0:
            assignment_timeout = time_remaining - NETWORK_TIMEOUT
            self._assign_work_to_bot_session(bot_session, timeout=assignment_timeout)

        self._publish_bot_status(bot_session)

        for lease in bot_session.leases:
            self._send_operation_update(lease)

        return bot_session, ()

    def _cancel_msg_callback(self, body: bytes, delivery_tag: str) -> None:
        self._consumer.ack_message(delivery_tag)
        cancelled_job_id = body.decode()

        # Create a cache of all lease ids so we can keep track of which jobs need to be cancelled
        if self._cancellation_cache.get(cancelled_job_id) is None:
            self._cancellation_cache.update(cancelled_job_id, cancelled_job_id)

    def _send_operation_update(self, lease: Lease):
        """Issue an Operation update to the
        ``buildgrid.server.rabbitmq._enums.Exchanges.OPERATION_UPDATES`` exchange.
        """
        stage = _state_to_stage[LeaseState(lease.state)]
        operation, operation_name, cacheable = self._construct_operation(lease)
        # TODO: Check state transition to determine whether to send the update
        self._publish_operation_status(operation, operation_name, cacheable, stage)

    def _construct_operation(self, lease: Lease) -> Tuple[Operation, str, bool]:
        """Get the ``Operation`` for the ``Lease``.
        Returns a tuple with the Operation message, the operation name,
        and whether the action is cacheable.
        """
        # Unpack the Action to calculate the digest (the operation name)
        action = Action()
        lease.payload.Unpack(action)
        action_digest = create_digest(action.SerializeToString())
        action_is_cacheable = not action.do_not_cache

        operation_name = action_digest.hash
        operation = Operation(name=operation_name)

        lease_state = LeaseState(lease.state)
        operation.done = lease_state in (LeaseState.COMPLETED, LeaseState.CANCELLED)

        execute_response = self._construct_execute_response(lease)
        operation.response.Pack(execute_response)

        metadata = self._construct_operation_metadata(lease, action_digest)
        operation.metadata.Pack(metadata)

        return operation, operation_name, action_is_cacheable

    def _construct_execute_response(self, lease: Lease) -> ExecuteResponse:
        """Get the ``ExecuteResponse`` message for the ``Lease``."""
        response = ExecuteResponse()
        response.status.CopyFrom(lease.status)
        if lease.status == LeaseState.CANCELLED.value:
            response.status.message = "Operation cancelled by client."

        result = ActionResult()
        if lease.result is not None and lease.result.Is(result.DESCRIPTOR):
            lease.result.Unpack(result)
        response.result.CopyFrom(result)

        # TODO: timestamps

        return response

    def _construct_operation_metadata(
            self, lease: Lease, action_digest: Digest) -> ExecuteOperationMetadata:
        """Given a ``Lease`` and the digest of its ``Action``, return
        the associated ``ExecuteOperationMetadata`` message."""
        metadata = ExecuteOperationMetadata()
        metadata.stage = _state_to_stage[LeaseState(lease.state)].value
        metadata.action_digest.CopyFrom(action_digest)
        return metadata
