# Copyright (C) 2021 Bloomberg LP
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

import asyncio
from concurrent import futures
import logging
import logging.handlers
import os
import signal
import sys
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING, Tuple

import grpc
import janus

from buildgrid._exceptions import PermissionDeniedError
from buildgrid.server.rabbitmq.bots.service import BotsService
from buildgrid.settings import (
    LOG_RECORD_FORMAT,
    MIN_THREAD_POOL_SIZE
)

if TYPE_CHECKING:
    from buildgrid.server.rabbitmq.bots.instance import BotsInstance


class RMQServer:
    """Creates a BuildGrid server instance that is powered by RabbitMQ.

    The :class:`RMQServer` class binds together all the gRPC services.
    """

    def __init__(self, *, max_workers: Optional[int]=None, enable_metrics: bool=False, **kwargs):
        """Initializes a new :class:`RMQServer` instance.
        """
        self._logger = logging.getLogger(__name__)

        self._main_loop = asyncio.get_event_loop()

        self._logging_queue = None  # type: Optional[janus.Queue]
        self._logging_handler = None  # type: Optional[logging.handlers.QueueHandler]
        self._logging_formatter = logging.Formatter(fmt=LOG_RECORD_FORMAT)
        self._logging_task: Optional[asyncio.Future] = None

        self._bots_service = None

        self._action_cache_instances: Dict[str, Any] = {}
        self._bots_instances: Dict[str, "BotsInstance"] = {}
        self._execution_instances: Dict[str, Any] = {}
        self._operations_instances: Dict[str, Any] = {}
        self._cas_instances: Dict[str, Any] = {}
        self._bytestream_instances: Dict[str, Any] = {}
        self._logstream_instances: Dict[str, Any] = {}

        self._ports: List[Tuple[str, grpc.ServerCredentials]] = []
        self._port_map: Dict[Tuple[str, grpc.ServerCredentials], int] = {}

        self._enable_metrics = enable_metrics

        if max_workers is None:
            # Use max_workers default from Python 3.4+
            self._max_workers = max(MIN_THREAD_POOL_SIZE, (os.cpu_count() or 1) * 5)

        elif max_workers < MIN_THREAD_POOL_SIZE:
            self._logger.warning(f"Specified thread-limit=[{max_workers}] is too small, "
                                 f"bumping it to [{MIN_THREAD_POOL_SIZE}]")
            # Enforce a minumun for max_workers
            self._max_workers = MIN_THREAD_POOL_SIZE

        try:
            # pylint: disable=consider-using-with
            self._grpc_executor = futures.ThreadPoolExecutor(
                self._max_workers, thread_name_prefix="gRPC_Executor")
        except TypeError:
            # We need python >= 3.6 to support `thread_name_prefix`, so fallback
            # to ugly thread names if that didn't work

            # pylint: disable=consider-using-with
            self._grpc_executor = futures.ThreadPoolExecutor(self._max_workers)

        self._grpc_server: Optional[grpc.Server] = None

    async def _logging_worker(self) -> None:
        """Worker to read messages off the logging queue and write them to stdout.

        """
        self._logging_queue = janus.Queue()
        self._logging_handler = logging.handlers.QueueHandler(self._logging_queue.sync_q)  # type: ignore

        # Setup the main logging handler:
        root_logger = logging.getLogger()

        for log_filter in root_logger.filters[:]:
            self._logging_handler.addFilter(log_filter)
            root_logger.removeFilter(log_filter)

        for log_handler in root_logger.handlers[:]:
            root_logger.removeHandler(log_handler)
        root_logger.addHandler(self._logging_handler)

        async def _logging_worker() -> None:
            if self._logging_queue is not None:
                log_record = await self._logging_queue.async_q.get()
                record = self._logging_formatter.format(log_record)

            # TODO: Investigate if async write would be worth here.
            sys.stdout.write(f'{record}\n')
            sys.stdout.flush()

            # TODO: publish log records to a monitoring bus

        while True:
            try:
                await _logging_worker()

            except asyncio.CancelledError:
                break
            except Exception:
                # The thread shouldn't exit on exceptions, but log at a severe enough level
                # that it doesn't get lost in logs
                self._logger.exception("Exception in logging worker")

    def _start_logging(self) -> None:
        """Start the logging coroutine."""
        self._logging_task = asyncio.ensure_future(
            self._logging_worker(), loop=self._main_loop)

    def _instantiate_grpc(self) -> None:
        """Instantiate the gRPC objects.

        This creates the gRPC server, and causes the instances attached to
        this server to instantiate any gRPC channels they need. This also
        sets up the services which route to those instances, and sets up
        gRPC reflection.

        """
        # NOTE: maximum_concurrent_rpcs is set equal to max_workers to avoid
        # deadlocking remote execution when multiple services being run in
        # one process. eg. A server running CAS, Execution, and Bots services
        # can get into a state where all the executor threads are handling
        # long-lived execution connections. Allowing more concurrent RPCs
        # will queue CAS or Bots service requests until they timeout in this
        # scenario, rather than failing fast.
        self._grpc_server = grpc.server(
            self._grpc_executor,
            maximum_concurrent_rpcs=self._max_workers
        )

        # TODO: Capabilities service

        for instance_name, instance in self._execution_instances.items():
            instance.setup_grpc()
            self._add_execution_instance(instance, instance_name)

        for instance_name, instance in self._operations_instances.items():
            instance.setup_grpc()
            self._add_operations_instance(instance, instance_name)

        for instance_name, instance in self._bots_instances.items():
            instance.setup_grpc()
            self._add_bots_instance(instance, instance_name)

        for instance_name, instance in self._cas_instances.items():
            instance.setup_grpc()
            self._add_cas_instance(instance, instance_name)

        for instance_name, instance in self._bytestream_instances.items():
            instance.setup_grpc()
            self._add_bytestream_instance(instance, instance_name)

        for instance_name, instance in self._logstream_instances.items():
            instance.setup_grpc()
            self._add_logstream_instance(instance, instance_name)

        for instance_name, instance in self._action_cache_instances.items():
            instance.setup_grpc()
            self._add_action_cache_instance(instance, instance_name)

        # Add the requested ports to the gRPC server
        for address, credentials in self._ports:
            if credentials is not None:
                self._logger.info(f"Adding secure connection on: [{address}]")
                port_number = self._grpc_server.add_secure_port(address, credentials)

            else:
                self._logger.info(f"Adding insecure connection on [{address}]")
                port_number = self._grpc_server.add_insecure_port(address)
            self._port_map[(address, credentials)] = port_number

            if not port_number:
                raise PermissionDeniedError("Unable to configure socket")

        # TODO: Server reflection

    def _start_services(self):
        if self._bots_service is not None:
            self._bots_service.start()

    # --- Public API ---
    def start(
        self,
        *,
        on_server_start_cb: Callable=None,
        port_assigned_callback: Optional[Callable]=None
    ) -> None:
        """Starts the BuildGrid server.

        BuildGrid server startup consists of 3 stages,

        1. Starting logging and monitoring

        This step starts up the logging coroutine, the periodic status metrics
        coroutine, and the monitoring bus' publishing subprocess. Since this
        step involves forking, anything not fork-safe needs to be done *after*
        this step.

        2. Instantiate gRPC

        This step instantiates the gRPC server. It is also responsible for
        creating the various service objects and connecting them to the server
        and the instances. This step starts up any background threads needed
        for the gRPC servicers, and tells the instances attached to those
        servicers to instantiate their gRPC objects (such as channels to
        remote servers).

        After this step, gRPC core is running and its no longer safe to fork
        the process.

        3. Start the gRPC server

        The final step is starting up the gRPC server. The callback passed in
        via ``on_server_start_cb`` is executed in this step once the server
        has started. After this point BuildGrid is ready to serve requests.

        The final thing done by this method is adding a ``SIGTERM`` handler
        which calls the ``Server.stop`` method to the event loop, and then
        that loop is started up using ``run_forever()``.

        Args:
            on_server_start_cb (Callable): Callback function to execute once
                the gRPC server has started up.
            port_assigned_callback (Callable): Callback function to execute
                once the gRPC server has started up. The mapping of addresses
                to ports is passed to this callback.

        """
        # 1. Start logging and monitoring
        self._start_logging()
        # TODO: Start the monitoring bus

        # 2. Instantiate gRPC objects
        self._instantiate_grpc()

        # Call the start method of the gRPC services, so they can start up any
        # background threads they need
        self._start_services()

        # 3. Start the gRPC server
        if self._grpc_server is not None:
            self._grpc_server.start()
            if on_server_start_cb:
                on_server_start_cb()
            if port_assigned_callback:
                port_assigned_callback(port_map=self._port_map)

        # Add the stop handler and start the event loop
        self._main_loop.add_signal_handler(signal.SIGTERM, self.stop)
        self._main_loop.run_forever()

    def stop(self):
        """Stops the BuildGrid server."""
        # TODO: Stop the monitoring bus tasks
        if self._logging_task is not None:
            self._logging_task.cancel()

        # Stop the gRPC server to prevent new incoming requests
        self._grpc_server.stop(None)

        # Stop the gRPC services, to cleanly shut down background threads
        self._bots_service.stop()

        self._main_loop.stop()

    def add_port(self, address, credentials):
        """Adds a port to the server.

        Must be called before the server starts. If a credentials object exists,
        it will make a secure port.

        Args:
            address (str): The address with port number.
            credentials (:obj:`grpc.ChannelCredentials`): Credentials object.

        """
        self._ports.append((address, credentials))

    def add_execution_instance(self, instance, instance_name):
        """Adds an :obj:`ExecutionInstance` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`ExecutionInstance`): Instance to add.
            instance_name (str): Instance name.
        """
        self._execution_instances[instance_name] = instance

    def _add_execution_instance(self, instance, instance_name):
        pass

    def add_bots_instance(self, instance, instance_name):
        """Adds a :obj:`BotsInstance` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`BotsInstance`): Instance to add.
            instance_name (str): Instance name.
        """
        self._bots_instances[instance_name] = instance

    def _add_bots_instance(self, instance, instance_name):
        if self._bots_service is None:
            self._bots_service = BotsService(self._grpc_server, self._enable_metrics)
        self._bots_service.add_instance(instance_name, instance)

    def add_operations_instance(self, instance, instance_name):
        """Adds an :obj:`OperationsInstance` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`OperationsInstance`): Instance to add.
            instance_name (str): Instance name.
        """
        self._operations_instances[instance_name] = instance

    def _add_operations_instance(self, instance, instance_name):
        pass

    def add_action_cache_instance(self, instance, instance_name):
        """Adds a :obj:`ReferenceCache` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`ReferenceCache`): Instance to add.
            instance_name (str): Instance name.
        """
        self._action_cache_instances[instance_name] = instance

    def _add_action_cache_instance(self, instance, instance_name):
        pass

    def add_cas_instance(self, instance, instance_name):
        """Adds a :obj:`ContentAddressableStorageInstance` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`ReferenceCache`): Instance to add.
            instance_name (str): Instance name.
        """
        self._cas_instances[instance_name] = instance

    def _add_cas_instance(self, instance, instance_name):
        pass

    def add_bytestream_instance(self, instance, instance_name):
        """Adds a :obj:`ByteStreamInstance` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`ByteStreamInstance`): Instance to add.
            instance_name (str): Instance name.
        """
        self._bytestream_instances[instance_name] = instance

    def _add_bytestream_instance(self, instance, instance_name):
        pass

    def add_logstream_instance(self, instance, instance_name):
        """Adds a :obj:`LogStreamInstance` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`LogStreamInstance`): Instance to add.
            instance_name (str): The name of the instance being added.

        """
        self._logstream_instances[instance_name] = instance

    def _add_logstream_instance(self, instance, instance_name):
        pass

    # --- Public API: Monitoring ---
    @property
    def is_instrumented(self):
        pass
