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


import asyncio
from concurrent import futures
from datetime import datetime, timedelta
from itertools import chain
import logging
import logging.handlers
import os
import signal
import sys
import time
import traceback
from typing import Callable, Dict, Iterable, Optional

import grpc
from grpc_reflection.v1alpha import reflection
import janus

from buildgrid._enums import (
    BotStatus, LeaseState, LogRecordLevel, MetricCategories, OperationStage)
from buildgrid._exceptions import PermissionDeniedError
from buildgrid._protos.buildgrid.v2 import monitoring_pb2
from buildgrid.server.actioncache.service import ActionCacheService
from buildgrid.server._authentication import AuthMetadataMethod, AuthMetadataAlgorithm
from buildgrid.server._authentication import AuthContext, AuthMetadataServerInterceptor
from buildgrid.server.bots.service import BotsService
from buildgrid.server.build_events.service import PublishBuildEventService, QueryBuildEventsService
from buildgrid.server.capabilities.instance import CapabilitiesInstance
from buildgrid.server.capabilities.service import CapabilitiesService
from buildgrid.server.cas.service import ByteStreamService, ContentAddressableStorageService
from buildgrid.server.execution.service import ExecutionService
from buildgrid.server.cas.logstream.service import LogStreamService
from buildgrid.server.metrics_utils import create_gauge_record, create_timer_record
from buildgrid.server.metrics_names import (
    AVERAGE_QUEUE_TIME_METRIC_NAME,
    CLIENT_COUNT_METRIC_NAME,
    BOT_COUNT_METRIC_NAME,
    LEASE_COUNT_METRIC_NAME,
    JOB_COUNT_METRIC_NAME
)
from buildgrid.server.monitoring import (
    get_monitoring_bus,
    MonitoringOutputType,
    MonitoringOutputFormat,
    setup_monitoring_bus,
    StatsDTagFormat
)
from buildgrid.server.operations.service import OperationsService
from buildgrid.server.referencestorage.service import ReferenceStorageService
from buildgrid.server._resources import ExecContext
from buildgrid.settings import (
    LOG_RECORD_FORMAT, MIN_THREAD_POOL_SIZE, MONITORING_PERIOD, DEFAULT_JWKS_REFETCH_INTERVAL_MINUTES
)
from buildgrid.utils import read_file

# Need protos here, to enable server reflection.
from buildgrid._protos.google.longrunning import operations_pb2
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2


def load_tls_server_credentials(server_key=None, server_cert=None, client_certs=None):
    """Looks-up and loads TLS server gRPC credentials.

    Every private and public keys are expected to be PEM-encoded.

    Args:
        server_key(str): private server key file path.
        server_cert(str): public server certificate file path.
        client_certs(str): public client certificates file path.

    Returns:
        :obj:`ServerCredentials`: The credentials for use for a
        TLS-encrypted gRPC server channel.
    """
    if not server_key or not os.path.exists(server_key):
        return None

    if not server_cert or not os.path.exists(server_cert):
        return None

    server_key_pem = read_file(server_key)
    server_cert_pem = read_file(server_cert)
    if client_certs and os.path.exists(client_certs):
        client_certs_pem = read_file(client_certs)
    else:
        client_certs_pem = None
        client_certs = None

    credentials = grpc.ssl_server_credentials([(server_key_pem, server_cert_pem)],
                                              root_certificates=client_certs_pem,
                                              require_client_auth=bool(client_certs))

    credentials.server_key = server_key
    credentials.server_cert = server_cert
    credentials.client_certs = client_certs

    return credentials


class Server:
    """Creates a BuildGrid server instance.

    The :class:`Server` class binds together all the gRPC services.
    """

    def __init__(self,
                 max_workers=None, monitor=False,
                 mon_endpoint_type=MonitoringOutputType.STDOUT,
                 mon_endpoint_location=None,
                 mon_serialisation_format=MonitoringOutputFormat.JSON,
                 mon_metric_prefix="",
                 mon_tag_format=StatsDTagFormat.NONE,
                 auth_method=AuthMetadataMethod.NONE,
                 auth_secret=None,
                 auth_jwks_url=None,
                 auth_audience=None,
                 auth_jwks_fetch_minutes=None,
                 auth_algorithm=AuthMetadataAlgorithm.UNSPECIFIED,
                 enable_server_reflection=True):
        """Initializes a new :class:`Server` instance.

        Args:
            max_workers (int, optional): A pool of max worker threads.
            monitor (bool, optional): Whether or not to globally activate server
                monitoring. Defaults to ``False``.
            auth_method (AuthMetadataMethod, optional): Authentication method to
                be used for request authorization. Defaults to ``NONE``.
            auth_secret (str, optional): The secret or key to be used for
                authorizing request using `auth_method`. Defaults to ``None``.
            auth_jwks_url (str, optional): The url to fetch the JWKs.
                Either secret or this field must be specified. Defaults to ``None``.
            auth_audience (str): The audience used to validate jwt tokens against.
                The tokens must have an audience field.
            auth_jwks_fetch_minutes (int): The number of minutes to wait before
                refetching the jwks set. Default: 60 minutes.
            auth_algorithm (AuthMetadataAlgorithm, optional): The crytographic
                algorithm to be uses in combination with `auth_secret` for
                authorizing request using `auth_method`. Defaults to
                ``UNSPECIFIED``.
        """
        self.__logger = logging.getLogger(__name__)

        self.__main_loop = asyncio.get_event_loop()

        self.__logging_queue = None  # type: Optional[janus.Queue]
        self.__logging_handler = None  # type: Optional[logging.handlers.QueueHandler]
        self.__logging_formatter = logging.Formatter(fmt=LOG_RECORD_FORMAT)
        self.__print_log_records = True

        self.__build_metadata_queues = None

        self.__state_monitoring_task = None
        self.__build_monitoring_tasks = None
        self.__logging_task = None

        self._capabilities_service = None
        self._execution_service = None
        self._bots_service = None
        self._operations_service = None
        self._reference_storage_service = None
        self._action_cache_service = None
        self._cas_service = None
        self._bytestream_service = None
        self._logstream_service = None
        self._build_events_service = None
        self._build_events_storage_backend = None
        self._query_build_events_service = None

        self._schedulers = {}

        self._instances = set()
        self._execution_instances = {}
        self._operations_instances = {}
        self._bots_instances = {}
        self._cas_instances = {}
        self._bytestream_instances = {}
        self._logstream_instances = {}
        self._action_cache_instances = {}
        self._reference_storage_instances = {}

        self._ports = []
        self._port_map = {}

        self._server_reflection = enable_server_reflection
        self._reflection_services = [reflection.SERVICE_NAME]

        self._is_instrumented = monitor

        if self._is_instrumented:
            monitoring_bus = setup_monitoring_bus(endpoint_type=mon_endpoint_type,
                                                  endpoint_location=mon_endpoint_location,
                                                  metric_prefix=mon_metric_prefix,
                                                  serialisation_format=mon_serialisation_format,
                                                  tag_format=mon_tag_format)

            self.__build_monitoring_tasks = []

        if self._is_instrumented and monitoring_bus.prints_records:
            self.__print_log_records = False

        if max_workers is None:
            # Use max_workers default from Python 3.4+
            max_workers = max(MIN_THREAD_POOL_SIZE, (os.cpu_count() or 1) * 5)

        elif max_workers < MIN_THREAD_POOL_SIZE:
            self.__logger.warning(f"Specified thread-limit=[{max_workers}] is too small, "
                                  f"bumping it to [{MIN_THREAD_POOL_SIZE}]")
            # Enforce a minumun for max_workers
            max_workers = MIN_THREAD_POOL_SIZE

        self._max_grpc_workers = max_workers

        ExecContext.init(max_workers)

        self.__grpc_auth_interceptor = None

        if auth_method != AuthMetadataMethod.NONE:
            if auth_jwks_fetch_minutes is None:
                auth_jwks_fetch_minutes = DEFAULT_JWKS_REFETCH_INTERVAL_MINUTES
            self.__grpc_auth_interceptor = AuthMetadataServerInterceptor(
                method=auth_method, secret=auth_secret, algorithm=auth_algorithm,
                jwks_url=auth_jwks_url, audience=auth_audience, jwks_fetch_minutes=auth_jwks_fetch_minutes)

            AuthContext.interceptor = self.__grpc_auth_interceptor

        try:
            # pylint: disable=consider-using-with
            self.__grpc_executor = futures.ThreadPoolExecutor(
                max_workers, thread_name_prefix="gRPC_Executor")
        except TypeError:
            # We need python >= 3.6 to support `thread_name_prefix`, so fallback
            # to ugly thread names if that didn't work

            # pylint: disable=consider-using-with
            self.__grpc_executor = futures.ThreadPoolExecutor(max_workers)
        self.__grpc_server = None

        self.__logger.debug(f"Setting up gRPC server with thread-limit=[{max_workers}]")

    # --- Public API ---

    def _start_logging(self) -> None:
        """Start the logging coroutine."""
        self.__logging_task = asyncio.ensure_future(
            self._logging_worker(), loop=self.__main_loop)

    def _start_monitoring(self) -> None:
        """Start the monitoring functionality.

        This starts up the monitoring bus subprocess, and also starts the
        periodic status monitoring coroutine.

        """
        monitoring_bus = get_monitoring_bus()
        monitoring_bus.start()

        self.__state_monitoring_task = asyncio.ensure_future(
            self._state_monitoring_worker(period=MONITORING_PERIOD),
            loop=self.__main_loop
        )

    def _instantiate_grpc(self) -> None:
        """Instantiate the gRPC objects.

        This creates the gRPC server, and causes the instances attached to
        this server to instantiate any gRPC channels they need. This also
        sets up the services which route to those instances, and sets up
        gRPC reflection.

        """
        self.__grpc_server = grpc.server(
            self.__grpc_executor,
            maximum_concurrent_rpcs=self._max_grpc_workers
        )

        # We always want a capabilities service
        self._capabilities_service = CapabilitiesService(self.__grpc_server)

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

        for instance_name, instance in self._reference_storage_instances.items():
            instance.setup_grpc()
            self._add_reference_storage_instance(instance, instance_name)

        # The Build Events Services have no support for instance names, so this
        # is a bit of a special case where the storage backend itself is the
        # trigger for creating the gRPC services.
        if self._build_events_storage_backend is not None:
            self._add_build_events_services()

        # Add the requested ports to the gRPC server
        for address, credentials in self._ports:
            if credentials is not None:
                self.__logger.info(f"Adding secure connection on: [{address}]")
                server_key = credentials.get('tls-server-key')
                server_cert = credentials.get('tls-server-cert')
                client_certs = credentials.get('tls-client-certs')
                credentials = load_tls_server_credentials(
                    server_cert=server_cert,
                    server_key=server_key,
                    client_certs=client_certs
                )
                port_number = self.__grpc_server.add_secure_port(address, credentials)

            else:
                self.__logger.info(f"Adding insecure connection on [{address}]")
                port_number = self.__grpc_server.add_insecure_port(address)
            self._port_map[address] = port_number

            if not port_number:
                raise PermissionDeniedError("Unable to configure socket")

        self.__enable_server_reflection()

    @property
    def _all_instances(self) -> Iterable:
        return chain(
            self._execution_instances.values(),
            self._operations_instances.values(),
            self._bots_instances.values(),
            self._cas_instances.values(),
            self._bytestream_instances.values(),
            self._logstream_instances.values(),
            self._action_cache_instances.values(),
            self._reference_storage_instances.values()
        )

    def _start_instances(self) -> None:
        for instance in self._all_instances:
            instance.start()

    def _stop_instances(self) -> None:
        for instance in self._all_instances:
            instance.stop()

    def start(
        self,
        *,
        on_server_start_cb: Optional[Callable]=None,
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

        This step instantiates the gRPC server, and tells all the instances
        which have been attached to the server to instantiate their gRPC
        objects. It is also responsible for creating the various service
        objects and connecting them to the server and the instances.

        After this step, gRPC core is running and its no longer safe to fork
        the process.

        3. Start instances

        Several of BuildGrid's services use background threads that need to
        be explicitly started when BuildGrid starts up. Rather than doing
        this at configuration parsing time, this step provides a hook for
        services to start up in a more organised fashion.

        4. Start the gRPC server

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
        if self._is_instrumented:
            self._start_monitoring()

        # 2. Instantiate gRPC objects
        self._instantiate_grpc()

        # 3. Start background threads
        self._start_instances()

        # 4. Start the gRPC server
        self.__grpc_server.start()
        if on_server_start_cb:
            on_server_start_cb()
        if port_assigned_callback:
            port_assigned_callback(port_map=self._port_map)

        # Add the stop handler and run the event loop
        self.__main_loop.add_signal_handler(signal.SIGTERM, self.stop)
        self.__main_loop.run_forever()

    def stop(self):
        """Stops the BuildGrid server."""
        if self._is_instrumented:
            if self.__state_monitoring_task is not None:
                self.__state_monitoring_task.cancel()

            monitoring_bus = get_monitoring_bus()
            monitoring_bus.stop()

        if self.__logging_task is not None:
            self.__logging_task.cancel()

        self.__main_loop.stop()

        if self.__grpc_server is not None:
            self.__grpc_server.stop(None)

        self._stop_instances()

    def add_port(self, address, credentials):
        """Adds a port to the server.

        Must be called before the server starts. If a credentials object exists,
        it will make a secure port.

        Args:
            address (str): The address with port number.
            credentials (:obj:`grpc.ChannelCredentials`): Credentials object.

        Returns:
            int: Number of the bound port.

        Raises:
            PermissionDeniedError: If socket binding fails.

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
        self._instances.add(instance_name)

    def _add_execution_instance(self, instance, instance_name):
        if self._execution_service is None:
            self._execution_service = ExecutionService(
                self.__grpc_server, monitor=self._is_instrumented)

        self._execution_service.add_instance(instance_name, instance)
        self._add_capabilities_instance(instance_name, execution_instance=instance)

        self._schedulers.setdefault(instance_name, set()).add(instance.scheduler)

        if self._is_instrumented:
            instance.scheduler.activate_monitoring()
        self._reflection_services.append(remote_execution_pb2.DESCRIPTOR.services_by_name['Execution'].full_name)

    def add_bots_interface(self, instance, instance_name):
        """Adds a :obj:`BotsInterface` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`BotsInterface`): Instance to add.
            instance_name (str): Instance name.
        """
        self._bots_instances[instance_name] = instance
        self._instances.add(instance_name)

    def _add_bots_instance(self, instance, instance_name):
        if self._bots_service is None:
            self._bots_service = BotsService(
                self.__grpc_server, monitor=self._is_instrumented)

        self._bots_service.add_instance(instance_name, instance)

        self._schedulers.setdefault(instance_name, set()).add(instance.scheduler)

        if self._is_instrumented:
            instance.scheduler.activate_monitoring()

    def add_operations_instance(self, instance, instance_name):
        """Adds an :obj:`OperationsInstance` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`OperationsInstance`): Instance to add.
            instance_name (str): Instance name.
        """
        self._operations_instances[instance_name] = instance

    def _add_operations_instance(self, instance, instance_name):
        if self._operations_service is None:
            self._operations_service = OperationsService(self.__grpc_server)

        self._operations_service.add_instance(instance_name, instance)
        self._reflection_services.append(operations_pb2.DESCRIPTOR.services_by_name['Operations'].full_name)

    def add_reference_storage_instance(self, instance, instance_name):
        """Adds a :obj:`ReferenceCache` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`ReferenceCache`): Instance to add.
            instance_name (str): Instance name.
        """
        self._reference_storage_instances[instance_name] = instance

    def _add_reference_storage_instance(self, instance, instance_name):
        if self._reference_storage_service is None:
            self._reference_storage_service = ReferenceStorageService(self.__grpc_server)

        self._reference_storage_service.add_instance(instance_name, instance)

    def add_action_cache_instance(self, instance, instance_name):
        """Adds a :obj:`ReferenceCache` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`ReferenceCache`): Instance to add.
            instance_name (str): Instance name.
        """
        self._action_cache_instances[instance_name] = instance

    def _add_action_cache_instance(self, instance, instance_name):
        if self._action_cache_service is None:
            self._action_cache_service = ActionCacheService(self.__grpc_server)

        self._action_cache_service.add_instance(instance_name, instance)
        self._add_capabilities_instance(instance_name, action_cache_instance=instance)
        self._reflection_services.append(remote_execution_pb2.DESCRIPTOR.services_by_name['ActionCache'].full_name)

    def add_cas_instance(self, instance, instance_name):
        """Adds a :obj:`ContentAddressableStorageInstance` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`ReferenceCache`): Instance to add.
            instance_name (str): Instance name.
        """
        self._cas_instances[instance_name] = instance

    def _add_cas_instance(self, instance, instance_name):
        if self._cas_service is None:
            self._cas_service = ContentAddressableStorageService(self.__grpc_server)

        self._cas_service.add_instance(instance_name, instance)
        self._add_capabilities_instance(instance_name, cas_instance=instance)
        self._reflection_services.append(
            remote_execution_pb2.DESCRIPTOR.services_by_name['ContentAddressableStorage'].full_name)

    def add_bytestream_instance(self, instance, instance_name):
        """Adds a :obj:`ByteStreamInstance` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`ByteStreamInstance`): Instance to add.
            instance_name (str): Instance name.
        """
        self._bytestream_instances[instance_name] = instance

    def _add_bytestream_instance(self, instance, instance_name):
        if self._bytestream_service is None:
            self._bytestream_service = ByteStreamService(self.__grpc_server)

        self._bytestream_service.add_instance(instance_name, instance)

    def add_logstream_instance(self, instance, instance_name):
        """Adds a :obj:`LogStreamInstance` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`LogStreamInstance`): Instance to add.
            instance_name (str): The name of the instance being added.

        """
        self._logstream_instances[instance_name] = instance

    def _add_logstream_instance(self, instance, instance_name):
        if self._logstream_service is None:
            self._logstream_service = LogStreamService(self.__grpc_server)

        self._logstream_service.add_instance(instance_name, instance)

    def add_build_events_storage(self, storage_backend):
        """Adds a :obj:`BuildEventStreamStorage` to the server.

        This is used to decide whether to create the Build Events services in
        the server. No instance name is passed in since the Build Events
        protocol has no support for instance names.

        """
        self._build_events_storage_backend = storage_backend

    def _add_build_events_services(self):
        self._build_events_service = PublishBuildEventService(
            self.__grpc_server, self._build_events_storage_backend)
        self._query_build_events_service = QueryBuildEventsService(
            self.__grpc_server, self._build_events_storage_backend)

    # --- Public API: Monitoring ---

    @property
    def is_instrumented(self):
        return self._is_instrumented

    # --- Private API ---

    def _add_capabilities_instance(self, instance_name,
                                   cas_instance=None,
                                   action_cache_instance=None,
                                   execution_instance=None):
        """Adds a :obj:`CapabilitiesInstance` to the service.

        Args:
            instance (:obj:`CapabilitiesInstance`): Instance to add.
            instance_name (str): Instance name.
        """

        try:
            if cas_instance:
                self._capabilities_service.add_cas_instance(instance_name, cas_instance)
            if action_cache_instance:
                self._capabilities_service.add_action_cache_instance(instance_name, action_cache_instance)
            if execution_instance:
                self._capabilities_service.add_execution_instance(instance_name, execution_instance)

        except KeyError:
            capabilities_instance = CapabilitiesInstance(cas_instance,
                                                         action_cache_instance,
                                                         execution_instance)
            self._capabilities_service.add_instance(instance_name, capabilities_instance)

    async def _logging_worker(self):
        """Publishes log records to the monitoring bus."""
        self.__logging_queue = janus.Queue()
        self.__logging_handler = logging.handlers.QueueHandler(self.__logging_queue.sync_q)

        # Setup the main logging handler:
        root_logger = logging.getLogger()

        for log_filter in root_logger.filters[:]:
            self.__logging_handler.addFilter(log_filter)
            root_logger.removeFilter(log_filter)

        for log_handler in root_logger.handlers[:]:
            root_logger.removeHandler(log_handler)
        root_logger.addHandler(self.__logging_handler)

        async def __logging_worker():
            monitoring_bus = get_monitoring_bus()
            log_record = await self.__logging_queue.async_q.get()

            # Print log records to stdout, if required:
            if self.__print_log_records:
                record = self.__logging_formatter.format(log_record)

                # TODO: Investigate if async write would be worth here.
                sys.stdout.write(f'{record}\n')
                sys.stdout.flush()

            # Emit a log record if server is instrumented:
            if self._is_instrumented:
                log_record_level = LogRecordLevel(int(log_record.levelno / 10))
                log_record_creation_time = datetime.fromtimestamp(log_record.created)
                # logging.LogRecord.extra must be a str to str dict:
                if 'extra' in log_record.__dict__ and log_record.extra:
                    log_record_metadata = log_record.extra
                else:
                    log_record_metadata = None
                record = self._forge_log_record(
                    domain=log_record.name, level=log_record_level, message=log_record.message,
                    creation_time=log_record_creation_time, metadata=log_record_metadata)

                await monitoring_bus.send_record(record)

        while True:
            try:
                await __logging_worker()

            except asyncio.CancelledError:
                break
            except Exception:
                # The thread shouldn't exit on exceptions, but output the exception so that
                # it can be found in the logs.
                #
                # Note, we DO NOT use `self.__logger` here, because we don't want to write
                # anything new to the logging queue in case the Exception isn't some transient
                # issue.
                try:
                    sys.stdout.write("Exception in logging worker\n")
                    sys.stdout.flush()
                    traceback.print_exc()
                except Exception:
                    # There's not a lot we can do at this point really.
                    pass

    def _forge_log_record(self, *,
                          domain: str,
                          level: LogRecordLevel,
                          message: str,
                          creation_time: datetime,
                          metadata: Dict[str, str]=None):
        log_record = monitoring_pb2.LogRecord()

        log_record.creation_timestamp.FromDatetime(creation_time)
        log_record.domain = domain
        log_record.level = level.value
        log_record.message = message
        if metadata is not None:
            log_record.metadata.update(metadata)

        return log_record

    async def _state_monitoring_worker(self, period=1.0):
        """Periodically publishes state metrics to the monitoring bus."""
        async def __state_monitoring_worker():
            monitoring_bus = get_monitoring_bus()
            # Execution metrics
            if self._execution_service:
                # Emit total clients count record:
                _, record = self._query_n_clients()
                await monitoring_bus.send_record(record)

                for instance_name in self._instances:
                    # Emit instance clients count record:
                    _, record = self._query_n_clients_for_instance(instance_name)
                    await monitoring_bus.send_record(record)

            if self._bots_service:
                # Emit total bots count record:
                _, record = self._query_n_bots()
                await monitoring_bus.send_record(record)

                for instance_name in self._instances:
                    # Emit instance bots count record:
                    _, record = self._query_n_bots_for_instance(instance_name)
                    await monitoring_bus.send_record(record)

                    # Emits records by bot status:
                    for bot_status in (botstatus for botstatus in BotStatus):
                        # Emit status bots count record:
                        _, record = self._query_n_bots_for_status(bot_status)
                        await monitoring_bus.send_record(record)

            if self._schedulers:
                queue_times = []
                # Emits records by instance:
                for instance_name in self._instances:
                    # Emit instance average queue time record:
                    queue_time, record = self._query_am_queue_time_for_instance(instance_name)
                    await monitoring_bus.send_record(record)
                    if queue_time:
                        queue_times.append(queue_time)

                    scheduler_metrics = self._query_scheduler_metrics_for_instance(instance_name)
                    # This will be skipped if there were no stage changes
                    if scheduler_metrics:
                        for _, record in self._records_from_scheduler_metrics(scheduler_metrics, instance_name):
                            await monitoring_bus.send_record(record)

                # Emit overall average queue time record:
                if queue_times:
                    am_queue_time = sum(queue_times, timedelta()) / len(queue_times)
                else:
                    am_queue_time = timedelta()
                record = create_timer_record(
                    AVERAGE_QUEUE_TIME_METRIC_NAME,
                    am_queue_time)

                await monitoring_bus.send_record(record)

        while True:
            start = time.time()
            try:
                await __state_monitoring_worker()

            except asyncio.CancelledError:
                break
            except Exception:
                # The thread shouldn't exit on exceptions, but log at a severe enough level
                # that it doesn't get lost in logs
                self.__logger.exception("Exception while gathering state metrics")

            end = time.time()
            await asyncio.sleep(period - (end - start))

    # --- Private API: Monitoring ---

    def _query_n_clients(self):
        """Queries the number of clients connected."""
        n_clients = self._execution_service.query_n_clients()
        gauge_record = create_gauge_record(
            CLIENT_COUNT_METRIC_NAME, n_clients)

        return n_clients, gauge_record

    def _query_n_clients_for_instance(self, instance_name):
        """Queries the number of clients connected for a given instance"""
        n_clients = self._execution_service.query_n_clients_for_instance(instance_name)
        gauge_record = create_gauge_record(
            CLIENT_COUNT_METRIC_NAME, n_clients,
            metadata={'instance-name': instance_name or ''})

        return n_clients, gauge_record

    def _query_n_bots(self):
        """Queries the number of bots connected."""
        n_bots = self._bots_service.query_n_bots()
        gauge_record = create_gauge_record(
            BOT_COUNT_METRIC_NAME, n_bots)

        return n_bots, gauge_record

    def _query_n_bots_for_instance(self, instance_name):
        """Queries the number of bots connected for a given instance."""
        n_bots = self._bots_service.query_n_bots_for_instance(instance_name)
        gauge_record = create_gauge_record(
            BOT_COUNT_METRIC_NAME, n_bots,
            metadata={'instance-name': instance_name or ''})

        return n_bots, gauge_record

    def _query_n_bots_for_status(self, bot_status):
        """Queries the number of bots connected for a given health status."""
        n_bots = self._bots_service.query_n_bots_for_status(bot_status)
        gauge_record = create_gauge_record(BOT_COUNT_METRIC_NAME, n_bots,
                                           metadata={'bot-status': bot_status.name, 'statsd-bucket': bot_status.name})

        return n_bots, gauge_record

    def _query_am_queue_time_for_instance(self, instance_name):
        """Queries the average job's queue time for a given instance."""
        # Multiple schedulers may be active for this instance, but only
        # one (the one associated with the BotsInterface) actually keeps
        # track of this metric. So publish the first one that has a non-default
        # value
        for scheduler in self._schedulers[instance_name]:
            am_queue_time = scheduler.query_am_queue_time()
            if am_queue_time != timedelta():
                break
        timer_record = create_timer_record(
            AVERAGE_QUEUE_TIME_METRIC_NAME, am_queue_time,
            metadata={'instance-name': instance_name or ''})

        return am_queue_time, timer_record

    def _query_scheduler_metrics_for_instance(self, instance_name):
        # Since multiple schedulers may be active for this instance, but should
        # be using the same data-store, just use the first one
        for scheduler in self._schedulers[instance_name]:
            return scheduler.get_metrics()

    def _records_from_scheduler_metrics(self, scheduler_metrics, instance_name):
        # Jobs
        for stage, n_jobs in scheduler_metrics[MetricCategories.JOBS.value].items():

            stage = OperationStage(stage)
            gauge_record = create_gauge_record(
                JOB_COUNT_METRIC_NAME, n_jobs,
                metadata={'instance-name': instance_name or '',
                          'operation-stage': stage.name,
                          'statsd-bucket': stage.name})
            yield n_jobs, gauge_record
        # Leases
        for state, n_leases in scheduler_metrics[MetricCategories.LEASES.value].items():
            state = LeaseState(state)
            gauge_record = create_gauge_record(
                LEASE_COUNT_METRIC_NAME, n_leases,
                metadata={'instance-name': instance_name or '',
                          'lease-state': state.name,
                          'statsd-bucket': state.name})
            yield n_leases, gauge_record

    # --- Private API ---

    def __enable_server_reflection(self) -> None:

        if self._server_reflection:
            services = ', '.join(self._reflection_services[1:])
            self.__logger.info(
                f"Server reflection is enabled for the following services: {services}")
            reflection.enable_server_reflection(self._reflection_services, self.__grpc_server)
        else:
            self.__logger.info("Server reflection is not enabled.")
