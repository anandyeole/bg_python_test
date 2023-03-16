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


"""
OperationsInstance
==================
An instance of the LongRunningOperations Service.
"""

from collections import OrderedDict
import logging
from threading import Lock
from typing import Optional

from google.protobuf import timestamp_pb2
import pika  # type: ignore

from buildgrid._enums import ExchangeNames, QueueNames
from buildgrid._exceptions import InvalidArgumentError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.buildgrid.v2.messaging_pb2 import CreateOperation, UpdateOperations
from buildgrid._protos.google.longrunning import operations_pb2
from buildgrid.server.metrics_names import (
    OPERATIONS_CANCEL_OPERATION_TIME_METRIC_NAME,
    OPERATIONS_GET_OPERATION_TIME_METRIC_NAME,
    OPERATIONS_LIST_OPERATIONS_TIME_METRIC_NAME
)
from buildgrid.server.metrics_utils import DurationMetric
from buildgrid.server.operations.filtering import FilterParser, DEFAULT_OPERATION_FILTERS
from buildgrid.server.persistence.sql.impl import DataStoreInterface
from buildgrid.server.rabbitmq.pika_consumer import RetryingPikaConsumer, QueueBinding
from buildgrid.settings import DEFAULT_MAX_LIST_OPERATION_PAGE_SIZE


class OperationState:
    def __init__(self,
                 operation: Optional[operations_pb2.Operation],
                 timestamp: timestamp_pb2.Timestamp,
                 request_metadata: Optional[remote_execution_pb2.RequestMetadata] = None):

        self.operation: Optional[operations_pb2.Operation] = operation
        self.timestamp: timestamp_pb2.Timestamp = timestamp
        self.request_metadata: Optional[remote_execution_pb2.RequestMetadata] = request_metadata


class OperationStateCache:
    def __init__(self, capacity: int):
        """Simple in-memory LRU storage container.

        Args:
            capacity (int): the maximum number of entries that can be cached.
                After exceeding the capacity, older entries will be dropped
                to make room for new ones, following a Least Recently Used
                policy.
        """
        if capacity <= 0:
            raise ValueError("Capacity must be positive")

        self._capacity = capacity

        self._ordered_dict_lock = Lock()
        self._ordered_dict = OrderedDict()  # type: ignore

    @property
    def size(self) -> int:
        """Number of elements in the cache."""
        with self._ordered_dict_lock:
            return len(self._ordered_dict)

    @property
    def capacity(self) -> int:
        """Maximum number of elements that fit in the cache."""
        return self._capacity

    def update(self,
               operation_name: str,
               operation: Optional[operations_pb2.Operation],
               timestamp: timestamp_pb2.Timestamp,
               request_metadata: remote_execution_pb2.RequestMetadata=None):
        """Create or update a cache entry for the given operation.
        If no `request_metadata` is given for a `job_name` that is
        present, it will keep the value of `request_metadata` previously
        written.
        """
        with self._ordered_dict_lock:
            if operation_name in self._ordered_dict:
                # Existing operation, leave `RequestMetadata` untouched:
                self._ordered_dict[operation_name].operation = operation
                self._ordered_dict[operation_name].timestamp = timestamp

                self._ordered_dict.move_to_end(operation_name, last=True)
            else:
                self._ordered_dict[operation_name] = OperationState(operation=operation,
                                                                    timestamp=timestamp,
                                                                    request_metadata=request_metadata)

                if len(self._ordered_dict) > self._capacity:
                    self._ordered_dict.popitem(last=False)

    def get(self, operation_name: str) -> Optional[OperationState]:
        """Get a value defined in the cache. If a value for the given key is
        not found, returns None.
        Updates the last access time of the entry.
        """
        with self._ordered_dict_lock:
            state = self._ordered_dict.get(operation_name, None)
            if state is not None:
                self._ordered_dict.move_to_end(operation_name, last=True)
            return state


class OperationsInstance:

    def __init__(self,
                 instance_name: str,
                 rabbitmq_connection_parameters: pika.connection.Parameters,
                 operations_datastore: DataStoreInterface,
                 operation_state_cache_capacity: int = 1000,
                 max_connection_attempts: int = 0,
                 max_list_operations_page_size=DEFAULT_MAX_LIST_OPERATION_PAGE_SIZE):
        """Instantiate a new ``OperationsInstance``.

        Args:
            instance_name (str): name of the instance
            rabbitmq_connection_parameters (pika.connection.Parameters):
                connection details of the RabbitMQ server from which to read
                updates
            operations_datastore (DataStoreInterface): underlying storage
                for Operations
            operation_state_cache_capacity (int): maximum number of Operations
                that can be cached in memory
            max_connection_attempts (int): maximum connection attempts to
                the RabbitMQ server (default: 0 = no limit)
            max_list_operations_page_size (int): size limit for pages returned
                by ``ListOperations()``
        """
        self._logger = logging.getLogger(__name__)

        self._operations_datastore = operations_datastore

        self._max_list_operations_page_size = max_list_operations_page_size

        self._operation_state_cache = OperationStateCache(capacity=operation_state_cache_capacity)

        self._instance_name = instance_name
        self._instance_is_registered = False

        self._stopped = False

        queue = QueueNames.OPERATION_UPDATES.value
        exchange = ExchangeNames.OPERATION_UPDATES.value
        binding_key = f'*.{self._instance_name}'  # "<state>.<instance_name>"

        self._logger.debug(f"Consuming messages from queue '{queue}', "
                           f"exchange '{exchange}', and routing key '{binding_key}'")
        queue_binding = QueueBinding(queue=queue, exchange=exchange, routing_key=binding_key)
        self._rabbitmq_consumer = RetryingPikaConsumer(connection_parameters=rabbitmq_connection_parameters,
                                                       exchanges={ExchangeNames.OPERATION_UPDATES.value:
                                                                  pika.exchange_type.ExchangeType.topic},
                                                       bindings={queue_binding},
                                                       max_connection_attempts=max_connection_attempts)
        self._rabbitmq_consumer.subscribe(queue_name=QueueNames.OPERATION_UPDATES.value,
                                          callback=self._process_operation_update)

    def __del__(self):
        self.stop()

    # --- Public API ---
    @property
    def instance_name(self):
        return self._instance_name

    def register_instance_with_server(self, server):
        """Names and registers the operations instance with a given server."""
        if self._instance_is_registered:
            raise AssertionError("Instance already registered")

        server.add_operations_instance(self, self._instance_name)
        self._instance_is_registered = True

    @DurationMetric(OPERATIONS_GET_OPERATION_TIME_METRIC_NAME, instanced=True)
    def get_operation(self, job_name):
        # Local cache:
        operation_state = self._operation_state_cache.get(job_name)
        if operation_state:
            return operation_state.operation, operation_state.request_metadata

        # Centralized datastore:
        operation = self._operations_datastore.get_job_by_operation(job_name)
        if not operation:
            raise InvalidArgumentError(f"Operation name does not exist: [{job_name}]")
        metadata = self._operations_datastore.get_operation_request_metadata_by_name(job_name)
        return operation, metadata

    @DurationMetric(OPERATIONS_LIST_OPERATIONS_TIME_METRIC_NAME, instanced=True)
    def list_operations(self, filter_string, page_size, page_token):
        if page_size and page_size > self._max_list_operations_page_size:
            raise InvalidArgumentError(f"The maximum page size is "
                                       f"{self._max_list_operations_page_size}.")
        if not page_size:
            page_size = self._max_list_operations_page_size

        operation_filters = FilterParser.parse_listoperations_filters(filter_string)
        if not operation_filters:
            operation_filters = DEFAULT_OPERATION_FILTERS

        response = operations_pb2.ListOperationsResponse()

        results, next_token = self._operations_datastore.list_operations(operation_filters,
                                                                         page_size,
                                                                         page_token)
        response.operations.extend(results)
        response.next_page_token = next_token

        return response

    def delete_operation(self, job_name):
        """ DeleteOperation is not supported in BuildGrid. """
        pass

    @DurationMetric(OPERATIONS_CANCEL_OPERATION_TIME_METRIC_NAME, instanced=True)
    def cancel_operation(self, job_name):
        pass

    def stop(self):
        if not self._stopped:
            self._rabbitmq_consumer.stop()

    # --- Private API ---
    def _process_operation_update(self, message: bytes, delivery_tag: str):
        # Checking whether the messages is Create/UpdateOperation.

        create_operation = CreateOperation()
        create_operation.ParseFromString(message)

        request_metadata = None
        operation = None

        if create_operation.action_digest.size_bytes > 0:
            self._logger.debug(f"Received CreateOperation message "
                               f"[{create_operation}] (delivery tag: {delivery_tag})")
            operation_name = create_operation.job_id
            message_creation_timestamp = create_operation.timestamp
            request_metadata = create_operation.request_metadata
        else:
            update_operations = UpdateOperations()
            update_operations.ParseFromString(message)

            operation_name = update_operations.job_id
            if operation_name:
                self._logger.debug(f"Received UpdateOperations message "
                                   f"[{update_operations}] (delivery tag: {delivery_tag})")
                operation = update_operations.operation_state
                message_creation_timestamp = update_operations.timestamp

        if operation_name:
            # Updating in-memory cache:
            self._operation_state_cache.update(operation_name=operation_name,
                                               operation=operation,
                                               timestamp=message_creation_timestamp,
                                               request_metadata=request_metadata)
            # TODO: update `self._operations_datastore` as well

        self._rabbitmq_consumer.ack_message(delivery_tag)
