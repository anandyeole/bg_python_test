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
OperationsInstance
==================
An instance of the LongRunningOperations Service.
"""

import logging

from buildgrid._exceptions import InvalidArgumentError, NotFoundError
from buildgrid._protos.google.longrunning import operations_pb2
from buildgrid.server.operations.filtering import FilterParser, DEFAULT_OPERATION_FILTERS
from buildgrid.server.metrics_names import (
    OPERATIONS_CANCEL_OPERATION_TIME_METRIC_NAME,
    OPERATIONS_GET_OPERATION_TIME_METRIC_NAME,
    OPERATIONS_LIST_OPERATIONS_TIME_METRIC_NAME
)
from buildgrid.server.metrics_utils import DurationMetric
from buildgrid.settings import DEFAULT_MAX_LIST_OPERATION_PAGE_SIZE


class OperationsInstance:

    def __init__(self, scheduler, max_list_operations_page_size=DEFAULT_MAX_LIST_OPERATION_PAGE_SIZE):
        self.__logger = logging.getLogger(__name__)

        self._scheduler = scheduler
        self._max_list_operations_page_size = max_list_operations_page_size
        self._instance_name = None

    # --- Public API ---

    @property
    def instance_name(self):
        return self._instance_name

    @property
    def scheduler(self):
        return self._scheduler

    def setup_grpc(self):
        # The operations instance doesn't currently have any gRPC setup to do
        pass

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def register_instance_with_server(self, instance_name, server):
        """Names and registers the operations instance with a given server."""
        if self._instance_name is None:
            server.add_operations_instance(self, instance_name)

            self._instance_name = instance_name

        else:
            raise AssertionError("Instance already registered")

    @DurationMetric(OPERATIONS_GET_OPERATION_TIME_METRIC_NAME, instanced=True)
    def get_operation(self, job_name):
        try:
            operation = self._scheduler.get_job_operation(job_name)

        except NotFoundError:
            raise InvalidArgumentError(f"Operation name does not exist: [{job_name}]")

        metadata = self._scheduler.get_operation_request_metadata(job_name)
        return operation, metadata

    @DurationMetric(OPERATIONS_LIST_OPERATIONS_TIME_METRIC_NAME, instanced=True)
    def list_operations(self, filter_string, page_size, page_token):
        if page_size and page_size > self._max_list_operations_page_size:
            raise InvalidArgumentError(f"The maximum page size is {self._max_list_operations_page_size}.")
        if not page_size:
            page_size = self._max_list_operations_page_size

        operation_filters = FilterParser.parse_listoperations_filters(filter_string)
        if not operation_filters:
            operation_filters = DEFAULT_OPERATION_FILTERS

        response = operations_pb2.ListOperationsResponse()

        results, next_token = self._scheduler.list_operations(operation_filters, page_size, page_token)
        response.operations.extend(results)
        response.next_page_token = next_token

        return response

    def delete_operation(self, job_name):
        """ DeleteOperation is not supported in BuildGrid. """
        pass

    @DurationMetric(OPERATIONS_CANCEL_OPERATION_TIME_METRIC_NAME, instanced=True)
    def cancel_operation(self, job_name):
        try:
            self._scheduler.cancel_job_operation(job_name)

        except NotFoundError:
            raise InvalidArgumentError(f"Operation name does not exist: [{job_name}]")
