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
OperationsService
=================

"""

import logging

import grpc

from google.protobuf.empty_pb2 import Empty

from buildgrid._exceptions import InvalidArgumentError, RetriableError
from buildgrid._protos.google.longrunning import operations_pb2_grpc, operations_pb2
from buildgrid.server._authentication import AuthContext, authorize
from buildgrid.server.metrics_names import (
    OPERATIONS_CANCEL_OPERATION_TIME_METRIC_NAME,
    OPERATIONS_DELETE_OPERATION_TIME_METRIC_NAME,
    OPERATIONS_GET_OPERATION_TIME_METRIC_NAME,
    OPERATIONS_LIST_OPERATIONS_TIME_METRIC_NAME
)
from buildgrid.server.metrics_utils import DurationMetric
from buildgrid.server.request_metadata_utils import request_metadata_from_scheduler_dict


class OperationsService(operations_pb2_grpc.OperationsServicer):

    def __init__(self, server):
        self.__logger = logging.getLogger(__name__)

        self._instances = {}

        operations_pb2_grpc.add_OperationsServicer_to_server(self, server)

    # --- Public API ---

    def add_instance(self, instance_name, instance):
        """Registers a new servicer instance.

        Args:
            instance_name (str): The new instance's name.
            instance (OperationsInstance): The new instance itself.
        """
        self._instances[instance_name] = instance

    # --- Public API: Servicer ---

    @authorize(AuthContext)
    @DurationMetric(OPERATIONS_GET_OPERATION_TIME_METRIC_NAME)
    def GetOperation(self, request, context):
        self.__logger.info(f"GetOperation request from [{context.peer()}]")

        try:
            name = request.name

            instance_name = self._parse_instance_name(name)
            instance = self._get_instance(instance_name)

            operation_name = self._parse_operation_name(name)
            operation, metadata = instance.get_operation(operation_name)
            op = operations_pb2.Operation()
            op.CopyFrom(operation)
            op.name = name

            if metadata is not None:
                metadata_entry = self._operation_request_metadata_entry(metadata)
                context.set_trailing_metadata([metadata_entry])

            return op

        except InvalidArgumentError as e:
            self.__logger.info(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        except RetriableError as e:
            self.__logger.info(f"Retriable error, client should retry in: {e.retry_info.retry_delay}")
            context.abort_with_status(e.error_status)

        except Exception as e:
            self.__logger.exception(
                f"Unexpected error in GetOperation; request=[{request}]"
            )
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)

        return operations_pb2.Operation()

    @authorize(AuthContext)
    @DurationMetric(OPERATIONS_LIST_OPERATIONS_TIME_METRIC_NAME)
    def ListOperations(self, request, context):
        self.__logger.info(f"ListOperations request from [{context.peer()}]")

        try:
            # The request name should be the collection name
            # In our case, this is just the instance_name
            instance_name = request.name
            instance = self._get_instance(instance_name)

            result = instance.list_operations(request.filter,
                                              request.page_size,
                                              request.page_token)

            for operation in result.operations:
                operation.name = f"{instance_name}/{operation.name}"

            return result

        except InvalidArgumentError as e:
            # This is a client error. Don't log at error on the server side
            self.__logger.info(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        except RetriableError as e:
            self.__logger.info(f"Retriable error, client should retry in: {e.retry_info.retry_delay}")
            context.abort_with_status(e.error_status)

        except Exception as e:
            self.__logger.exception(
                f"Unexpected error in ListOperations; request=[{request}]"
            )
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)

        return operations_pb2.ListOperationsResponse()

    @authorize(AuthContext)
    @DurationMetric(OPERATIONS_DELETE_OPERATION_TIME_METRIC_NAME)
    def DeleteOperation(self, request, context):
        self.__logger.info(f"DeleteOperation request from [{context.peer()}]")

        context.set_details("BuildGrid does not support DeleteOperation.")
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        return Empty()

    @authorize(AuthContext)
    @DurationMetric(OPERATIONS_CANCEL_OPERATION_TIME_METRIC_NAME)
    def CancelOperation(self, request, context):
        self.__logger.info(f"CancelOperation request from [{context.peer()}]")

        try:
            name = request.name

            instance_name = self._parse_instance_name(name)
            instance = self._get_instance(instance_name)

            operation_name = self._parse_operation_name(name)
            instance.cancel_operation(operation_name)

        except InvalidArgumentError as e:
            self.__logger.info(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        except RetriableError as e:
            self.__logger.info(f"Retriable error, client should retry in: {e.retry_info.retry_delay}")
            context.abort_with_status(e.error_status)

        except Exception as e:
            self.__logger.exception(
                f"Unexpected error in CancelOperation; request=[{request}]"
            )
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)

        return Empty()

    # --- Private API ---

    def _parse_instance_name(self, name):
        """ If the instance name is not blank, 'name' will have the form
        {instance_name}/{operation_uuid}. Otherwise, it will just be
        {operation_uuid} """
        names = name.split('/')
        return '/'.join(names[:-1]) if len(names) > 1 else ''

    def _parse_operation_name(self, name):
        names = name.split('/')
        return names[-1] if len(names) > 1 else name

    def _get_instance(self, name):
        try:
            return self._instances[name]

        except KeyError:
            raise InvalidArgumentError(f"Instance doesn't exist on server: [{name}] "
                                       "(operation ids have the form \"instance_name/operation_uuid\")")

    @staticmethod
    def _operation_request_metadata_entry(operation_metadata):
        request_metadata = request_metadata_from_scheduler_dict(operation_metadata)
        return 'requestmetadata-bin', request_metadata.SerializeToString()
