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
Operations command
=================

Check the status of operations
"""

import sys
from textwrap import indent

import click
from google.protobuf import json_format
from grpc import RpcError, StatusCode

from buildgrid.client.channel import setup_channel
from buildgrid._enums import OperationStage
from buildgrid._exceptions import InvalidArgumentError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2, remote_execution_pb2_grpc
from buildgrid._protos.google.longrunning import operations_pb2, operations_pb2_grpc
from buildgrid._protos.google.rpc import code_pb2
from buildgrid.server.request_metadata_utils import printable_request_metadata, extract_request_metadata

from .rpc_utils import cancel_operation
from ..cli import pass_context


@click.group(name='operation', short_help="Long running operations commands.")
@click.option('--remote', type=click.STRING, default='http://localhost:50051', show_default=True,
              help="Remote execution server's URL (port defaults to 50051 if no specified).")
@click.option('--auth-token', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Authorization token for the remote.")
@click.option('--client-key', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Private client key for TLS (PEM-encoded).")
@click.option('--client-cert', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Public client certificate for TLS (PEM-encoded).")
@click.option('--server-cert', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Public server certificate for TLS (PEM-encoded).")
@click.option('--instance-name', type=click.STRING, default=None, show_default=True,
              help="Targeted farm instance name.")
@click.option('--action-id', type=str, help='Action ID.')
@click.option('--invocation-id', type=str, help='Tool invocation ID.')
@click.option('--correlation-id', type=str, help='Correlated invocation ID.')
@pass_context
def cli(context, remote, instance_name, auth_token, client_key, client_cert,
        server_cert, action_id, invocation_id, correlation_id):
    """Entry point for the bgd-operation CLI command group."""
    try:
        context.channel, _ = setup_channel(remote, auth_token=auth_token,
                                           client_key=client_key,
                                           client_cert=client_cert,
                                           server_cert=server_cert,
                                           action_id=action_id,
                                           tool_invocation_id=invocation_id,
                                           correlated_invocations_id=correlation_id)

    except InvalidArgumentError as e:
        click.echo(f"Error: {e}.", err=True)
        sys.exit(-1)

    context.instance_name = instance_name


def _print_operation_status(operation, print_details=False):
    metadata = remote_execution_pb2.ExecuteOperationMetadata()
    # The metadata is expected to be an ExecuteOperationMetadata message:
    if not operation.metadata.Is(metadata.DESCRIPTOR):
        raise InvalidArgumentError('Metadata is not an ExecuteOperationMetadata '
                                   'message')
    operation.metadata.Unpack(metadata)

    stage = OperationStage(metadata.stage)

    if not operation.done:
        if stage == OperationStage.CACHE_CHECK:
            click.echo(f'CacheCheck: {operation.name}: '
                       f'Querying action-cache (stage={metadata.stage})')
        elif stage == OperationStage.QUEUED:
            click.echo(f'Queued: {operation.name}: '
                       f'Waiting for execution (stage={metadata.stage})')
        elif stage == OperationStage.EXECUTING:
            click.echo(f'Executing: {operation.name}: '
                       f'Currently running (stage={metadata.stage})')
        else:
            click.echo(f'Error: {operation.name}: '
                       f'In an invalid state (stage={metadata.stage})', err=True)
        return

    assert stage == OperationStage.COMPLETED

    response = remote_execution_pb2.ExecuteResponse()
    # The response is expected to be an ExecutionResponse message:
    operation.response.Unpack(response)

    if response.status.code != code_pb2.OK:
        click.echo(f'Failure: {operation.name}: {response.status.message} '
                   f'(code={response.status.code})')
    else:
        if response.result.exit_code != 0:
            click.echo(f'Success: {operation.name}: Completed with failure '
                       f'(stage={metadata.stage}, exit_code={response.result.exit_code})')
        else:
            click.echo(f'Success: {operation.name}: Completed succesfully '
                       f'(stage={metadata.stage}, exit_code={response.result.exit_code})')

    if print_details:
        metadata = response.result.execution_metadata
        click.echo(indent(f'worker={metadata.worker}', '  '))

        queued = metadata.queued_timestamp.ToDatetime()
        click.echo(indent(f'queued_at={queued}', '  '))

        worker_start = metadata.worker_start_timestamp.ToDatetime()
        worker_completed = metadata.worker_completed_timestamp.ToDatetime()
        click.echo(indent(f'work_duration={worker_completed - worker_start}', '  '))

        fetch_start = metadata.input_fetch_start_timestamp.ToDatetime()
        fetch_completed = metadata.input_fetch_completed_timestamp.ToDatetime()
        click.echo(indent(f'fetch_duration={fetch_completed - fetch_start}', '    '))

        execution_start = metadata.execution_start_timestamp.ToDatetime()
        execution_completed = metadata.execution_completed_timestamp.ToDatetime()
        click.echo(indent(f'exection_duration={execution_completed - execution_start}', '    '))

        upload_start = metadata.output_upload_start_timestamp.ToDatetime()
        upload_completed = metadata.output_upload_completed_timestamp.ToDatetime()
        click.echo(indent(f'upload_duration={upload_completed - upload_start}', '    '))

        click.echo(indent(f'total_duration={worker_completed - queued}', '  '))


@cli.command('status', short_help="Get the status of an operation.")
@click.argument('operation-name', nargs=1, type=click.STRING, required=True)
@click.option('--json', is_flag=True, show_default=True,
              help="Print operations status in JSON format.")
@click.option('--show-request-metadata', is_flag=True, show_default=False,
              help='Show RequestMetadata message')
@pass_context
def status(context, operation_name, json, show_request_metadata):
    stub = operations_pb2_grpc.OperationsStub(context.channel)
    request = operations_pb2.GetOperationRequest(name=operation_name)

    try:
        operation, call = stub.GetOperation.with_call(request)
    except RpcError as e:
        click.echo(f'Error: {e.details()}', err=True)
        sys.exit(-1)

    if not json:
        _print_operation_status(operation, print_details=True)
    else:
        click.echo(json_format.MessageToJson(operation))

    if show_request_metadata:
        metadata = call.trailing_metadata()
        if json:
            request_metadata_proto = extract_request_metadata(metadata)
            click.echo(json_format.MessageToJson(request_metadata_proto))
        else:
            click.echo(f'Request metadata: [{printable_request_metadata(call.trailing_metadata())}]')


@cli.command('cancel', short_help="Cancel an operation.")
@click.argument('operation-name', nargs=1, type=click.STRING, required=True)
@pass_context
def cancel(context, operation_name):
    cancel_successful = cancel_operation(context, operation_name)
    if not cancel_successful:
        sys.exit(-1)


@cli.command('list', short_help="List operations.")
@click.option('--json', is_flag=True, show_default=True,
              help="Print operations list in JSON format.")
@click.option('--page-token', type=click.STRING,
              help="Token to start from. Set this to the \"next_page_token\" of the previous "
                   "ListOperationsResult to get the next page of results.")
@click.option('--page-size', type=click.INT,
              help="Number of operations to request per page. Cannot be larger than the server maximum "
                   "(throws an error).")
@click.option('--filter-string', '--filter', type=click.STRING, default="",
              help="Filter which operations are returned. By default, only incomplete operations are shown. "
              "See docs for syntax.")
@pass_context
def lists(context, json, page_token, page_size, filter_string):
    stub = operations_pb2_grpc.OperationsStub(context.channel)
    request = operations_pb2.ListOperationsRequest(
        name=context.instance_name, page_token=page_token, page_size=page_size, filter=filter_string)

    try:
        response = stub.ListOperations(request)
    except RpcError as e:
        click.echo(f'Error: {e.details()}', err=True)
        sys.exit(-1)

    if not response.operations:
        click.echo('Error: No operations to list.', err=True)
        return

    for operation in response.operations:
        if not json:
            _print_operation_status(operation)
        else:
            click.echo(json_format.MessageToJson(operation))

    if response.next_page_token:
        next_cmd = "bgd operation list "
        if filter_string:
            next_cmd = next_cmd + f"--filter {filter_string} "
        if page_size:
            next_cmd = next_cmd + f"--page-size {page_size} "
        next_cmd = next_cmd + f"--page-token \"{response.next_page_token}\""
        click.echo(f"Get the next page of operations with [{next_cmd}]")


@cli.command('wait', short_help="Streams an operation until it is complete.")
@click.argument('operation-name', nargs=1, type=click.STRING, required=True)
@click.option('--json', is_flag=True, show_default=True,
              help="Print operations statuses in JSON format.")
@pass_context
def wait(context, operation_name, json):
    stub = remote_execution_pb2_grpc.ExecutionStub(context.channel)
    request = remote_execution_pb2.WaitExecutionRequest(name=operation_name)

    operation_iterator = stub.WaitExecution(request)

    try:
        for operation in operation_iterator:
            if not json and operation.done:
                _print_operation_status(operation, print_details=True)
            elif not json:
                _print_operation_status(operation)
            else:
                click.echo(json_format.MessageToJson(operation))

    except InvalidArgumentError as e:
        click.echo(f'Error: In the reply: {e}', err=True)
        sys.exit(-1)

    except RpcError as e:
        if e.code() != StatusCode.CANCELLED:
            click.echo(f'Error: {e.details()}', err=True)
            sys.exit(-1)
