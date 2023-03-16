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
Execute command
=================

Request work to be executed and monitor status of jobs.
"""

import os
import signal
import stat
import sys

import click
from grpc import RpcError

from buildgrid.client.channel import setup_channel
from buildgrid.client.cas import download, upload
from buildgrid._exceptions import InvalidArgumentError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2, remote_execution_pb2_grpc
from buildgrid.utils import create_digest

from .rpc_utils import cancel_operation
from ..cli import pass_context


@click.group(name='execute', short_help="Execute simple operations.")
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
@click.option('--remote-cas', type=click.STRING, default=None, show_default=False,
              help="Remote CAS server's URL (defaults to --remote if not specified).")
@click.option('--cas-client-key', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Private CAS client key for TLS (PEM-encoded).")
@click.option('--cas-client-cert', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Public CAS client certificate for TLS (PEM-encoded).")
@click.option('--cas-server-cert', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Public CAS server certificate for TLS (PEM-encoded).")
@click.option('--action-id', type=str, help='Action ID.')
@click.option('--invocation-id', type=str, help='Tool invocation ID.')
@click.option('--correlation-id', type=str, help='Correlated invocation ID.')
@pass_context
def cli(context, remote, instance_name, auth_token, client_key, client_cert,
        server_cert, remote_cas, cas_client_key, cas_client_cert,
        cas_server_cert, action_id, invocation_id, correlation_id):
    """Entry point for the bgd-execute CLI command group."""
    try:
        context.channel, details = setup_channel(remote, auth_token=auth_token,
                                                 client_key=client_key,
                                                 client_cert=client_cert,
                                                 server_cert=server_cert,
                                                 action_id=action_id,
                                                 tool_invocation_id=invocation_id,
                                                 correlated_invocations_id=correlation_id)

        if remote_cas and remote_cas != remote:
            context.cas_channel, details = setup_channel(remote_cas, auth_token=auth_token,
                                                         server_cert=cas_server_cert,
                                                         client_key=cas_client_key,
                                                         client_cert=cas_client_cert,
                                                         action_id=action_id,
                                                         tool_invocation_id=invocation_id,
                                                         correlated_invocations_id=correlation_id)
            context.remote_cas_url = remote_cas

        else:
            context.cas_channel = context.channel
            context.remote_cas_url = remote

        context.cas_client_key, context.cas_client_cert, context.cas_server_cert = details

    except InvalidArgumentError as e:
        click.echo(f"Error: {e}.", err=True)
        sys.exit(-1)

    context.instance_name = instance_name


@cli.command('request-dummy', short_help="Send a dummy action.")
@click.option('--number', type=click.INT, default=1, show_default=True,
              help="Number of request to send.")
@click.option('--wait-for-completion', is_flag=True,
              help="Stream updates until jobs are completed.")
@pass_context
def request_dummy(context, number, wait_for_completion):

    click.echo("Sending execution request...")
    command = remote_execution_pb2.Command()
    command_digest = create_digest(command.SerializeToString())

    action = remote_execution_pb2.Action(command_digest=command_digest,
                                         do_not_cache=True)
    action_digest = create_digest(action.SerializeToString())

    stub = remote_execution_pb2_grpc.ExecutionStub(context.channel)

    request = remote_execution_pb2.ExecuteRequest(instance_name=context.instance_name,
                                                  action_digest=action_digest,
                                                  skip_cache_lookup=True)

    responses = []
    for _ in range(0, number):
        responses.append(stub.Execute(request))

    for response in responses:
        try:
            if wait_for_completion:
                result = None
                for stream in response:
                    result = stream
                    click.echo(result)

                if not result.done:
                    click.echo("Result did not return True." +
                               "Was the action uploaded to CAS?", err=True)
                    sys.exit(-1)
            else:
                click.echo(next(response))
        except RpcError as e:
            click.echo(f'Error: Requesting dummy: {e.details()}', err=True)


@cli.command('command', short_help="Send a command to be executed.")
@click.option('--output-file', nargs=1, type=click.STRING, multiple=True,
              help="Expected output file.")
@click.option('--output-directory', default='testing', show_default=True,
              help="Output directory for the output files.")
@click.option('-p', '--platform-property', nargs=2, type=(click.STRING, click.STRING), multiple=True,
              help="List of key-value pairs of required platform properties.")
@click.option('--disable-cancellation', is_flag=True,
              help="By default, SIGINT sends a CancelOperation request. Set this flag to disable this behavior.")
@click.option('-c', '--enable-action-cache', is_flag=True,
              help="By default, the result is not cached and the cache isn't checked for a previous result. "
                   "Set this flag to enable caching.")
@click.argument('input-root', nargs=1, type=click.Path(), required=True)
@click.argument('commands', nargs=-1, type=click.STRING, required=True)
@click.option('--priority', nargs=1, type=click.INT, show_default=True, default=0,
              help="Set priority of execution request, the value can be negative or positive. "
                   "Smaller values result in higher priority.")
@pass_context
def run_command(context, input_root, commands, output_file, output_directory,
                platform_property, disable_cancellation, enable_action_cache, priority):

    stub = remote_execution_pb2_grpc.ExecutionStub(context.channel)

    try:
        action_digest = upload_action(commands, context, input_root, output_file,
                                      platform_property, enable_action_cache)
    except Exception as e:
        click.echo(f'Error: Uploading action: {e}', err=True)
        sys.exit(-1)

    execution_policy = remote_execution_pb2.ExecutionPolicy(priority=priority)
    request = remote_execution_pb2.ExecuteRequest(instance_name=context.instance_name,
                                                  action_digest=action_digest,
                                                  skip_cache_lookup=not enable_action_cache,
                                                  execution_policy=execution_policy)

    response = stub.Execute(request)

    operation_name = None

    # Set up a signal handler for SIGINT
    # Needs to be done here rather than as a context manager because the operation
    # name is not known a priori
    if not disable_cancellation:
        def cancel_handler(signum, stack):
            if operation_name is not None:
                cancel_operation(context, operation_name)
            else:
                click.echo("Unable to cancel operation: operation name unknown")
            sys.exit(1)
        old_sigint_handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, cancel_handler)

    # Read the response until it is finished
    stream = None
    for stream in response:
        operation_name = stream.name
        click.echo(stream)

    # The last message will be the ExecuteResponse, so read it
    execute_response = remote_execution_pb2.ExecuteResponse()
    stream.response.Unpack(execute_response)

    # Restore the original signal handler
    if not disable_cancellation:
        signal.signal(signal.SIGINT, old_sigint_handler)

    try:
        with download(context.cas_channel, instance=context.instance_name) as downloader:
            for output_file_response in execute_response.result.output_files:
                path = os.path.join(output_directory, output_file_response.path)

                if not os.path.exists(os.path.dirname(path)):
                    os.makedirs(os.path.dirname(path), exist_ok=True)

                downloader.download_file(output_file_response.digest, path)
    except Exception as e:
        click.echo(f'Error: Downloading output files: {e}', err=True)
        sys.exit(-1)

    for output_file_response in execute_response.result.output_files:
        if output_file_response.is_executable:
            path = os.path.join(output_directory, output_file_response.path)
            st = os.stat(path)
            os.chmod(path, st.st_mode | stat.S_IXUSR)


def upload_action(commands, context, input_root, output_file,
                  platform_property, enable_action_cache):
    with upload(context.cas_channel, instance=context.instance_name) as uploader:
        command = remote_execution_pb2.Command()

        for arg in commands:
            command.arguments.extend([arg])

        command.output_paths.extend(output_file)

        for attribute_name, attribute_value in platform_property:
            new_property = command.platform.properties.add()
            new_property.name = attribute_name
            new_property.value = attribute_value

        command_digest = uploader.put_message(command, queue=True)

        click.echo(f"Sent command=[{command_digest}]")

        # TODO: Check for missing blobs
        input_root_digest = uploader.upload_directory(input_root)

        click.echo(f"Sent input=[{input_root_digest}]")

        action = remote_execution_pb2.Action(command_digest=command_digest,
                                             input_root_digest=input_root_digest,
                                             do_not_cache=not enable_action_cache)
        action.platform.CopyFrom(command.platform)

        action_digest = uploader.put_message(action, queue=True)

        click.echo(f"Sent action=[{action_digest}]")

        return action_digest
