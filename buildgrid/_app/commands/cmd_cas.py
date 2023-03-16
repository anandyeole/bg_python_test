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
import sys

import click

from buildgrid.client.channel import setup_channel
from buildgrid.client.cas import download, upload
from buildgrid._exceptions import InvalidArgumentError, NotFoundError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid.utils import create_digest, parse_digest, merkle_tree_maker, read_file

from ..cli import pass_context


@click.group(name='cas', short_help="Interact with the CAS server.")
@click.option('--remote', type=click.STRING, default='http://localhost:50051', show_default=True,
              help="Remote execution server's URL (port defaults to 50051 if no specified).")
@click.option('--auth-token', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Authorization token for the remote.")
@click.option('--client-key', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Private client key for TLS (PEM-encoded).")
@click.option('--client-cert', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Public client certificate for TLS (PEM-encoded).")
@click.option('--server-cert', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Public server certificate for TLS (PEM-encoded)")
@click.option('--instance-name', type=click.STRING, default=None, show_default=True,
              help="Targeted farm instance name.")
@click.option('--action-id', type=str, help='Action ID.')
@click.option('--invocation-id', type=str, help='Tool invocation ID.')
@click.option('--correlation-id', type=str, help='Correlated invocation ID.')
@pass_context
def cli(context, remote, instance_name, auth_token, client_key, client_cert,
        server_cert, action_id, invocation_id, correlation_id):
    """Entry point for the bgd-cas CLI command group."""
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


@cli.command('upload-dummy', short_help="Upload a dummy action. Should be used with `execute dummy-request`")
@pass_context
def upload_dummy(context):
    command = remote_execution_pb2.Command()
    try:
        with upload(context.channel, instance=context.instance_name) as uploader:
            command_digest = uploader.put_message(command)
    except Exception as e:
        click.echo(f'Error: Uploading dummy: {e}', err=True)
        sys.exit(-1)

    if command_digest.ByteSize():
        click.echo(f'Success: Pushed Command, digest=["{command_digest.hash}/{command_digest.size_bytes}]"')
    else:
        click.echo("Error: Failed pushing empty Command.", err=True)

    action = remote_execution_pb2.Action(command_digest=command_digest,
                                         do_not_cache=True)

    with upload(context.channel, instance=context.instance_name) as uploader:
        action_digest = uploader.put_message(action)

    if action_digest.ByteSize():
        click.echo(f'Success: Pushed Action, digest=["{action_digest.hash}/{action_digest.size_bytes}]"')
    else:
        click.echo("Error: Failed pushing empty Action.", err=True)


@cli.command('upload-file', short_help="Upload files to the CAS server.")
@click.argument('file_path', nargs=-1, type=click.Path(exists=True, dir_okay=False), required=True)
@click.option('--verify', is_flag=True, show_default=True,
              help="Check uploaded files integrity.")
@pass_context
def upload_file(context, file_path, verify):
    sent_digests = []
    try:
        with upload(context.channel, instance=context.instance_name) as uploader:
            for path in file_path:
                if not os.path.isabs(path):
                    path = os.path.relpath(path)

                click.echo(f"Queueing path=[{path}]")

                file_digest = uploader.upload_file(path, queue=True)

                sent_digests.append((file_digest, path))
    except Exception as e:
        click.echo(f'Error: Uploading file: {e}', err=True)
        sys.exit(-1)

    for sent_file_digest, sent_file_path in sent_digests:
        if verify and sent_file_digest.size_bytes != os.stat(sent_file_path).st_size:
            click.echo(f"Error: Failed to verify '{sent_file_path}'", err=True)
        elif sent_file_digest.ByteSize():
            click.echo(f"Success: Pushed path=[{sent_file_path}] with digest="
                       f"[{sent_file_digest.hash}/{sent_file_digest.size_bytes}]")
        else:
            click.echo(f"Error: Failed pushing path=[{sent_file_path}]", err=True)


@cli.command('upload-dir', short_help="Upload a directory to the CAS server.")
@click.argument('directory-path', nargs=1, type=click.Path(exists=True, file_okay=False), required=True)
@click.option('--verify', is_flag=True, show_default=True,
              help="Check uploaded directory's integrity.")
@pass_context
def upload_directory(context, directory_path, verify):
    sent_digests = []
    try:
        with upload(context.channel, instance=context.instance_name) as uploader:
            for node, blob, path in merkle_tree_maker(directory_path):
                if not os.path.isabs(directory_path):
                    path = os.path.relpath(path)
                click.echo(f"Queueing path=[{path}]")

                node_digest = uploader.put_blob(blob, digest=node.digest, queue=True)
                sent_digests.append((node_digest, path))
    except Exception as e:
        click.echo(f'Error: Uploading directory: {e}', err=True)
        sys.exit(-1)

    for node_digest, node_path in sent_digests:
        if verify and (os.path.isfile(node_path) and
                       node_digest.size_bytes != os.stat(node_path).st_size):
            click.echo(f"Error: Failed to verify path=[{node_path}]", err=True)
        elif node_digest.ByteSize():
            click.echo(f"Success: Pushed path=[{node_path}] with digest="
                       f"[{node_digest.hash}/{node_digest.size_bytes}]")
        else:
            click.echo(f"Error: Failed pushing path=[{node_path}]", err=True)


@cli.command('download-file', short_help="Download one or more files from the CAS server. "
                                         "(Specified as a space-separated list of DIGEST FILE_PATH)")
@click.argument('digest-path-list', nargs=-1, type=str, required=True)  # 'digest path' pairs
@click.option('--verify', is_flag=True, show_default=True,
              help="Check downloaded file's integrity.")
@pass_context
def download_file(context, digest_path_list, verify):
    # Downloading files:
    downloaded_files = {}
    try:
        with download(context.channel, instance=context.instance_name) as downloader:
            for (digest_string, file_path) in zip(digest_path_list[0::2],
                                                  digest_path_list[1::2]):
                if os.path.exists(file_path):
                    click.echo("Error: Invalid value for " +
                               f"path=[{file_path}] already exists.", err=True)
                    continue

                digest = parse_digest(digest_string)

                downloader.download_file(digest, file_path)
                downloaded_files[file_path] = digest
    except NotFoundError:
        click.echo('Error: Blob not found in CAS', err=True)
        sys.exit(-1)
    except Exception as e:
        click.echo(f'Error: Downloading file: {e}', err=True)
        sys.exit(-1)

    # Verifying:
    for (file_path, digest) in downloaded_files.items():
        if verify:
            file_digest = create_digest(read_file(file_path))
            if file_digest != digest:
                click.echo(f"Error: Failed to verify path=[{file_path}]", err=True)
                continue

        if os.path.isfile(file_path):
            click.echo(f"Success: Pulled path=[{file_path}] from digest="
                       f"[{digest.hash}/{digest.size_bytes}]")
        else:
            click.echo(f'Error: Failed pulling "{file_path}"', err=True)


@cli.command('download-dir', short_help="Download a directory from the CAS server.")
@click.argument('digest-string', nargs=1, type=click.STRING, required=True)
@click.argument('directory-path', nargs=1, type=click.Path(exists=False), required=True)
@click.option('--verify', is_flag=True, show_default=True,
              help="Check downloaded directory's integrity.")
@pass_context
def download_directory(context, digest_string, directory_path, verify):
    if os.path.exists(directory_path):
        if not os.path.isdir(directory_path) or os.listdir(directory_path):
            click.echo("Error: Invalid value, " +
                       f"path=[{directory_path}] already exists.", err=True)
            return

    digest = parse_digest(digest_string)

    try:
        with download(context.channel, instance=context.instance_name) as downloader:
            downloader.download_directory(digest, directory_path)
    except Exception as e:
        click.echo(f'Error: Downloading directory: {e}', err=True)
        sys.exit(-1)

    if verify:
        last_directory_node = None
        for node, _, _ in merkle_tree_maker(directory_path):
            if node.DESCRIPTOR is remote_execution_pb2.DirectoryNode.DESCRIPTOR:
                last_directory_node = node
        if last_directory_node.digest != digest:
            click.echo(f"Error: Failed to verify path=[{directory_path}]", err=True)
            return

    if os.path.isdir(directory_path):
        click.echo(f"Success: Pulled path=[{directory_path}] from digest="
                   f"[{digest.hash}/{digest.size_bytes}]")
    else:
        click.echo(f"Error: Failed pulling path=[{directory_path}]", err=True)
