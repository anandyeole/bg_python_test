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


import sys
from typing import Optional, Sequence

from aiohttp import web
import click

from buildgrid._exceptions import InvalidArgumentError
from buildgrid.client.channel import setup_channel
from buildgrid.browser.app import create_app
from buildgrid.browser.utils import ResponseCache

from ..cli import Context, pass_context, setup_logging


pass_cache = click.make_pass_decorator(ResponseCache, ensure=True)


@click.group(name='browser-backend', short_help="bgd-browser backend API.")
@click.option('--remote', type=click.STRING, default='http://localhost:50051', show_default=True,
              help="Remote server's URL (port defaults to 50051 if not specified).")
@click.option('--remote-cas', type=click.STRING, default=None, show_default=False,
              help="Remote CAS server's URL, defaults to `--remote` if unspecified.")
@click.option('--remote-operations', type=click.STRING, default=None, show_default=False,
              help="Remote Operations server's URL, defaults to `--remote` if unspecified.")
@click.option('--remote-cache', type=click.STRING, default=None, show_default=False,
              help="Remote ActionCache server's URL, defaults to `--remote` if unspecified.")
@click.option('--remote-logstream', type=click.STRING, default=None, show_default=False,
              help="Remote LogStream server's URL, defaults to `--remote` if unspecified.")
@click.option('--auth-token', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Authorization token for the remote.")
@click.option('--client-key', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Private client key for TLS (PEM-encoded).")
@click.option('--client-cert', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Public client certificate for TLS (PEM-encoded).")
@click.option('--server-cert', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Public server certificate for TLS (PEM-encoded).")
@click.option('--instance-name', type=click.STRING, default='', show_default=True,
              help="Targeted BuildGrid instance name.")
@pass_context
def cli(context: Context, remote: str, remote_cas: str, remote_operations: str, remote_cache: str,
        remote_logstream: str, instance_name: str, auth_token: Optional[str],
        client_key: Optional[str], client_cert: Optional[str], server_cert: Optional[str]) -> None:
    try:
        context.channel, _ = setup_channel(remote, auth_token=auth_token,
                                           client_key=client_key,
                                           client_cert=client_cert,
                                           server_cert=server_cert,
                                           asynchronous=True)

        if remote_cas and remote_cas != remote:
            context.cas_channel, _ = setup_channel(remote_cas, auth_token=auth_token,
                                                   client_key=client_key,
                                                   client_cert=client_cert,
                                                   server_cert=server_cert,
                                                   asynchronous=True)
        else:
            context.cas_channel = context.channel

        if remote_operations and remote_operations != remote:
            context.operations_channel, _ = setup_channel(remote_operations, auth_token=auth_token,
                                                          client_key=client_key,
                                                          client_cert=client_cert,
                                                          server_cert=server_cert,
                                                          asynchronous=True)
        else:
            context.operations_channel = context.channel

        if remote_cache and remote_cache != remote:
            context.cache_channel, _ = setup_channel(remote_cache, auth_token=auth_token,
                                                     client_key=client_key,
                                                     client_cert=client_cert,
                                                     server_cert=server_cert,
                                                     asynchronous=True)
        else:
            context.cache_channel = context.channel

        if remote_logstream and remote_logstream != remote:
            context.logstream_channel, _ = setup_channel(remote_logstream, auth_token=auth_token,
                                                         client_key=client_key,
                                                         client_cert=client_cert,
                                                         server_cert=server_cert,
                                                         asynchronous=True)
        else:
            context.logstream_channel = context.channel

    except InvalidArgumentError as e:
        click.echo(f"Error: {e}.", err=True)
        sys.exit(-1)

    context.instance_name = instance_name


@cli.command('serve', short_help="Serve the bgd-browser backend.")
@click.option('--port', '-p', type=click.INT, default=8083, show_default=True,
              help="Port to serve the API on")
@click.option('--cors-allowed-origin', '-o', multiple=True, default=[], show_default=True,
              help="URLs to allow CORS requests from.")
@click.option('--allow-cancelling-operations', default=False, show_default=True,
              help="Forward DELETE requests to the 'operations/' endpoint as CancelOperation() RPCs.")
@click.option('-v', '--verbose', count=True,
              help='Increase log verbosity level.')
@click.option('--static-path', '-s', type=click.STRING, default=None,
              help='Optional path to a directory to serve at /')
@click.option('--tarball-directory', '-t', type=click.STRING, default=None,
              help='Path to a directory to use when constructing tarballs')
@pass_cache
@pass_context
def serve(context: Context, cache: ResponseCache, port: int, verbose: int, cors_allowed_origin: Sequence,
          allow_cancelling_operations: bool, static_path: Optional[str],
          tarball_directory: Optional[str]) -> None:
    setup_logging(verbosity=verbose)
    app = create_app(
        context,
        cache,
        cors_allowed_origin,
        allow_cancelling_operations=allow_cancelling_operations,
        static_path=static_path,
        tarball_dir=tarball_directory
    )
    web.run_app(app, port=port)
