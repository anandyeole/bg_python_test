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
Server command
=================

Create a BuildGrid server.
"""

import os
import sys
import logging
import functools

import click
from jsonschema.exceptions import ValidationError

from buildgrid._exceptions import PermissionDeniedError
from buildgrid.server._authentication import AuthMetadataMethod, AuthMetadataAlgorithm
from buildgrid.server.server import Server
from buildgrid.server.monitoring import MonitoringOutputType, MonitoringOutputFormat, StatsDTagFormat
from buildgrid.utils import read_file

from ..cli import pass_context, setup_logging
from ..settings import parser


@click.group(name='server', short_help="Start a local server instance.")
@pass_context
def cli(context):
    pass


@cli.command('start', short_help="Setup a new server instance.")
@click.argument('CONFIG',
                type=click.Path(file_okay=True, dir_okay=False, exists=True, writable=False))
@click.option('-v', '--verbose', count=True,
              help='Increase log verbosity level.')
@click.option('--pid-file', type=click.Path(dir_okay=False),
              help='Path to PID file')
@pass_context
def start(context, config, verbose, pid_file):
    """Entry point for the bgd-server CLI command group."""
    setup_logging(verbosity=verbose)

    click.echo(f"\nLoading config from {config}")

    with open(config, encoding='utf-8') as f:
        settings = parser.get_parser().safe_load(f)

    validator = parser.get_validator()
    try:
        validator.validate(instance=settings)
    except ValidationError as e:
        click.echo(click.style(
            f"ERROR: Config ({config}) failed validation: {e}",
            fg="red", bold=True
        ), err=True)
        sys.exit(-1)

    try:
        server = _create_server_from_config(settings)

    except KeyError as e:
        click.echo(f"ERROR: Could not parse config: {e}.\n", err=True)
        sys.exit(-1)

    try:
        on_server_start_cb = functools.partial(_create_new_pid_file, pid_file)
        click.echo(click.style("Starting BuildGrid server...", fg="green", bold=True))
        server.start(on_server_start_cb=on_server_start_cb)

    except KeyboardInterrupt:
        pass

    finally:
        server.stop()
        _remove_old_pid_file(pid_file)


def _remove_old_pid_file(pid_file):
    """Remove pid_file if it's set """
    if not pid_file:
        return

    logger = logging.getLogger(__name__)
    try:
        os.remove(pid_file)
    except os.error as e:
        logger.error(f"Error deleting pid-file \"{pid_file}\", exception = [{e}]")


def _create_new_pid_file(pid_file):
    logger = logging.getLogger(__name__)
    if pid_file:
        with open(pid_file, "w", encoding='utf-8') as f:
            f.write(str(os.getpid()))

        logger.info(f"Created pid-file \"{pid_file}\"")


def _create_server_from_config(configuration):
    """Parses configuration and setup a fresh server instance."""
    kargs = {}

    try:
        network = configuration['server']
        instances = configuration['instances']

    except KeyError as e:
        click.echo(f"Error: Section missing from configuration: {e}.", err=True)
        sys.exit(-1)

    if 'authorization' in configuration:
        authorization = configuration['authorization']

        try:
            if 'method' in authorization:
                kargs['auth_method'] = AuthMetadataMethod(authorization['method'])

            if 'secret' in authorization:
                kargs['auth_secret'] = read_file(authorization['secret']).decode().strip()

            if 'jwks-url' in authorization:
                kargs['auth_jwks_url'] = str(authorization['jwks-url'])

            if 'audience' in authorization:
                kargs['auth_audience'] = str(authorization['audience'])

            if 'jwks-fetch-minutes' in authorization:
                kargs['auth_jwks_fetch_minutes'] = int(authorization['jwks-fetch-minutes'])

            if 'algorithm' in authorization:
                kargs['auth_algorithm'] = AuthMetadataAlgorithm(authorization['algorithm'])

        except (ValueError, OSError) as e:
            click.echo(f"Error: Configuration, {e}.", err=True)
            sys.exit(-1)

    if 'monitoring' in configuration:
        monitoring = configuration['monitoring']

        try:
            if 'enabled' in monitoring:
                kargs['monitor'] = monitoring['enabled']

            if 'endpoint-type' in monitoring:
                kargs['mon_endpoint_type'] = MonitoringOutputType(monitoring['endpoint-type'])

            if 'endpoint-location' in monitoring:
                kargs['mon_endpoint_location'] = monitoring['endpoint-location']

            if 'serialization-format' in monitoring:
                kargs['mon_serialisation_format'] = MonitoringOutputFormat(monitoring['serialization-format'])

            if 'metric-prefix' in monitoring:
                # Ensure there's only one period at the end of the prefix
                kargs['mon_metric_prefix'] = monitoring['metric-prefix'].strip().rstrip('.') + "."

            if 'tag-format' in monitoring:
                kargs['mon_tag_format'] = StatsDTagFormat(monitoring['tag-format'])

        except (ValueError, OSError) as e:
            click.echo(f"Error: Configuration, {e}.", err=True)
            sys.exit(-1)

    if 'thread-pool-size' in configuration:
        try:
            kargs['max_workers'] = int(configuration['thread-pool-size'])

        except ValueError as e:
            click.echo(f"Error: Configuration, {e}.", err=True)
            sys.exit(-1)

    if 'server-reflection' in configuration:
        try:
            kargs['enable_server_reflection'] = bool(configuration['server-reflection'])

        except ValueError as e:
            click.echo(f"Error: Configuration, {e}.", err=True)
            sys.exit(-1)

    server = Server(**kargs)

    try:
        for channel in network:
            server.add_port(channel.address, channel.credentials)

    except PermissionDeniedError as e:
        click.echo(f"Error: {e}.", err=True)
        sys.exit(-1)

    for instance in instances:
        instance_name = instance['name']
        services = instance['services']

        for service in services:
            service.register_instance_with_server(instance_name, server)

    return server
