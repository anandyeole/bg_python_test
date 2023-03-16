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
RabbitMQ Server command
=======================

Create a BuildGrid server that is powered by RabbitMQ.
"""

import functools
import sys

import click
from jsonschema.exceptions import ValidationError

from buildgrid._app.commands.cmd_server import _create_new_pid_file, _remove_old_pid_file
from buildgrid.server.rabbitmq.server import RMQServer
from ..cli import pass_context, setup_logging
from ..settings import rmq_parser as parser


@click.group(name='rmq-server', short_help="Start a local server instance that is powered by RabbitMQ (experimental).")
@pass_context
def cli(context):
    pass


@cli.command('start', short_help="Setup instance of a server that uses RabbitMQ.")
@click.argument('CONFIG',
                type=click.Path(file_okay=True, dir_okay=False, exists=True, writable=False))
@click.option('-v', '--verbose', count=True,
              help='Increase log verbosity level.')
@click.option('--pid-file', type=click.Path(dir_okay=False),
              help='Path to PID file')
@pass_context
def start(context, config, verbose, pid_file):
    """Entry point for the bgd rmq-server CLI command group."""
    setup_logging(verbosity=verbose)

    settings = read_settings_from_config(config)

    try:
        server = _create_server_from_config(settings)
    except KeyError as e:
        click.echo(f"ERROR: Could not parse config: {e}.\n", err=True)
        sys.exit(-1)

    try:
        on_server_start_cb = functools.partial(_create_new_pid_file, pid_file)
        click.echo(click.style("Starting BuildGrid server (powered by RabbitMQ)...", fg="green", bold=True))
        server.start(on_server_start_cb=on_server_start_cb)
    except KeyboardInterrupt:
        pass

    finally:
        server.stop()
        _remove_old_pid_file(pid_file)


def read_settings_from_config(config):
    click.echo(f"\nLoading load config from {config}")

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
    return settings


def _create_server_from_config(configuration):
    """Parses configuration and setup a fresh server instance."""
    kwargs = {}

    max_workers = configuration.get('thread-pool-size')
    if max_workers is not None:
        kwargs['max_workers'] = int(max_workers)

    try:
        bind = configuration['server']['bind']
        rabbitmq_connection = configuration['server']['rabbitmq']
        instances = configuration['instances']
        click.echo(f"Starting server listening on: {bind}, "
                   f"connected to RabbitMQ server {rabbitmq_connection}.")

    except KeyError as e:
        click.echo(f"Error: Section missing from configuration: {e}.", err=True)
        sys.exit(-1)

    server = RMQServer(**kwargs)
    server.add_port(bind.address, bind.credentials)

    for instance in instances:
        name = instance['name']
        services = instance['services']

        for service_instance in services:
            service_instance.register_instance_with_server(name, server)

    return server
