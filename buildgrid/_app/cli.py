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
CommandLineInterface
===================

Any files in the commands/ folder with the name cmd_*.py
will be attempted to be imported.
"""

import importlib
import logging
import os
import sys

import click

from buildgrid.settings import LOG_RECORD_FORMAT

CONTEXT_SETTINGS = dict(auto_envvar_prefix='BUILDGRID')


class Context:

    def __init__(self):
        self.verbose = False
        self.channel = None
        self.cas_channel = None
        self.operations_channel = None
        self.cache_channel = None
        self.logstream_channel = None
        self.instance_name = ''

        self.user_home = os.getcwd()


pass_context = click.make_pass_decorator(Context, ensure=True)
cmd_folder = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                          'commands'))


class App(click.MultiCommand):

    def list_commands(self, ctx):
        """Lists available command names."""
        commands = []
        for filename in os.listdir(cmd_folder):
            if filename.endswith('.py') and filename.startswith('cmd_'):
                command_name = filename[4:-3].replace('_', '-')
                commands.append(command_name)
        commands.sort()

        return commands

    def get_command(self, ctx, cmd_name):
        """Looks-up and loads a particular command by name."""
        cmd_name = cmd_name.replace('-', '_')

        try:
            module = importlib.import_module(
                f'buildgrid._app.commands.cmd_{cmd_name}')

        except ImportError as e:
            if cmd_name not in self.list_commands(ctx):
                click.echo(f"Error: No such command: [{cmd_name}].", err=True)
            else:
                click.echo(f"Error: {e}.", err=True)
            sys.exit(-1)

        return module.cli


class DebugFilter(logging.Filter):

    def __init__(self, debug_domains, name=''):
        super().__init__(name=name)
        self.__domains_tree = {}

        for domain in debug_domains.split(':'):
            domains_tree = self.__domains_tree
            for label in domain.split('.'):
                if all(key not in domains_tree for key in [label, '*']):
                    domains_tree[label] = {}
                domains_tree = domains_tree[label]

    def filter(self, record):
        # Only evaluate DEBUG records for filtering
        if record.levelname != "DEBUG":
            return True
        domains_tree, last_match = self.__domains_tree, None
        for label in record.name.split('.'):
            if all(key not in domains_tree for key in [label, '*']):
                return False
            last_match = label if label in domains_tree else '*'
            domains_tree = domains_tree[last_match]
        if domains_tree and '*' not in domains_tree:
            return False
        return True


def setup_logging(verbosity=0, debug_mode=False):
    """Deals with loggers verbosity"""
    asyncio_logger = logging.getLogger('asyncio')
    root_logger = logging.getLogger()

    log_handler = logging.StreamHandler(stream=sys.stdout)
    for log_filter in root_logger.filters:
        log_handler.addFilter(log_filter)

    logging.basicConfig(format=LOG_RECORD_FORMAT, handlers=[log_handler])

    if verbosity == 1:
        root_logger.setLevel(logging.WARNING)
    elif verbosity == 2:
        root_logger.setLevel(logging.INFO)
    elif verbosity >= 3:
        root_logger.setLevel(logging.DEBUG)
    else:
        root_logger.setLevel(logging.ERROR)

    if not debug_mode:
        asyncio_logger.setLevel(logging.CRITICAL)
    else:
        asyncio_logger.setLevel(logging.DEBUG)
        root_logger.setLevel(logging.DEBUG)


@click.command(cls=App, context_settings=CONTEXT_SETTINGS)
@pass_context
def cli(context):
    """BuildGrid's client and server CLI front-end."""
    root_logger = logging.getLogger()

    # Clean-up root logger for any pre-configuration:
    for log_handler in root_logger.handlers[:]:
        root_logger.removeHandler(log_handler)
    for log_filter in root_logger.filters[:]:
        root_logger.removeFilter(log_filter)

    # Filter debug messages using BGD_MESSAGE_DEBUG value:
    debug_domains = os.environ.get('BGD_MESSAGE_DEBUG', None)
    if debug_domains:
        root_logger.addFilter(DebugFilter(debug_domains))
