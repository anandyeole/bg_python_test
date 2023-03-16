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


import os
import sys
from typing import Dict, Optional, Set

import click
from jsonschema import validators, Draft7Validator, ValidationError
import pika  # type: ignore
import yaml

from buildgrid._types import LogStreamConnectionConfig
from buildgrid.server.cas.storage.lru_memory_cache import LRUMemoryCache
from buildgrid.server.persistence.sql.impl import SQLDataStore
from buildgrid.server.rabbitmq.bots.instance import BotsInstance
from buildgrid.server.rabbitmq.operations.instance import OperationsInstance
from ..settings.parser import YamlFactory, Channel, check_type, get_logstream_connection_info

RABBITMQ_CONFIG_SCHEMA_FILENAME = 'config-rmq.yaml'
RABBITMQ_CONNECTION_SCHEMA_FILENAME = 'rabbitmq-connection.yaml'

RABBITMQ_DEFAULT_PORT = 5672


class RmqYamlFactory(YamlFactory):
    @classmethod
    def _validate(cls, values):
        click.echo(click.style(
            f"\nValidating {cls.yaml_tag}...", fg="yellow"
        ))
        schema = cls._get_schema()
        validator = get_validator(schema=schema)
        try:
            validator.validate(instance=values)
        except ValidationError as e:
            click.echo(click.style(
                f"ERROR: {cls.yaml_tag} failed validation: {e}",
                fg="red", bold=True
            ), err=True)
            sys.exit(-1)


class RabbitMqConnection(RmqYamlFactory):
    yaml_tag = u'!rabbitmq-connection'
    schema = os.path.join('misc', RABBITMQ_CONNECTION_SCHEMA_FILENAME)

    def __init__(self, _yaml_filename: str,
                 address: str,
                 port: Optional[int]=None,
                 virtual_host: Optional[str]=None,
                 credentials=None, external_credentials=None):

        self.address = address
        self.port = port or RABBITMQ_DEFAULT_PORT
        self.virtual_host = virtual_host
        self.credentials = None
        if credentials:
            self.credentials = pika.credentials.PlainCredentials(credentials['username'], credentials['password'])
        self.external_credentials = external_credentials

    def to_pika_connection_parameters(self) -> pika.ConnectionParameters:
        pika_params = pika.ConnectionParameters(host=self.address,
                                                port=self.port,
                                                virtual_host=self.virtual_host)
        if self.external_credentials:
            pika_params.credentials = self.external_credentials
        return pika_params


class BotsService(RmqYamlFactory):
    yaml_tag = u'!bots-service'
    schema = os.path.join('services', 'rabbitmq', 'bots.yaml')

    def __new__(cls, _yaml_filename: str, rabbitmq: RabbitMqConnection,
                platform_queues_file: str,
                logstream: LogStreamConnectionConfig=None):
        if logstream is None:
            logstream = {}
        logstream_url, logstream_credentials, logstream_instance_name = get_logstream_connection_info(logstream)

        click.echo(click.style(
            f"Creating a Bots service using RabbitMQ on {rabbitmq.address}:{rabbitmq.port}\n",
            fg="green", bold=True
        ))

        if not os.path.isabs(platform_queues_file):
            # Resolving the path to the queue-definition file as
            # relative to the config file:
            config_directory = os.path.dirname(_yaml_filename)
            platform_queues_file = os.path.join(config_directory, platform_queues_file)

        platform_queues = None
        try:
            platform_queues = cls.read_platform_queues_from_file(platform_queues_file)
            click.echo(f"Read queues for {len(platform_queues.keys())} "
                       f"platform/s from '{platform_queues_file}': "
                       f"{platform_queues}")
        except Exception as e:
            click.echo(f"Failed to read platform queues file : {e}", err=True)
            sys.exit(-1)

        return BotsInstance(
            rabbitmq,
            platform_queues,
            logstream_url,
            logstream_credentials,
            logstream_instance_name
        )

    @classmethod
    def read_platform_queues_from_file(cls, config_file_path: str) -> Dict[str, Set[str]]:
        """Parse a config file that associates a semicolon-separated string
        representation of a platform to one or more RabbitMQ queues.
        Returns a dictionary from platform property strings to a list of
        queue names.
        On errors raises `jsonschema.ValidationError`.
        """
        schema_file_path = os.path.join(os.path.dirname(__file__),
                                        'schemas', 'services', 'platform-queues.yml')

        with open(schema_file_path, encoding='utf-8') as schema_file:
            schema = yaml.safe_load(schema_file)

        with open(config_file_path, encoding='utf-8') as config_file:
            config = yaml.safe_load(config_file)

        validator = get_validator(schema=schema)
        validator.validate(config)

        queues: Dict[str, Set[str]] = {}  # [platform properties] -> [queues]
        for entry in config['platform-queues']:
            platform_properties = entry['platform']
            platform_queues = entry['queues']

            if platform_properties in queues:
                raise ValidationError(f"Multiple definitions for platform "
                                      f"'{platform_properties}'")

            if not cls.platform_string_is_valid(platform_properties):
                raise ValidationError(f"Platform '{platform_properties}' "
                                      f"is not valid")

            for queue in platform_queues:
                if not cls.platform_string_is_valid(queue):
                    raise ValidationError(f"Queue name '{queue}' "
                                          f"for platform '{platform_properties}' "
                                          f"is not valid")

            queues[platform_properties] = set(platform_queues)

        return queues

    @classmethod
    def platform_string_is_valid(cls, platform_string: str) -> bool:
        """Given a string of semicolon-separated 'key=value' assignments, return
        whether all the key get assigned non-empty values and the keys appear
        sorted in the string.

        For example: 'osfamily=linux;isa=x86_64' returns False, and so does
        'osfamily='. A valid example is 'isa=x86_64;osfamily=linux'.
        """
        try:
            previous_key = None
            assignments = platform_string.split(';')
            for assignment in assignments:
                key, _ = assignment.split('=')
                if previous_key and previous_key >= key:
                    return False
                previous_key = key
        except ValueError:
            return False

        return True


class OperationsSQLDataStoreConfig(YamlFactory):
    """Generates :class:`buildgrid.server.persistence.sql.impl.SQLDataStore` using
    the tag ``!sql-data-store``.

    Usage
        .. code:: yaml

            - !operations-sql-storage
              connection_string: postgresql://bgd:insecure@database/bgd
              automigrate: yes
              connection_timeout: 5

    Args:
        connection_string (str): SQLAlchemy connection string to use for connecting
            to the database.
        automigrate (bool): Whether to attempt to automatically upgrade an existing
            DB schema to the newest version (this will also create everything from
            scratch if given an empty database).
        connection_timeout (int): Time to wait for an SQLAlchemy connection to be
            available in the pool before timing out.

    """

    yaml_tag = u'!operations-sql-data-store'
    schema = os.path.join('storage', 'operations_sql_storage.yaml')

    def __new__(cls, connection_string: Optional[str]=None, automigrate: bool=False,
                connection_timeout: int=5, **kwargs):
        click.echo(click.style(
            "Creating an SQL scheduler backend\n",
            fg="green", bold=True
        ))
        try:
            storage = LRUMemoryCache(limit=0)
            return SQLDataStore(storage,
                                connection_string=connection_string,
                                automigrate=automigrate,
                                connection_timeout=connection_timeout,
                                **kwargs)
        except TypeError as type_error:
            click.echo(type_error, err=True)
            sys.exit(-1)


class OperationsService(RmqYamlFactory):
    yaml_tag = u'!operations-service'
    schema = os.path.join('services', 'rabbitmq', 'operations.yaml')

    def __new__(cls,
                rabbitmq: RabbitMqConnection,
                data_store: SQLDataStore,
                instance_name: Optional[str] = None,
                max_list_operations_page_size: Optional[int]=None):
        click.echo(click.style(
            f"Creating an Operations service using RabbitMQ on "
            f"{rabbitmq.address}:{rabbitmq.port}\n", fg="green", bold=True))

        return OperationsInstance(instance_name=instance_name or "",
                                  operations_datastore=data_store,
                                  rabbitmq_connection_parameters=rabbitmq.to_pika_connection_parameters(),
                                  max_list_operations_page_size=max_list_operations_page_size or 1000)


def get_parser():
    yaml.SafeLoader.add_constructor(Channel.yaml_tag, Channel.from_yaml)
    yaml.SafeLoader.add_constructor(RabbitMqConnection.yaml_tag, RabbitMqConnection.from_yaml)
    yaml.SafeLoader.add_constructor(BotsService.yaml_tag, BotsService.from_yaml)
    yaml.SafeLoader.add_constructor(OperationsService.yaml_tag, OperationsService.from_yaml)
    yaml.SafeLoader.add_constructor(OperationsSQLDataStoreConfig.yaml_tag, OperationsSQLDataStoreConfig.from_yaml)
    return yaml


def get_schema():
    path = os.path.join(os.path.dirname(__file__), 'schemas', RABBITMQ_CONFIG_SCHEMA_FILENAME)
    with open(path, encoding='utf-8') as schema_file:
        schema = yaml.safe_load(schema_file)
    return schema


def get_validator(schema=None):
    if schema is None:
        schema = get_schema()

    type_checker = Draft7Validator.TYPE_CHECKER.redefine_many(definitions={
        'rabbitmq-connection': check_type(RabbitMqConnection)
    })

    BgdValidator = validators.create(
        meta_schema=Draft7Validator.META_SCHEMA,
        validators=dict(Draft7Validator.VALIDATORS),
        type_checker=type_checker
    )

    return BgdValidator(schema)
