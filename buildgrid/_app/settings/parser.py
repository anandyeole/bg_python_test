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


import os
import sys
from typing import Any, Callable, Dict, List, Optional, Type
from urllib.parse import urlparse

import click
from jsonschema import validators, Draft7Validator
from jsonschema.exceptions import ValidationError
# TODO: Use the standard library version of this when we drop support
# for Python 3.7
from typing_extensions import TypedDict
import yaml

from buildgrid._enums import ActionCacheEntryType
from buildgrid.server.bots.instance import BotsInterface
from buildgrid.server.controller import ExecutionController
from buildgrid.server.actioncache.instance import ActionCache
from buildgrid.server.actioncache.caches.action_cache_abc import ActionCacheABC
from buildgrid.server.actioncache.caches.lru_cache import LruActionCache
from buildgrid.server.actioncache.caches.remote_cache import RemoteActionCache
from buildgrid.server.actioncache.caches.s3_cache import S3ActionCache
from buildgrid.server.actioncache.caches.write_once_cache import WriteOnceActionCache
from buildgrid.server.build_events.storage import BuildEventStreamStorage
from buildgrid.server.actioncache.caches.with_cache import WithCacheActionCache
from buildgrid.server.referencestorage.storage import ReferenceCache
from buildgrid.server.cas.instance import ByteStreamInstance, ContentAddressableStorageInstance
from buildgrid.server.cas.storage.disk import DiskStorage
from buildgrid.server.cas.storage.lru_memory_cache import LRUMemoryCache
from buildgrid.server.cas.storage.remote import RemoteStorage
from buildgrid.server.cas.storage.s3 import S3Storage
from buildgrid.server.cas.storage.size_differentiated import (
    SizeDifferentiatedStorage,
    SizeLimitedStorageType
)
from buildgrid.server.cas.storage.storage_abc import StorageABC
from buildgrid.server.cas.storage.with_cache import WithCacheStorage
from buildgrid.server.cas.storage.index.sql import SQLIndex
from buildgrid.server.cas.logstream.instance import LogStreamInstance
from buildgrid.server.cas.logstream.stream_storage.stream_storage_abc import StreamStorageABC
from buildgrid.server.cas.logstream.stream_storage.memory import MemoryStreamStorage
from buildgrid.server.persistence.interface import DataStoreInterface
from buildgrid.server.persistence.mem.impl import MemoryDataStore
from buildgrid.server.persistence.sql.impl import (PruningOptions, SQLDataStore)
from buildgrid.settings import (
    DEFAULT_PLATFORM_PROPERTY_KEYS,
    DEFAULT_MAX_EXECUTION_TIMEOUT,
    DEFAULT_MAX_LIST_OPERATION_PAGE_SIZE,
    S3_USERAGENT_NAME,
    S3_MAX_RETRIES,
    S3_TIMEOUT_CONNECT,
    S3_TIMEOUT_READ
)
from buildgrid.utils import insecure_uri_schemes, secure_uri_schemes
from ..._enums import ServiceName


class YamlFactory(yaml.YAMLObject):
    """ Base class for contructing maps or scalars from tags.
    """

    yaml_tag: Optional[str] = None
    schema: Optional[str] = None

    @classmethod
    def _get_schema(cls):
        schema = {}
        if cls.schema is not None:
            path = os.path.join(os.path.dirname(__file__), 'schemas', cls.schema)
            with open(path, encoding='utf-8') as schema_file:
                schema = yaml.safe_load(schema_file)
        return schema

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

    @classmethod
    def from_yaml(cls, loader, node):
        yaml_filename = loader.name
        # We'll pass the name of the file being parsed as
        # `_yaml_filename`.
        # (Enables things like resolving a path in the config
        # relative to the YAML file itself.)

        if isinstance(node, yaml.ScalarNode):
            value = loader.construct_scalar(node)
            return cls(_yaml_filename=yaml_filename, path=value)

        else:
            values = loader.construct_mapping(node, deep=True)
            cls._validate(values)
            for key, value in dict(values).items():
                values[key.replace('-', '_')] = values.pop(key)

            values['_yaml_filename'] = yaml_filename
            return cls(**values)


class Channel(YamlFactory):
    """Creates a GRPC channel.

    The :class:`Channel` class returns a `grpc.Channel` and is generated from
    the tag ``!channel``. Creates either a secure or insecure channel.

    Usage
        .. code:: yaml

            - !channel
              address (str): Address for the channel. (For example,
                'localhost:50055' or 'unix:///tmp/sock')
              port (int): A port for the channel (only if no address was specified).
              insecure-mode: false
              credentials:
                tls-server-key: !expand-path ~/.config/buildgrid/server.key
                tls-server-cert: !expand-path ~/.config/buildgrid/server.cert
                tls-client-certs: !expand-path ~/.config/buildgrid/client.cert

    Args:
        port (int): A port for the channel.
        insecure_mode (bool): If ``True``, generates an insecure channel, even
            if there are credentials. Defaults to ``True``.
        credentials (dict, optional): A dictionary in the form::

            tls-server-key: /path/to/server-key
            tls-server-cert: /path/to/server-cert
            tls-client-certs: /path/to/client-certs
    """

    yaml_tag = u'!channel'
    schema = os.path.join('misc', 'channel.yaml')

    def __init__(self, _yaml_filename: str,
                 insecure_mode: bool,
                 address: Optional[str]=None,
                 port: Optional[int]=None, credentials=None):
        # TODO: When safe, deprecate the `port` option.
        if port:
            click.echo(click.style(
                "Warning: the 'port' option will be deprecated. "
                f"Consider specifying 'address: localhost:{port}' instead.",
                fg="bright_yellow"))

        self.address = address if address else f'[::]:{port}'
        self.credentials = None

        if not insecure_mode:
            self.credentials = credentials
            _validate_server_credentials(self.credentials)


class ExpandPath(YamlFactory):
    """Returns a string of the user's path after expansion.

    The :class:`ExpandPath` class returns a string and is generated from the
    tag ``!expand-path``.

    Usage
        .. code:: yaml

            path: !expand-path ~/bgd-data/cas

    Args:
        path (str): Can be used with strings such as: ``~/dir/to/something``
            or ``$HOME/certs``
    """

    yaml_tag = u'!expand-path'

    def __new__(cls, _yaml_filename: str, path: str):
        path = os.path.expanduser(path)
        path = os.path.expandvars(path)
        return path


class ExpandVars(YamlFactory):
    """Expand environment variables in a string.

    The :class:`ExpandVars` class returns a string and is generated from the
    tag ``!expand-vars``.

    Usage
        .. code:: yaml

            endpoint: !expand-vars $ENDPOINT

    Args:
        path (str): Can be used with strings such as: ``http://$ENDPOINT``
    """

    yaml_tag = u'!expand-vars'

    def __new__(cls, _yaml_filename: str, path: str):
        return os.path.expandvars(path)


class ReadFile(YamlFactory):
    """Returns a string of the contents of the specified file.

    The :class:`ReadFile` class returns a string and is generated from the
    tag ``!read-file``.

    Usage
        .. code:: yaml

            secret_key: !read-file /var/bgd/s3-secret-key

    Args:
        path (str): Can be used with strings such as: ``~/path/to/some/file``
            or ``$HOME/myfile`` or ``/path/to/file``
    """

    yaml_tag = u'!read-file'

    def __new__(cls, _yaml_filename: str, path):
        # Expand path
        path = os.path.expanduser(path)
        path = os.path.expandvars(path)

        if not os.path.exists(path):
            click.echo(click.style(
                f"ERROR: read-file `{path}` failed due to it not existing or "
                "bad permissions.", fg="red", bold=True),
                err=True)
            sys.exit(-1)
        else:
            with open(path, 'r', encoding='utf-8') as file:
                try:
                    file_contents = "\n".join(file.readlines()).strip()
                    return file_contents
                except IOError as e:
                    click.echo(f"ERROR: read-file failed to read file `{path}`: {e}", err=True)
                    sys.exit(-1)


class Disk(YamlFactory):
    """Generates :class:`buildgrid.server.cas.storage.disk.DiskStorage` using the tag ``!disk-storage``.

    Usage
        .. code:: yaml

            - !disk-storage
              path: /opt/bgd/cas-storage

    Args:
        path (str): Path to directory to storage.

    """

    yaml_tag = u'!disk-storage'
    schema = os.path.join('storage', 'disk.yaml')

    def __new__(cls, _yaml_filename: str, path: str):
        """Creates a new disk

        Args:
           path (str): Some path
        """
        return DiskStorage(path)


class LRU(YamlFactory):
    """Generates :class:`buildgrid.server.cas.storage.lru_memory_cache.LRUMemoryCache` using the tag ``!lru-storage``.

    Usage
        .. code:: yaml

            - !lru-storage
              size: 2048M

    Args:
        size (int): Size e.g ``10kb``. Size parsed with
            :meth:`buildgrid._app.settings.parser._parse_size`.
    """

    yaml_tag = u'!lru-storage'
    schema = os.path.join('storage', 'lru.yaml')

    def __new__(cls, _yaml_filename: str, size: str):
        return LRUMemoryCache(_parse_size(size))


class S3(YamlFactory):
    """Generates :class:`buildgrid.server.cas.storage.s3.S3Storage` using the tag ``!s3-storage``.

    Usage
        .. code:: yaml

            - !s3-storage
              bucket: bgd-bucket-{digest[0]}{digest[1]}
              endpoint: http://127.0.0.1:9000
              access_key: !read-file /var/bgd/s3-access-key
              secret_key: !read-file /var/bgd/s3-secret-key

    Args:
        bucket (str): Name of bucket
        endpoint (str): URL of endpoint.
        access-key (str): S3-ACCESS-KEY
        secret-key (str): S3-SECRET-KEY
    """

    yaml_tag = u'!s3-storage'
    schema = os.path.join('storage', 's3.yaml')

    def __new__(cls, _yaml_filename: str, bucket: str, endpoint: str,
                access_key: str, secret_key: str):
        from botocore.config import Config as BotoConfig  # pylint: disable=import-outside-toplevel

        boto_config = BotoConfig(
            user_agent=S3_USERAGENT_NAME,
            connect_timeout=S3_TIMEOUT_CONNECT,
            read_timeout=S3_TIMEOUT_READ,
            retries={'max_attempts': S3_MAX_RETRIES},
            signature_version='s3v4')

        return S3Storage(bucket,
                         endpoint_url=endpoint,
                         aws_access_key_id=access_key,
                         aws_secret_access_key=secret_key,
                         config=boto_config)


class Redis(YamlFactory):
    """Generates :class:`buildgrid.server.cas.storage.redis.RedisStorage` using the tag ``!redis-storage``.

    Usage
        .. code:: yaml

            - !redis-storage
              host: 127.0.0.1
              port: 6379
              password: !read-file /var/bgd/redis-pass
              db: 0

    Args:
        host (str): hostname of endpoint.
        port (int): port on host.
        password (str): redis database password
        db (int) : db number
    """

    yaml_tag = u'!redis-storage'
    schema = os.path.join('storage', 'redis.yaml')

    def __new__(cls, _yaml_filename: str, host: str, port: int,
                password: Optional[str]=None, db: Optional[int]=None):
        # Import here so there is no global buildgrid dependency on redis
        # # pylint: disable=import-outside-toplevel
        from buildgrid.server.cas.storage.redis import RedisStorage
        return RedisStorage(host=host, port=port, password=password, db=db)


class Remote(YamlFactory):
    """Generates :class:`buildgrid.server.cas.storage.remote.RemoteStorage`
    using the tag ``!remote-storage``.

    Usage
        .. code:: yaml

            - !remote-storage
              url: https://storage:50052/
              instance-name: main
              credentials:
                tls-server-key: !expand-path ~/.config/buildgrid/server.key
                tls-server-cert: !expand-path ~/.config/buildgrid/server.cert
                tls-client-certs: !expand-path ~/.config/buildgrid/client.cert
              channel-options:
                lb-policy-name: round_robin
              request-timeout: 15

    Args:
        url (str): URL to remote storage. If used with ``https``, needs credentials.
        instance_name (str): Instance of the remote to connect to.
        credentials (dict, optional): A dictionary in the form::

           tls-client-key: /path/to/client-key
           tls-client-cert: /path/to/client-cert
           tls-server-cert: /path/to/server-cert
        channel-options (dict, optional): A dictionary of grpc channel options in the form::

          some-channel-option: channel_value
          other-channel-option: another-channel-value
        See https://github.com/grpc/grpc/blob/master/include/grpc/impl/codegen/grpc_types.h
        for the valid channel options
        retries (int): Max number of times to retry (default 3). Backoff between retries is about 2^(N-1),
            where N is the number of attempts
        max_backoff (int): Maximum backoff in seconds (default 64)
        request_timeout (float): gRPC request timeout in seconds (default None)

    """

    yaml_tag = u'!remote-storage'
    schema = os.path.join('storage', 'remote.yaml')

    def __new__(cls, _yaml_filename: str, url: str, instance_name: str,
                credentials: Optional[Dict[str, str]]=None,
                channel_options: Optional[Dict[str, Any]]=None,
                retries: int = 3, max_backoff: int = 64,
                request_timeout: Optional[float] = None):
        options_tuple = None
        if channel_options:
            # Transform the channel options into the format expected
            # by grpc channel creation
            parsed_options = []
            for option_name, option_value in channel_options.items():
                parsed_options.append((f"grpc.{option_name.replace('-','_')}", option_value))
            options_tuple = tuple(parsed_options)
        else:
            options_tuple = ()

        if not _validate_url_and_credentials(url, credentials=credentials):
            sys.exit(-1)

        return RemoteStorage(url, instance_name, options_tuple,
                             credentials, retries, max_backoff, request_timeout)


class WithCache(YamlFactory):
    """Generates :class:`buildgrid.server.cas.storage.with_cache.WithCacheStorage`
    using the tag ``!with-cache-storage``.

    Usage
        .. code:: yaml

            - !with-cache-storage
              cache:
                !lru-storage
                size: 2048M
              fallback:
                !disk-storage
                path: /opt/bgd/cas-storage
              defer-fallback-writes: no

    Args:
        cache (StorageABC): Storage instance to use as a cache
        fallback (StorageABC): Storage instance to use as a fallback on
            cache misses
        defer-fallback-writes (bool): If true, `commit_write` returns once
            writing to the cache is done, and the write into the fallback
            storage is done in a background thread
        fallback-writer-threads (int): The maximum number of threads to use
            for writing blobs into the fallback storage. Defaults to 20.
    """

    yaml_tag = u'!with-cache-storage'
    schema = os.path.join('storage', 'with-cache.yaml')

    def __new__(cls, _yaml_filename: str, cache: StorageABC, fallback: StorageABC,
                defer_fallback_writes: bool = False,
                fallback_writer_threads: int = 20):
        return WithCacheStorage(
            cache, fallback, defer_fallback_writes=defer_fallback_writes,
            fallback_writer_threads=fallback_writer_threads)


_SizeLimitedStorageConfig = TypedDict(
    '_SizeLimitedStorageConfig',
    {'max-size': int, 'storage': StorageABC}
)


class SizeDifferentiated(YamlFactory):
    """Generates :class:`buildgrid.server.cas.storage.size_differentiated.SizeDifferentiatedStorage`
    using the tag ``!size-differentiated-storage``.

    Usage
        .. code:: yaml

            - !size-differentiated-storage
              size-limited-storages:
                - max-size: 1M
                  storage:
                    !lru-storage
                    size: 2048M
              fallback:
                !disk-storage
                path: /opt/bgd/cas-storage

    Args:
        size_limited_storages (list): List of dictionaries. The dictionaries are expected
            to have ``max-size`` and ``storage`` keys, defining a storage provider to use
            to store blobs with size up to ``max-size``.
        fallback (StorageABC): Storage instance to use as a fallback for blobs which
            are too big for the options defined in ``size_limited_storages``.
    """

    yaml_tag = u'!size-differentiated-storage'
    schema = os.path.join('storage', 'size-differentiated.yaml')

    def __new__(cls, _yaml_filename: str,
                size_limited_storages: List[_SizeLimitedStorageConfig],
                fallback: StorageABC):
        parsed_storages: List[SizeLimitedStorageType] = []
        for storage_config in size_limited_storages:
            parsed_storages.append({
                'max_size': _parse_size(storage_config['max-size']),
                'storage': storage_config['storage']
            })
        return SizeDifferentiatedStorage(parsed_storages, fallback)


class SQLSchedulerConfig(YamlFactory):
    """Generates :class:`buildgrid.server.persistence.sql.impl.SQLDataStore` using
    the tag ``!sql-scheduler``.

    Usage
        .. code:: yaml

            - !sql-scheduler
              # This assumes that a storage instance is defined elsewhere
              # with a `&cas-storage` anchor
              storage: *cas-storage
              connection_string: postgresql://bgd:insecure@database/bgd
              automigrate: yes
              connection_timeout: 5
              pruner-job-max-age:
                days: 90

    Args:
        storage(:class:`buildgrid.server.cas.storage.storage_abc.StorageABC`): Instance
            of storage to use for getting actions and storing job results. This must be
            an object constructed using a YAML tag ending in ``-storage``, for example
            ``!disk-storage``.
        connection_string (str): SQLAlchemy connection string to use for connecting
            to the database.
        automigrate (bool): Whether to attempt to automatically upgrade an existing
            DB schema to the newest version (this will also create everything from
            scratch if given an empty database).
        connection_timeout (int): Time to wait for an SQLAlchemy connection to be
            available in the pool before timing out.
        pruner_job_max_age (dict): Allow the storage to remove old entries by specifying the
            maximum amount of time that a row should be kept after its job finished. If
            this value is None, pruning is disabled and the background pruning thread
            is never created.
        pruner_period (dict): How often to attempt to remove old entries. If pruning
            is enabled (see above) and this value is None, it is set to 5 minutes by default.
        pruner_max_delete_window (int): Maximum number of records removed in a single
            cleanup pass. If pruning is enabled and this value is None, it is set to 10000
            by default. This allows to put a limit on the time that the database
            will be blocked on a single invocation of the cleanup routine.
            (A smaller value reduces the performance impact of removing entries,
            but makes the recovery of storage space slower.)
    """

    yaml_tag = u'!sql-scheduler'
    schema = os.path.join('scheduler', 'sql.yaml')

    def __new__(cls, _yaml_filename: str, storage: StorageABC,
                connection_string: Optional[str] = None,
                automigrate: bool = False, connection_timeout: int = 5,
                pruner_job_max_age: Optional[Dict[str, float]] = None,
                pruner_period: Optional[Dict[str, float]] = None,
                pruner_max_delete_window: Optional[int] = None,
                **kwargs):

        click.echo(f"SQLScheduler: storage={type(storage).__name__}, "
                   f"automigrate={automigrate}, "
                   f"connection_timeout={connection_timeout}, "
                   f"pruner_job_max_age={pruner_job_max_age}, "
                   f"pruner_period={pruner_period}, "
                   f"pruner_max_delete_window={pruner_max_delete_window}")
        click.echo(click.style(
            "Creating an SQL scheduler backend\n",
            fg="green", bold=True
        ))
        try:
            pruning_options = PruningOptions.from_config(
                pruner_job_max_age, pruner_period, pruner_max_delete_window) if pruner_job_max_age else None
            return SQLDataStore(storage,
                                connection_string=connection_string,
                                automigrate=automigrate,
                                connection_timeout=connection_timeout,
                                pruning_options=pruning_options,
                                **kwargs)
        except TypeError as type_error:
            click.echo(type_error, err=True)
            sys.exit(-1)


class SQLDataStoreConfig(YamlFactory):
    """Generates :class:`buildgrid.server.persistence.sql.impl.SQLDataStore` using
    the tag ``!sql-data-store``.

    .. warning::
        This is deprecated and only used for compatibility with old configs.

    Usage
        .. code:: yaml

            - !sql-data-store
              # This assumes that a storage instance is defined elsewhere
              # with a `&cas-storage` anchor
              storage: *cas-storage
              connection_string: postgresql://bgd:insecure@database/bgd
              automigrate: yes
              connection_timeout: 5

    Args:
        storage(:class:`buildgrid.server.cas.storage.storage_abc.StorageABC`): Instance
            of storage to use for getting actions and storing job results. This must be
            an object constructed using a YAML tag ending in ``-storage``, for example
            ``!disk-storage``.
        connection_string (str): SQLAlchemy connection string to use for connecting
            to the database.
        automigrate (bool): Whether to attempt to automatically upgrade an existing
            DB schema to the newest version (this will also create everything from
            scratch if given an empty database).
        connection_timeout (int): Time to wait for an SQLAlchemy connection to be
            available in the pool before timing out.

    """

    yaml_tag = u'!sql-data-store'
    schema = os.path.join('scheduler', 'sql.yaml')

    def __new__(cls, _yaml_filename: str, storage: StorageABC, connection_string: Optional[str]=None,
                automigrate: bool=False, connection_timeout: int=5, **kwargs):
        click.echo(click.style(
            "Warning: !sql-data-store YAML tag is deprecated. Use !sql-scheduler instead.",
            fg="bright_yellow"
        ))
        click.echo(f"SQLScheduler: storage={type(storage).__name__}, "
                   f"automigrate={automigrate}, "
                   f"connection_timeout={connection_timeout}")
        click.echo(click.style(
            "Creating an SQL scheduler backend\n",
            fg="green", bold=True
        ))
        try:
            return SQLDataStore(storage,
                                connection_string=connection_string,
                                automigrate=automigrate,
                                connection_timeout=connection_timeout,
                                **kwargs)
        except TypeError as type_error:
            click.echo(type_error, err=True)
            sys.exit(-1)


class MemorySchedulerConfig(YamlFactory):
    """Generates :class:`buildgrid.server.persistence.mem.impl.MemoryDataStore` using
    the tag ``!memory-scheduler``.

    Usage
        .. code:: yaml

            - !memory-scheduler
              # This assumes that a storage instance is defined elsewhere
              # with a `&cas-storage` anchor
              storage: *cas-storage

    Args:
        storage(:class:`buildgrid.server.cas.storage.storage_abc.StorageABC`): Instance
            of storage to use for getting actions and storing job results. This must be
            an object constructed using a YAML tag ending in ``-storage``, for example
            ``!disk-storage``.

    """

    yaml_tag = u'!memory-scheduler'
    schema = os.path.join('scheduler', 'memory.yaml')

    def __new__(cls, _yaml_filename: str, storage: StorageABC):
        click.echo(f"MemoryScheduler: storage={type(storage).__name__}")
        click.echo(click.style(
            "Creating an in-memory scheduler backend\n",
            fg="green", bold=True
        ))
        return MemoryDataStore(storage)


class MemoryDataStoreConfig(YamlFactory):
    """Generates :class:`buildgrid.server.persistence.mem.impl.MemoryDataStore` using
    the tag ``!memory-data-store``.

    .. warning::
        This is deprecated and only used for compatibility with old configs. Use
        :class:`MemorySchedulerConfig` instead.

    Usage
        .. code:: yaml

            - !memory-data-store
              # This assumes that a storage instance is defined elsewhere
              # with a `&cas-storage` anchor
              storage: *cas-storage

    Args:
        storage(:class:`buildgrid.server.cas.storage.storage_abc.StorageABC`): Instance
            of storage to use for getting actions and storing job results. This must be
            an object constructed using a YAML tag ending in ``-storage``, for example
            ``!disk-storage``.

    """

    yaml_tag = u'!memory-data-store'
    schema = os.path.join('scheduler', 'memory.yaml')

    def __new__(cls, _yaml_filename: str, storage: StorageABC):
        click.echo(click.style(
            "Warning: !memory-data-store YAML tag is deprecated. Use !memory-scheduler instead.",
            fg="bright_yellow"
        ))
        click.echo(f"MemoryScheduler: storage={type(storage).__name__}")
        click.echo(click.style(
            "Creating an in-memory scheduler backend\n",
            fg="green", bold=True
        ))
        return MemoryDataStore(storage)


class SQL_Index(YamlFactory):
    """Generates :class:`buildgrid.server.cas.storage.index.sql.SQLIndex`
    using the tag ``!sql-index``.

    Usage
        .. code:: yaml

            - !sql-index
              # This assumes that a storage instance is defined elsewhere
              # with a `&cas-storage` anchor
              storage: *cas-storage
              connection_string: postgresql://bgd:insecure@database/bgd
              automigrate: yes
              window-size: 1000
              inclause-limit: -1
              fallback-on-get: no

    Args:
        storage(:class:`buildgrid.server.cas.storage.storage_abc.StorageABC`):
            Instance of storage to use. This must be a storage object constructed using
            a YAML tag ending in ``-storage``, for example ``!disk-storage``.
        connection_string (str): SQLAlchemy connection string
        automigrate (bool): Attempt to automatically upgrade an existing DB schema to
            the newest version.
        window_size (uint): Maximum number of blobs to fetch in one SQL operation
            (larger resultsets will be automatically split into multiple queries)
        inclause_limit (int): If nonnegative, overrides the default number of variables
            permitted per "in" clause. See the buildgrid.server.cas.storage.index.sql.SQLIndex
            comments for more details.
        fallback_on_get (bool): By default, the SQL Index only fetches blobs from the
            underlying storage if they're present in the index on ``get_blob``/``bulk_read_blobs``
            requests to minimize interactions with the storage. If this is set, the index
            instead checks the underlying storage directly on ``get_blob``/``bulk_read_blobs``
            requests, then loads all blobs found into the index.
        max_inline_blob_size (int): Blobs of this size or smaller are stored directly in the index
            and not in the backing storage (must be nonnegative).
    """

    yaml_tag = u'!sql-index'
    schema = os.path.join('storage', 'sql-index.yaml')

    def __new__(cls, _yaml_filename: str, storage: StorageABC,
                connection_string: str, automigrate: bool=False,
                window_size: int=1000, inclause_limit: int=-1,
                fallback_on_get: bool=False, max_inline_blob_size: int=0,
                **kwargs):
        storage_type = type(storage).__name__
        click.echo(f"SQLIndex: storage={storage_type}, "
                   f"automigrate={automigrate}, "
                   f"window_size={window_size}, "
                   f"inclause_limit={inclause_limit}, "
                   f"fallback_on_get={fallback_on_get}")
        click.echo(click.style(
            f"Creating an SQL CAS Index for {storage_type}\n",
            fg="green", bold=True
        ))
        return SQLIndex(storage=storage, connection_string=connection_string,
                        automigrate=automigrate, window_size=window_size,
                        inclause_limit=inclause_limit,
                        fallback_on_get=fallback_on_get,
                        max_inline_blob_size=max_inline_blob_size,
                        **kwargs)


class Execution(YamlFactory):
    """Generates :class:`buildgrid.server.execution.service.ExecutionService`
    using the tag ``!execution``.

    Usage
        .. code:: yaml

            # This assumes that the YAML anchors are defined elsewhere
            - !execution
              storage: *cas-storage
              action-cache: *remote-cache
              action-browser-url: http://localhost:8080
              scheduler: *state-database
              property-keys:
                - runnerType
              wildcard-property-keys:
                - chrootDigest
              operation-stream-keepalive-timeout: 600
              bot-session-keepalive-timeout: 600
              endpoints:
                - execution
                - operations
                - bots
              discard-unwatched-jobs: no
              max-execution-timeout: 7200
              max-list-operations-page-size: 1000

    Args:
        storage (:class:`buildgrid.server.cas.storage.storage_abc.StorageABC`):
            Instance of storage to use. This must be an object constructed using
            a YAML tag ending in ``-storage``, for example ``!disk-storage``.
        action_cache (:class:`ActionCache`): Instance of action cache to use.
        action_browser_url (str): The base URL to use to generate Action Browser
            links to users
        scheduler(:class:`DataStoreInterface`): Instance of data store to use for
            the scheduler's state.
        property_keys (list): The platform property keys available to use in routing
            Actions to workers
        wildcard_property_keys (list): The platform property keys which can be set
            in Actions but are not used to select workers
        operation_stream_keepalive_timeout (int): The longest time (in seconds)
            we'll wait before sending the current status in an Operation response
            stream of an `Execute` or `WaitExecution` request. Defaults to 600s
            (10 minutes).
        bot_session_keepalive_timeout (int): The longest time (in seconds) we'll wait
            for a bot to send an update before it assumes it's dead. Defaults to 600s
            (10 minutes).
        permissive_bot_session: Whether to accept UpdateBotSession requests from bots that haven't explicitly
            called CreateBotSession with this particular server instance.
        endpoints (list): List of service/endpoint types to enable. Possible services are
            ``execution``, ``operations``, and ``bots``. By default all three are enabled.
        discard_unwatched_jobs (bool): Allow operation id to persist without a
            client connection.
        max_execution_timeout (int): The maximum time jobs are allowed to be in
            'OperationStage.EXECUTING'. This is a lazy check tha happens before
            job deduplication and when new peers register for an ongoing Operation.
            When this time is exceeded in executing stage, the job will be cancelled.
        max_list_operations_page_size (int): The maximum number of operations that can
            be returned in a ListOperations response. A page token will be returned
            with the response to allow the client to get the next page of results.

    """

    yaml_tag = u'!execution'
    schema = os.path.join('services', 'execution.yaml')

    def __new__(cls, _yaml_filename: str, storage, action_cache=None, action_browser_url=None, data_store=None,
                scheduler=None, property_keys=None, wildcard_property_keys=None,
                operation_stream_keepalive_timeout=600,
                bot_session_keepalive_timeout=600, permissive_bot_session=False,
                endpoints=ServiceName.default_services(), discard_unwatched_jobs=False,
                max_execution_timeout=DEFAULT_MAX_EXECUTION_TIMEOUT,
                max_list_operations_page_size=DEFAULT_MAX_LIST_OPERATION_PAGE_SIZE,
                logstream=None):
        scheduler = _validate_scheduler(
            cls, scheduler, data_store, fallback=True, storage=storage)

        if isinstance(scheduler, MemoryDataStore) and permissive_bot_session is True:
            click.echo("ERROR: Permissive Bot Session mode is not compatible with the in-memory scheduler."
                       "Please fix the config.\n", err=True)
            sys.exit(-1)

        if isinstance(action_cache, ActionCache):
            click.echo(click.style(
                "Warning: Passing an ActionCache instance (!action-cache) to an Execution "
                "service is deprecated. Use a cache backend such as !lru-action-cache instead.",
                fg="bright_yellow"
            ))

        # Create the full set of platform property keys, and also the set of
        # keys to actually use when matching Jobs to workers
        merged_property_keys = DEFAULT_PLATFORM_PROPERTY_KEYS.copy()
        match_properties = DEFAULT_PLATFORM_PROPERTY_KEYS.copy()
        if property_keys:
            if isinstance(property_keys, str):
                match_properties.add(property_keys)
                merged_property_keys.add(property_keys)
            else:
                match_properties.update(property_keys)
                merged_property_keys.update(property_keys)

        if wildcard_property_keys:
            if isinstance(wildcard_property_keys, str):
                merged_property_keys.add(wildcard_property_keys)
            else:
                merged_property_keys.update(wildcard_property_keys)

        click.echo(
            f"Execution: storage={type(storage).__name__}, "
            f"scheduler={type(scheduler).__name__}, "
            f"max_execution_timeout={max_execution_timeout}, "
            f"operation_stream_keepalive_timeout={operation_stream_keepalive_timeout}, "
            f"bot_session_keepalive_timeout={bot_session_keepalive_timeout}")

        click.echo(f"Enabled endpoints:\n{yaml.dump(list(endpoints)).strip()}")
        click.echo("Supported platform property keys:\n"
                   f"{yaml.dump(list(merged_property_keys)).strip()}")

        logstream_url, logstream_credentials, logstream_instance_name = get_logstream_connection_info(logstream)

        click.echo(click.style(
            f"Creating an Execution service using {scheduler}\n",
            fg="green", bold=True
        ))
        return ExecutionController(scheduler, storage=storage, action_cache=action_cache,
                                   action_browser_url=action_browser_url,
                                   property_keys=merged_property_keys,
                                   match_properties=match_properties,
                                   operation_stream_keepalive_timeout=operation_stream_keepalive_timeout,
                                   bot_session_keepalive_timeout=bot_session_keepalive_timeout,
                                   permissive_bot_session=permissive_bot_session,
                                   services=endpoints, discard_unwatched_jobs=discard_unwatched_jobs,
                                   max_execution_timeout=max_execution_timeout,
                                   max_list_operations_page_size=max_list_operations_page_size,
                                   logstream_url=logstream_url,
                                   logstream_credentials=logstream_credentials,
                                   logstream_instance_name=logstream_instance_name)


class Bots(YamlFactory):
    """Generates :class:`buildgrid.server.bots.instance.BotsInterface`
    using the tag ``!bots``.

    Usage
        .. code:: yaml

            # This assumes that the YAML anchors are defined elsewhere
            - !bots
              storage: *cas-storage
              action-cache: *remote-cache
              scheduler: *state-database
              bot-session-keepalive-timeout: 600
              permissive-bot-session: yes

    Args:
        storage(:class:`buildgrid.server.cas.storage.storage_abc.StorageABC`):
            Instance of storage to use. This must be an object constructed using
            a YAML tag ending in ``-storage``, for example ``!disk-storage``.
        action_cache(:class:`Action`): Instance of action cache to use.
        scheduler(:class:`DataStoreInterface`): Instance of data store to use for the
            scheduler's state.
        bot_session_keepalive_timeout (int): The longest time (in seconds) we'll wait for a
            bot to send an update before it assumes it's dead. Defaults to 600s (10 minutes).
        permissive_bot_session: Whether to accept UpdateBotSession requests from bots that haven't explicitly
            called CreateBotSession with this particular server instance.
    """

    yaml_tag = u'!bots'
    schema = os.path.join('services', 'bots.yaml')

    def __new__(cls, _yaml_filename: str, storage, action_cache=None,
                bot_session_keepalive_timeout=600,
                data_store=None, scheduler=None, permissive_bot_session=False,
                logstream=None):
        scheduler = _validate_scheduler(cls, scheduler, data_store)

        if isinstance(scheduler, MemoryDataStore) and permissive_bot_session is True:
            click.echo("ERROR: Permissive Bot Session mode is not compatible with the in-memory scheduler."
                       "Please fix the config.\n", err=True)
            sys.exit(-1)

        if isinstance(action_cache, ActionCache):
            click.echo(click.style(
                "Warning: Passing an ActionCache instance (!action-cache) to an Execution "
                "service is deprecated. Use a cache backend such as !lru-action-cache instead.",
                fg="bright_yellow"
            ))

        click.echo(
            f"Bots: storage={type(storage).__name__}, "
            f"scheduler={type(scheduler).__name__}, "
            f"bot_session_keepalive_timeout={bot_session_keepalive_timeout}")

        logstream_url, logstream_credentials, logstream_instance_name = get_logstream_connection_info(logstream)

        click.echo(click.style(
            f"Creating a Bots service using {scheduler}\n", fg="green", bold=True
        ))
        return BotsInterface(scheduler, action_cache=action_cache,
                             bot_session_keepalive_timeout=bot_session_keepalive_timeout,
                             permissive_bot_session=permissive_bot_session,
                             logstream_url=logstream_url,
                             logstream_credentials=logstream_credentials,
                             logstream_instance_name=logstream_instance_name)


class Action(YamlFactory):
    """Generates :class:`buildgrid.server.actioncache.service.ActionCacheService`
    using the tag ``!action-cache``.

    Usage
        .. code:: yaml

            # This assumes that the YAML anchors are defined elsewhere
            - !action-cache
              cache: *lru-cache

    Args:
        cache (ActionCacheABC): The ActionCache backend to use for this cache.

    """

    yaml_tag = u'!action-cache'
    schema = os.path.join('services', 'action-cache.yaml')

    def __new__(cls, _yaml_filename: str, storage: Optional[StorageABC]=None,
                max_cached_refs: Optional[int]=None,
                allow_updates: bool=True, cache_failed_actions: bool=True,
                cache: Optional[ActionCacheABC]=None):
        if cache is None:
            # Old-style configuration, create an LRU Action Cache
            click.echo(click.style(
                "Warning: !action-cache YAML tag now takes a `cache` key. Old-style "
                "config should be changed to use an !lru-action-cache in the `cache` key.",
                fg="bright_yellow"
            ))
            storage_type = type(storage).__name__
            click.echo(f"LruActionCache: storage={storage_type}, "
                       f"max_cached_refs={max_cached_refs}, "
                       f"allow_updates={allow_updates}, "
                       f"cache_failed_actions={cache_failed_actions}")
            click.echo(click.style(
                f"Creating an LruActionCache using `{storage_type}` storage\n",
                fg="green", bold=True
            ))
            cache = LruActionCache(
                storage, max_cached_refs, allow_updates, cache_failed_actions)  # type: ignore

        cache_type = type(cache).__name__
        click.echo(f"ActionCache: cache={cache_type}")
        click.echo(click.style(
            f"Creating an ActionCache service using `{cache_type}`\n",
            fg="green", bold=True
        ))
        return ActionCache(cache)


class WithCacheAction(YamlFactory):
    """Generates:class:`buildgrid.server.actioncache.caches.with_cache.WithCacheActionCache`
    using the tag ``!with-cache-action-cache``.

    Usage
        .. code:: yaml

            # This assumes that the YAML anchors are defined elsewhere
            - !with-cache-action-cache
              storage: *cas-storage
              cache: *cache-ac
              fallback: *fallback-ac

    Args:
        cache (ActionCacheABC): ActionCache instance to use as a local cache
        fallback (ActionCacheABC): ActionCache instance to use as a fallback on
            local cache misses
        allow_updates(bool): Allow updates pushed to the Action Cache.
            Defaults to ``True``.
        cache_failed_actions(bool): Whether to store failed (non-zero exit
            code) actions. Default to ``True``.
    """

    yaml_tag = u'!with-cache-action-cache'
    schema = os.path.join('caches', 'with-cache.yaml')

    def __new__(cls, _yaml_filename: str, cache: ActionCacheABC,
                fallback: ActionCacheABC, allow_updates: bool=True,
                cache_failed_actions: bool=True):
        return WithCacheActionCache(
            cache, fallback, allow_updates=allow_updates,
            cache_failed_actions=cache_failed_actions)


class LruAction(YamlFactory):
    """Generates :class:`buildgrid.server.actioncache.caches.lru_cache.LruActionCache`
    using the tag ``!lru-action-cache``.

    Usage
        .. code:: yaml

            # This assumes that the YAML anchors are defined elsewhere
            - !lru-action-cache
              storage: *cas-storage
              max-cached-refs: 1024
              cache-failed-actions: yes
              allow-updates: yes

    Args:
        storage(:class:`buildgrid.server.cas.storage.storage_abc.StorageABC`):
            Instance of storage to use.
        max_cached_refs(int): Max number of cached actions.
        allow_updates(bool): Allow updates pushed to the Action Cache.
            Defaults to ``True``.
        cache_failed_actions(bool): Whether to store failed (non-zero exit
            code) actions. Default to ``True``.

    """

    yaml_tag = u'!lru-action-cache'
    schema = os.path.join('caches', 'lru-action-cache.yaml')

    def __new__(cls, _yaml_filename: str, storage: StorageABC,
                max_cached_refs: int,
                allow_updates: bool=True, cache_failed_actions: bool=True):
        storage_type = type(storage).__name__
        click.echo(f"LruActionCache: storage={storage_type}, max_cached_refs={max_cached_refs}, "
                   f"allow_updates={allow_updates}, cache_failed_actions={cache_failed_actions}")
        click.echo(click.style(
            f"Creating an LruActionCache using `{storage_type}` storage\n",
            fg="green", bold=True
        ))
        return LruActionCache(
            storage, max_cached_refs, allow_updates, cache_failed_actions)


class S3Action(YamlFactory):
    """Generates :class:`buildgrid.server.actioncache.caches.s3_cache.S3ActionCache`
    using the tag ``!s3action-cache``.

    Usage
        .. code:: yaml

            # This assumes that the YAML anchors are defined elsewhere
            - !s3action-cache
              storage: *cas-storage
              allow-updates: yes
              cache-failed-actions: yes
              entry-type: action-result-digest
              migrate-entries: no
              bucket: bgd-action-cache
              endpoint: http://localhost:9000/
              access-key: !read-file /var/bgd/s3-access-key
              secret-key: !read-file /var/bgd/s3-secret-key

    Args:
        storage(:class:`buildgrid.server.cas.storage.storage_abc.StorageABC`):
            Instance of storage to use. This must be an object constructed using
            a YAML tag ending in ``-storage``, for example ``!disk-storage``.
        allow_updates(bool): Allow updates pushed to the Action Cache.
            Defaults to ``True``.
        cache_failed_actions(bool): Whether to store failed (non-zero exit code)
            actions. Default to ``True``.
        entry_type (str): whether entries in S3 will store an ``'action-result'``
            or an ``'action-result-digest'`` (default).
        migrate_entries (bool): Whether to automatically update the values of
            entries that contain a different type of value to `entry_type` as
            they are queried. Default to ``False``.
        bucket (str): Name of bucket
        endpoint (str): URL of endpoint.
        access-key (str): S3-ACCESS-KEY
        secret-key (str): S3-SECRET-KEY

    """

    yaml_tag = u'!s3action-cache'
    schema = os.path.join('services', 's3-action-cache.yaml')

    def __new__(cls, _yaml_filename: str, storage: StorageABC, allow_updates: bool=True,
                cache_failed_actions: bool=True, entry_type: Optional[str]=None,
                migrate_entries: Optional[bool]=False,
                bucket: str=None,
                endpoint: Optional[str]=None, access_key: Optional[str]=None,
                secret_key: Optional[str]=None):
        storage_type = type(storage).__name__

        if entry_type is None or entry_type.lower() == 'action-result-digest':
            cache_entry_type = ActionCacheEntryType.ACTION_RESULT_DIGEST
        elif entry_type.lower() == 'action-result':
            cache_entry_type = ActionCacheEntryType.ACTION_RESULT
        else:
            click.echo(click.style(
                f"ERROR: entry_type value is not valid: {cache_entry_type}",
                fg="red", bold=True
            ), err=True)
            sys.exit(-1)

        click.echo(f"S3ActionCache: storage={storage_type}, allow_updates={allow_updates}, "
                   f"cache_failed_actions={cache_failed_actions}, bucket={bucket}, "
                   f"entry_type={entry_type}, migrate_entries={migrate_entries}, "
                   f"endpoint={endpoint}")
        click.echo(click.style(
            f"Creating an S3ActionCache service using `{storage_type}` storage\n",
            fg="green", bold=True
        ))

        from botocore.config import Config as BotoConfig  # pylint: disable=import-outside-toplevel

        boto_config = BotoConfig(
            user_agent=S3_USERAGENT_NAME,
            connect_timeout=S3_TIMEOUT_CONNECT,
            read_timeout=S3_TIMEOUT_READ,
            retries={'max_attempts': S3_MAX_RETRIES})

        return S3ActionCache(storage,
                             allow_updates=allow_updates,
                             cache_failed_actions=cache_failed_actions,
                             entry_type=cache_entry_type,
                             migrate_entries=migrate_entries,
                             bucket=bucket,
                             endpoint=endpoint,
                             access_key=access_key,
                             secret_key=secret_key,
                             config=boto_config)


class RemoteAction(YamlFactory):
    """Generates :class:`buildgrid.server.actioncache.caches.remote.RemoteActionCache`
    using the tag ``!remote-action-cache``.

    Usage
        .. code:: yaml

            - !remote-action-cache
              url: https://action-cache:50053
              instance-name: main
              credentials:
                tls-server-key: !expand-path ~/.config/buildgrid/server.key
                tls-server-cert: !expand-path ~/.config/buildgrid/server.cert
                tls-client-certs: !expand-path ~/.config/buildgrid/client.cert
              channel-options:
                lb-policy-name: round_robin

    Args:
        url (str): URL to remote action cache. If used with ``https``, needs credentials.
        instance_name (str): Instance of the remote to connect to.
        credentials (dict, optional): A dictionary in the form::

           tls-client-key: /path/to/client-key
           tls-client-cert: /path/to/client-cert
           tls-server-cert: /path/to/server-cert
        channel-options (dict, optional): A dictionary of grpc channel options in the form::

          some-channel-option: channel_value
          other-channel-option: another-channel-value
        See https://github.com/grpc/grpc/blob/master/include/grpc/impl/codegen/grpc_types.h
        for the valid channel options

    """

    yaml_tag = u'!remote-action-cache'
    schema = os.path.join('services', 'remote-action-cache.yaml')

    def __new__(cls, _yaml_filename: str, url: str, instance_name: str,
                retries: int = 3, max_backoff: int = 64,
                request_timeout: Optional[float] = None,
                credentials: Optional[Dict[str, str]]=None,
                channel_options: Optional[Dict[str, Any]]=None):

        options_tuple = None
        if channel_options:
            # Transform the channel options into the format expected
            # by grpc channel creation
            parsed_options = []
            for option_name, option_value in channel_options.items():
                parsed_options.append((f"grpc.{option_name.replace('-','_')}", option_value))
            options_tuple = tuple(parsed_options)
        else:
            options_tuple = ()

        if not _validate_url_and_credentials(url, credentials=credentials):
            sys.exit(-1)

        click.echo(f"RemoteActionCache: url={url}, instance_name={instance_name}, ")
        click.echo(click.style(
            f"Creating an RemoteActionCache service for {url}\n",
            fg="green", bold=True
        ))

        return RemoteActionCache(
            url,
            instance_name,
            retries,
            max_backoff,
            request_timeout,
            channel_options=options_tuple,
            tls_credentials=credentials
        )


class WriteOnceAction(YamlFactory):
    """Generates :class:`buildgrid.server.actioncache.caches.write_once_cache.WriteOnceActionCache`
    using the tag ``!write-once-action-cache``.

    This allows a single update for a given key, essentially making it possible
    to create immutable ActionCache entries, rather than making the cache read-only
    as the ``allow-updates`` property of other ActionCache implementations does.

    Usage
        .. code:: yaml

            # This assumes that the YAML anchors are defined elsewhere
            - !write-once-action-cache
              action-cache: *remote-cache

    Args:
        action_cache (ActionCache): The action cache instance to make immutable.

    """

    yaml_tag = u'!write-once-action-cache'
    schema = os.path.join('services', 'write-once-action-cache.yaml')

    def __new__(cls, _yaml_filename: str, action_cache):
        return WriteOnceActionCache(action_cache)


class RedisAction(YamlFactory):
    """Generates :class:`buildgrid.server.actioncache.caches.redis_cache.RedisActionCache`
    using the tag ``!redis-action-cache``.

    This creates an Action Cache which stores the mapping from Action digests to
    ActionResults in Redis.

    Usage
        .. code:: yaml

            # This assumes that the YAML anchors are defined elsewhere
            - !redis-action-cache
              storage: *cas-storage
              allow-updates: yes
              cache-failed-actions: yes
              entry-type: action-result-digest
              migrate-entries: no
              host: redis
              port: 6379
              db: 0
              dns-srv-record: <Domain name of SRV record>
              sentinel-master-name: <service_name of Redis sentinel's master instance>
              retries: 3


    Args:
        storage(:class:`buildgrid.server.cas.storage.storage_abc.StorageABC`):
            Instance of storage to use. This must be an object constructed using
            a YAML tag ending in ``-storage``, for example ``!disk-storage``.
        allow_updates(bool): Allow updates pushed to the Action Cache.
            Defaults to ``True``.
        cache_failed_actions(bool): Whether to store failed (non-zero exit code)
            actions. Default to ``True``.
        entry_type (str): whether entries in Redis will store an ``'action-result'``
            or an ``'action-result-digest'`` (default).
        migrate_entries (bool): Whether to automatically update the values of
            entries that contain a different type of value to `entry_type` as
            they are queried. Default to ``False``.
        host (str): The hostname of the Redis server to use.
        port (int): The port that Redis is served on.
        db (int): The Redis database number to use.
        dns-srv-record (str): Domain name of SRV record used to discover host/port
        sentinel-master-name (str): Service name of Redis master instance, used
            in a Redis sentinel configuration
        retries (int): Max number of times to retry (default 3). Backoff between retries is about 2^(N-1),
            where N is the number of attempts

    """

    yaml_tag = u'!redis-action-cache'
    schema = os.path.join('caches', 'redis-action-cache.yaml')

    def __new__(cls, _yaml_filename: str, storage: StorageABC,
                host: Optional[str]=None, port: Optional[int]=None,
                allow_updates: bool=True, cache_failed_actions: bool=True,
                entry_type: Optional[str]=None,
                migrate_entries: Optional[bool]=False,
                password: Optional[str]=None, db: int=0,
                dns_srv_record: Optional[str]=None,
                sentinel_master_name: Optional[str]=None,
                retries: int=3):
        if entry_type is None or entry_type.lower() == 'action-result-digest':
            cache_entry_type = ActionCacheEntryType.ACTION_RESULT_DIGEST
        elif entry_type.lower() == 'action-result':
            cache_entry_type = ActionCacheEntryType.ACTION_RESULT
        else:
            click.echo(click.style(
                f"ERROR: entry_type value is not valid: {cache_entry_type}",
                fg="red", bold=True
            ), err=True)
            sys.exit(-1)
        # Import here so there is no global buildgrid dependency on redis
        # pylint: disable=import-outside-toplevel
        from buildgrid.server.actioncache.caches.redis_cache import RedisActionCache
        try:
            return RedisActionCache(storage, allow_updates=allow_updates,
                                    cache_failed_actions=cache_failed_actions,
                                    entry_type=cache_entry_type,
                                    migrate_entries=migrate_entries,
                                    host=host, port=port, password=password, db=db,
                                    dns_srv_record=dns_srv_record,
                                    sentinel_master_name=sentinel_master_name,
                                    max_retries=retries)
        except Exception as e:
            click.echo(click.style(
                f"ERROR: {e},", fg="red", bold=True
            ), err=True)
            sys.exit(-1)


class Reference(YamlFactory):
    """Generates :class:`buildgrid.server.referencestorage.service.ReferenceStorageService`
    using the tag ``!reference-cache``.

    .. note::
        This is intended for use by old versions of BuildStream as an artifact cache,
        and isn't part of the REAPI/RWAPI specifications. There's no need for this component
        if you aren't using BuildStream.

    Usage
        .. code:: yaml

            # This assumes that the YAML anchors are defined elsewhere
            - !reference-storage
              storage: *cas-storage
              max-cached-refs: 1024
              allow-updates: yes

    Args:
        storage(:class:`buildgrid.server.cas.storage.storage_abc.StorageABC`):
            Instance of storage to use. This must be an object constructed using
            a YAML tag ending in ``-storage``, for example ``!disk-storage``.
        max_cached_refs(int): Max number of cached actions.
        allow_updates(bool): Allow updates pushed to CAS. Defaults to ``True``.

    """

    yaml_tag = u'!reference-cache'
    schema = os.path.join('services', 'reference-cache.yaml')

    def __new__(cls, _yaml_filename: str, storage: StorageABC, max_cached_refs: int,
                allow_updates: bool=True):
        storage_type = type(storage).__name__
        click.echo(f"ReferenceStorage: storage={storage_type}, "
                   f"max_cached_refs={max_cached_refs}, "
                   f"allow_updates={allow_updates}")
        click.echo(click.style(
            f"Creating a ReferenceStorage service using {storage_type}\n",
            fg="green", bold=True
        ))
        return ReferenceCache(storage, max_cached_refs, allow_updates)


class CAS(YamlFactory):
    """Generates :class:`buildgrid.server.cas.service.ContentAddressableStorageService`
    using the tag ``!cas``.

    Usage
        .. code:: yaml

            # This assumes that the YAML anchors are defined elsewhere
            - !cas
              storage: *cas-storage

    Args:
        storage(:class:`buildgrid.server.cas.storage.storage_abc.StorageABC`):
            Instance of storage to use. This must be an object constructed using
            a YAML tag ending in ``-storage``, for example ``!disk-storage``.
    """

    yaml_tag = u'!cas'
    schema = os.path.join('services', 'cas.yaml')

    def __new__(cls, _yaml_filename: str, storage: StorageABC, read_only: bool=False):
        storage_type = type(storage).__name__
        click.echo(f"CAS: storage={storage_type}, read_only={read_only}")
        click.echo(click.style(
            f"Creating a CAS service using {storage_type}\n",
            fg="green", bold=True
        ))
        return ContentAddressableStorageInstance(storage, read_only=read_only)


class ByteStream(YamlFactory):
    """Generates :class:`buildgrid.server.cas.service.ByteStreamService`
    using the tag ``!bytestream``.

    Usage
        .. code:: yaml

            # This assumes that the YAML anchors are defined elsewhere
            - !bytestream
              storage: *cas-storage

    Args:
        storage(:class:`buildgrid.server.cas.storage.storage_abc.StorageABC`):
            Instance of storage to use. This must be an object constructed using
            a YAML tag ending in ``-storage``, for example ``!disk-storage``.
    """

    yaml_tag = u'!bytestream'
    schema = os.path.join('services', 'bytestream.yaml')

    def __new__(cls, _yaml_filename: str, storage: Optional[StorageABC]=None,
                stream_storage: Optional[StreamStorageABC]=None,
                read_only: bool=False,
                disable_overwrite_early_return: bool=False):
        storage_type = type(storage).__name__
        stream_storage_type = type(stream_storage).__name__
        click.echo(f"ByteStream: storage={storage_type}, "
                   f"stream_storage={stream_storage_type}, "
                   f"read_only={read_only}, "
                   f"disable_overwrite_early_return={disable_overwrite_early_return}")

        click.echo(click.style(
            f"Creating a ByteStream service using storage {storage_type}, "
            f"stream storage {stream_storage_type}\n",
            fg="green", bold=True
        ))
        return ByteStreamInstance(storage, read_only=read_only,
                                  stream_storage=stream_storage,
                                  disable_overwrite_early_return=disable_overwrite_early_return)


class LogStream(YamlFactory):
    """Generates :class:`buildgrid.server.cas.logstream.instance.LogStreamInstance`
    using the tag ``!logstream``.

    Usage
        .. code:: yaml

            - !logstream
              prefix: test
    """

    yaml_tag = u'!logstream'
    schema = os.path.join('services', 'logstream.yaml')

    def __new__(cls, _yaml_filename: str, prefix='', stream_storage=None):
        click.echo(f"LogStream: prefix={prefix}")
        click.echo(click.style(
            "Creating a LogStream service\n",
            fg="green", bold=True
        ))
        return LogStreamInstance(prefix=prefix, storage=stream_storage)


class MemoryStream(YamlFactory):
    """Generates :class:`buildgrid.server.cas.logstream.stream_storage.MemoryStreamStorage`
    using the tag ``!memory-logstream-storage``.

    Usage
        .. code:: yaml

            - !memory-logstream-storage

    """
    yaml_tag = u'!memory-logstream-storage'

    def __new__(cls, _yaml_filename: str, *args, **kwargs):
        return MemoryStreamStorage()


class MemoryBuildEvents(YamlFactory):
    """Generates :class:`buildgrid.server.build_events.storage.BuildEventStreamStorage`
    using the tag ``!memory-build-events-storage``.

    Usage
        .. code:: yaml

            - !memory-build-events

    """
    yaml_tag = u'!memory-build-events'

    def __new__(cls, _yaml_filename: str, *args, **kwargs):
        return BuildEventStreamStorage()


def _parse_size(size):
    """Convert a string containing a size in bytes (e.g. '2GB') to a number."""
    _size_prefixes = {'k': 2 ** 10, 'm': 2 ** 20, 'g': 2 ** 30, 't': 2 ** 40}
    size = size.lower()

    if size[-1] == 'b':
        size = size[:-1]
    if size[-1] in _size_prefixes:
        return int(size[:-1]) * _size_prefixes[size[-1]]
    return int(size)


def _validate_url_and_credentials(url: str, credentials: Optional[Dict[str, str]]) -> bool:
    """Validate a URL and set of credentials for the URL.

    This parses the given URL, to determine if it should be used with
    credentials (ie. to create a secure gRPC channel), or not (ie. to create
    an insecure gRPC channel).

    Credentials will be ignored for insecure channels, but if specified need
    to be valid for secure channels. Secure client channels with no specified
    credentials are valid, since gRPC will attempt to fall back to a default
    root certificate location used with no private key or certificate chain.

    If the credentials are invalid, then this function will output the error
    using ``click.echo``, and return ``False``. Otherwise this function will
    return True

    Args:
        url (str): The URL to use for validation.
        credentials (dict, optional): The credentials configuration to validate.

    """
    try:
        parsed_url = urlparse(url)
    except ValueError:
        click.echo(click.style(
            "ERROR: Failed to parse URL for gRPC channel construction.\n" +
            f"The problematic URL was: {url}.\n", fg="red", bold=True
        ), err=True)
        return False
    unix_socket = parsed_url.scheme == "unix"

    if parsed_url.scheme in insecure_uri_schemes:
        # Its a URL for an insecure channel that we recognize
        if credentials is not None:
            click.echo(click.style(
                "WARNING: credentials were specified for a gRPC channel, but "
                f"`{url}` uses an insecure scheme. The credentials will be "
                "ignored.\n", fg="bright_yellow"
            ))
        return True

    elif parsed_url.scheme not in secure_uri_schemes and not unix_socket:
        # Its not insecure, and its not a recognized secure scheme, so error out.
        click.echo(click.style(
            f"ERROR: URL {url} uses an unsupported scheme.\n", fg="red", bold=True
        ), err=True)
        return False

    if not credentials:
        # Unix sockets are treated as secure only if credentials are set
        if not unix_socket:
            click.echo(click.style(
                f"WARNING: {url} uses a secure scheme but no credentials were "
                "specified. gRPC will attempt to fall back to defaults.\n",
                fg="bright_yellow"
            ))
        return True

    client_key = credentials.get('tls-client-key')
    client_cert = credentials.get('tls-client-cert')
    server_cert = credentials.get('tls-server-cert')

    valid = True
    missing = {}
    if server_cert is not None and not os.path.exists(server_cert):
        valid = False
        missing['tls-server-cert'] = server_cert
    if client_key is not None and not os.path.exists(client_key):
        valid = False
        missing['tls-client-key'] = client_key
    if client_cert is not None and not os.path.exists(client_cert):
        valid = False
        missing['tls-client-cert'] = client_cert

    if not valid:
        click.echo(click.style(
            "ERROR: one or more configured TLS credentials files were " +
            "missing.\nSet remote url scheme to `http` or `grpc` in order to " +
            "deactivate TLS encryption.\nMissing files:", fg="red", bold=True
        ), err=True)
        for key, path in missing.items():
            click.echo(click.style(
                f"  - {key}: {path}", fg="red", bold=True
            ), err=True)
        return False
    return True


def _validate_server_credentials(credentials: Optional[Dict[str, str]]) -> None:
    """Validate a configured set of credentials.

    If the credentials are invalid, then this function will call ``sys.exit``
    and stop the process, since there's no point continuing. If this function
    returns without exiting the program, then the credentials were valid.

    Args:
        credentials (dict): The credentials configuration to validate.

    """
    if not credentials:
        click.echo(click.style(
            "ERROR: no TLS certificates were specified for the server's network config.\n" +
            "Set `insecure-mode` to True to deactivate TLS encryption.\n", fg="red", bold=True
        ), err=True)
        sys.exit(-1)

    server_key = credentials.get('tls-server-key')
    server_cert = credentials.get('tls-server-cert')
    client_certs = credentials.get('tls-client-certs')

    valid = True
    missing = {}
    if server_cert is None or not os.path.exists(server_cert):
        valid = False
        missing['tls-server-cert'] = server_cert
    if server_key is None or not os.path.exists(server_key):
        valid = False
        missing['tls-server-key'] = server_key
    if client_certs is not None and not os.path.exists(client_certs):
        valid = False
        missing['tls-client-certs'] = client_certs

    if not valid:
        click.echo(click.style(
            "ERROR: Couldn't find certificates for secure server port.\n"
            "Set `insecure-mode` to True to deactivate TLS encryption.\n"
            "Missing files:", fg="red", bold=True
        ), err=True)
        for key, path in missing.items():
            click.echo(click.style(
                f"  - {key}: {path}", fg="red", bold=True
            ), err=True)
        sys.exit(-1)


def _validate_scheduler(cls: Type[YamlFactory], scheduler: Any,
                        data_store: Any, fallback: bool=False,
                        storage: Optional[Type[StorageABC]]=None) -> DataStoreInterface:
    """Validate an object that is supposed to be a DataStoreInterface implementation.

    This function handles falling back to a default or exiting with a useful error if
    neither of the two keys are given, as well as warning of deprecation for the
    ``data-store`` key and checking that the provided object actually is a
    ``DataStoreInterface`` implementation.

    Args:
        cls (YamlFactory): The class being used to parse a given YAML tag.
        scheduler (Any): The object given in the ``scheduler`` key, to be
            validated as a ``DataStoreInterface`` implementation.
        data_store (Any): The object given in the ``data_store`` key, to be
            validated as a ``DataStoreInterface`` implementation.
        fallback (bool): If set, fallback to a default MemoryDataStore
            backed scheduler if both keys are unset. If this is true,
            then ``storage`` must also be provided.
        storage (StorageABC): The storage backend to use for the default data
            store (only used when ``fallback`` is True and both ``scheduler``
            and ``data_store`` are None).

    Returns:
        DataStoreInterface: An instance of an implementation of the
            ``DataStoreInterface``, for the scheduler to use to store state.

    """
    # If the configuration doesn't define a data store type, fallback to the
    # in-memory data store implementation from the old scheduler.
    if not (data_store or scheduler):
        if fallback and storage:
            click.echo(click.style(
                f"WARNING: No `scheduler` key provided in {cls.yaml_tag}, "
                f"falling back to default `{MemorySchedulerConfig.yaml_tag}`.",
                fg="bright_yellow"
            ))
            scheduler = MemoryDataStore(storage)
        else:
            click.echo(click.style(
                f"ERROR: No `scheduler` key provided in {cls.yaml_tag}. "
                f"{cls.yaml_tag} requires a scheduler backend to be defined.",
                fg="red", bold=True
            ), err=True)
            sys.exit(-1)

    # If the data_store key is specified, warn about deprecation but still use
    # it if no scheduler key is available.
    if data_store is not None:
        click.echo(click.style(
            f"WARNING: `data-store` key in {cls.yaml_tag} config is deprecated "
            "and will be removed in the future. Use `scheduler` instead.",
            fg="bright_yellow"
        ))
        if scheduler is None:
            scheduler = data_store

    return scheduler


def get_logstream_connection_info(logstream):
    logstream_url = None
    credentials = None
    logstream_instance_name = None
    if logstream:
        logstream_url = logstream['url']
        credentials = logstream.get('credentials')
        if not _validate_url_and_credentials(logstream_url, credentials=credentials):
            sys.exit(-1)
        logstream_instance_name = logstream.get('instance-name', '')

    return logstream_url, credentials, logstream_instance_name


def get_parser():

    yaml.SafeLoader.add_constructor(Channel.yaml_tag, Channel.from_yaml)
    yaml.SafeLoader.add_constructor(ExpandPath.yaml_tag, ExpandPath.from_yaml)
    yaml.SafeLoader.add_constructor(ExpandVars.yaml_tag, ExpandVars.from_yaml)
    yaml.SafeLoader.add_constructor(ReadFile.yaml_tag, ReadFile.from_yaml)
    yaml.SafeLoader.add_constructor(Execution.yaml_tag, Execution.from_yaml)
    yaml.SafeLoader.add_constructor(Bots.yaml_tag, Bots.from_yaml)
    yaml.SafeLoader.add_constructor(Action.yaml_tag, Action.from_yaml)
    yaml.SafeLoader.add_constructor(LruAction.yaml_tag, LruAction.from_yaml)
    yaml.SafeLoader.add_constructor(RemoteAction.yaml_tag, RemoteAction.from_yaml)
    yaml.SafeLoader.add_constructor(S3Action.yaml_tag, S3Action.from_yaml)
    yaml.SafeLoader.add_constructor(WriteOnceAction.yaml_tag, WriteOnceAction.from_yaml)
    yaml.SafeLoader.add_constructor(RedisAction.yaml_tag, RedisAction.from_yaml)
    yaml.SafeLoader.add_constructor(WithCacheAction.yaml_tag, WithCacheAction.from_yaml)
    yaml.SafeLoader.add_constructor(Reference.yaml_tag, Reference.from_yaml)
    yaml.SafeLoader.add_constructor(Disk.yaml_tag, Disk.from_yaml)
    yaml.SafeLoader.add_constructor(LRU.yaml_tag, LRU.from_yaml)
    yaml.SafeLoader.add_constructor(S3.yaml_tag, S3.from_yaml)
    yaml.SafeLoader.add_constructor(Redis.yaml_tag, Redis.from_yaml)
    yaml.SafeLoader.add_constructor(Remote.yaml_tag, Remote.from_yaml)
    yaml.SafeLoader.add_constructor(WithCache.yaml_tag, WithCache.from_yaml)
    yaml.SafeLoader.add_constructor(SizeDifferentiated.yaml_tag, SizeDifferentiated.from_yaml)
    yaml.SafeLoader.add_constructor(SQL_Index.yaml_tag, SQL_Index.from_yaml)
    yaml.SafeLoader.add_constructor(CAS.yaml_tag, CAS.from_yaml)
    yaml.SafeLoader.add_constructor(ByteStream.yaml_tag, ByteStream.from_yaml)
    yaml.SafeLoader.add_constructor(LogStream.yaml_tag, LogStream.from_yaml)
    yaml.SafeLoader.add_constructor(MemoryStream.yaml_tag, MemoryStream.from_yaml)
    yaml.SafeLoader.add_constructor(SQLDataStoreConfig.yaml_tag, SQLDataStoreConfig.from_yaml)
    yaml.SafeLoader.add_constructor(MemoryDataStoreConfig.yaml_tag, MemoryDataStoreConfig.from_yaml)
    yaml.SafeLoader.add_constructor(SQLSchedulerConfig.yaml_tag, SQLSchedulerConfig.from_yaml)
    yaml.SafeLoader.add_constructor(MemorySchedulerConfig.yaml_tag, MemorySchedulerConfig.from_yaml)
    yaml.SafeLoader.add_constructor(MemoryBuildEvents.yaml_tag, MemoryBuildEvents.from_yaml)

    return yaml


def get_schema():
    path = os.path.join(os.path.dirname(__file__), 'schemas', 'config.yaml')
    with open(path, encoding='utf-8') as schema_file:
        schema = yaml.safe_load(schema_file)
    return schema


def check_type(expected_type: Any) -> Callable:
    def _check_type(checker, instance) -> bool:
        return isinstance(instance, expected_type)
    return _check_type


def get_validator(schema=None):
    if schema is None:
        schema = get_schema()

    type_checker = Draft7Validator.TYPE_CHECKER.redefine_many(definitions={
        "cache": check_type(ActionCacheABC),
        "storage": check_type(StorageABC),
        "streamstorage": check_type(StreamStorageABC),
        "scheduler": check_type(DataStoreInterface),
        "!execution": check_type(ExecutionController),
        "!bots": check_type(BotsInterface),
        "!cas": check_type(ContentAddressableStorageInstance),
        "!bytestream": check_type(ByteStreamInstance),
        "!reference-cache": check_type(ReferenceCache),
        "!action-cache": check_type(ActionCache),
        "!s3action-cache": check_type(S3ActionCache),
        "!remote-action-cache": check_type(RemoteActionCache),
        "!write-once-action-cache": check_type(WriteOnceActionCache),
        "!logstream": check_type(LogStreamInstance),
        "!memory-build-events": check_type(BuildEventStreamStorage),
        "!with-cache-action-cache": check_type(WithCacheActionCache)
    })

    BgdValidator = validators.create(
        meta_schema=Draft7Validator.META_SCHEMA,
        validators=dict(Draft7Validator.VALIDATORS),
        type_checker=type_checker
    )
    return BgdValidator(schema)
