# Copyright (C) 2019 Bloomberg LP
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
SQLIndex
==================

A SQL index implementation. This can be pointed to either a remote SQL server
or a local SQLite database.

"""

from collections import deque
from contextlib import contextmanager
from datetime import datetime, timedelta
import logging
import os
import itertools
from typing import Any, BinaryIO, Deque, Dict, Iterator, List, Optional, Sequence, Tuple, Type, Union
from io import BytesIO

from alembic import command
from alembic.config import Config
from sqlalchemy import and_, create_engine, delete, event, func, select, text, Column, update
from sqlalchemy.sql.elements import BooleanClauseList
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm.query import Query
from sqlalchemy.orm.session import sessionmaker, Session as SessionType
from sqlalchemy.orm.exc import StaleDataError
from sqlalchemy.exc import SQLAlchemyError, DBAPIError

from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import Digest
from buildgrid._protos.google.rpc import code_pb2
from buildgrid._protos.google.rpc.status_pb2 import Status
from buildgrid.server.sql import sqlutils
from buildgrid.server.monitoring import get_monitoring_bus
from buildgrid.server.metrics_names import (
    CAS_INDEX_BULK_SELECT_DIGEST_TIME_METRIC_NAME,
    CAS_INDEX_BULK_TIMESTAMP_UPDATE_TIME_METRIC_NAME,
    CAS_INDEX_GET_BLOB_TIME_METRIC_NAME,
    CAS_INDEX_SAVE_DIGESTS_TIME_METRIC_NAME,
    CAS_INDEX_SIZE_CALCULATION_TIME_METRIC_NAME,
    CLEANUP_INDEX_PREMARKED_BLOBS_METRIC_NAME,
)
from buildgrid.server.metrics_utils import (
    DurationMetric,
    generator_method_duration_metric,
    publish_counter_metric
)
from buildgrid.server.persistence.sql.impl import sqlite_on_connect
from buildgrid.server.persistence.sql.models import IndexEntry
from buildgrid.settings import MIN_TIME_BETWEEN_SQL_POOL_DISPOSE_MINUTES, COOLDOWN_TIME_AFTER_POOL_DISPOSE_SECONDS
from buildgrid._exceptions import DatabaseError, StorageFullError, RetriableDatabaseError
from buildgrid.utils import read_and_rewind, validate_digest_data
from ..storage_abc import StorageABC
from .index_abc import IndexABC
from .sql_dialect_delegates import PostgreSQLDelegate, SQLiteDelegate


# Each dialect has a limit on the number of bind parameters allowed. This
# matters because it determines how large we can allow our IN clauses to get.
#
# SQLite: 1000 https://www.sqlite.org/limits.html#max_variable_number
# PostgreSQL: 32767 (Int16.MAX_VALUE) https://www.postgresql.org/docs/9.4/protocol-message-formats.html
#
# We'll refer to this as the "inlimit" in the code. The inlimits are
# set to 75% of the bind parameter limit of the implementation.
DIALECT_INLIMIT_MAP = {
    "postgresql": 24000,
    "sqlite": 750
}
DEFAULT_INLIMIT = 100

DIALECT_DELEGATES = {
    "postgresql": PostgreSQLDelegate,
    "sqlite": SQLiteDelegate
}

INLINE_BLOB_SIZE_HARD_MAXIMUM = 1000000000


class SQLIndex(IndexABC):

    def _get_default_inlimit(self, dialect) -> int:
        """ Map a connection string to its inlimit. """
        if dialect not in DIALECT_INLIMIT_MAP:
            self.__logger.warning(
                f"The SQL dialect [{dialect}] is unsupported, and "
                f"errors may occur. Supported dialects are {list(DIALECT_INLIMIT_MAP.keys())}. "
                f"Using default inclause limit of {DEFAULT_INLIMIT}.")
            return DEFAULT_INLIMIT

        return DIALECT_INLIMIT_MAP[dialect]

    def __init__(self, storage: StorageABC, connection_string: str,
                 automigrate: bool=False, window_size: int=1000,
                 inclause_limit: int=-1, connection_timeout: int=5,
                 max_inline_blob_size: int=0, **kwargs):
        base_argnames = ['fallback_on_get']
        base_args = {}
        for arg in base_argnames:
            if arg in kwargs:
                base_args[arg] = kwargs.pop(arg)
        super().__init__(**base_args)
        self.__logger = logging.getLogger(__name__)

        self._storage = storage
        self._instance_name = None

        if max_inline_blob_size > INLINE_BLOB_SIZE_HARD_MAXIMUM:
            raise ValueError(
                f"Max inline blob size is [{max_inline_blob_size}], "
                f"but must be less than [{INLINE_BLOB_SIZE_HARD_MAXIMUM}].")
        if max_inline_blob_size >= 0:
            self._max_inline_blob_size = max_inline_blob_size
        else:
            raise ValueError(
                f"Max inline blob size is [{max_inline_blob_size}], but must be nonnegative.")

        # Max # of rows to fetch while iterating over all blobs
        # (e.g. in least_recent_digests)
        self._all_blobs_window_size = window_size

        # This variable stores the list of whereclauses (SQLAlchemy BooleanClauseList objects)
        # generated from the _column_windows() using time-expensive SQL query.
        # These whereclauses are used to construct the final SQL query
        # during cleanup in order to fetch blobs by time windows.
        #
        # Inside the _column_windows() a list of timestamp boarders are obtained:
        #   intervals = [t1, t2, t3, ...]
        # Then the generated whereclauses might represent semantically as, for example,:
        #   self._queue_of_whereclauses = [
        #     "WHERE t1 <= IndexEntry.accessed_timestamp < t2",
        #     "WHERE t2 <= IndexEntry.accessed_timestamp < t3",
        #     "WHERE t3 <= IndexEntry.accessed_timestamp < t4",
        #     ... and so on]
        # Note the number of entries in each window is determined by
        # the instance variable "_all_blobs_window_size".
        self._queue_of_whereclauses: Deque[BooleanClauseList] = deque()

        # Only pass known kwargs to db session
        available_options = {'pool_size', 'max_overflow', 'pool_timeout',
                             'pool_pre_ping', 'pool_recycle'}
        kwargs_keys = kwargs.keys()
        if kwargs_keys > available_options:
            unknown_args = kwargs_keys - available_options
            raise TypeError(f"Unknown keyword arguments: [{unknown_args}]")

        self._create_sqlalchemy_engine(
            connection_string, automigrate, connection_timeout, **kwargs)

        self._sql_pool_dispose_helper = sqlutils.SQLPoolDisposeHelper(COOLDOWN_TIME_AFTER_POOL_DISPOSE_SECONDS,
                                                                      MIN_TIME_BETWEEN_SQL_POOL_DISPOSE_MINUTES,
                                                                      self._engine)

        # Dialect-specific initialization
        dialect = self._engine.dialect.name
        self._dialect_delegate = DIALECT_DELEGATES.get(dialect)

        if inclause_limit > 0:
            if inclause_limit > window_size:
                self.__logger.warning(
                    f"Configured inclause_limit [{inclause_limit}] "
                    f"is greater than window_size [{window_size}]")
            self._inclause_limit = inclause_limit
        else:
            # If the inlimit isn't explicitly set, we use a default that
            # respects both the window size and the db implementation's
            # inlimit.
            self._inclause_limit = min(
                window_size,
                self._get_default_inlimit(dialect))
            self.__logger.debug("SQL index: using default inclause limit "
                                f"of {self._inclause_limit}")

        session_factory = sessionmaker(future=True)
        self.Session = scoped_session(session_factory)
        self.Session.configure(bind=self._engine)

        # Make a test query against the database to ensure the connection is valid
        with self.session() as session:
            session.query(IndexEntry).first()

    def _create_or_migrate_db(self, connection_string: str) -> None:
        self.__logger.warning(
            "Will attempt migration to latest version if needed.")

        config = Config()

        config.set_main_option("script_location",
                               os.path.join(
                                   os.path.dirname(__file__),
                                   "../../../persistence/sql/alembic"))

        with self._engine.begin() as connection:
            config.attributes['connection'] = connection
            command.upgrade(config, "head")

    def _create_sqlalchemy_engine(self, connection_string, automigrate, connection_timeout, **kwargs):
        self.automigrate = automigrate

        # Disallow sqlite in-memory because multi-threaded access to it is
        # complex and potentially problematic at best
        # ref: https://docs.sqlalchemy.org/en/13/dialects/sqlite.html#threading-pooling-behavior
        if sqlutils.is_sqlite_inmemory_connection_string(connection_string):
            raise ValueError("Cannot use SQLite in-memory with BuildGrid (connection_string="
                             f"[{connection_string}]). Use a file or leave the connection_string "
                             "empty for a tempfile.")

        if connection_timeout is not None:
            if "connect_args" not in kwargs:
                kwargs["connect_args"] = {}
            if sqlutils.is_sqlite_connection_string(connection_string):
                kwargs["connect_args"]["timeout"] = connection_timeout
            elif sqlutils.is_psycopg2_connection_string(connection_string):
                kwargs["connect_args"]["connect_timeout"] = connection_timeout
                # Additional postgres specific timeouts
                # Additional libpg options
                # Note that those timeouts are in milliseconds (so *1000)
                kwargs["connect_args"]["options"] = f'-c lock_timeout={connection_timeout * 1000}'

        # Only pass the (known) kwargs that have been explicitly set by the user
        available_options = set([
            'pool_size', 'max_overflow', 'pool_timeout', 'pool_pre_ping',
            'pool_recycle', 'connect_args'
        ])
        kwargs_keys = set(kwargs.keys())
        if not kwargs_keys.issubset(available_options):
            unknown_options = kwargs_keys - available_options
            raise TypeError(f"Unknown keyword arguments: [{unknown_options}]")

        self.__logger.debug(f"SQLAlchemy additional kwargs: [{kwargs}]")

        self._engine = create_engine(connection_string, echo=False, future=True, **kwargs)

        self.__logger.info(f"Using SQL backend for index at connection [{repr(self._engine.url)}] "
                           f"using additional SQL options {kwargs}")

        if self._engine.dialect.name == "sqlite":
            event.listen(self._engine, "connect", sqlite_on_connect)

        if self.automigrate:
            self._create_or_migrate_db(connection_string)

    @contextmanager
    def session(self, *, sqlite_lock_immediately: bool=False,
                exceptions_to_not_raise_on: Optional[List[Type[SQLAlchemyError]]]=None,
                exceptions_to_not_rollback_on: Optional[List[Type[SQLAlchemyError]]]=None) -> Any:
        """ Context manager for convenience use of sessions. Automatically
        commits when the context ends and rolls back failed transactions.

        Setting sqlite_lock_immediately will only yield a session once the
        SQLite database has been locked for exclusive use.

        exceptions_to_not_raise_on is a list of exceptions which the session will not re-raise on.
        However, the session will still rollback on these errors.

        exceptions_to_not_rollback_on is a list of exceptions which the session manager will not
        re-raise or rollback on. Being very careful specifying this option. It should only be used
        in cases where you know the exception will not put the database in a bad state.
        """
        # If we recently disposed of the SQL pool due to connection issues
        # allow for some cooldown period before we attempt more SQL
        self._sql_pool_dispose_helper.wait_if_cooldown_in_effect()

        # Try to obtain a session
        try:
            session = self.Session()
            if sqlite_lock_immediately and session.bind.name == "sqlite":  # type: ignore
                session.execute(text("BEGIN IMMEDIATE"))
        except Exception as e:
            self.__logger.error("Unable to obtain an Index database session.", exc_info=True)
            raise DatabaseError("Unable to obtain an Index database session.") from e

        # Yield the session and catch exceptions that occur while using it
        # to roll-back if needed
        try:
            yield session
            session.commit()
        except Exception as e:
            transient_dberr = self._sql_pool_dispose_helper.check_dispose_pool(session, e)
            if exceptions_to_not_rollback_on and type(e) in exceptions_to_not_rollback_on:
                pass
            else:
                session.rollback()
                if transient_dberr:
                    self.__logger.warning("Rolling back Index database session due to transient database error.",
                                          exc_info=True)
                else:
                    self.__logger.error("Error committing Index database session. Rolling back.", exc_info=True)
                if exceptions_to_not_raise_on is None or type(e) not in exceptions_to_not_raise_on:
                    if transient_dberr:
                        raise RetriableDatabaseError(
                            "Database connection was temporarily interrupted, please retry",
                            timedelta(seconds=COOLDOWN_TIME_AFTER_POOL_DISPOSE_SECONDS)) from e
                    raise
        finally:
            session.close()

    def setup_grpc(self):
        self._storage.setup_grpc()

    def has_blob(self, digest: Digest) -> bool:
        with self.session() as session:
            statement = select(func.count(IndexEntry.digest_hash)).where(
                IndexEntry.digest_hash == digest.hash
            )

            num_entries = session.execute(statement).scalar()
            if num_entries == 1:
                return True
            elif num_entries < 1:
                return False
            else:
                raise RuntimeError(
                    f"Multiple results found for blob [{digest}]. "
                    "The index is in a bad state.")

    def _fetch_one_entry(self, digest: Digest, session: SessionType) -> Optional[IndexEntry]:
        """ Fetch an IndexEntry by its corresponding digest. Returns None if not found. """
        statement = select(IndexEntry).where(
            IndexEntry.digest_hash == digest.hash
        )

        result = session.execute(statement)
        return result.scalar()  # Raises MultipleResultsFound if multiple rows found

    def _backfill_digest_and_return_reader(self, digest: Digest, session: SessionType) -> Optional[BinaryIO]:
        """ Fetch a blob from storage and call _save_digests_to_index on it.

        Returns the value of storage.get_blob() (a readable IO object if found, or None if not found).
        """
        blob_read_head = self._storage.get_blob(digest)

        if blob_read_head:
            blob_data = None
            if digest.size_bytes <= self._max_inline_blob_size:
                blob_data = read_and_rewind(blob_read_head)
            self._save_digests_to_index([(digest, blob_data)], session)
        else:
            self.__logger.warning(f"Unable to find blob [{digest.hash}] in storage")

        return blob_read_head

    def _update_blob_timestamp(self, digest: Digest, session: SessionType,
                               sync_mode: Union[bool, str]=False) -> int:
        """ Refresh a blob's timestamp and return the number of rows updated """
        statement = update(IndexEntry).where(
            IndexEntry.digest_hash == digest.hash
        ).values(accessed_timestamp=datetime.utcnow())

        options = {"synchronize_session": sync_mode}
        num_rows_updated = session.execute(statement, execution_options=options).rowcount  # type: ignore
        return num_rows_updated

    @DurationMetric(CAS_INDEX_GET_BLOB_TIME_METRIC_NAME, instanced=True)
    def get_blob(self, digest: Digest) -> Optional[BinaryIO]:
        blob_read_head = None
        with self.session() as session:
            entry = self._fetch_one_entry(digest, session)

            if entry is None:
                # No blob was found
                # If fallback is enabled, try to fetch the blob and add it to the index
                if self._fallback_on_get:
                    return self._backfill_digest_and_return_reader(digest, session)

                return None
            else:
                # Found the entry, now try to find the blob inline
                if entry.inline_blob is not None:
                    self._update_blob_timestamp(digest, session)
                    return BytesIO(entry.inline_blob)
                else:
                    blob_read_head = self._backfill_digest_and_return_reader(digest, session)
                    if blob_read_head is None:
                        self.__logger.warning(f"Blob [{digest.hash}] was indexed but not in storage")

                    return blob_read_head

    def delete_blob(self, digest: Digest) -> None:
        statement = delete(IndexEntry).where(
            IndexEntry.digest_hash == digest.hash
        )
        options = {"synchronize_session": False}

        with self.session() as session:
            session.execute(statement, execution_options=options)

        self._storage.delete_blob(digest)

    def begin_write(self, digest: Digest) -> BinaryIO:
        if digest.size_bytes <= self._max_inline_blob_size:
            return BytesIO()
        else:
            return self._storage.begin_write(digest)

    def commit_write(self, digest: Digest, write_session: BinaryIO) -> None:
        # pylint: disable=no-name-in-module,import-outside-toplevel
        inline_blob = None
        if digest.size_bytes > self._max_inline_blob_size:
            self._storage.commit_write(digest, write_session)
        else:
            write_session.seek(0)
            inline_blob = write_session.read()
        try:
            with self.session() as session:
                self._save_digests_to_index([(digest, inline_blob)], session)
        except DBAPIError as error:
            # Error has pgcode attribute (Postgres only)
            if hasattr(error.orig, 'pgcode'):
                # imported here to avoid global dependency on psycopg2
                from psycopg2.errors import DiskFull, OutOfMemory  # type: ignore
                # 53100 == DiskFull && 53200 == OutOfMemory
                if error.orig.pgerror in [DiskFull, OutOfMemory]:
                    raise StorageFullError(f"Postgres Error: {error.orig.pgcode}") from error
            raise error

    def _partitioned_hashes(self, digests: Sequence[Digest]) -> Iterator[Iterator[str]]:
        """ Given a long list of digests, split it into parts no larger than
        _inclause_limit and yield the hashes in each part.
        """
        for part_start in range(0, len(digests), self._inclause_limit):
            part_end = min(len(digests), part_start + self._inclause_limit)
            part_digests = itertools.islice(digests, part_start, part_end)
            yield map(lambda digest: digest.hash, part_digests)

    @generator_method_duration_metric(CAS_INDEX_BULK_SELECT_DIGEST_TIME_METRIC_NAME)
    def _bulk_select_digests(self, digests: Sequence[Digest], fetch_blobs: bool = False) -> Iterator[IndexEntry]:
        """ Generator that selects all rows matching a digest list.

        SQLAlchemy Core is used for this because the ORM has problems with
        large numbers of bind variables for WHERE IN clauses.

        We only select on the digest hash (not hash and size) to allow for
        index-only queries on db backends that support them.
        """
        index_table = IndexEntry.__table__
        with self.session() as session:
            columns = [index_table.c.digest_hash]
            if fetch_blobs:
                columns.append(index_table.c.inline_blob)
            for part in self._partitioned_hashes(digests):
                stmt = select(
                    columns
                ).where(
                    index_table.c.digest_hash.in_(part)
                )
                entries = session.execute(stmt)
                yield from entries

    @DurationMetric(CAS_INDEX_BULK_TIMESTAMP_UPDATE_TIME_METRIC_NAME, instanced=True)
    def _bulk_refresh_timestamps(self, digests: Sequence[Digest], session: SessionType,
                                 update_time: Optional[datetime]=None):
        """ Refresh all timestamps of the input digests.

        SQLAlchemy Core is used for this because the ORM is not suitable for
        bulk inserts and updates.

        https://docs.sqlalchemy.org/en/13/faq/performance.html#i-m-inserting-400-000-rows-with-the-orm-and-it-s-really-slow
        """
        index_table = IndexEntry.__table__
        # If a timestamp was passed in, use it
        if update_time:
            timestamp = update_time
        else:
            timestamp = datetime.utcnow()
        for part in self._partitioned_hashes(digests):
            stmt = index_table.update().where(
                index_table.c.digest_hash.in_(
                    select([index_table.c.digest_hash], index_table.c.digest_hash.in_(part)
                           ).with_for_update(skip_locked=True))
            ).values(
                accessed_timestamp=timestamp
            )
            session.execute(stmt)

    def missing_blobs(self, digests: List[Digest]) -> List[Digest]:
        entries = self._bulk_select_digests(digests)

        found_hashes = {entry.digest_hash for entry in entries}

        # Split the digests into two found/missing lists
        found_digests, missing_digests = [], []
        for digest in digests:
            if digest.hash in found_hashes:
                found_digests.append(digest)
            else:
                missing_digests.append(digest)

        # Update all timestamps for blobs which were found
        with self.session() as session:
            self._bulk_refresh_timestamps(found_digests, session)

        return missing_digests

    @DurationMetric(CAS_INDEX_SAVE_DIGESTS_TIME_METRIC_NAME, instanced=True)
    def _save_digests_to_index(self,
                               digest_blob_pairs: List[Tuple[Digest, Optional[bytes]]],
                               session: SessionType) -> None:
        """ Helper to persist a list of digest/blob pairs to the index.

        Any digests present are updated, and new digests are inserted along with their inline blobs (if provided).
        Only blobs with size less than or equal to the max_inline_blob_size are inserted directly into the index.
        """
        if not digest_blob_pairs:
            return

        if self._dialect_delegate:
            try:
                self._dialect_delegate._save_digests_to_index(  # type: ignore
                    digest_blob_pairs, session, self._max_inline_blob_size)
                return
            except AttributeError:
                pass

        update_time = datetime.utcnow()
        # Figure out which digests we can just update
        digests = [digest for (digest, blob) in digest_blob_pairs]
        entries = self._bulk_select_digests(digests)
        # Map digests to new entries
        entries_not_present = {
            digest.hash: {
                'digest_hash': digest.hash,
                'digest_size_bytes': digest.size_bytes,
                'accessed_timestamp': update_time,
                'inline_blob': (blob if digest.size_bytes <= self._max_inline_blob_size else None)
            }
            for (digest, blob) in digest_blob_pairs
        }

        entries_present = {}
        for entry in entries:
            entries_present[entry.digest_hash] = entries_not_present[entry.digest_hash]
            del entries_not_present[entry.digest_hash]

        if entries_not_present:
            session.bulk_insert_mappings(IndexEntry, entries_not_present.values())  # type: ignore
        if entries_present:
            session.bulk_update_mappings(IndexEntry, entries_present.values())  # type: ignore

    def bulk_update_blobs(self, digest_blob_pairs: List[Tuple[Digest, bytes]]) -> List[Status]:
        """ Implement the StorageABC's bulk_update_blobs method.

        The StorageABC interface takes in a list of digest/blob pairs and
        returns a list of results. The list of results MUST be ordered to
        correspond with the order of the input list. """
        pairs_to_store = []
        result_map = {}

        # For each blob, determine whether to store it in the backing storage or inline it
        for (digest, blob) in digest_blob_pairs:
            if digest.size_bytes > self._max_inline_blob_size:
                pairs_to_store.append((digest, blob))
            else:
                if validate_digest_data(digest, blob):
                    result_map[digest.hash] = Status(code=code_pb2.OK)
                else:
                    result_map[digest.hash] = Status(
                        code=code_pb2.INVALID_ARGUMENT,
                        message="Data doesn't match hash"
                    )
        backup_results = self._storage.bulk_update_blobs(pairs_to_store)

        for (digest, blob), result in zip(pairs_to_store, backup_results):
            if digest.hash in result_map:
                # ERROR: blob was both inlined and backed up
                raise RuntimeError(
                    "Blob was both inlined and backed up.")
            result_map[digest.hash] = result

        # Generate the final list of results
        pairs_to_inline = []
        results = []
        for (digest, blob) in digest_blob_pairs:
            status = result_map.get(
                digest.hash,
                Status(code=code_pb2.UNKNOWN, message="SQL Index: unable to determine the status of this blob")
            )
            results.append(status)
            if status.code == code_pb2.OK:
                pairs_to_inline.append((digest, blob))

        with self.session() as session:
            self._save_digests_to_index(pairs_to_inline, session)

        return results

    def _bulk_read_blobs_with_fallback(self, digests: List[Digest]) -> Dict[str, BinaryIO]:
        hash_to_digest = {digest.hash: digest for digest in digests}
        results: Dict[str, BinaryIO] = {}
        digests_to_refresh_timestamps = []

        # Fetch inlined blobs directly from the index and mark them for needing their timestamps refreshed
        entries = self._bulk_select_digests(digests, fetch_blobs=True)
        for e in entries:
            blob, digest_hash, digest = e.inline_blob, e.digest_hash, hash_to_digest[e.digest_hash]
            if blob is not None:
                results[digest_hash] = BytesIO(blob)
                digests_to_refresh_timestamps.append(digest)
                hash_to_digest.pop(digest_hash)

        # Fetch everything that wasn't inlined from the backing storage
        fetched_digests = self._storage.bulk_read_blobs(hash_to_digest.values())

        # Save everything fetched from storage, inlining the blobs if they're small enough
        digest_pairs_to_save = []
        for digest_hash, blob_read_head in fetched_digests.items():
            if blob_read_head is not None:
                digest = hash_to_digest[digest_hash]
                blob_data = None
                if digest.size_bytes <= self._max_inline_blob_size:
                    blob_data = read_and_rewind(blob_read_head)
                digest_pairs_to_save.append((digest, blob_data))
                results[digest_hash] = blob_read_head

        with self.session() as session:
            self._save_digests_to_index(digest_pairs_to_save, session)
            # Update the timestamps on the existing inlined blobs
            self._bulk_refresh_timestamps(digests_to_refresh_timestamps, session)

        return results

    def bulk_read_blobs(self, digests: List[Digest]) -> Dict[str, BinaryIO]:
        if self._fallback_on_get:
            return self._bulk_read_blobs_with_fallback(digests)

        # If fallback is disabled, query the index first and only
        # query the storage for blobs that weren't inlined there

        hash_to_digest = {digest.hash: digest for digest in digests}  # hash -> digest map
        results: Dict[str, BinaryIO] = {}  # The final list of results (return value)
        digests_to_fetch = []  # Digests that need to be fetched from storage
        digest_pairs_to_save = []  # Digests that need to be updated in the index
        digests_to_refresh_timestamps = []  # Digests that only need their timestamps refreshed

        # Fetch all of the digests in the database
        # Anything that wasn't already inlined needs to be fetched
        entries = self._bulk_select_digests(digests, fetch_blobs=True)
        for index_entry in entries:
            digest = hash_to_digest[index_entry.digest_hash]
            if index_entry.inline_blob is not None:
                results[index_entry.digest_hash] = BytesIO(index_entry.inline_blob)
                digests_to_refresh_timestamps.append(digest)
            else:
                digests_to_fetch.append(digest)

        fetched_digests = {}
        if digests_to_fetch:
            fetched_digests = self._storage.bulk_read_blobs(digests_to_fetch)

        # Generate the list of inputs for _save_digests_to_index
        #
        # We only need to send blob data for small blobs fetched
        # from the storage since everything else is either too
        # big or already inlined
        for digest in digests_to_fetch:
            blob_data = None
            if (digest.size_bytes <= self._max_inline_blob_size and
                fetched_digests.get(digest.hash) is not None):
                blob_data = read_and_rewind(fetched_digests[digest.hash])
                digest_pairs_to_save.append((digest, blob_data))
            else:
                digests_to_refresh_timestamps.append(digest)

        # Update any blobs which need to be inlined
        with self.session() as session:
            self._save_digests_to_index(digest_pairs_to_save, session)
            # Update the timestamps on existing blobs
            self._bulk_refresh_timestamps(digests_to_refresh_timestamps, session)

        results.update(fetched_digests)
        return results

    def _column_windows(self, session: SessionType, column: Column) -> Iterator[BooleanClauseList]:
        """ Adapted from the sqlalchemy WindowedRangeQuery recipe.
        https://github.com/sqlalchemy/sqlalchemy/wiki/WindowedRangeQuery

        This method breaks the timestamp range into windows and yields
        the borders of these windows to the callee. For example, the borders
        yielded by this might look something like
        ('2019-10-08 18:25:03.699863', '2019-10-08 18:25:03.751018')
        ('2019-10-08 18:25:03.751018', '2019-10-08 18:25:03.807867')
        ('2019-10-08 18:25:03.807867', '2019-10-08 18:25:03.862192')
        ('2019-10-08 18:25:03.862192',)

        _windowed_lru_digests uses these borders to form WHERE clauses for its
        SELECTs. In doing so, we make sure to repeatedly query the database for
        live updates, striking a balance between loading the entire resultset
        into memory and querying each row individually, both of which are
        inefficient in the context of a large index.

        The window size is a parameter and can be configured. A larger window
        size will yield better performance (fewer SQL queries) at the cost of
        memory (holding on to the results of the query) and accuracy (blobs
        may get updated while you're working on them), and vice versa for a
        smaller window size.
        """

        def int_for_range(start_id: Any, end_id: Any) -> BooleanClauseList:
            if end_id:
                return and_(
                    column >= start_id,
                    column < end_id
                )
            else:
                return column >= start_id

        # Constructs a query that:
        # 1. Gets all the timestamps in sorted order.
        # 2. Assign a row number to each entry.
        # 3. Only keep timestamps that are every other N row number apart. N="_all_blobs_window_size".
        # SELECT
        #   anon_1.index_accessed_timestamp AS anon_1_index_accessed_timestamp
        #   FROM (
        #       SELECT
        #       index.accessed_timestamp AS index_accessed_timestamp,
        #       row_number() OVER (ORDER BY index.accessed_timestamp) AS rownum
        #       FROM index
        #       )
        #  AS anon_1
        #  WHERE rownum % 1000=1
        #
        # Note:
        #  - This query can be slow due to checking each entry with "WHERE rownum % 1000=1".
        #  - These timestamps will be the basis for constructing the SQL "WHERE" clauses later.
        q = session.query(
            column,
            func.row_number()
                .over(order_by=column)
                .label('rownum')
        ).from_self(column)
        if self._all_blobs_window_size > 1:
            q = q.filter(text(f"rownum % {self._all_blobs_window_size}=1"))

        # Execute the underlying query against the database.
        # Ex: intervals = [t1, t1001, t2001, ...], q = [(t1, ), (t1001, ), (t2001, ), ...]
        intervals = [id for id, in q]

        # Generate the whereclauses
        while intervals:
            start = intervals.pop(0)
            if intervals:
                end = intervals[0]
            else:
                end = None
            # Ex: yield "WHERE IndexEntry.accessed_timestamp >= start AND IndexEntry.accessed_timestamp < end"
            yield int_for_range(start, end)

    def _windowed_lru_digests(self, q: Query, column: Column, renew_whereclauses=True) -> Iterator[IndexEntry]:
        """ Generate a query for each window produced by _column_windows
        and yield the results one by one.
        """
        # Determine whether the conditions are met to make an SQL call to get new windows.
        msg = "Using stored LRU windows"
        if renew_whereclauses or len(self._queue_of_whereclauses) == 0:
            msg = "Requesting new LRU windows."
            self._queue_of_whereclauses = deque(self._column_windows(q.session, column))  # type: ignore

        msg += f" Number of windows remaining: {len(self._queue_of_whereclauses)}"
        self.__logger.debug(msg)

        while self._queue_of_whereclauses:
            whereclause = self._queue_of_whereclauses[0]  # type: ignore
            window = q.filter(whereclause).order_by(column.asc())
            yield from window

            # If yield from window doesn't get to this point that means
            # the cleanup hasn't consumed all the content in a whereclause and exited.
            # Otherwise, the whereclause is exhausted and can be discarded.
            self._queue_of_whereclauses.popleft()

    def least_recent_digests(self) -> Iterator[Digest]:
        with self.session() as session:
            q = session.query(IndexEntry)
            for index_entry in self._windowed_lru_digests(q, IndexEntry.accessed_timestamp):
                yield Digest(hash=index_entry.digest_hash, size_bytes=index_entry.digest_size_bytes)

    @DurationMetric(CAS_INDEX_SIZE_CALCULATION_TIME_METRIC_NAME)
    def get_total_size(self) -> int:
        statement = select(func.sum(IndexEntry.digest_size_bytes))
        with self.session() as session:
            return session.execute(statement).scalar()

    def mark_n_bytes_as_deleted(self, n_bytes: int, dry_run: bool=False,
                                protect_blobs_after: Optional[datetime]=None,
                                renew_windows: bool=True) -> List[Digest]:
        # pylint: disable=singleton-comparison
        if protect_blobs_after is None:
            protect_blobs_after = datetime.utcnow()
        gathered = 0
        with self.session(exceptions_to_not_rollback_on=[StaleDataError]) as session:
            size_query = session.query(func.sum(IndexEntry.digest_size_bytes))
            premarked_size = size_query.filter_by(deleted=True).scalar()
            if premarked_size is None:
                premarked_size = 0

            q = session.query(IndexEntry)
            entries = q.filter_by(deleted=True).order_by(IndexEntry.accessed_timestamp).all()
            monitoring_bus = get_monitoring_bus()
            if monitoring_bus.is_enabled:
                metadata = {}
                if self._instance_name:
                    metadata["instance-name"] = self._instance_name
                publish_counter_metric(
                    CLEANUP_INDEX_PREMARKED_BLOBS_METRIC_NAME,
                    len(entries),
                    metadata
                )

            if premarked_size < n_bytes:
                gathered = premarked_size
                q = q.filter(IndexEntry.deleted == False, IndexEntry.accessed_timestamp <
                             protect_blobs_after).with_for_update(skip_locked=True)
                iterator = self._windowed_lru_digests(q, IndexEntry.accessed_timestamp,
                                                      renew_windows)
                while gathered < n_bytes:
                    try:
                        index_entry = next(iterator)
                    except StopIteration:
                        break
                    gathered += index_entry.digest_size_bytes  # type: ignore
                    self.__logger.debug(f"Gathering {gathered} out of {n_bytes} bytes(max)")
                    entries.append(index_entry)
                    if not dry_run:
                        index_entry.deleted = True

            return [
                Digest(hash=entry.digest_hash, size_bytes=entry.digest_size_bytes)
                for entry in entries
            ]

    def bulk_delete(self, digests: List[Digest]) -> List[str]:
        self.__logger.debug(f"Deleting {len(digests)} digests from the index")
        index_table = IndexEntry.__table__
        hashes = [x.hash for x in digests]

        # Make sure we don't exceed maximum size of an IN clause
        n = self._inclause_limit
        hash_chunks = [hashes[i:i + n] for i in range(0, len(hashes), n)]

        # We will not raise, rollback, or log on StaleDataErrors.
        # These errors occur when we delete fewer rows than we were expecting.
        # This is fine, since the missing rows will get deleted eventually.
        # When running bulk_deletes concurrently, StaleDataErrors
        # occur too often to log.
        num_blobs_deleted = 0
        with self.session(exceptions_to_not_rollback_on=[StaleDataError]) as session:
            for chunk in hash_chunks:
                # Do not wait for locks when deleting rows. Skip locked rows to
                # avoid deadlocks.
                stmt = index_table.delete().where(
                    index_table.c.digest_hash.in_(select(
                        [index_table.c.digest_hash], index_table.c.digest_hash.in_(chunk)
                    ).with_for_update(skip_locked=True))
                )
                num_blobs_deleted += session.execute(stmt).rowcount
        self.__logger.debug(f"{num_blobs_deleted}/{len(digests)} blobs deleted from the index")

        # bulk_delete is typically expected to return the digests that were not deleted,
        # but delete only returns the number of rows deleted and not what was/wasn't
        # deleted. Getting this info would require extra queries, so assume that
        # everything was either deleted or already deleted. Failures will continue to throw
        return []
