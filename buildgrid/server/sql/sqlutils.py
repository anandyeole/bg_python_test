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

import logging
import time

from typing import Any, Tuple, Optional

from threading import Lock, Event
from datetime import datetime, timedelta

from sqlalchemy.orm.session import Session as SessionType


def is_sqlite_connection_string(connection_string: str) -> bool:
    if connection_string:
        return connection_string.startswith("sqlite")
    return False


def is_psycopg2_connection_string(connection_string: str) -> bool:
    if connection_string:
        if connection_string.startswith("postgresql:"):
            return True
        if connection_string.startswith("postgresql+psycopg2:"):
            return True
    return False


def is_sqlite_inmemory_connection_string(full_connection_string: str) -> bool:
    if is_sqlite_connection_string(full_connection_string):
        # Valid connection_strings for in-memory SQLite which we don't support could look like:
        # "sqlite:///file:memdb1?option=value&cache=shared&mode=memory",
        # "sqlite:///file:memdb1?mode=memory&cache=shared",
        # "sqlite:///file:memdb1?cache=shared&mode=memory",
        # "sqlite:///file::memory:?cache=shared",
        # "sqlite:///file::memory:",
        # "sqlite:///:memory:",
        # "sqlite:///",
        # "sqlite://"
        # ref: https://www.sqlite.org/inmemorydb.html
        # Note that a user can also specify drivers, so prefix could become 'sqlite+driver:///'
        connection_string = full_connection_string

        uri_split_index = connection_string.find("?")
        if uri_split_index != -1:
            connection_string = connection_string[0:uri_split_index]

        if connection_string.endswith((":memory:", ":///", "://")):
            return True
        elif uri_split_index != -1:
            opts = full_connection_string[uri_split_index + 1:].split("&")
            if "mode=memory" in opts:
                return True

    return False


class SQLPoolDisposeHelper():
    """ Helper class for disposing of SQL session connections """

    def __init__(self, cooldown_time_in_secs: int,
                 min_time_between_dispose_in_minutes: int,
                 sql_engine) -> None:
        self._logger = logging.getLogger(__name__)

        self._cooldown_time_in_secs = cooldown_time_in_secs
        self._min_time_between_dispose_in_minutes = min_time_between_dispose_in_minutes
        self._last_pool_dispose_time: Optional[datetime] = None
        self._last_pool_dispose_time_lock = Lock()
        # When this event is set, SQL queries are executed right away.
        # During pool disposal, this event is unset for a period of time and new SQL queries
        # wait until it's set (or a timeout) before they attempt to execute the query.
        self._pool_dispose_cooldown_event = Event()
        self._pool_dispose_cooldown_event.set()
        self._sql_engine = sql_engine
        self._dispose_pool_on_exceptions: Tuple[Any, ...] = tuple()
        if self._sql_engine.dialect.name == 'postgresql':
            import psycopg2  # pylint: disable=import-outside-toplevel
            self._dispose_pool_on_exceptions = (psycopg2.errors.ReadOnlySqlTransaction, psycopg2.errors.AdminShutdown)

    def check_dispose_pool(self, session: SessionType, e: Exception) -> bool:
        """ For selected exceptions invalidate the SQL session and then sleep
            - returns True when a transient sql connection error is detected
            - returns False otherwise
        """

        # Only do this if the config is relevant
        if not self._dispose_pool_on_exceptions:
            return False

        # Make sure we have a SQL-related cause to check, otherwise skip
        if e.__cause__ and not isinstance(e.__cause__, Exception):
            return False

        cause_type = type(e.__cause__)
        # Let's see if this exception is related to known disconnect exceptions
        is_connection_error = cause_type in self._dispose_pool_on_exceptions
        if not is_connection_error:
            return False

        # Make sure this connection will not be re-used
        session.invalidate()
        self._logger.info(
            f'Detected a SQL exception=[{cause_type.__name__}] related to the connection. '
            'Invalidating this connection.'
        )

        # Only allow disposal every self.__min_time_between_dispose_in_minutes
        now = datetime.utcnow()
        only_if_after = None

        # Check if we should dispose the rest of the checked in connections
        with self._last_pool_dispose_time_lock:
            if self._last_pool_dispose_time:
                only_if_after = self._last_pool_dispose_time + \
                    timedelta(minutes=self._min_time_between_dispose_in_minutes)
            if only_if_after and now < only_if_after:
                return True

            # OK, we haven't disposed the pool recently
            self._last_pool_dispose_time = now
            self._logger.info('Disposing connection pool and will make new requests wait until '
                              f'{self._cooldown_time_in_secs}s from now before attempting'
                              ' to reconnect. This will give new requests a fresh SQL connection.')
            self._sql_engine.dispose()

            # Clear the cooldown event to inform all subsequent SQL sessions to wait a bit
            self._pool_dispose_cooldown_event.clear()
            time.sleep(self._cooldown_time_in_secs)
            self._pool_dispose_cooldown_event.set()

        return True

    def wait_if_cooldown_in_effect(self):
        # If we recently disposed of the SQL pool due to connection issues
        # allow for some cooldown period before we attempt more SQL
        if not self._pool_dispose_cooldown_event.is_set():
            self._pool_dispose_cooldown_event.wait(timeout=self._cooldown_time_in_secs)
            # Set this anyway, just in case the thread responsible for this failed
            self._pool_dispose_cooldown_event.set()
