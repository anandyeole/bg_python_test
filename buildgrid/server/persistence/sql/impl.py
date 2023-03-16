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


from contextlib import contextmanager
import logging
import os
import select
from threading import Thread
import time
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile
from typing import Callable, Dict, List, Tuple, NamedTuple, Optional

from alembic import command
from alembic.config import Config
from sqlalchemy import and_, create_engine, delete, event, func, union, literal_column, update
from sqlalchemy import select as sql_select
from sqlalchemy.future import Connection
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.sql.expression import Select

from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.google.longrunning import operations_pb2
from buildgrid._enums import LeaseState, MetricCategories, OperationStage
from buildgrid.server.sql import sqlutils
from buildgrid.server.metrics_names import (
    BOTS_ASSIGN_JOB_LEASES_TIME_METRIC_NAME,
    DATA_STORE_CHECK_FOR_UPDATE_TIME_METRIC_NAME,
    DATA_STORE_CREATE_JOB_TIME_METRIC_NAME,
    DATA_STORE_CREATE_LEASE_TIME_METRIC_NAME,
    DATA_STORE_CREATE_OPERATION_TIME_METRIC_NAME,
    DATA_STORE_GET_JOB_BY_DIGEST_TIME_METRIC_NAME,
    DATA_STORE_GET_JOB_BY_NAME_TIME_METRIC_NAME,
    DATA_STORE_GET_JOB_BY_OPERATION_TIME_METRIC_NAME,
    DATA_STORE_LIST_OPERATIONS_TIME_METRIC_NAME,
    DATA_STORE_PRUNER_NUM_ROWS_DELETED_METRIC_NAME,
    DATA_STORE_PRUNER_DELETE_TIME_METRIC_NAME,
    DATA_STORE_QUEUE_JOB_TIME_METRIC_NAME,
    DATA_STORE_STORE_RESPONSE_TIME_METRIC_NAME,
    DATA_STORE_UPDATE_JOB_TIME_METRIC_NAME,
    DATA_STORE_UPDATE_LEASE_TIME_METRIC_NAME,
    DATA_STORE_UPDATE_OPERATION_TIME_METRIC_NAME
)
from buildgrid.server.job import Job as InternalJob
from buildgrid.server.job_metrics import JobMetrics
from buildgrid.server.metrics_utils import DurationMetric, Counter
from buildgrid.server.operations.filtering import OperationFilter, SortKey, DEFAULT_SORT_KEYS
from buildgrid.server.persistence.interface import DataStoreInterface
from buildgrid.server.persistence.sql.models import digest_to_string, Job, Lease, Operation
from buildgrid.server.persistence.sql.utils import (
    build_page_filter,
    build_page_token,
    extract_sort_keys,
    build_custom_filters,
    build_sort_column_list
)
from buildgrid.settings import (
    MIN_TIME_BETWEEN_SQL_POOL_DISPOSE_MINUTES,
    COOLDOWN_TIME_AFTER_POOL_DISPOSE_SECONDS,
    SQL_SCHEDULER_METRICS_PUBLISH_INTERVAL_SECONDS
)
from buildgrid.utils import JobState, hash_from_dict, convert_values_to_sorted_lists

from buildgrid._exceptions import DatabaseError, RetriableDatabaseError


Session = sessionmaker(future=True)


def sqlite_on_connect(conn, record):
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")


class PruningOptions(NamedTuple):
    pruner_job_max_age: timedelta = timedelta(days=30)
    pruner_period: timedelta = timedelta(minutes=5)
    pruner_max_delete_window: int = 10000

    @staticmethod
    def from_config(pruner_job_max_age_cfg: Dict[str, float],
                    pruner_period_cfg: Dict[str, float] = None,
                    pruner_max_delete_window_cfg: int = None):
        """ Helper method for creating ``PruningOptions`` objects
            If input configs are None, assign defaults """
        def _dict_to_timedelta(config: Dict[str, float]) -> timedelta:
            return timedelta(weeks=config.get('weeks', 0),
                             days=config.get('days', 0),
                             hours=config.get('hours', 0),
                             minutes=config.get('minutes', 0),
                             seconds=config.get('seconds', 0))

        return PruningOptions(pruner_job_max_age=_dict_to_timedelta(
            pruner_job_max_age_cfg) if pruner_job_max_age_cfg else timedelta(days=30),
            pruner_period=_dict_to_timedelta(
            pruner_period_cfg) if pruner_period_cfg else timedelta(minutes=5),
            pruner_max_delete_window=pruner_max_delete_window_cfg
            if pruner_max_delete_window_cfg else 10000)


class SQLDataStore(DataStoreInterface):

    def __init__(self, storage, *, connection_string=None, automigrate=False,
                 connection_timeout=5, poll_interval=1,
                 pruning_options: Optional[PruningOptions] = None,
                 **kwargs):
        super().__init__(storage)
        self.__logger = logging.getLogger(__name__)
        self.__logger.info("Creating SQL scheduler with: "
                           f"automigrate=[{automigrate}], connection_timeout=[{connection_timeout}] "
                           f"poll_interval=[{poll_interval}], "
                           f"pruning_options=[{pruning_options}], "
                           f"kwargs=[{kwargs}]")

        self.response_cache: Dict[str, remote_execution_pb2.ExecuteResponse] = {}
        self.connection_timeout = connection_timeout
        self.max_get_timeout = connection_timeout
        self.poll_interval = poll_interval
        self.watcher = Thread(name="JobWatcher", target=self.wait_for_job_updates, daemon=True)
        self.watcher_keep_running = False

        # Set-up temporary SQLite Database when connection string is not specified
        if not connection_string:
            # pylint: disable=consider-using-with
            tmpdbfile = NamedTemporaryFile(prefix='bgd-', suffix='.db')
            self._tmpdbfile = tmpdbfile  # Make sure to keep this tempfile for the lifetime of this object
            self.__logger.warning("No connection string specified for the DataStore, "
                                  f"will use SQLite with tempfile: [{tmpdbfile.name}]")
            automigrate = True  # since this is a temporary database, we always need to create it
            connection_string = f"sqlite:///{tmpdbfile.name}"

        self._create_sqlalchemy_engine(connection_string, automigrate, connection_timeout, **kwargs)

        self._sql_pool_dispose_helper = sqlutils.SQLPoolDisposeHelper(COOLDOWN_TIME_AFTER_POOL_DISPOSE_SECONDS,
                                                                      MIN_TIME_BETWEEN_SQL_POOL_DISPOSE_MINUTES,
                                                                      self.engine)

        # Make a test query against the database to ensure the connection is valid
        with self.session(reraise=True) as session:
            session.execute(sql_select([1])).all()

        self.capabilities_cache: Dict[str, List[str]] = {}

        # Pruning configuration parameters
        if pruning_options is not None:
            self.pruner_keep_running = True
            self.__logger.info(f"Scheduler pruning enabled: {pruning_options}")
            self.__pruner_thread = Thread(name="JobsPruner", target=self._do_prune, args=(
                pruning_options.pruner_job_max_age, pruning_options.pruner_period,
                pruning_options.pruner_max_delete_window), daemon=True)
            self.__pruner_thread.start()
        else:
            self.__logger.info("Scheduler pruning not enabled")

        # Overall Scheduler Metrics (totals of jobs/leases in each state)
        # Publish those metrics a bit more sparsely since the SQL requests
        # required to gather them can become expensive
        self.__last_scheduler_metrics_publish_time = None
        self.__scheduler_metrics_publish_interval = timedelta(
            seconds=SQL_SCHEDULER_METRICS_PUBLISH_INTERVAL_SECONDS)

    def _create_sqlalchemy_engine(self, connection_string, automigrate, connection_timeout, **kwargs):
        self.automigrate = automigrate

        # Disallow sqlite in-memory because multi-threaded access to it is
        # complex and potentially problematic at best
        # ref: https://docs.sqlalchemy.org/en/13/dialects/sqlite.html#threading-pooling-behavior
        if sqlutils.is_sqlite_inmemory_connection_string(connection_string):
            raise ValueError(
                f"Cannot use SQLite in-memory with BuildGrid (connection_string=[{connection_string}]). "
                "Use a file or leave the connection_string empty for a tempfile.")

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

        self.engine = create_engine(connection_string, echo=False, future=True, **kwargs)
        Session.configure(bind=self.engine)

        if self.engine.dialect.name == "sqlite":
            event.listen(self.engine, "connect", sqlite_on_connect)

        if self.automigrate:
            self._create_or_migrate_db(connection_string)

    def __repr__(self):
        return f"SQL data store interface for `{repr(self.engine.url)}`"

    def activate_monitoring(self):
        # Don't do anything. This function needs to exist but there's no
        # need to actually toggle monitoring in this implementation.
        pass

    def deactivate_monitoring(self):
        # Don't do anything. This function needs to exist but there's no
        # need to actually toggle monitoring in this implementation.
        pass

    def start(self, *, start_job_watcher: bool = True) -> None:
        if start_job_watcher and not self.watcher_keep_running:
            self.watcher_keep_running = True
            self.watcher.start()

    def stop(self) -> None:
        if self.watcher_keep_running:
            self.watcher_keep_running = False
            self.watcher.join()

    def _create_or_migrate_db(self, connection_string):
        self.__logger.warning("Will attempt migration to latest version if needed.")

        config = Config()
        config.set_main_option("script_location", os.path.join(os.path.dirname(__file__), "alembic"))

        with self.engine.begin() as connection:
            config.attributes['connection'] = connection
            command.upgrade(config, "head")

    @contextmanager
    def session(self, *, sqlite_lock_immediately=False, reraise=False):
        # If we recently disposed of the SQL pool due to connection issues
        # allow for some cooldown period before we attempt more SQL
        self._sql_pool_dispose_helper.wait_if_cooldown_in_effect()

        # Try to obtain a session
        try:
            session = Session()
            if sqlite_lock_immediately and session.bind.name == "sqlite":
                session.execute("BEGIN IMMEDIATE")
        except Exception as e:
            self.__logger.error("Unable to obtain a database session.", exc_info=True)
            raise DatabaseError("Unable to obtain a database session.") from e

        # Yield the session and catch exceptions that occur while using it
        # to roll-back if needed
        try:
            yield session
            session.commit()
        except Exception as e:
            transient_dberr = self._sql_pool_dispose_helper.check_dispose_pool(session, e)
            if transient_dberr:
                self.__logger.warning("Rolling back database session due to transient database error.", exc_info=True)
            else:
                self.__logger.error("Error committing database session. Rolling back.", exc_info=True)
            try:
                session.rollback()
            except Exception:
                self.__logger.warning("Rollback error.", exc_info=True)

            if reraise:
                if transient_dberr:
                    raise RetriableDatabaseError("Database connection was temporarily interrupted, please retry",
                                                 timedelta(seconds=COOLDOWN_TIME_AFTER_POOL_DISPOSE_SECONDS)) from e
                raise
        finally:
            session.close()

    def _get_job(self, job_name, session, with_for_update=False):
        statement = sql_select(Job).filter_by(name=job_name)
        if with_for_update:
            statement = statement.with_for_update()

        job = session.execute(statement).scalars().first()
        if job:
            self.__logger.debug(f"Loaded job from db: name=[{job_name}], stage=[{job.stage}], result=[{job.result}]")

        return job

    def _check_job_timeout(self, job_internal, *, max_execution_timeout=None):
        """ Do a lazy check of maximum allowed job timeouts when clients try to retrieve
            an existing job.
            Cancel the job and related operations/leases, if we detect they have
            exceeded timeouts on access.

            Returns the `buildgrid.server.Job` object, possibly updated with `cancelled=True`.
        """
        if job_internal and max_execution_timeout and job_internal.worker_start_timestamp_as_datetime:
            if job_internal.operation_stage == OperationStage.EXECUTING:
                executing_duration = datetime.utcnow() - job_internal.worker_start_timestamp_as_datetime
                if executing_duration.total_seconds() >= max_execution_timeout:
                    self.__logger.warning(f"Job=[{job_internal}] has been executing for "
                                          f"executing_duration=[{executing_duration}]. "
                                          f"max_execution_timeout=[{max_execution_timeout}] "
                                          "Cancelling.")
                    job_internal.cancel_all_operations(data_store=self)
                    self.__logger.info(f"Job=[{job_internal}] has been cancelled.")
        return job_internal

    @DurationMetric(DATA_STORE_GET_JOB_BY_DIGEST_TIME_METRIC_NAME, instanced=True)
    def get_job_by_action(self, action_digest, *, max_execution_timeout=None):
        statement = sql_select(Job).where(
            and_(
                Job.action_digest == digest_to_string(action_digest),
                Job.stage != OperationStage.COMPLETED.value
            )
        )

        with self.session() as session:
            job = session.execute(statement).scalars().first()
            if job:
                internal_job = job.to_internal_job(self, action_browser_url=self._action_browser_url,
                                                   instance_name=self._instance_name)
                return self._check_job_timeout(internal_job, max_execution_timeout=max_execution_timeout)
        return None

    @DurationMetric(DATA_STORE_GET_JOB_BY_NAME_TIME_METRIC_NAME, instanced=True)
    def get_job_by_name(self, name, *, max_execution_timeout=None):
        with self.session() as session:
            job = self._get_job(name, session)
            if job:
                internal_job = job.to_internal_job(self, action_browser_url=self._action_browser_url,
                                                   instance_name=self._instance_name)
                return self._check_job_timeout(internal_job, max_execution_timeout=max_execution_timeout)
        return None

    @DurationMetric(DATA_STORE_GET_JOB_BY_OPERATION_TIME_METRIC_NAME, instanced=True)
    def get_job_by_operation(self, operation_name, *, max_execution_timeout=None):
        with self.session() as session:
            operation = self._get_operation(operation_name, session)
            if operation and operation.job:
                job = operation.job
                internal_job = job.to_internal_job(self, action_browser_url=self._action_browser_url,
                                                   instance_name=self._instance_name)
                return self._check_job_timeout(internal_job, max_execution_timeout=max_execution_timeout)
        return None

    def get_all_jobs(self):
        statement = sql_select(Job).where(
            Job.stage != OperationStage.COMPLETED.value
        )

        with self.session() as session:
            jobs = session.execute(statement).scalars()
            return [j.to_internal_job(self, action_browser_url=self._action_browser_url,
                                      instance_name=self._instance_name) for j in jobs]

    def get_jobs_by_stage(self, operation_stage):
        statement = sql_select(Job).where(
            Job.stage == operation_stage.value
        )

        with self.session() as session:
            jobs = session.execute(statement).scalars()
            return [j.to_internal_job(self, no_result=True, action_browser_url=self._action_browser_url,
                                      instance_name=self._instance_name) for j in jobs]

    def get_operation_request_metadata_by_name(self, operation_name):
        with self.session() as session:
            operation = self._get_operation(operation_name, session)
            if not operation:
                return None

            return {'tool-name': operation.tool_name or '',
                    'tool-version': operation.tool_version or '',
                    'invocation-id': operation.invocation_id or '',
                    'correlated-invocations-id': operation.correlated_invocations_id or ''}

    @DurationMetric(DATA_STORE_CREATE_JOB_TIME_METRIC_NAME, instanced=True)
    def create_job(self, job):
        with self.session() as session:
            if self._get_job(job.name, session) is None:
                # Convert requirements values to sorted lists to make them json-serializable
                platform_requirements = job.platform_requirements
                normalized_requirements = convert_values_to_sorted_lists(platform_requirements)
                # Serialize the requirements
                platform_requirements_hash = hash_from_dict(normalized_requirements)

                session.add(Job(
                    name=job.name,
                    action=job.action.SerializeToString(),
                    action_digest=digest_to_string(job.action_digest),
                    do_not_cache=job.do_not_cache,
                    priority=job.priority,
                    operations=[],
                    platform_requirements=platform_requirements_hash,
                    stage=job.operation_stage.value,
                    queued_timestamp=job.queued_timestamp_as_datetime,
                    queued_time_duration=job.queued_time_duration.seconds,
                    worker_start_timestamp=job.worker_start_timestamp_as_datetime,
                    worker_completed_timestamp=job.worker_completed_timestamp_as_datetime
                ))

    @DurationMetric(DATA_STORE_QUEUE_JOB_TIME_METRIC_NAME, instanced=True)
    def queue_job(self, job_name):
        with self.session(sqlite_lock_immediately=True) as session:
            job = self._get_job(job_name, session, with_for_update=True)
            job.assigned = False

    @DurationMetric(DATA_STORE_UPDATE_JOB_TIME_METRIC_NAME, instanced=True)
    def update_job(self, job_name, changes, *, skip_notify=False):
        if "result" in changes:
            changes["result"] = digest_to_string(changes["result"])
        if "action_digest" in changes:
            changes["action_digest"] = digest_to_string(changes["action_digest"])

        initial_values_for_metrics_use = {}

        with self.session() as session:
            job = self._get_job(job_name, session)

            # Keep track of the state right before we perform this update
            initial_values_for_metrics_use["stage"] = OperationStage(job.stage)

            job.update(changes)
            if not skip_notify:
                self._notify_job_updated(job_name, session)

        # Upon successful completion of the transaction above, publish metrics
        JobMetrics.publish_metrics_on_job_updates(initial_values_for_metrics_use, changes, self._instance_name)

    def _notify_job_updated(self, job_names, session):
        if self.engine.dialect.name == "postgresql":
            if isinstance(job_names, str):
                job_names = [job_names]
            for job_name in job_names:
                session.execute(f"NOTIFY job_updated, '{job_name}';")

    def delete_job(self, job_name):
        if job_name in self.response_cache:
            del self.response_cache[job_name]

    def wait_for_job_updates(self):
        self.__logger.info("Starting job watcher thread")
        if self.engine.dialect.name == "postgresql":
            self._listen_for_updates()
        else:
            self._poll_for_updates()

    def _listen_for_updates(self):
        def _listen_loop(engine_conn: Connection):
            try:
                # Get the DBAPI connection object from the SQLAlchemy Engine.Connection wrapper
                connection_fairy = engine_conn.connection
                connection_fairy.cursor().execute("LISTEN job_updated;")  # type: ignore
                connection_fairy.commit()
            except Exception:
                self.__logger.warning(
                    "Could not start listening to DB for job updates",
                    exc_info=True)
                # Let the context manager handle this
                raise

            while self.watcher_keep_running:
                # Get the actual DBAPI connection
                dbapi_connection = connection_fairy.dbapi_connection  # type: ignore
                # Wait until the connection is ready for reading. Timeout after 5 seconds
                # and try again if there was nothing to read. If the connection becomes
                # readable, collect the notifications it has received and handle them.
                #
                # See https://www.psycopg.org/docs/advanced.html#asynchronous-notifications
                if select.select([dbapi_connection], [], [], self.poll_interval) == ([], [], []):
                    pass
                else:

                    try:
                        dbapi_connection.poll()
                    except Exception:
                        self.__logger.warning("Error while polling for job updates", exc_info=True)
                        # Let the context manager handle this
                        raise

                    while dbapi_connection.notifies:
                        notify = dbapi_connection.notifies.pop()
                        with DurationMetric(DATA_STORE_CHECK_FOR_UPDATE_TIME_METRIC_NAME,
                                            instanced=True, instance_name=self._instance_name):
                            with self.watched_jobs_lock:
                                spec = self.watched_jobs.get(notify.payload)
                                if spec is not None:
                                    try:
                                        new_job = self.get_job_by_name(notify.payload)
                                    except Exception:
                                        self.__logger.warning(
                                            f"Couldn't get watched job=[{notify.payload}] from DB",
                                            exc_info=True)
                                        # Let the context manager handle this
                                        raise

                                    # If the job doesn't exist or an exception was supressed by
                                    # get_job_by_name, it returns None instead of the job
                                    if new_job is None:
                                        raise DatabaseError(
                                            f"get_job_by_name returned None for job=[{notify.payload}]")

                                    new_state = JobState(new_job)
                                    if spec.last_state != new_state:
                                        spec.last_state = new_state
                                        spec.event.notify_change()

        while self.watcher_keep_running:
            # Wait a few seconds if a database exception occurs and then try again
            # This could be a short disconnect
            try:
                # Use the session contextmanager
                # so that we can benefit from the common SQL error-handling
                with self.session(reraise=True) as session:
                    # In our `LISTEN` call, we want to *bypass the ORM*
                    # and *use the underlying Engine connection directly*.
                    # (This is because using a `session.execute()` will
                    #  implicitly create a SQL transaction, causing
                    #  notifications to only be delivered when that transaction
                    #  is committed)
                    _listen_loop(session.connection())
            except Exception as e:
                self.__logger.warning(f"JobWatcher encountered exception: [{e}];"
                                      f"Retrying in poll_interval=[{self.poll_interval}] seconds.")
                # Sleep for a bit so that we give enough time for the
                # database to potentially recover
                time.sleep(self.poll_interval)

    def _get_watched_jobs(self):
        statement = sql_select(Job).where(
            Job.name.in_(self.watched_jobs)
        )

        with self.session() as sess:
            jobs = sess.execute(statement).scalars().all()
            return [job.to_internal_job(self) for job in jobs]

    def _poll_for_updates(self):
        def _poll_loop():
            while self.watcher_keep_running:
                time.sleep(self.poll_interval)
                if self.watcher_keep_running:
                    with DurationMetric(DATA_STORE_CHECK_FOR_UPDATE_TIME_METRIC_NAME,
                                        instanced=True, instance_name=self._instance_name):
                        with self.watched_jobs_lock:
                            if self.watcher_keep_running:
                                try:
                                    watched_jobs = self._get_watched_jobs()
                                except Exception as e:
                                    raise DatabaseError("Couldn't retrieve watched jobs from DB") from e

                                if watched_jobs is None:
                                    raise DatabaseError("_get_watched_jobs returned None")

                                for new_job in watched_jobs:
                                    if self.watcher_keep_running:
                                        spec = self.watched_jobs[new_job.name]
                                        new_state = JobState(new_job)
                                        if spec.last_state != new_state:
                                            spec.last_state = new_state
                                            spec.event.notify_change()

        while self.watcher_keep_running:
            # Wait a few seconds if a database exception occurs and then try again
            try:
                _poll_loop()
            except DatabaseError as e:
                self.__logger.warning(f"JobWatcher encountered exception: [{e}];"
                                      f"Retrying in poll_interval=[{self.poll_interval}] seconds.")
                # Sleep for a bit so that we give enough time for the
                # database to potentially recover
                time.sleep(self.poll_interval)

    @DurationMetric(DATA_STORE_STORE_RESPONSE_TIME_METRIC_NAME, instanced=True)
    def store_response(self, job, commit_changes=True):
        digest = self.storage.put_message(job.execute_response)
        changes = {"result": digest, "status_code": job.execute_response.status.code}
        self.response_cache[job.name] = job.execute_response

        if commit_changes:
            self.update_job(job.name,
                            changes,
                            skip_notify=True)
            return None
        else:
            # The caller will batch the changes and commit to db
            return changes

    def _get_operation(self, operation_name, session):
        statement = sql_select(Operation).where(
            Operation.name == operation_name
        )
        return session.execute(statement).scalars().first()

    def get_operations_by_stage(self, operation_stage):
        statement = sql_select(Operation).where(
            Operation.job.has(stage=operation_stage.value)
        )

        with self.session() as session:
            operations = session.execute(statement).scalars().all()
            # Return a set of job names here for now, to match the `MemoryDataStore`
            # implementation's behaviour
            return set(op.job.name for op in operations)

    def _cancel_jobs_exceeding_execution_timeout(self, max_execution_timeout: int=None) -> None:
        if max_execution_timeout:
            stale_job_names = []
            lazy_execution_timeout_threshold = datetime.utcnow() - timedelta(seconds=max_execution_timeout)

            with self.session(sqlite_lock_immediately=True) as session:
                # Get the full list of jobs exceeding execution timeout
                stale_jobs_statement = sql_select(Job).with_for_update().where(
                    and_(
                        Job.stage == OperationStage.EXECUTING.value,
                        Job.worker_start_timestamp <= lazy_execution_timeout_threshold
                    )
                )
                stale_job_names = [job.name for job in session.execute(stale_jobs_statement).scalars().all()]

                if stale_job_names:
                    # Mark operations as cancelled
                    stmt_mark_operations_cancelled = update(Operation).where(
                        Operation.job_name.in_(stale_job_names)
                    ).values(cancelled=True)
                    session.execute(stmt_mark_operations_cancelled)

                    # Mark jobs as cancelled
                    stmt_mark_jobs_cancelled = update(Job).where(
                        Job.name.in_(stale_job_names)
                    ).values(stage=OperationStage.COMPLETED.value, cancelled=True)
                    session.execute(stmt_mark_jobs_cancelled)

                    # Notify all jobs updated
                    self._notify_job_updated(stale_job_names, session)

            if stale_job_names:
                self.__logger.info(f"Cancelled n=[{len(stale_job_names)}] jobs "
                                   f"with names={stale_job_names}"
                                   f"due to them exceeding execution_timeout=["
                                   f"{max_execution_timeout}")

    @DurationMetric(DATA_STORE_LIST_OPERATIONS_TIME_METRIC_NAME, instanced=True)
    def list_operations(self,
                        operation_filters: List[OperationFilter]=None,
                        page_size: int=None,
                        page_token: str=None,
                        max_execution_timeout: int=None) -> Tuple[List[operations_pb2.Operation], str]:
        # Lazily timeout jobs as needed before returning the list!
        self._cancel_jobs_exceeding_execution_timeout(max_execution_timeout=max_execution_timeout)

        # Build filters and sort order
        sort_keys = DEFAULT_SORT_KEYS
        custom_filters = None
        if operation_filters:
            # Extract custom sort order (if present)
            specified_sort_keys, non_sort_filters = extract_sort_keys(operation_filters)

            # Only override sort_keys if there were sort keys actually present in the filter string
            if specified_sort_keys:
                sort_keys = specified_sort_keys
                # Attach the operation name as a sort key for a deterministic order
                # This will ensure that the ordering of results is consistent between queries
                if not any(sort_key.name == "name" for sort_key in sort_keys):
                    sort_keys.append(SortKey(name="name", descending=False))

            # Finally, compile the non-sort filters into a filter list
            custom_filters = build_custom_filters(non_sort_filters)

        sort_columns = build_sort_column_list(sort_keys)

        with self.session() as session:
            statement = sql_select(Operation).join(Job, Operation.job_name == Job.name)

            # Apply custom filters (if present)
            if custom_filters:
                statement = statement.filter(*custom_filters)

            # Apply sort order
            statement = statement.order_by(*sort_columns)

            # Apply pagination filter
            if page_token:
                page_filter = build_page_filter(page_token, sort_keys)
                statement = statement.filter(page_filter)
            if page_size:
                # We limit the number of operations we fetch to the page_size. However, we
                # fetch an extra operation to determine whether we need to provide a
                # next_page_token.
                statement = statement.limit(page_size + 1)

            operations = session.execute(statement).scalars().all()

            if not page_size or not operations:
                next_page_token = ""

            # If the number of results we got is less than or equal to our page_size,
            # we're done with the operations listing and don't need to provide another
            # page token
            elif len(operations) <= page_size:
                next_page_token = ""
            else:
                # Drop the last operation since we have an extra
                operations.pop()
                # Our page token will be the last row of our set
                next_page_token = build_page_token(operations[-1], sort_keys)
            return [operation.to_protobuf(self) for operation in operations], next_page_token

    @DurationMetric(DATA_STORE_CREATE_OPERATION_TIME_METRIC_NAME, instanced=True)
    def create_operation(self, operation_name, job_name, request_metadata=None):
        with self.session() as session:
            operation = Operation(
                name=operation_name,
                job_name=job_name
            )
            if request_metadata is not None:
                if request_metadata.tool_invocation_id:
                    operation.invocation_id = request_metadata.tool_invocation_id
                if request_metadata.correlated_invocations_id:
                    operation.correlated_invocations_id = request_metadata.correlated_invocations_id
                if request_metadata.tool_details:
                    operation.tool_name = request_metadata.tool_details.tool_name
                    operation.tool_version = request_metadata.tool_details.tool_version
            session.add(operation)

    @DurationMetric(DATA_STORE_UPDATE_OPERATION_TIME_METRIC_NAME, instanced=True)
    def update_operation(self, operation_name, changes):
        with self.session() as session:
            operation = self._get_operation(operation_name, session)
            operation.update(changes)

    def delete_operation(self, operation_name):
        # Don't do anything. This function needs to exist but there's no
        # need to actually delete operations in this implementation.
        pass

    def get_leases_by_state(self, lease_state):
        statement = sql_select(Lease).where(
            Lease.state == lease_state.value
        )

        with self.session() as session:
            leases = session.execute(statement).scalars().all()
            # `lease.job_name` is the same as `lease.id` for a Lease protobuf
            return set(lease.job_name for lease in leases)

    def get_metrics(self):
        # Skip publishing overall scheduler metrics if we have recently published them
        last_publish_time = self.__last_scheduler_metrics_publish_time
        time_since_publish = None
        if last_publish_time:
            time_since_publish = datetime.utcnow() - last_publish_time
        if time_since_publish and time_since_publish < self.__scheduler_metrics_publish_interval:
            # Published too recently, skip
            return None

        def _get_query_leases_by_state(category: str) -> Select:
            # Using func.count here to avoid generating a subquery in the WHERE
            # clause of the resulting query.
            # https://docs.sqlalchemy.org/en/13/orm/query.html#sqlalchemy.orm.query.Query.count
            return sql_select([
                literal_column(f"'{category}'").label("category"),
                Lease.state.label("bucket"),
                func.count(Lease.id).label("value")
            ]).group_by(Lease.state)

        def _cb_query_leases_by_state(leases_by_state):
            # The database only returns counts > 0, so fill in the gaps
            for state in LeaseState:
                if state.value not in leases_by_state:
                    leases_by_state[state.value] = 0
            return leases_by_state

        def _get_query_jobs_by_stage(category: str) -> Select:
            # Using func.count here to avoid generating a subquery in the WHERE
            # clause of the resulting query.
            # https://docs.sqlalchemy.org/en/13/orm/query.html#sqlalchemy.orm.query.Query.count
            return sql_select([
                literal_column(f"'{category}'").label("category"),
                Job.stage.label("bucket"),
                func.count(Job.name).label("value")
            ]).group_by(Job.stage)

        def _cb_query_jobs_by_stage(jobs_by_stage):
            # The database only returns counts > 0, so fill in the gaps
            for stage in OperationStage:
                if stage.value not in jobs_by_stage:
                    jobs_by_stage[stage.value] = 0
            return jobs_by_stage

        metrics = {}
        # metrics to gather: (category_name, function_returning_query, callback_function)
        metrics_to_gather = [
            (MetricCategories.LEASES.value, _get_query_leases_by_state, _cb_query_leases_by_state),
            (MetricCategories.JOBS.value, _get_query_jobs_by_stage, _cb_query_jobs_by_stage)
        ]

        statements = [query_fn(category) for category, query_fn, _ in metrics_to_gather]
        metrics_statement = union(*statements)

        try:
            with self.session() as session:
                results = session.execute(metrics_statement).all()

                grouped_results = {category: {} for category, _, _ in results}
                for category, bucket, value in results:
                    grouped_results[category][bucket] = value

                for category, _, category_cb in metrics_to_gather:
                    metrics[category] = category_cb(grouped_results.setdefault(category, {}))
        except DatabaseError:
            self.__logger.warning("Unable to gather metrics due to a Database Error.")
            return {}

        # This is only updated within the metrics asyncio loop; no race conditions
        self.__last_scheduler_metrics_publish_time = datetime.utcnow()

        return metrics

    def _create_lease(self, lease, session, job=None, worker_name=None):
        if job is None:
            job = self._get_job(lease.id, session)
        # We only allow one lease, so if there's an existing one update it
        if job.active_leases:
            job.active_leases[0].state = lease.state
            job.active_leases[0].status = None
            job.active_leases[0].worker_name = worker_name
        else:
            session.add(Lease(
                job_name=lease.id,
                state=lease.state,
                status=None,
                worker_name=worker_name
            ))

    def create_lease(self, lease):
        with self.session() as session:
            self._create_lease(lease, session)

    @DurationMetric(DATA_STORE_UPDATE_LEASE_TIME_METRIC_NAME, instanced=True)
    def update_lease(self, job_name, changes):
        initial_values_for_metrics_use = {}

        with self.session() as session:
            job = self._get_job(job_name, session)
            try:
                lease = job.active_leases[0]
            except IndexError:
                return

            # Keep track of the state right before we perform this update
            initial_values_for_metrics_use["state"] = lease.state

            lease.update(changes)

        # Upon successful completion of the transaction above, publish metrics
        JobMetrics.publish_metrics_on_lease_updates(initial_values_for_metrics_use, changes, self._instance_name)

    def load_unfinished_jobs(self):
        statement = sql_select(Job).where(
            Job.stage != OperationStage.COMPLETED.value
        ).order_by(Job.priority)

        with self.session() as session:
            jobs = session.execute(statement).scalars().all()
            return [j.to_internal_job(self) for j in jobs]

    @DurationMetric(BOTS_ASSIGN_JOB_LEASES_TIME_METRIC_NAME, instanced=True)
    def assign_n_leases(
        self,
        *,
        capability_hash: str,
        lease_count: int,
        assignment_callback: Callable[[List[InternalJob]], Dict[str, InternalJob]]
    ) -> None:
        # pylint: disable=singleton-comparison
        job_statement = sql_select(Job).with_for_update(skip_locked=True).where(
            Job.stage == OperationStage.QUEUED.value,
            Job.assigned != True,
            Job.platform_requirements == capability_hash
        ).order_by(Job.priority, Job.queued_timestamp).limit(lease_count)

        try:
            with self.session(sqlite_lock_immediately=True) as session:
                jobs = session.execute(job_statement).scalars().all()
                internal_jobs = [job.to_internal_job(self) for job in jobs]
                assigned_job_map = assignment_callback(internal_jobs)

                # Update the database job records for the jobs which were
                # successfully assigned to workers.
                for job in jobs:
                    if job.name in assigned_job_map:
                        internal_job = assigned_job_map[job.name]
                        job.assigned = True
                        job.worker_start_timestamp = internal_job.worker_start_timestamp_as_datetime
                        with DurationMetric(DATA_STORE_CREATE_LEASE_TIME_METRIC_NAME,
                                            instance_name=self._instance_name,
                                            instanced=True):
                            self._create_lease(
                                internal_job.lease,
                                session,
                                job=job,
                                worker_name=internal_job.worker_name)
        except DatabaseError:
            self.__logger.warning("Will not assign any leases this time due to a Database Error.")

    def _do_prune(self, job_max_age: timedelta, pruning_period: timedelta, limit: int) -> None:
        """ Running in a background thread, this method wakes up periodically and deletes older records
        from the jobs tables using configurable parameters """

        utc_last_prune_time = datetime.utcnow()
        while self.pruner_keep_running:
            utcnow = datetime.utcnow()
            if (utcnow - pruning_period) < utc_last_prune_time:
                self.__logger.info(f"Pruner thread sleeping for {pruning_period}(until {utcnow + pruning_period})")
                time.sleep(pruning_period.total_seconds())
                continue

            delete_before_datetime = utcnow - job_max_age
            try:
                with DurationMetric(DATA_STORE_PRUNER_DELETE_TIME_METRIC_NAME,
                                    instance_name=self._instance_name,
                                    instanced=True):
                    num_rows = self._delete_jobs_prior_to(delete_before_datetime, limit)

                self.__logger.info(
                    f"Pruned {num_rows} row(s) from the jobs table older than {delete_before_datetime}")

                if num_rows > 0:
                    with Counter(metric_name=DATA_STORE_PRUNER_NUM_ROWS_DELETED_METRIC_NAME,
                                 instance_name=self._instance_name) as num_rows_deleted:
                        num_rows_deleted.increment(num_rows)

            except Exception:
                self.__logger.exception("Caught exception while deleting jobs records")

            finally:
                # Update even if error occurred to avoid potentially infinitely retrying
                utc_last_prune_time = utcnow

        self.__logger.info("Exiting pruner thread")

    def _delete_jobs_prior_to(self, delete_before_datetime: datetime, limit: int) -> int:
        """ Deletes older records from the jobs tables constrained by `delete_before_datetime` and `limit` """
        delete_stmt = delete(Job).where(
            Job.name.in_(
                sql_select(Job.name).with_for_update(skip_locked=True).where(
                    Job.worker_completed_timestamp <= delete_before_datetime
                ).limit(limit)
            )
        )

        with self.session(reraise=True) as session:
            options = {"synchronize_session": "fetch"}
            num_rows_deleted = session.execute(delete_stmt, execution_options=options).rowcount

        return num_rows_deleted
