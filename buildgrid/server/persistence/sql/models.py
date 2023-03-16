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
#
# pylint: disable=multiple-statements

from typing import List

from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp
from sqlalchemy import BigInteger, Boolean, Column, DateTime, ForeignKey, Index, Integer, LargeBinary, String, false
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from buildgrid.utils import BrowserURL
from ...._enums import LeaseState, OperationStage
from ...._exceptions import NotFoundError
from ...._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import Action, Digest
from ...._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import ExecuteOperationMetadata
from ...._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import ExecuteResponse
from ...._protos.google.devtools.remoteworkers.v1test2 import bots_pb2
from ...._protos.google.rpc import code_pb2, status_pb2
from ...._protos.google.longrunning import operations_pb2
from ... import job


class Base:

    """Base class which implements functionality relevant to all models."""

    def update(self, changes):
        for key, val in changes.items():
            setattr(self, key, val)


Base = declarative_base(cls=Base)  # type: ignore


class Job(Base):
    __tablename__ = 'jobs'

    name = Column(String, primary_key=True)
    action_digest = Column(String, index=True, nullable=False)
    action = Column(LargeBinary)
    priority = Column(Integer, default=1, index=True, nullable=False)
    stage = Column(Integer, default=0, index=True, nullable=False)
    do_not_cache = Column(Boolean, default=False, nullable=False)
    cancelled = Column(Boolean, default=False, nullable=False)
    queued_timestamp = Column(DateTime, index=True)
    queued_time_duration = Column(Integer)
    worker_start_timestamp = Column(DateTime)
    worker_completed_timestamp = Column(DateTime)
    result = Column(String)
    assigned = Column(Boolean, default=False)
    n_tries = Column(Integer, default=0)
    platform_requirements = Column(String, nullable=True)
    status_code = Column(Integer, nullable=True)
    stdout_stream_name = Column(String, nullable=True)
    stdout_stream_write_name = Column(String, nullable=True)
    stderr_stream_name = Column(String, nullable=True)
    stderr_stream_write_name = Column(String, nullable=True)

    leases: List = relationship('Lease', backref='job')
    active_states: List[int] = [
        LeaseState.UNSPECIFIED.value,
        LeaseState.PENDING.value,
        LeaseState.ACTIVE.value
    ]
    active_leases: List = relationship(
        'Lease',
        primaryjoin=f'and_(Lease.job_name==Job.name, Lease.state.in_({active_states}))'
    )

    operations: List = relationship('Operation', backref='job')

    __table_args__ = (
        Index('ix_worker_start_timestamp',
              worker_start_timestamp, unique=False, postgresql_where=(worker_start_timestamp.isnot(None))),
        Index('ix_worker_start_timestamp',
              worker_start_timestamp, unique=False, sqlite_where=(worker_start_timestamp.isnot(None))),
        Index('ix_worker_completed_timestamp',
              worker_completed_timestamp, unique=False, postgresql_where=(worker_completed_timestamp.isnot(None))),
        Index('ix_worker_completed_timestamp',
              worker_completed_timestamp, unique=False, sqlite_where=(worker_completed_timestamp.isnot(None)))
    )

    def get_execute_response_protobuf_from_result_digest(self, data_store):
        if self.result is None:
            return None

        # Check if this was stored in the response_cache; if not, load and cache it
        if self.name not in data_store.response_cache:
            result_digest = string_to_digest(self.result)
            result_proto = data_store.storage.get_message(result_digest, ExecuteResponse)
            if result_proto is None:
                raise NotFoundError(f"The result for job name=[{self.name}] with "
                                    f"result_digest=[{result_digest}] does not exist in the storage.")
            data_store.response_cache[self.name] = result_proto

        # Return ExecuteResponse proto from response_cache
        return data_store.response_cache[self.name]

    def to_internal_job(self, data_store, no_result=False, action_browser_url=None, instance_name=None):
        # There should never be more than one active lease for a job. If we
        # have more than one for some reason, just take the first one.
        # TODO(SotK): Log some information here if there are multiple active
        # (ie. not completed or cancelled) leases.
        lease = self.active_leases[0].to_protobuf() if self.active_leases else None
        q_timestamp = Timestamp()
        if self.queued_timestamp:
            q_timestamp.FromDatetime(self.queued_timestamp)
        q_time_duration = Duration()
        if self.queued_time_duration:
            q_time_duration.FromSeconds(self.queued_time_duration)
        ws_timestamp = Timestamp()
        if self.worker_start_timestamp:
            ws_timestamp.FromDatetime(self.worker_start_timestamp)
        wc_timestamp = Timestamp()
        if self.worker_completed_timestamp:
            wc_timestamp.FromDatetime(self.worker_completed_timestamp)

        action_proto = None
        if self.action is not None:
            action_proto = Action()
            action_proto.ParseFromString(self.action)

        result_proto = None
        if not no_result:
            result_proto = self.get_execute_response_protobuf_from_result_digest(data_store)

        internal_job = job.Job(
            do_not_cache=self.do_not_cache,
            action=action_proto,
            action_digest=string_to_digest(self.action_digest),
            platform_requirements=self.platform_requirements,
            priority=self.priority,
            name=self.name,
            operation_names=set(op.name for op in self.operations),
            cancelled_operation_names=set(op.name for op in self.operations if op.cancelled),
            lease=lease,
            stage=OperationStage(self.stage),
            cancelled=self.cancelled,
            queued_timestamp=q_timestamp,
            queued_time_duration=q_time_duration,
            worker_start_timestamp=ws_timestamp,
            worker_completed_timestamp=wc_timestamp,
            result=result_proto,
            worker_name=self.active_leases[0].worker_name if self.active_leases else None,
            n_tries=self.n_tries,
            status_code=self.status_code,
            stdout_stream_name=self.stdout_stream_name,
            stdout_stream_write_name=self.stdout_stream_write_name,
            stderr_stream_name=self.stderr_stream_name,
            stderr_stream_write_name=self.stderr_stream_write_name
        )

        # Set action browser url if url and instance names are passed in
        if action_browser_url is not None:
            internal_job.set_action_url(BrowserURL(action_browser_url, instance_name))

        return internal_job


class Lease(Base):
    __tablename__ = 'leases'

    id = Column(Integer, primary_key=True)
    job_name = Column(String, ForeignKey('jobs.name', ondelete='CASCADE', onupdate='CASCADE'),
                      index=True, nullable=False)
    status = Column(Integer)
    state = Column(Integer, nullable=False)
    worker_name = Column(String, index=True, default=None)

    def to_protobuf(self):
        lease = bots_pb2.Lease()
        lease.id = self.job_name

        if self.job.action is not None:
            action = Action()
            action.ParseFromString(self.job.action)
            lease.payload.Pack(action)
        else:
            lease.payload.Pack(string_to_digest(self.job.action_digest))

        lease.state = self.state
        if self.status is not None:
            lease.status.code = self.status
        return lease


class Operation(Base):
    __tablename__ = 'operations'

    name = Column(String, primary_key=True)
    job_name = Column(String, ForeignKey('jobs.name', ondelete='CASCADE', onupdate='CASCADE'),
                      index=True, nullable=False)
    cancelled = Column(Boolean, default=False, nullable=False)
    tool_name = Column(String, nullable=True)
    tool_version = Column(String, nullable=True)
    invocation_id = Column(String, nullable=True)
    correlated_invocations_id = Column(String, nullable=True)

    def to_protobuf(self, data_store, no_result=False):
        """Returns the protobuf representation of the operation.

        When expecting a result, if the `ActionResult` message is
        missing from `data_store`, populates the `error` field of the
        result with `code_pb2.DATA_LOSS`.
        """
        operation = operations_pb2.Operation()
        operation.name = self.name
        operation.done = self.job.stage == OperationStage.COMPLETED.value or self.cancelled
        operation.metadata.Pack(ExecuteOperationMetadata(
            stage=self.job.stage,
            action_digest=string_to_digest(self.job.action_digest)))

        if self.cancelled:
            operation.error.CopyFrom(status_pb2.Status(code=code_pb2.CANCELLED))
        elif self.job.status_code != code_pb2.OK:
            operation.error.CopyFrom(status_pb2.Status(code=self.job.status_code))

        if self.job.result and not no_result:
            try:
                execute_response = self.job.get_execute_response_protobuf_from_result_digest(data_store)
                operation.response.Pack(execute_response)
            except NotFoundError:
                operation.error.CopyFrom(status_pb2.Status(code=code_pb2.DATA_LOSS))

        return operation


class IndexEntry(Base):
    __tablename__ = 'index'

    digest_hash = Column(String, nullable=False, index=True, primary_key=True)
    digest_size_bytes = Column(BigInteger, nullable=False)
    accessed_timestamp = Column(DateTime, index=True, nullable=False)
    deleted = Column(Boolean, nullable=False, server_default=false())
    inline_blob = Column(LargeBinary, nullable=True)


def digest_to_string(digest):
    return f'{digest.hash}/{digest.size_bytes}'


def string_to_digest(string):
    digest_hash, size_bytes = string.split('/', 1)
    return Digest(hash=digest_hash, size_bytes=int(size_bytes))
