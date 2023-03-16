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


from enum import Enum

from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.buildgrid.v2 import monitoring_pb2
from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2


# RWAPI enumerations
# From google/devtools/remoteworkers/v1test2/bots.proto:

class BotStatus(Enum):
    # Initially unknown state.
    UNSPECIFIED = bots_pb2.BotStatus.Value('BOT_STATUS_UNSPECIFIED')
    # The bot is healthy, and will accept leases as normal.
    OK = bots_pb2.BotStatus.Value('OK')
    # The bot is unhealthy and will not accept new leases.
    UNHEALTHY = bots_pb2.BotStatus.Value('UNHEALTHY')
    # The bot has been asked to reboot the host.
    HOST_REBOOTING = bots_pb2.BotStatus.Value('HOST_REBOOTING')
    # The bot has been asked to shut down.
    BOT_TERMINATING = bots_pb2.BotStatus.Value('BOT_TERMINATING')


class LeaseState(Enum):
    # Initially unknown state.
    UNSPECIFIED = bots_pb2.LeaseState.Value('LEASE_STATE_UNSPECIFIED')
    # The server expects the bot to accept this lease.
    PENDING = bots_pb2.LeaseState.Value('PENDING')
    # The bot has accepted this lease.
    ACTIVE = bots_pb2.LeaseState.Value('ACTIVE')
    # The bot is no longer leased.
    COMPLETED = bots_pb2.LeaseState.Value('COMPLETED')
    # The bot should immediately release all resources associated with the lease.
    CANCELLED = bots_pb2.LeaseState.Value('CANCELLED')


# REAPI enumerations
# From build/bazel/remote/execution/v2/remote_execution.proto:

class OperationStage(Enum):
    # Initially unknown stage.
    UNKNOWN = remote_execution_pb2.ExecutionStage.Value.Value('UNKNOWN')
    # Checking the result against the cache.
    CACHE_CHECK = remote_execution_pb2.ExecutionStage.Value.Value('CACHE_CHECK')
    # Currently idle, awaiting a free machine to execute.
    QUEUED = remote_execution_pb2.ExecutionStage.Value.Value('QUEUED')
    # Currently being executed by a worker.
    EXECUTING = remote_execution_pb2.ExecutionStage.Value.Value('EXECUTING')
    # Finished execution.
    COMPLETED = remote_execution_pb2.ExecutionStage.Value.Value('COMPLETED')


# Internal enumerations
# From buildgrid.v2/monitoring.proto:

class LogRecordLevel(Enum):
    # Initially unknown level.
    NOTSET = monitoring_pb2.LogRecord.Level.Value('NOTSET')
    # Debug message severity level.
    DEBUG = monitoring_pb2.LogRecord.Level.Value('DEBUG')
    # Information message severity level.
    INFO = monitoring_pb2.LogRecord.Level.Value('INFO')
    # Warning message severity level.
    WARNING = monitoring_pb2.LogRecord.Level.Value('WARNING')
    # Error message severity level.
    ERROR = monitoring_pb2.LogRecord.Level.Value('ERROR')
    # Critical message severity level.
    CRITICAL = monitoring_pb2.LogRecord.Level.Value('CRITICAL')


class MetricRecordType(Enum):
    # Initially unknown type.
    NONE = monitoring_pb2.MetricRecord.Type.Value('NONE')
    # A metric for counting.
    COUNTER = monitoring_pb2.MetricRecord.Type.Value('COUNTER')
    # A metric for mesuring a duration.
    TIMER = monitoring_pb2.MetricRecord.Type.Value('TIMER')
    # A metric in arbitrary value.
    GAUGE = monitoring_pb2.MetricRecord.Type.Value('GAUGE')
    # A metric with distribution semantics
    DISTRIBUTION = monitoring_pb2.MetricRecord.Type.Value('DISTRIBUTION')


class DataStoreType(Enum):
    MEM = "mem"
    SQL = "sql"


class JobEventType(Enum):
    STOP = "stop"
    CHANGE = "change"


class MetricCategories(Enum):
    LEASES = 'leases'
    JOBS = 'jobs'


class ServiceName(Enum):
    EXECUTION = "execution"
    OPERATIONS = "operations"
    BOTS = "bots"

    @classmethod
    def default_services(cls):
        return (
            cls.EXECUTION.value,
            cls.OPERATIONS.value,
            cls.BOTS.value
        )


class ByteStreamResourceType(Enum):
    CAS = 'cas'
    LOGSTREAM = 'logstream'


class ActionCacheEntryType(Enum):
    """ Type of the value stored in an Action Cache entry. """
    ACTION_RESULT_DIGEST = 0
    ACTION_RESULT = 1


class ExchangeNames(Enum):
    BOT_STATUS = 'bot-status'
    OPERATION_UPDATES = 'operation-updates-exchange'


class QueueNames(Enum):
    OPERATION_UPDATES = 'operation-updates-queue'
