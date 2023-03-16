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
Exceptions
===========
"""


from collections import namedtuple
from enum import Enum
from datetime import timedelta

import grpc
from google.protobuf.duration_pb2 import Duration

from buildgrid._protos.google.rpc import error_details_pb2, status_pb2


# grpc.Status is a metaclass class, so we derive
# a local _Status and associate the expected Attributes
# with it
class _Status(
    namedtuple('_Status',
               ('code', 'details', 'trailing_metadata')),
    grpc.Status):
    pass


class BgdError(Exception):
    """
    Base BuildGrid Error class for internal exceptions.
    """

    def __init__(self, message, *, detail=None, domain=None, reason=None):
        super().__init__(message)

        # Additional detail and extra information
        self.detail = detail

        # Domand and reason
        self.domain = domain
        self.reason = reason


class ErrorDomain(Enum):
    SERVER = 1
    BOT = 2


class ServerError(BgdError):
    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)


class BotError(BgdError):
    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.BOT, reason=reason)


class CancelledError(BgdError):
    """The job was cancelled and any callers should be notified"""

    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)
        self.last_response = None


class InvalidArgumentError(BgdError):
    """A bad argument was passed, such as a name which doesn't exist."""

    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)


class NotFoundError(BgdError):
    """Requested resource not found."""

    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)


class UpdateNotAllowedError(BgdError):
    """UpdateNotAllowedError error."""

    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)


class OutOfRangeError(BgdError):
    """ByteStream service read data out of range."""

    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)


class FailedPreconditionError(BgdError):
    """One or more errors occurred in setting up the action requested, such as
    a missing input or command or no worker being available. The client may be
    able to fix the errors and retry."""

    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)


class PermissionDeniedError(BgdError):
    """The caller does not have permission to execute the specified operation."""

    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)


class BotSessionError(BgdError):
    """Parent class of BotSession Exceptions"""


class BotSessionClosedError(BotSessionError):
    """The requested BotSession has been closed recently."""

    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)


class UnknownBotSessionError(BotSessionError):
    """Buildgrid does not know the requested BotSession."""

    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)


class BotSessionMismatchError(BotSessionError):
    """The BotSession details don't match those in BuildGrid's records."""

    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)


class DuplicateBotSessionError(BotSessionError):
    """The bot with this ID already has a BotSession."""

    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)


class BotSessionCancelledError(BotSessionError):
    """The BotSession update was cancelled"""

    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)


class DatabaseError(BgdError):
    """BuildGrid encountered a database error"""

    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)


class RetriableError(BgdError):
    """BuildGrid encountered a retriable error
       `retry_info` to instruct clients when to retry
       `error_status` a grpc.Status message suitable to call with context.abort_with_status()
    """

    def __init__(self, message, retry_period: timedelta, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)
        retry_delay = Duration()
        retry_delay.FromTimedelta(retry_period)
        retry_info = error_details_pb2.RetryInfo(retry_delay=retry_delay)

        # We could get the integer value of the UNAVAILABLE
        # status code using grpc.StatusCode.UNAVAILABLE.value[0],
        # but the grpc-stubs library complains if we do that. So
        # instead we just hardcode the value, which is unlikely to
        # change as it's a standard code: https://github.com/grpc/grpc/blob/master/doc/statuscodes.md
        status_proto = status_pb2.Status(
            code=14,
            message=message)
        error_detail = status_proto.details.add()
        error_detail.Pack(retry_info)

        error_status = _Status(
            code=grpc.StatusCode.UNAVAILABLE,
            details=status_proto.message,
            trailing_metadata=(('grpc-status-details-bin', status_proto.SerializeToString()),)
        )
        self.retry_info = retry_info
        self.error_status = error_status


class RetriableDatabaseError(RetriableError):
    """BuildGrid encountered a retriable database error"""

    def __init__(self, message, retry_period: timedelta, detail=None, reason=None):
        super().__init__(message, retry_period, detail=detail, reason=reason)


class StreamAlreadyExistsError(BgdError):
    """BuildGrid tried to create a LogStream that already exists."""

    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)


class StreamFinishedError(BgdError):
    """BuildGrid tried to append to a finished LogStream."""

    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)


class StreamWritePendingError(BgdError):
    """BuildGrid tried to read a chunk of the stream that has not been commited yet."""

    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)


class StorageFullError(BgdError):
    """BuildGrid's storage is full, cannot commit to it."""

    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)


class GrpcUninitializedError(BgdError):
    """BuildGrid tried to use a gRPC stub before gRPC was initialized."""

    def __init__(self, message, detail=None, reason=None):
        super().__init__(message, detail=detail, domain=ErrorDomain.SERVER, reason=reason)
