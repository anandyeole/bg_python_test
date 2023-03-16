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
PublishBuildEvent service
=========================

"""


import logging
from typing import Iterable

from google.protobuf.empty_pb2 import Empty
import grpc

from buildgrid._protos.buildgrid.v2.query_build_events_pb2 import (
    QueryEventStreamsRequest,
    QueryEventStreamsResponse
)
from buildgrid._protos.buildgrid.v2.query_build_events_pb2_grpc import (
    QueryBuildEventsServicer,
    add_QueryBuildEventsServicer_to_server
)
from buildgrid._protos.google.devtools.build.v1.build_events_pb2 import (
    BuildEvent
)
from buildgrid._protos.google.devtools.build.v1.publish_build_event_pb2_grpc import (
    PublishBuildEventServicer,
    add_PublishBuildEventServicer_to_server
)
from buildgrid._protos.google.devtools.build.v1.publish_build_event_pb2 import (
    OrderedBuildEvent,
    PublishBuildToolEventStreamRequest,
    PublishBuildToolEventStreamResponse,
    PublishLifecycleEventRequest
)
from buildgrid.server.build_events.storage import BuildEventStream, BuildEventStreamStorage


def is_lifecycle_event(event: BuildEvent):
    lifecycle_events = [
        "build_enqueued",
        "invocation_attempt_started",
        "invocation_attempt_finished",
        "build_finished"
    ]
    return event.WhichOneof("event") in lifecycle_events


class PublishBuildEventService(PublishBuildEventServicer):

    """PublishBuildEvent service implementation."""

    def __init__(
        self,
        server: grpc.Server,
        stream_storage: BuildEventStreamStorage
    ):
        self._logger = logging.getLogger(__name__)
        self._streams = stream_storage

        add_PublishBuildEventServicer_to_server(self, server)
        self._logger.info("Created PublishBuildEventService")

    def PublishLifecycleEvent(
        self,
        request: PublishLifecycleEventRequest,
        context: grpc.ServicerContext
    ) -> Empty:
        """Handler for PublishLifecycleEvent requests.

        This method takes a request containing a build lifecycle event, and
        uses it to update the high-level state of a build (with a corresponding)
        event stream.

        """
        self._logger.info("Received a PublishLifecycleEvent request")
        ordered_build_event = request.build_event
        if is_lifecycle_event(ordered_build_event.event):
            stream = self._get_stream_for_event(ordered_build_event)
            stream.publish_event(ordered_build_event)

        else:
            self._logger.warning("Got a build tool event in a PublishLifecycleEvent request")
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        return Empty()

    def PublishBuildToolEventStream(
        self,
        request_iterator: Iterable[PublishBuildToolEventStreamRequest],
        context: grpc.ServicerContext
    ) -> Iterable[PublishBuildToolEventStreamResponse]:
        self._logger.info("Opened a BuildToolEvent stream")

        for request in request_iterator:
            self._logger.info("Got a BuildToolEvent on the stream")

            # We don't need to give any special treatment to BuildToolEvents, so
            # just call the underlying `get_stream` method here.
            stream = self._streams.get_stream(request.ordered_build_event.stream_id)

            # `stream` should never be `None`, but in case the internals change
            # in future lets be safe.
            if stream is not None:
                stream.publish_event(request.ordered_build_event)

            yield PublishBuildToolEventStreamResponse(
                stream_id=request.ordered_build_event.stream_id,
                sequence_number=request.ordered_build_event.sequence_number
            )

    def _get_stream_for_event(self, event: OrderedBuildEvent) -> BuildEventStream:
        # If this is the start of a new build, then we want a new stream
        if event.event.WhichOneof("event") == "build_enqueued":
            return self._streams.new_stream(event.stream_id)

        return self._streams.get_stream(event.stream_id)


class QueryBuildEventsService(QueryBuildEventsServicer):

    def __init__(
        self,
        server: grpc.Server,
        stream_storage: BuildEventStreamStorage
    ):
        self._logger = logging.getLogger(__name__)
        self._streams = stream_storage
        add_QueryBuildEventsServicer_to_server(self, server)
        self._logger.info("Created QueryBuildEventsService")

    def QueryEventStreams(
        self,
        request: QueryEventStreamsRequest,
        context: grpc.ServicerContext
    ) -> QueryEventStreamsResponse:
        self._logger.info("Got a QueryEventStreams request")

        streams = self._streams.get_matching_streams(
            stream_key_regex=request.build_id_pattern)
        return QueryEventStreamsResponse(
            streams=[stream.to_grpc_message() for stream in streams]
        )
