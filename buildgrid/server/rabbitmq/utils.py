# Copyright (C) 2021 Bloomberg LP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  <http://www.apache.org/licenses/LICENSE-2.0>
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License' is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
RabbitMQ-related utilities
==========================

This module contains common utilities related to using RabbitMQ and the
related internal publishing/consuming thread mechanisms.

"""


from threading import Event

from buildgrid.server.rabbitmq._enums import Exchange


class MessageSpec:

    """Information about a message to be published on a RabbitMQ exchange.

    This class is a simple wrapper around a message payload, intended to be
    used to communicate a request to publish a RabbitMQ message from a gRPC
    handler thread to the RabbitMQ publisher thread.

    It contains all the data a publisher thread needs to publish the message
    on a RabbitMQ exchange.

    """

    def __init__(self, *, exchange: Exchange, payload: bytes, routing_key: str=''):
        """Instantiate a new MessageSpec.

        Args:
            exchange (Exchange): The name of the RabbitMQ exchange that this
                message should be published to.
            payload (bytes): The desired message payload, in bytes. This
                could be a serialized proto for example.
            routing_key (str): The key to use when routing this message
                in RabbitMQ. Optional, defaults to ''.

        """
        self._event = Event()
        self.error = None
        self.exchange = exchange
        self.payload = payload
        self.routing_key = routing_key

    def get_completion_event(self):
        """Return the ``threading.Event`` which indicates publish completion.

        This event gets set by the publisher once either the message has been
        published successfully or the publisher has run out of retry attempts
        to publish the message.

        """
        return self._event

    def set_completion_event(self):
        """Set the ``threading.Event`` to mark publishing completed.

        This should be called by the publisher thread once either the message
        has been published successfully or the publishing has failed and run
        out of retries.

        Calling this method will wake up any threads that are waiting for
        the publishing of a message to be completed.

        """
        self._event.set()
