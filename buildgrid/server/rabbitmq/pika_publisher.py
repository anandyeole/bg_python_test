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
from queue import Empty, Queue
from time import sleep
from threading import Thread
from typing import Dict, Optional

import pika  # type: ignore

from buildgrid.server.rabbitmq.utils import MessageSpec
from buildgrid.settings import RABBITMQ_PUBLISHER_CREATION_RETRIES
from buildgrid.utils import retry_delay


ExchangesDict = Dict[str, pika.exchange_type.ExchangeType]


class PikaPublisher:
    """Helper class to send messages to a RabbitMQ exchange."""
    def __init__(self, connection_parameters: pika.ConnectionParameters,
                 exchanges: Optional[ExchangesDict]=None):
        """
        Args:
            connection_parameters: pika server's connection information
            exchanges (dict(str, pika.exchange_type.ExchangeType)):
                optional list of exchanges to be declared during
                initialization
        """
        self._logger = logging.getLogger(__name__)

        self._connection = pika.BlockingConnection(parameters=connection_parameters)
        # Unlike `PikaConsumer`, this class uses pika's `BlockingConnection`
        # adapter.
        # That is because it enables `send()` to be implemented as a single
        # call to `pika.channel.Channel.basic_publish()`, which provides
        # error-handling by raising detailed exception types.
        # It also allows to enable delivery confirmations for the channel,
        # which makes `basic_publish()` raise `UnroutableError` exceptions
        # for messages that fail to be delivered.
        # Therefore, users of this class can simply write a single try/except
        # block around `send()` (which is also blocking) rather than requiring
        # the involvement of a pair of callbacks.
        # Note that the cost for code simplicity might be a loss in performance,
        # so in the future it could be worth slightly changing the API of the
        # Publisher and using pika's `SelectConnection` adapter.

        self._channel = self._connection.channel()
        # Configure channel to confirm that messages were delivered to
        # the broker:
        self._channel.confirm_delivery()

        if exchanges:
            for exchange_name, exchange_type in exchanges.items():
                self._channel.exchange_declare(exchange_name,
                                               exchange_type=exchange_type,
                                               durable=True)

    def __del__(self):
        if not self._connection.is_closed:
            try:
                self._connection.close()
                self._logger.debug("Connection closed after deleting PikaPublisher.")
            except Exception as e:
                self._logger.debug(f"Failed to close connection while deleting PikaPublisher: {e}")

    def send(self, exchange: str, routing_key: str, body: bytes):
        """Send a message to an exchange (blocking call).

        Args:
            exchange (str): name of the destination exchange
            routing_key (str): routing key to bind on
            body (bytes): payload of message

        Note that, in addition to `pika.exceptions` related to connection
        or channel issues, this method may raise:
            * `UnroutableError`: if the message is returned by the broker

            * `NackError`: if the message is negatively-acknowledged by the broker
        """
        self._channel.basic_publish(exchange=exchange,
                                    routing_key=routing_key,
                                    mandatory=True,
                                    body=body)


def get_publisher(
    params: pika.ConnectionParameters,
    exchanges: Optional[ExchangesDict]
) -> Optional[PikaPublisher]:
    """Create a PikaPublisher, retrying on connection failure.

    This method will retry :const:`buildgrid.settings.RABBITMQ_PUBLISHER_CREATION_RETRIES`
    times before giving up and returning ``None``.

    Args:
        params (pika.ConnectionParameters): The Pika connection parameters to
            use when opening the connection to RabbitMQ.
        exchanges (dict): Map of exchange names to types. This should contain
            all the exchanges that this publisher will publish messages on,
            so they can be declared before use.

    Returns:
        :class:``PikaPublisher`` if one can be created,
        ``None`` otherwise.

    """
    attempt = 1
    while attempt < RABBITMQ_PUBLISHER_CREATION_RETRIES:
        try:
            return PikaPublisher(params, exchanges=exchanges)
        except Exception as e:
            delay = retry_delay(attempt)
            sleep(delay)
            attempt += 1
            if attempt >= RABBITMQ_PUBLISHER_CREATION_RETRIES:
                raise e
    return None


class RetryingPikaPublisher:

    """Class to handle message publishing with retry handling.

    This class runs a "publisher thread" which creates a :class:`PikaPublisher`
    and uses it to publish messages to RabbitMQ. This thread also handles any
    Exceptions which get raised in the process of publishing the messages,
    retrying the publishing a configurable number of times (or indefinitely).

    The :meth:`RetryingPikaPublisher.send` method handles handing messages that
    need to be sent through to the publisher thread, which allows clean and
    thread-safe message publishing using ``RetryingPikaPublisher`` across
    multiple threads (e.g. gRPC request handlers).

    """

    def __init__(
        self,
        connection_parameters: pika.ConnectionParameters,
        thread_name: Optional[str]=None,
        max_publish_attempts: int=0,
        exchanges: Optional[ExchangesDict]=None,
        retry_delay_base: int=1
    ):
        """Instantiate a new RetryingPikaPublisher.

        Args:
            connection_parameters (pika.ConnectionParameters): The Pika connection
                parameters to use when opening the connection to RabbitMQ.
            thread_name (str): Optional name to use for the publisher thread. If
                unset then the thread will be named according to Python's default
                thread naming scheme.
            max_publish_attempts (int): The number of times to attempt to publish
                a specific message before giving up. If set to 0 (the default) then
                publishing will be retried indefinitely until successful.
            exchanges (dict): Map of exchange names to types. This should contain
                all the exchanges that this publisher will publish messages on,
                so they can be declared before use.
            retry_delay_base (int): The base multiplier to use when calculating delay
                between retry attempts. Defaults to 1.

        """
        self._logger = logging.getLogger(__name__)
        self._connection_parameters = connection_parameters
        self._exchanges = exchanges
        self._max_publish_attempts = max_publish_attempts
        self._publisher_thread = Thread(target=self._publish_from_queue, name=thread_name)
        self._publish_queue: Queue = Queue()
        self._retry_delay_base = retry_delay_base
        self._run_publisher_thread = False

    def _get_publisher(self) -> Optional[PikaPublisher]:
        """Attempt to get a publisher until success or no longer needed.

        This method repeatedly attempts to construct a new PikaPublisher,
        logging an error (at DEBUG level) after every
        ``RABBITMQ_PUBLISHER_CREATION_RETRIES`` consecutive failures.

        Unlike the ``get_publisher`` helper, this method only returns
        when either a publisher is obtained or the ``RetryingPikaPublisher``
        is shutting down.

        .. warning::
            This method is not thread-safe, so should only be called from
            within the publisher thread itself.

        Returns:
            A :class:`PikaPublisher` if one can be created successfully.
            ``None`` if the internal flag for checking whether to run the
                publisher thread is set to ``False``.

        """
        publisher = None
        while self._run_publisher_thread and not publisher:
            try:
                publisher = get_publisher(self._connection_parameters, self._exchanges)
            except Exception:
                self._logger.debug(
                    "Failed to construct a RabbitMQ publisher in "
                    f"{RABBITMQ_PUBLISHER_CREATION_RETRIES} attempts",
                    exc_info=True
                )
                if not self._run_publisher_thread:
                    break
        return publisher

    def _publish_from_queue(self) -> None:
        """Publish messages from the internal publisher queue to RabbitMQ exchanges.

        This method runs in a separate thread to allow reuse of a single
        RabbitMQ connection to publish all the messages that the owner of this
        RetryingPikaPublisher needs to publish.

        The messages to be published are read from the internal publishing queue
        and then sent on to RabbitMQ by this method.

        """
        self._logger.debug("Starting RabbitMQ publisher thread")
        publisher = self._get_publisher()

        while self._run_publisher_thread:
            try:
                # Get the next message to publish, with a timeout.
                # We set a timeout here to allow relatively responsive
                # shutdown of this thread when `self._run_publisher_thread`
                # becomes false.
                message_spec = self._publish_queue.get(timeout=5)
            except Empty:
                continue

            published = False
            attempts = 0

            def attempts_remaining():
                if self._max_publish_attempts == 0:
                    return True
                return attempts < self._max_publish_attempts

            # NOTE: Checking `self._run_publisher_thread` serves to both allow
            # early shutdown if `RetryingPikaPublisher.stop` is called whilst
            # we're in a retry loop, and also guard against `publisher` being
            # `None` rather than a `PikaPublisher` instance.
            while not published and attempts_remaining() and self._run_publisher_thread:
                attempts += 1
                try:
                    if publisher is not None:
                        publisher.send(
                            message_spec.exchange.name,
                            message_spec.routing_key,
                            message_spec.payload
                        )
                        published = True
                    else:
                        publisher = self._get_publisher()
                except (pika.exceptions.UnroutableError, pika.exceptions.NackError) as e:
                    # Not much point attempting to retry these errors
                    self._logger.warning("Message could not be routed", exc_info=True)
                    message_spec.error = e
                    break
                except (
                    pika.exceptions.ChannelClosed,
                    pika.exceptions.ChannelWrongStateError,
                    pika.exceptions.StreamLostError
                ):
                    # Throw away the publisher and try again
                    publisher = self._get_publisher()
                except Exception as e:
                    self._logger.debug("Message publishing failed", exc_info=True)
                    if attempts_remaining():
                        delay = retry_delay(attempts, self._retry_delay_base)
                        self._logger.debug(f"Retrying message publishing in {delay} seconds")
                        sleep(delay)
                    else:
                        # We're done retrying, set the error and give up
                        message_spec.error = e
            message_spec.set_completion_event()
        del publisher

    def start(self):
        """Start the publisher thread to get ready to publish to RabbitMQ.

        This method sets the internal flag that determines whether to keep the
        publisher thread running, and starts the thread. This should be called
        before calling :meth:`RetryingPikaPublisher.send` to avoid a backlog of
        messages on startup, although messages sent before calling this method
        will still be published.

        """
        self._run_publisher_thread = True
        self._publisher_thread.start()

    def stop(self):
        """Cleanly stop the RabbitMQ publisher thread.

        This method sets the internal flag that determines whether to keep the
        publisher thread running to ``False``, and then blocks until that thread
        finishes.

        """
        self._run_publisher_thread = False
        self._publisher_thread.join()

    def send(
        self,
        message_spec: MessageSpec,
        reraise_exceptions: bool=False,
        wait_for_delivery: bool=False,
        wait_for_delivery_timeout: Optional[float]=None
    ) -> bool:
        """Send a message to be published to a RabbitMQ exchange.

        This method puts the given :class:`MessageSpec` onto an internal queue
        which is consumed by the publisher thread. The message will get processed
        by that thread at some point in the future assuming RabbitMQ is accessible.

        Setting ``wait_for_delivery`` to ``True`` will make this method block
        until the publisher thread has handled the message and successfully
        published it to the specified RabbitMQ exchange.

        Setting ``reraise_exceptions`` to ``True`` will additionally re-raise
        the final exception after running out of retries when a message fails
        to be published to RabbitMQ.

        .. warning::
            If using an unlimited number of retries, then setting
            ``reraise_exceptions`` can lead to this method blocking for an
            indefinite period of time.

        Args:
            message_spec (MessageSpec): The specification of the message that
                needs to be published to RabbitMQ. This includes the exchange
                to publish to and the message content, as well as providing
                a way of communicating publish success/failure from the
                publisher thread back to the caller.
            reraise_exceptions (bool): If the message can't be published and
                the publisher runs out of retry attempts, the Exception that
                caused the failure will be re-raised if this is ``True``.
                Note, setting this to ``True`` will make this method block
                until the publisher thread finishes trying to publish the
                message, regardless of ``wait_for_delivery_timeout``.
            wait_for_delivery (bool): Whether or not to block until the
                publisher thread has confirmed the delivery of the message
                to the RabbitMQ exchange.
            wait_for_delivery_timeout (float): Time to wait for delivery
                confirmation if ``wait_for_delivery`` is set. If ``None``
                (the default), then wait indefinitely.

        """
        self._publish_queue.put(message_spec)

        if reraise_exceptions and not wait_for_delivery:
            self._logger.debug(
                "Publisher got a request to reraise exceptions for "
                "unroutable messages without wait_for_delivery being "
                "explicitly set. Waiting for delivery confirmation "
                "anyway."
            )
            wait_for_delivery = True
            wait_for_delivery_timeout = None

        completion_event = message_spec.get_completion_event()
        timed_out = False
        if wait_for_delivery:
            timed_out = completion_event.wait(wait_for_delivery_timeout)
            if reraise_exceptions and message_spec.error is not None:
                raise message_spec.error
        return timed_out
