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

import functools
import logging
import threading
import time

from typing import Callable, Dict, Optional, Set

import pika  # type: ignore

from buildgrid.utils import retry_delay


class QueueBinding:
    def __init__(self, queue: str, exchange: str, routing_key: str,
                 auto_delete_queue: bool=False):
        self.queue = queue
        self.exchange = exchange
        self.routing_key = routing_key
        self.auto_delete_queue = auto_delete_queue


class PikaConsumer:
    def __init__(self,
                 connection_parameters: pika.ConnectionParameters,
                 exchanges: Dict[str, pika.exchange_type.ExchangeType],
                 bindings: Set[QueueBinding],
                 prefetch_size: int = 0,
                 prefetch_count: int = 0,
                 on_connection_established_callback: Optional[Callable] = None,
                 on_connection_error_callback: Optional[Callable] = None):
        self._connection_parameters = connection_parameters

        if prefetch_size < 0 or prefetch_count < 0:
            raise ValueError("prefetch_size and prefetch_count cannot be negative")

        self._prefetch_size = prefetch_size
        self._prefetch_count = prefetch_count

        self._exchanges = exchanges
        self._bindings = bindings

        self._connection: Optional[pika.connection.Connection] = None
        self._channel: Optional[pika.channel.Channel] = None

        # Status of the consumer. The Condition allows `subscribe()` to
        # block until either a connection attempt succeeds or is aborted.
        self._connection_status_cv = threading.Condition()
        self._stopped = False
        self._connected = False

        # Callback to notify the caller that the connection is no longer active:
        self._on_connection_error_callback = on_connection_error_callback
        self._on_connection_established_callback = on_connection_established_callback

        self._logger = logging.getLogger(__name__)

        self._successful_bindings_counter = 0
        self._successful_bindings_counter_lock = threading.Lock()

    def start(self):
        """Start the consumer, blocking while it runs."""
        self._connect()
        self._connection.ioloop.start()

    def stop(self):
        """Stop the consumer task."""
        with self._connection_status_cv:
            if not self._stopped:
                self._stopped = True
                self._connection_status_cv.notifyAll()

            self._logger.debug('Stopping consumer...')
            self._close_connection()
            self._connection.ioloop.stop()
            self._logger.info('Consumer stopped')

    def subscribe(self, queue_name: str, callback: Callable) -> str:
        """Register a callback to be invoked when receiving a message
        from the specified queue. That callback must take an argument
        of type `bytes` that will contain the message body and a
        `delivery_tag` string that should be used to ACK or NACK the
        message.

        Returns a consumer tag that can later be passed to `unsubscribe()`.

        A sample usage looks like this:

         .. code-block::

            consumer = PikaConsumer(...)
            def process_message_callback(body: bytes, delivery_tag: str):
                # make some preparations
                if process_message_and_save_results(body):
                    consumer.ack_message(delivery_tag)
                else:
                    consumer.nack_message(delivery_tag)
                # clean up (message already ACK'd or NACK'd)

            consumer_tag = consumer.subscribe('queue', process_message_callback)
            ...
             // Stop receiving messages from 'queue':
            consumer.unsubscribe(consumer_tag)

        """

        self._logger.debug(f"Registering callback for queue '{queue_name}'")

        self._wait_for_connection_open_or_stop_request()

        if self._channel is None:
            raise RuntimeError("Channel is not open.")

        # pika passes the channel as an argument to the `on_message_callback`,
        # but we do not want to expose that. We only forward the message
        # and the delivery tag to the user-provided callback.
        def callback_invoker(channel: pika.channel.Channel,
                             method: pika.spec.Basic.Deliver,
                             properties: pika.spec.BasicProperties,
                             body: bytes):
            self._logger.debug(f"Channel {channel} "
                               f"received message #{method.delivery_tag} "
                               f"from app_id='{properties.app_id}' "
                               f"with consumer tag='{method.consumer_tag}' and "
                               f"containing {len(body)} bytes in its body")
            callback(body, method.delivery_tag)

        consumer_tag = self._channel.basic_consume(queue=queue_name,
                                                   on_message_callback=callback_invoker)

        self._logger.debug(f"Registered callback for queue '{queue_name}', "
                           f"consumer_tag='{consumer_tag}'")
        return consumer_tag

    def unsubscribe(self, consumer_tag: str, timeout_seconds: int = 60):
        """Stop receiving messages with the subscribed callback.
        Issues a cancel request to the server and blocks waiting for it
        to return or the specified timeout to be exceeded.

        Returns `True` if `consumer_tag` was successfully cancelled, which
        guarantees that no more messages will arrive to the subscribed
        callback.
        """
        self._logger.debug(f"Unsubscribing consumer tag '{consumer_tag}'")

        cancelled_event = threading.Event()

        def on_cancelled(_method: pika.frame.Method):
            cancelled_event.set()

        self._channel.basic_cancel(consumer_tag, callback=on_cancelled)  # type: ignore

        return cancelled_event.wait(timeout=timeout_seconds)

    def ack_message(self, delivery_tag: str):
        """Schedule the ACK of a message. (The delivery tag
        was given to the subscribed callback that received the message.)
        """
        callback = functools.partial(self._channel.basic_ack,  # type: ignore
                                     delivery_tag=delivery_tag)
        self._connection.ioloop.add_callback_threadsafe(callback)  # type: ignore

    def nack_message(self, delivery_tag: str):
        """Schedule the NACK of a message. (The delivery tag
        was given to the subscribed callback that received the message.)
        """
        callback = functools.partial(self._channel.basic_nack,  # type: ignore
                                     delivery_tag=delivery_tag)
        self._connection.ioloop.add_callback_threadsafe(callback)  # type: ignore

    def _wait_for_connection_open_or_stop_request(self):
        with self._connection_status_cv:
            self._connection_status_cv.wait_for(lambda: self._connected or self._stopped)

    #
    # Set-up callbacks:
    #
    def _connect(self):
        self._logger.info(f'Connecting to {self._connection_parameters}')
        self._connection = pika.SelectConnection(  # 1) Open connection
            parameters=self._connection_parameters,
            on_open_callback=self._on_connection_open,
            on_open_error_callback=self._on_connection_open_error,
            on_close_callback=self._on_connection_closed)

    def _on_connection_open(self, connection: pika.connection.Connection):  # 2) Create channel
        self._logger.info('Creating new channel')
        self._connection.channel(on_open_callback=self._on_channel_open)  # type: ignore

    def _on_channel_open(self, channel: pika.channel.Channel):  # 3) Configure channel
        self._channel = channel
        self._logger.debug(f'Channel successfully created: {self._channel}')

        if self._prefetch_size > 0 or self._prefetch_count > 0:
            self._logger.info(f'Setting channel QOS: '
                              f'prefetch_size={self._prefetch_size}, '
                              f'prefetch_count={self._prefetch_count}')
            self._channel.basic_qos(prefetch_size=self._prefetch_size,
                                    prefetch_count=self._prefetch_count)

        self._channel.add_on_close_callback(self._on_channel_closed)

        self._declare_exchanges()  # 4) Declare exchanges

    def _declare_exchanges(self):
        for exchange_name, exchange_type in self._exchanges.items():
            self._logger.info(f"Declaring exchange '{exchange_name}' "
                              f"of type {exchange_type}")

            callback = functools.partial(self._on_exchange_declared,
                                         exchange=exchange_name)

            self._channel.exchange_declare(exchange=exchange_name,
                                           exchange_type=exchange_type,
                                           durable=True,
                                           callback=callback)

    def _on_exchange_declared(self, frame: pika.frame.Method, exchange: str):
        self._logger.debug(f"Exchange '{exchange}' of type "
                           f"'{self._exchanges[exchange]}' successfully "
                           f"declared: {frame}")

        self._declare_queues(exchange)

    def _declare_queues(self, exchange_name: str):  # 5) Declare queues
        for binding in self._bindings:
            if binding.exchange == exchange_name:
                self._logger.info(f"Declaring queue '{binding.queue}'")

                callback = functools.partial(self._on_queue_declared,
                                             queue=binding.queue)

                self._channel.queue_declare(queue=binding.queue,   # type: ignore
                                            auto_delete=binding.auto_delete_queue,
                                            durable=True,
                                            callback=callback)

    def _on_queue_declared(self, frame: pika.frame.Method, queue: str):
        self._logger.debug(f"Queue '{queue}' successfully declared: "
                           f"{frame}")

        self._bind_queue(queue)

    def _bind_queue(self, queue_name: str):  # 6) Bind queues
        for binding in self._bindings:
            if binding.queue == queue_name:
                self._logger.info(f"Binding queue '{binding.queue}' "
                                  f"to exchange '{binding.exchange}' "
                                  f"with routing key '{binding.routing_key}'")

                callback = functools.partial(self._on_queue_bind_succeeded,
                                             queue_name=binding.queue,
                                             exchange_name=binding.exchange,
                                             routing_key=binding.routing_key)

                self._channel.queue_bind(binding.queue, binding.exchange,  # type: ignore
                                         binding.routing_key,
                                         callback=callback)

    def _on_queue_bind_succeeded(self,
                                 frame: pika.frame.Method,
                                 queue_name: str,
                                 exchange_name: str,
                                 routing_key: str):
        self._logger.info(f"Queue '{queue_name}' successfully bound "
                          f"to exchange '{exchange_name}' "
                          f"with routing key '{routing_key}': {frame}")

        with self._successful_bindings_counter_lock:
            self._successful_bindings_counter += 1
            if self._successful_bindings_counter == len(self._bindings):
                with self._connection_status_cv:
                    self._connected = True
                    self._connection_status_cv.notifyAll()

                if self._on_connection_established_callback:
                    self._on_connection_established_callback()

    #
    # Error-handling callbacks
    #
    def _on_connection_open_error(self,
                                  connection: pika.connection.Connection,
                                  error: Exception):
        self._logger.error(f'Error opening connection: {error}')
        self.stop()
        if self._on_connection_error_callback:
            self._on_connection_error_callback()

    def _on_connection_closed(self,
                              connection: pika.connection.Connection,
                              reason: Exception):
        self.stop()
        if self._on_connection_error_callback:
            self._on_connection_error_callback()

    def _on_channel_closed(self,
                           channel: pika.channel.Channel,
                           reason: Exception):
        self._logger.error(f'Channel closed. Reason: {reason}')
        self.stop()
        if self._on_connection_error_callback:
            self._on_connection_error_callback()

    def _close_connection(self):
        if self._connection.is_closing or self._connection.is_closed:
            self._logger.debug('Connection is closing or already closed')
        else:
            self._logger.info('Closing connection')
            self._connection.close()


class RetryingPikaConsumer:
    def __init__(self,
                 connection_parameters: pika.ConnectionParameters,
                 exchanges: Dict[str, pika.exchange_type.ExchangeType],
                 bindings: Set[QueueBinding],
                 max_connection_attempts: int = 4,
                 retry_delay_base: int = 1,
                 prefetch_size: Optional[int] = 0,
                 prefetch_count: Optional[int] = 0,
                 on_connection_attempts_exceeded_callback: Optional[Callable] = None):
        """
        Create a `PikaConsumer` and watch it to ensure that it stays
        connected.

        If provided, the `on_connection_attempts_exceeded_callback` will
        be invoked after the last connection attempt has failed.
        """

        self._logger = logging.getLogger(__name__)

        self._connection_parameters = connection_parameters
        self._exchanges = exchanges
        self._bindings = bindings

        self._consumer = None
        self._consumer_thread = None

        # Status of the consumer. The Condition allows methods like
        # `un/subscribe()` to block until either a connection attempt
        # succeeds or is aborted.
        self._connection_status_cv = threading.Condition()
        self._connected = False
        self._stopped = False

        self._connection_attempts = 0

        # If `max_connection_attempts == 0`, retry forever.
        self._max_connection_attempts = max_connection_attempts
        self._retry_delay_base = retry_delay_base

        self._prefetch_size = prefetch_size
        self._prefetch_count = prefetch_count

        self._on_connection_attempts_exceeded_callback = on_connection_attempts_exceeded_callback

        self._connection_watcher_thread = threading.Thread(target=self._watch_consumer)
        self._connection_error_event = threading.Event()
        self._connection_watcher_thread.start()

        # Mapping of queues to user-provided callbacks:
        self._callbacks_lock = threading.Lock()
        self._callbacks: Dict[str, Callable] = {}
        # Mapping of queues to the consumer tags returned by the `PikaConsumer`
        # (needed to unsubscribe):
        self._consumer_tags: Dict[str, str] = {}

        self._start_consumer()

    def stop(self):
        """Stop the underlying consumer and the monitoring of it."""
        with self._connection_status_cv:
            if not self._stopped:
                self._stopped = True
                self._connection_status_cv.notifyAll()

            self._consumer.stop()
            self._connection_error_event.set()
            self._connection_watcher_thread.join()

    def _start_consumer(self):
        """Create a new `PikaConsumer` and start it in a background
        thread.
        (This method will also be called on reconnection attempts.)
        """
        with self._connection_status_cv:
            self._connected = False

        if self._consumer is not None:
            self._consumer.stop()
            self._consumer = None

        self._consumer_tags = {}

        self._connection_error_event.clear()

        self._consumer = PikaConsumer(connection_parameters=self._connection_parameters,
                                      exchanges=self._exchanges,
                                      bindings=self._bindings,
                                      prefetch_count=self._prefetch_count,
                                      prefetch_size=self._prefetch_size,
                                      on_connection_established_callback=self._on_connection_established,
                                      on_connection_error_callback=self._on_connection_error)
        self._consumer_thread = threading.Thread(target=self._consumer.start)
        self._consumer_thread.start()

    def _on_connection_established(self):
        """When the underlying consumer is connected, re-register the
        callbacks that might have been present on a previous connection.
        """
        self._logger.debug("on_connection_established() called")

        with self._connection_status_cv:
            self._connected = True
            self._connection_status_cv.notifyAll()

        # Re-adding existing subscriptions to the brand-new consumer:
        with self._callbacks_lock:
            for queue, callback in self._callbacks.items():
                consumer_tag = self._consumer.subscribe(queue, callback)
                self._consumer_tags[queue] = consumer_tag

    def _on_connection_error(self):
        """When the underlying consumer reports a connection failure,
        notify the watcher thread so that it can either reconnect or
        stop monitoring.
        """
        self._logger.debug("on_connection_error() called")
        self._connection_error_event.set()

    def _watch_consumer(self):
        """Thread that monitors the connection status of the underlying
        consumer and makes sure that the connection stays open.
        On connection failures, as long as the retry limit is not
        exceeded, it will call `_start_consumer()` to create and
        configure a new `PikaConsumer`.
        """
        while not self._stopped:
            self._logger.debug("Waiting for error event...")

            self._connection_error_event.wait()
            if self._stopped:
                return
            self._logger.debug("Connection error event set")

            if self._connected:
                self._logger.error("Consumer was disconnected, "
                                   "attempting to reconnect")
                self._connection_attempts = 1
            else:
                self._connection_attempts += 1
                if self._max_connection_attempts > 0:
                    self._logger.warning(f"Failed connection attempt "
                                         f"{self._connection_attempts}/"
                                         f"{self._max_connection_attempts}.")
                else:
                    self._logger.warning(f"Failed connection attempt "
                                         f"{self._connection_attempts}")

            if self._max_connection_attempts == 0 or \
                    self._connection_attempts < self._max_connection_attempts:
                delay = retry_delay(self._connection_attempts, self._retry_delay_base)
                self._logger.info(f"Waiting {delay}s before "
                                  f"attempting to reconnect")

                time.sleep(delay)
                self._start_consumer()
            else:
                self._logger.error(f"Failed last ({self._connection_attempts}/"
                                   f"{self._max_connection_attempts}) "
                                   f"connection attempt.")

                with self._connection_status_cv:
                    self._stopped = True
                    self._connection_status_cv.notifyAll()

                if self._on_connection_attempts_exceeded_callback:
                    self._on_connection_attempts_exceeded_callback()

    def _wait_for_connection_open_or_stop_request(self):
        with self._connection_status_cv:
            self._connection_status_cv.wait_for(lambda: self._connected or self._stopped)

    def subscribe(self, queue_name: str, callback: Callable):
        """Register a callback to process messages that arrive to the
        queue.
        """
        self._wait_for_connection_open_or_stop_request()

        if self._consumer is None:
            raise RuntimeError("Consumer was not created.")

        with self._callbacks_lock:
            self._callbacks[queue_name] = callback
            self._consumer_tags[queue_name] = self._consumer.subscribe(queue_name, callback)

    def unsubscribe(self, queue_name: str):
        """Stop receiving messages from the queue."""
        self._wait_for_connection_open_or_stop_request()

        if self._consumer is None:
            raise RuntimeError("Consumer was not created.")

        with self._callbacks_lock:
            tag = self._consumer_tags[queue_name]
            self._consumer.unsubscribe(tag)
            del self._callbacks[queue_name]

    def ack_message(self, delivery_tag: str):
        """ACK a message using the delivery tag that was given to the
        subscribed callback.
        If the consumer is stopped or disconnected, raises
        `ConnectionError`.
        """
        if self._consumer is None:
            self._logger.debug(f"Consumer is disconnected, could not "
                               f"ACK delivery tag '{delivery_tag}'")
            raise ConnectionError("Consumer is disconnected")

        self._consumer.ack_message(delivery_tag)

    def nack_message(self, delivery_tag: str):
        """NACK a message using the delivery tag that was given to the
        subscribed callback.
        If the consumer is stopped or disconnected, raises
        `ConnectionError`.
        """
        if self._consumer is None:
            self._logger.debug(f"Consumer is disconnected, could not "
                               f"NACK delivery tag '{delivery_tag}'")
            raise ConnectionError("Consumer is disconnected")

        self._consumer.nack_message(delivery_tag)
