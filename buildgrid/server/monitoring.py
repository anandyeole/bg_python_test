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


import asyncio
import ctypes
from enum import Enum
import logging
from multiprocessing import Event, Process, Queue
from queue import Empty
import sys
import socket
import time
import threading
from typing import IO, TYPE_CHECKING, List, Optional, Union

from google.protobuf import json_format

from buildgrid._exceptions import InvalidArgumentError
from buildgrid._protos.buildgrid.v2.monitoring_pb2 import LogRecord, MetricRecord, BusMessage

if TYPE_CHECKING:
    from asyncio.streams import StreamWriter


class MonitoringOutputType(Enum):
    # Standard output stream.
    STDOUT = 'stdout'
    # On-disk file.
    FILE = 'file'
    # UNIX domain socket.
    SOCKET = 'socket'
    # UDP IP:port
    UDP = 'udp'
    # Silent
    SILENT = 'silent'


class MonitoringOutputFormat(Enum):
    # Protobuf binary format.
    BINARY = 'binary'
    # JSON format.
    JSON = 'json'
    # StatsD format. Only metrics are kept - logs are dropped.
    STATSD = 'statsd'


class StatsDTagFormat(Enum):
    NONE = 'none'
    INFLUX_STATSD = 'influx-statsd'
    DOG_STATSD = 'dogstatsd'
    GRAPHITE = 'graphite'


class UdpWrapper:
    """ Wraps socket sendto() in write() so it can be used polymorphically """

    def __init__(self, endpoint_location):
        try:
            self._addr, self._port = endpoint_location.split(":")
            self._port = int(self._port)
        except ValueError as e:
            error_msg = f"udp endpoint-location {endpoint_location} does not have the form address:port"
            raise ValueError(error_msg) from e
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def write(self, message):
        self._socket.sendto(message, (self._addr, self._port))

    def close(self):
        return self._socket.close()


MonitoringEndpoint = Union[IO, UdpWrapper, "StreamWriter"]


EXCLUDED_METADATA_KEYS = [
    'instance-name',
    'statsd-bucket'
]


class _MonitoringBus:
    """ Class representing a singleton monitoring bus. Only one should exist at a time (as _instance). """
    _instance: Optional["_MonitoringBus"] = None  # The type needs quotes because it's the same as the class
    _instance_lock = threading.Lock()

    def __init__(self, endpoint_type=MonitoringOutputType.SOCKET,
                 endpoint_location=None,
                 metric_prefix="",
                 serialisation_format=MonitoringOutputFormat.STATSD,
                 tag_format=StatsDTagFormat.NONE):
        self._logger = logging.getLogger(__name__)
        self.__event_loop = None
        self._streaming_process = None
        self._stop_streaming_worker = Event()

        self.__message_queue = Queue()
        self.__sequence_number = 1

        self.__output_location = None
        self.__async_output = False
        self.__json_output = False
        self.__statsd_output = False
        self.__print_output = False
        self.__udp_output = False
        self.__is_silent = False

        if endpoint_type == MonitoringOutputType.FILE:
            self.__output_location = endpoint_location

        elif endpoint_type == MonitoringOutputType.SOCKET:
            self.__output_location = endpoint_location
            self.__async_output = True

        elif endpoint_type == MonitoringOutputType.STDOUT:
            self.__print_output = True

        elif endpoint_type == MonitoringOutputType.UDP:
            self.__output_location = endpoint_location
            self.__udp_output = True

        elif endpoint_type == MonitoringOutputType.SILENT:
            self.__is_silent = True

        else:
            raise InvalidArgumentError(
                f"Invalid endpoint output type: [{endpoint_type}]")

        self.__metric_prefix = metric_prefix

        if serialisation_format == MonitoringOutputFormat.JSON:
            self.__json_output = True
        elif serialisation_format == MonitoringOutputFormat.STATSD:
            self.__statsd_output = True

        self.__tag_format = tag_format

    # --- Public API ---

    @property
    def is_enabled(self) -> bool:
        """Whether monitoring is enabled.

        The send_record methods perform this check so clients don't need to
        check this before sending a record to the monitoring bus, but it is
        provided for convenience. """
        return self._streaming_process is not None and self._streaming_process.is_alive()

    @property
    def prints_records(self) -> bool:
        """Whether or not messages are printed to standard output."""
        return self.__print_output

    @property
    def is_silent(self) -> bool:
        """Whether or not this is a silent monitoring bus."""
        return self.__is_silent

    def start(self) -> None:
        """Starts the monitoring bus worker task."""
        if self.__is_silent or self._streaming_process is not None:
            return

        self._streaming_process = Process(target=self._streaming_worker)
        self._streaming_process.start()

    def stop(self) -> None:
        """Cancels the monitoring bus worker task."""
        if self.__is_silent or self._streaming_process is None:
            return

        self._stop_streaming_worker.set()
        self._streaming_process.join()

    async def prefix_record(self, record: MetricRecord) -> MetricRecord:
        """ Prefix the record's metric name.

        The new metric name is built according to the following rules. Each element is separated
        with a dot (.):

        1. The custom prefix specified in the configuration, if any.
        2. If this is an instance metric with a nonempty instance name, the instance name.
        3. If this is an instant metric (even with an empty instance name), "instance".
        4. The original record name.

        For example, consider the "widgets-built" metric, and let's say we have a custom prefix of "mycompany".

        If "widgets-built" is not an instanced metric: "mycompany.widgets-built".
        If "widgets-built" is an instanced metric in an empty instance: "mycompany.instance.widgets-built".
        If "widgets-built" is an instanced metric in an instance named "dev": "mycompany.dev.instance.widgets-built".

        Args:
            record (Message): The record to prefix.
        """
        instance_name = record.metadata.get('instance-name')
        if instance_name is not None:
            # This is an instance metric, so we'll add the instance name
            # to the prefix if it isn't empty
            if instance_name:
                instance_name = instance_name + "."

            # Prefix the metric with "instance_name.instance." if the instance
            # name isn't empty, otherwise just "instance."
            record.name = f"{self.__metric_prefix}{instance_name}instance.{record.name}"

        else:
            # Not an instance metric
            record.name = f"{self.__metric_prefix}{record.name}"

        return record

    async def send_record(self, record: MetricRecord) -> None:
        """Publishes a record onto the bus asynchronously.

        Args:
            record (Message): The record to send.
        """
        if not self.is_enabled:
            return

        if record.DESCRIPTOR is MetricRecord.DESCRIPTOR:
            record = await self.prefix_record(record)

        self.__message_queue.put(record)

    def prefix_record_nowait(self, record: MetricRecord) -> MetricRecord:
        """ Prefix the record's metric name. This is the same as prefix_record, but called synchronously.

        See the prefix_record docstring for notes on the prefixing rules.

        Args:
            record (Message): The record to prefix.
        """
        instance_name = record.metadata.get('instance-name')
        if instance_name is not None:
            # This is an instance metric, so we'll add the instance name
            # to the prefix if it isn't empty
            if instance_name:
                instance_name = instance_name + "."

            # Prefix the metric with "instance_name.instance." if the instance
            # name isn't empty, otherwise just "instance."
            record.name = f"{self.__metric_prefix}{instance_name}instance.{record.name}"

        else:
            # Not an instance metric
            record.name = f"{self.__metric_prefix}{record.name}"

        return record

    def send_record_nowait(self, record: MetricRecord) -> None:
        """Publishes a record onto the bus synchronously.

        Args:
            record (Message): The record to send.
        """
        if not self.is_enabled:
            return

        if record.DESCRIPTOR is MetricRecord.DESCRIPTOR:
            record = self.prefix_record_nowait(record)

        self.__message_queue.put_nowait(record)

    # --- Private API ---
    def _format_statsd_with_tags(self, name: str, tags: List[str], value: Union[int, float], metric_type: str) -> str:
        if not tags or self.__tag_format == StatsDTagFormat.NONE:
            return f"{name}:{value}|{metric_type}\n"

        if self.__tag_format == StatsDTagFormat.INFLUX_STATSD:
            tag_string = ",".join(tags)
            return f"{name},{tag_string}:{value}|{metric_type}\n"

        elif self.__tag_format == StatsDTagFormat.DOG_STATSD:
            tag_string = ",".join(tags)
            return f"{name}:{value}|{metric_type}|#{tag_string}\n"

        elif self.__tag_format == StatsDTagFormat.GRAPHITE:
            tag_string = ";".join(tags)
            return f"{name};{tag_string}:{value}|{metric_type}\n"

        else:
            return f"{name}:{value}|{metric_type}\n"

    def _format_record_as_statsd_string(self, record: MetricRecord) -> str:
        """ Helper function to convert metrics to a string in the statsd format.

        See https://github.com/statsd/statsd/blob/master/docs/metric_types.md for valid metric types.

        Note that BuildGrid currently only supports Counters, Timers, and Gauges, and it has the custom
        Distribution type as an alias for Timers.

        Args:
            record (Message): The record to convert.
        """
        bucket = record.metadata.get("statsd-bucket")

        tag_assignment_symbol = "="
        if self.__tag_format == StatsDTagFormat.DOG_STATSD:
            tag_assignment_symbol = ":"
        tags = [
            f"{key}{tag_assignment_symbol}{value}"
            for key, value in record.metadata.items()
            if key not in EXCLUDED_METADATA_KEYS and str(value) != ''
        ]

        if bucket:
            record.name = f"{record.name}.{bucket}"
        if record.type == MetricRecord.COUNTER:
            if record.count is None:
                raise ValueError(
                    f"COUNTER record {record.name} is missing a count")
            return self._format_statsd_with_tags(record.name, tags, record.count, "c")
        elif record.type is MetricRecord.TIMER:
            if record.duration is None:
                raise ValueError(
                    f"TIMER record {record.name} is missing a duration")
            return self._format_statsd_with_tags(record.name, tags, record.duration.ToMilliseconds(), "ms")
        elif record.type is MetricRecord.DISTRIBUTION:
            if record.count is None:
                raise ValueError(
                    f"DISTRIBUTION record {record.name} is missing a count")
            return self._format_statsd_with_tags(record.name, tags, record.count, "ms")
        elif record.type is MetricRecord.GAUGE:
            if record.value is None:
                raise ValueError(
                    f"GAUGE record {record.name} is missing a value")
            return self._format_statsd_with_tags(record.name, tags, record.value, "g")
        raise ValueError("Unknown record type.")

    def _streaming_worker(self) -> None:
        """Fetch records from the monitoring queue, and publish them.

        This method loops until the `self._stop_streaming_worker` event is set.
        Intended to run in a subprocess, it fetches messages from the message
        queue in this class, formats the record appropriately, and publishes
        them to whatever output endpoints were specified in the configuration
        passed to this monitoring bus.

        This method won't exit immediately when `self._stop_streaming_worker`
        is set. It may be waiting to fetch a message from the queue, which
        blocks for up to a second. It also needs to do some cleanup of the
        output endpoints once looping has finished.

        """

        def __streaming_worker(end_points: List[MonitoringEndpoint]) -> bool:
            """Get a LogRecord or a MetricRecord, and publish it.

            This function fetches the next record from the internal queue,
            formats it in the configured output style, and writes it to
            the endpoints provided in `end_points`.

            If there is no record available within 1 second, or the record
            received wasn't a LogRecord or MetricRecord protobuf message,
            then this function returns False. Otherwise this returns True
            if publishing was successful (an exception will be raised if
            publishing goes wrong for some reason).

            Args:
                end_points (List): The list of output endpoints to write
                    formatted records to.

            Returns:
                bool, indicating whether or not a record was written.

            """
            try:
                record = self.__message_queue.get(timeout=1)
            except Empty:
                return False

            message = BusMessage()
            message.sequence_number = self.__sequence_number

            if record.DESCRIPTOR is LogRecord.DESCRIPTOR:
                message.log_record.CopyFrom(record)

            elif record.DESCRIPTOR is MetricRecord.DESCRIPTOR:
                message.metric_record.CopyFrom(record)

            else:
                return False

            if self.__json_output:
                blob_message = json_format.MessageToJson(message).encode()

                for end_point in end_points:
                    end_point.write(blob_message)

            elif self.__statsd_output:
                if record.DESCRIPTOR is MetricRecord.DESCRIPTOR:
                    statsd_message = self._format_record_as_statsd_string(
                        record)
                    for end_point in end_points:
                        end_point.write(statsd_message.encode())

            else:
                blob_size = ctypes.c_uint32(message.ByteSize())
                blob_message = message.SerializeToString()

                for end_point in end_points:
                    end_point.write(bytes(blob_size))  # type: ignore
                    end_point.write(blob_message)

            return True

        output_writers, output_file = [], None

        async def __client_connected_callback(reader, writer) -> None:
            output_writers.append(writer)

        async def _wait_closed(event, writer):
            try:
                await writer.wait_closed()
            finally:
                event.set()

        self.__event_loop = asyncio.new_event_loop()

        # In good circumstances we stay in the first iteration of this loop forever.
        # The loop exists so that the subprocess is more resilient to temporary
        # failures (e.g. failing to connect to a socket immediately on startup)
        while not self._stop_streaming_worker.is_set():
            try:
                if self.__async_output and self.__output_location:
                    async_done = threading.Event()

                    async def _async_output():
                        await asyncio.start_unix_server(
                            __client_connected_callback, path=self.__output_location,
                            loop=self.__event_loop)

                        while not self._stop_streaming_worker.is_set():
                            try:
                                if __streaming_worker(output_writers):
                                    self.__sequence_number += 1

                                    for writer in output_writers:
                                        await writer.drain()
                            except asyncio.CancelledError:  # pylint: disable=try-except-raise
                                raise
                            except Exception:
                                self._logger.warning(
                                    "Caught exception when publishing metric", exc_info=True)
                        async_done.set()

                    asyncio.ensure_future(_async_output(), loop=self.__event_loop)
                    async_done.wait()

                elif self.__udp_output and self.__output_location:
                    output_writers.append(UdpWrapper(self.__output_location))
                    while not self._stop_streaming_worker.is_set():
                        try:
                            if __streaming_worker(output_writers):
                                self.__sequence_number += 1
                        except Exception:
                            self._logger.warning(
                                "Caught exception when publishing metric", exc_info=True)

                elif self.__output_location:
                    with open(self.__output_location, mode='wb') as output_file:

                        output_writers.append(output_file)

                        while not self._stop_streaming_worker.is_set():
                            try:
                                if __streaming_worker([output_file]):
                                    self.__sequence_number += 1

                                    output_file.flush()
                            except Exception:
                                self._logger.warning(
                                    "Caught exception when publishing metric", exc_info=True)

                elif self.__print_output:
                    output_writers.append(sys.stdout.buffer)

                    while not self._stop_streaming_worker.is_set():
                        try:
                            if __streaming_worker(output_writers):
                                self.__sequence_number += 1
                        except Exception:
                            self._logger.warning(
                                "Caught exception when publishing metric", exc_info=True)
                else:
                    self._logger.error(
                        "Unsupported monitoring configuration, metrics won't be published."
                        f"output_location={self.__output_location}, "
                        f"async_output={self.__async_output}, "
                        f"udp_output={self.__udp_output}",
                        f"print_output={self.__print_output}",
                        exc_info=True
                    )
                    raise InvalidArgumentError("Unsupported monitoring configuration")

            except Exception:
                self._logger.warning(
                    "Caught exception in metrics publisher loop, sleeping for 5s before retrying",
                    exc_info=True)
                time.sleep(5)

        # We exited the publishing loop, which means we've been told to shutdown
        # by the parent process. Clean up the output writers.
        if output_file is not None:
            output_file.close()

        elif output_writers:
            for writer in output_writers:
                writer.close()
                if self.__async_output and self.__output_location:
                    async_closed = threading.Event()
                    asyncio.ensure_future(_wait_closed(async_closed, writer))
                    async_closed.wait()


def setup_monitoring_bus(
        endpoint_type: MonitoringOutputType = MonitoringOutputType.SOCKET,
        endpoint_location: str = None,
        metric_prefix: str = "",
        serialisation_format: MonitoringOutputFormat = MonitoringOutputFormat.STATSD,
        tag_format: StatsDTagFormat = StatsDTagFormat.NONE) -> _MonitoringBus:
    """ Sets up the monitoring bus.

    Throws an error if a non-silent monitoring bus has already been set up. However a silent monitoring bus
    can be overridden by a non-silent one. If a silent monitoring bus attempts to override a non-silent one,
    it is not overridden and the non-silent one is returned"""

    with _MonitoringBus._instance_lock:
        if _MonitoringBus._instance is not None:
            # Don't override an existing monitoring bus with a silent one, and instead return
            # the already configured one
            if endpoint_type == MonitoringOutputType.SILENT:
                return _MonitoringBus._instance
            if not _MonitoringBus._instance.is_silent:
                raise ValueError("A non-silent monitoring bus has already been created and can't be overridden."
                                 "Please ensure that you are not attempting to call setup_monitoring_bus again.")
        # If we currently have a silent monitoring bus, there's no harm in reconfiguring to use a real one.
        _MonitoringBus._instance = _MonitoringBus(
            endpoint_type, endpoint_location, metric_prefix, serialisation_format, tag_format)
        return _MonitoringBus._instance


def get_monitoring_bus() -> _MonitoringBus:
    """ Get the monitoring bus.

        If a monitoring bus does not exist, a silent bus will be created and no metrics will be published. """
    if _MonitoringBus._instance is None:
        logger = logging.getLogger(__name__)
        logger.info(
            "get_monitoring_bus() was called before setup_monitoring_bus; no metrics will be published.")
        return setup_monitoring_bus(MonitoringOutputType.SILENT)
    return _MonitoringBus._instance
