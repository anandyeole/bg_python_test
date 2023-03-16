# Copyright (C) 2020 Bloomberg LP
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
import logging
import signal
from datetime import datetime
import time

from buildgrid.server.cas.storage.with_cache import WithCacheStorage
from buildgrid.server.monitoring import (
    get_monitoring_bus,
    setup_monitoring_bus
)
from buildgrid.server.metrics_names import (
    CLEANUP_BLOBS_DELETION_RATE_METRIC_NAME,
    CLEANUP_BYTES_DELETION_RATE_METRIC_NAME,
    CLEANUP_INDEX_BULK_DELETE_METRIC_NAME,
    CLEANUP_INDEX_MARK_DELETED_METRIC_NAME,
    CLEANUP_RUNTIME_METRIC_NAME,
    CLEANUP_STORAGE_BULK_DELETE_METRIC_NAME,
    CLEANUP_STORAGE_DELETION_FAILURES_METRIC_NAME
)
from buildgrid.server.metrics_utils import (
    publish_counter_metric,
    publish_gauge_metric,
    DurationMetric
)


class CASCleanUp:
    """Creates a LRU CAS cleanup service."""

    def __init__(self, dry_run, high_watermark, low_watermark, sleep_interval, batch_size,
                 only_if_unused_for, storages, indexes, monitor, mon_endpoint_type=None,
                 mon_endpoint_location=None, mon_serialisation_format=None, mon_metric_prefix=None):

        self._logger = logging.getLogger(__name__)

        self._dry_run = dry_run

        self._high_watermark = high_watermark
        self._low_watermark = low_watermark
        self._batch_size = batch_size
        self._only_if_unused_for = only_if_unused_for

        self._storages = storages
        self._indexes = indexes

        self._is_instrumented = monitor

        self._sleep_interval = sleep_interval

        self._main_loop = asyncio.get_event_loop()

        if self._is_instrumented:
            setup_monitoring_bus(
                endpoint_type=mon_endpoint_type,
                endpoint_location=mon_endpoint_location,
                metric_prefix=mon_metric_prefix,
                serialisation_format=mon_serialisation_format)

    # --- Public API ---

    def start(self, *, on_server_start_cb=None):
        """ Start cleanup service """
        self._main_loop.add_signal_handler(signal.SIGTERM, self.stop)
        if self._is_instrumented:
            monitoring_bus = get_monitoring_bus()
            monitoring_bus.start()

        # Retry on errors
        attempts = 0
        while True:
            tasks = []
            for instance_name in self._storages:
                if self._storages[instance_name].is_cleanup_enabled():
                    if self._dry_run:
                        self._calculate_cleanup(instance_name)
                    else:
                        tasks.append(self._cleanupWorker(instance_name))
                else:
                    self._logger.info(f"CleanUp for instance '{instance_name}' skipped.")
            if not self._dry_run:
                fut = self._gather_tasks(*tasks)
                try:
                    self._main_loop.run_until_complete(fut)
                except asyncio.CancelledError:
                    self._logger.info("Cleanup task cancelled, exiting")
                except Exception as e:
                    self._logger.exception(e)
                    sleep_time = 1.6 ** attempts
                    # Exponential backoff before retrying
                    self._logger.info(f"Retrying Cleanup in {sleep_time} seconds")
                    time.sleep(sleep_time)
                    attempts += 1
                    continue
            # Only retry on errors
            break

    def stop(self):
        """ Stops the cleanup service """
        self._logger.info("Stopping Cleanup Service")
        if self._is_instrumented:
            monitoring_bus = get_monitoring_bus()
            monitoring_bus.stop()
        if not self._dry_run:
            tasks = [t for t in asyncio.all_tasks(self._main_loop) if t is not asyncio.current_task(self._main_loop)]
            for task in tasks:
                task.cancel()

    # --- Private API ---

    async def _gather_tasks(self, *tasks):
        await asyncio.gather(*tasks)

    def _calculate_cleanup(self, instance_name):
        """Work out which blobs will be deleted by the cleanup command.

        Args:
            instance_name (str): The instance to do a cleanup dry-run for.

        """
        self._logger.info(f"Cleanup dry run for instance '{instance_name}'")
        index = self._indexes[instance_name]
        only_delete_before = self._get_last_accessed_threshold()
        total_size = index.get_total_size()
        self._logger.info(
            f"CAS size is {total_size} bytes, compared with a high water mark of "
            f"{self._high_watermark} bytes and a low water mark of {self._low_watermark} bytes.")
        if total_size >= self._high_watermark:
            required_space = total_size - self._low_watermark
            digests = index.mark_n_bytes_as_deleted(required_space, self._dry_run,
                                                    protect_blobs_after=only_delete_before)
            cleared_space = sum(digest.size_bytes for digest in digests)
            self._logger.info(
                f"{len(digests)} digests will be deleted, freeing up {cleared_space} bytes.")
        else:
            self._logger.info(
                f"Total size {total_size} is less than the high water mark, "
                f"nothing will be deleted.")

    def _do_cleanup_batch(self, instance_name, index, storage, only_delete_before, total_size,
                          renew_windows=True):
        batch_start_time = time.time()

        # Mark a batch of index entries for deletion
        with DurationMetric(CLEANUP_INDEX_MARK_DELETED_METRIC_NAME,
                            instance_name,
                            instanced=True):
            # If renew_windows==True, an expensive SQL will run to construct LRU windows.
            # Otherwise the previously constructed LRU windows will be used for speed up.
            digests = index.mark_n_bytes_as_deleted(self._batch_size,
                                                    protect_blobs_after=only_delete_before,
                                                    renew_windows=renew_windows)

        if not digests and total_size >= self._high_watermark:
            self._logger.error(f"Marked 0 digests for deletion, even though cleanup was triggered. "
                               f"This may be because the remaining digests have been accessed within "
                               f"{only_delete_before}. Total size still remains greater than high watermark!")
        elif not digests:
            self._logger.warning(f"Marked 0 digests for deletion, even though cleanup was triggered. "
                                 f"This may be because the remaining digests have been accessed within "
                                 f"{only_delete_before}.")
        else:
            self._logger.info(f"Marked {len(digests)} digests for deletion in the index")

            # Bulk delete the marked blobs from the actual storage backend
            with DurationMetric(CLEANUP_STORAGE_BULK_DELETE_METRIC_NAME,
                                instance_name,
                                instanced=True):
                failed_deletions = storage.bulk_delete(digests)
            self._logger.info(f"Requested bulk deletion of {len(digests)} digests from storage")

            if failed_deletions:
                # Separately handle blobs which failed to be deleted and blobs that
                # were already missing from the storage
                if self._is_instrumented:
                    publish_counter_metric(
                        CLEANUP_STORAGE_DELETION_FAILURES_METRIC_NAME,
                        len(failed_deletions),
                        {"instance-name": instance_name}
                    )

                self._logger.info(f"Failed to delete {len(failed_deletions)} blobs.")
                for failure in failed_deletions:
                    self._logger.debug(f"Failed to delete {failure}.")

                digests_to_delete = [
                    digest for digest in digests
                    if digest.hash not in failed_deletions
                ]
            else:
                if failed_deletions is None:
                    self._logger.error("Calling bulk_delete on storage returned 'None' instead of a list")
                digests_to_delete = digests

            # Bulk delete the entries for the successfully deleted (and already missing)
            # blobs from the storage
            with DurationMetric(CLEANUP_INDEX_BULK_DELETE_METRIC_NAME,
                                instance_name,
                                instanced=True):
                index_failed_deletions = index.bulk_delete(digests_to_delete)
            self._logger.info(f"Bulk deleted {len(digests_to_delete)} digests from the index")

            if index_failed_deletions is None:
                self._logger.error("Calling bulk_delete on the index returned 'None' instead of a list")

            if self._is_instrumented:
                batch_duration = time.time() - batch_start_time
                blobs_deleted_per_second = len(digests_to_delete) / batch_duration
                publish_gauge_metric(
                    CLEANUP_BLOBS_DELETION_RATE_METRIC_NAME,
                    blobs_deleted_per_second,
                    {"instance-name": instance_name}
                )

                bytes_deleted = sum(digest.size_bytes for digest in digests_to_delete)
                bytes_deleted_per_second = bytes_deleted / batch_duration
                publish_gauge_metric(
                    CLEANUP_BYTES_DELETION_RATE_METRIC_NAME,
                    bytes_deleted_per_second,
                    {"instance-name": instance_name}
                )

    async def _cleanupWorker(self, instance_name):
        """ Cleanup when full """
        storage = self._storages[instance_name]
        if isinstance(storage, WithCacheStorage):
            self._logger.warning("Cleaning up a WithCache storage will not cleanup local cache entries "
                                 "running with a total cache size larger than the configured low watermark "
                                 "may result in inconsistent cache state.")
        index = self._indexes[instance_name]
        index.set_instance_name(instance_name)
        storage.set_instance_name(instance_name)
        self._logger.info(f"Cleanup for instance '{instance_name}' started.")
        while True:
            total_size = index.get_total_size()
            if total_size >= self._high_watermark:
                to_delete = total_size - self._low_watermark
                self._logger.info(
                    f"CAS size for instance '{instance_name}' is {total_size} bytes, at least "
                    f"{to_delete} bytes will be cleared.")
                self._logger.info(f"Deleting items from storage/index for instance '{instance_name}'.")

                with DurationMetric(CLEANUP_RUNTIME_METRIC_NAME,
                                    instance_name,
                                    instanced=True):
                    renew_windows = True
                    while total_size > self._low_watermark:
                        only_delete_before = self._get_last_accessed_threshold()
                        await self._main_loop.run_in_executor(None, self._do_cleanup_batch,
                                                              instance_name, index,
                                                              storage, only_delete_before,
                                                              total_size, renew_windows)
                        renew_windows = False
                        total_size = index.get_total_size()
                        self._logger.info(f"After cleanup batch total CAS size is {total_size} bytes.")

                self._logger.info(f"Finished cleanup. CAS size is now {total_size} bytes.")
            await asyncio.sleep(self._sleep_interval)

    def _get_last_accessed_threshold(self):
        return datetime.utcnow() - self._only_if_unused_for
