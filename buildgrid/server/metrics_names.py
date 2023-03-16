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

#
# CAS metrics
#

#: Number of exceptions thrown from CAS servicer functions
CAS_EXCEPTION_COUNT_METRIC_NAME = 'cas-exception'

#: Number of bytes uploaded to a CAS instance
CAS_UPLOADED_BYTES_METRIC_NAME = 'cas-uploaded-bytes'

#: Number of bytes downloaded from a CAS instance
CAS_DOWNLOADED_BYTES_METRIC_NAME = 'cas-downloaded-bytes'

#: Number of blobs requested in ``FindMissingBlobs()`` calls
CAS_FIND_MISSING_BLOBS_NUM_REQUESTED_METRIC_NAME = 'find-missing-blobs-num-requested'

#: Size of blobs requested in ``FindMissingBlobs()`` calls
CAS_FIND_MISSING_BLOBS_SIZE_BYTES_REQUESTED_METRIC_NAME = 'find-missing-blobs-size-bytes-requested'

#: Number of blobs reported to be missing in ``FindMissingBlobs()`` calls
CAS_FIND_MISSING_BLOBS_NUM_MISSING_METRIC_NAME = 'find-missing-blobs-num-missing'

#: Percentage of blobs reported to be missing in ``FindMissingBlobs()`` calls
CAS_FIND_MISSING_BLOBS_PERCENT_MISSING_METRIC_NAME = 'find-missing-blobs-percent-missing'

#: Size of blobs reported to be missing in ``FindMissingBlobs()`` calls
CAS_FIND_MISSING_BLOBS_SIZE_BYTES_MISSING_METRIC_NAME = 'find-missing-blobs-size-bytes-missing'

#: Time that ``FindMissingBlobs()`` operations took to complete
CAS_FIND_MISSING_BLOBS_TIME_METRIC_NAME = 'find-missing-blobs'

#: Time that ``BatchUpdateBlobs()`` operations took to complete
CAS_BATCH_UPDATE_BLOBS_TIME_METRIC_NAME = 'batch-update-blobs'

#: Size of blobs written with ``BatchUpdateBlobs()`` calls
CAS_BATCH_UPDATE_BLOBS_SIZE_BYTES = 'batch-update-blobs-size-bytes'

#: Time that ``BatchReadBlobs()`` operations took to complete
CAS_BATCH_READ_BLOBS_TIME_METRIC_NAME = 'batch-read-blobs'

#: Size of blobs read with ``BatchReadBlobs()`` calls
CAS_BATCH_READ_BLOBS_SIZE_BYTES = 'batch-read-blobs-size-bytes'

#: Time that ``GetTree()`` operations took to complete
CAS_GET_TREE_TIME_METRIC_NAME = 'get-tree'

#: Time that ``ByteStream.Read()`` operations took to complete
CAS_BYTESTREAM_READ_TIME_METRIC_NAME = 'bytestream-read'

#: Size of blobs read with ``ByteStream.Read()``
CAS_BYTESTREAM_READ_SIZE_BYTES = 'bytestream-read-size-bytes'

#: Time that ``ByteStream.Write()`` operations took to complete
CAS_BYTESTREAM_WRITE_TIME_METRIC_NAME = 'bytestream-write'

#: Size of blobs written with ``ByteStream.Write()``
CAS_BYTESTREAM_WRITE_SIZE_BYTES = 'bytestream-write-size-bytes'

# CAS cache wrapper metrics

#: Count of cache misses in BatchReadBlobs requests to the
#  !with-cache-storage. This only counts the blobs which were
#  in the fallback storage; blobs that were entirely missing
#  don't count as cache misses, since this metric is intended
#  to measure how many things that *could* have been cached
#  were actually not.
CAS_CACHE_BULK_READ_MISS_COUNT_NAME = 'cas-withcache-bulk-read-misses'

#: Count of cache hits in BatchReadBlobs requests to the !with-cache-storage
CAS_CACHE_BULK_READ_HIT_COUNT_NAME = 'cas-withcache-bulk-read-hits'

#: Percentage of cache hits in a given BatchReadBlobs request in the
#  !with-cache-storage. This is as a percentage of total blobs requested,
#  including blobs which were missing entirely.
CAS_CACHE_BULK_READ_HIT_PERCENTAGE_NAME = 'cas-withcache-bulk-read-hit-percent'

#: Count of cache misses in ByteStream Read requests to the
#  !with-cache-storage. This only counts the blobs which were
#  in the fallback storage; blobs that were entirely missing
#  don't count as cache misses, since this metric is intended
#  to measure how many things that *could* have been cached
#  were actually not.
CAS_CACHE_GET_BLOB_MISS_COUNT_NAME = 'cas-withcache-get-blob-misses'

#: Count of cache hits in ByteStream Read requests to the !with-cache-storage
CAS_CACHE_GET_BLOB_HIT_COUNT_NAME = 'cas-withcache-get-blob-hits'

# Indexed CAS metrics

#: Time taken to bulk select a number of digests from the index
CAS_INDEX_BULK_SELECT_DIGEST_TIME_METRIC_NAME = 'cas.index.bulk-select-digest-time'

#: Time taken to update a blob timestamp in the index
CAS_INDEX_BLOB_TIMESTAMP_UPDATE_TIME_METRIC_NAME = 'cas.index.blob-timestamp-update-time'

#: Time taken to run a bulk timestamp update in the index
CAS_INDEX_BULK_TIMESTAMP_UPDATE_TIME_METRIC_NAME = 'cas.index.bulk-timestamp-update-time'

#: Time taken to return from `get_blob()`. This includes the time taken to
#  check and update the index, along with to time to fetch the blob from the
#  underlying storage, and update the index if `fallback_on_get` is enabled.
CAS_INDEX_GET_BLOB_TIME_METRIC_NAME = 'cas.index.get-blob-time'

#: Time taken to store a list of digests in the index
CAS_INDEX_SAVE_DIGESTS_TIME_METRIC_NAME = 'cas.index.save-digests-time'

#: Time taken to get the total size of the CAS the index is for
CAS_INDEX_SIZE_CALCULATION_TIME_METRIC_NAME = 'cas.index.total-size-calculation-time'

#
# ActionCache metrics
#

#: Time that ``GetActionResult()`` operations took to complete
AC_GET_ACTION_RESULT_TIME_METRIC_NAME = 'get-action-result'

#: Time that ``UpdateActionResult()`` operations took to complete
AC_UPDATE_ACTION_RESULT_TIME_METRIC_NAME = 'update-action-result'

#: Number of cache hits from the ActionCache
AC_CACHE_HITS_METRIC_NAME = 'action-cache-hits'

#: Number of cache misses from the ActionCache
AC_CACHE_MISSES_METRIC_NAME = 'action-cache-misses'

#: Number of cache hits which became misses due to missing blobs in CAS
AC_UNUSABLE_CACHE_HITS_METRIC_NAME = 'action-cache-hits-with-missing-blobs'

#
# S3 metrics
#

#: Time taken to check errors from a bulk_delete
S3_DELETE_ERROR_CHECK_METRIC_NAME = "s3-deletion-error-check-timer"


#
# Cleanup metrics
#

#: Number of blobs deleted per second in a cleanup batch
CLEANUP_BLOBS_DELETION_RATE_METRIC_NAME = "cleanup.blobs-deleted-per-second"

#: Number of bytes deleted per second in a cleanup batch
CLEANUP_BYTES_DELETION_RATE_METRIC_NAME = "cleanup.bytes-deleted-per-second"

#: Total time taken to clean enough blobs to get the CAS size down to the low watermark
CLEANUP_RUNTIME_METRIC_NAME = "cleanup.runtime-timer"

#: Time taken to bulk delete a set of blobs from the index
CLEANUP_INDEX_BULK_DELETE_METRIC_NAME = "cleanup.index.bulk-delete-timer"

#: Time taken to mark a set of blobs as deleted in the index
CLEANUP_INDEX_MARK_DELETED_METRIC_NAME = "cleanup.index.mark-as-deleted-timer"

#: Number of blobs that were already marked for deletion in the index when marking as deleted
CLEANUP_INDEX_PREMARKED_BLOBS_METRIC_NAME = "cleanup.index.premarked-blobs-count"

#: Time taken to bulk delete a set of blobs from the storage backend
CLEANUP_STORAGE_BULK_DELETE_METRIC_NAME = "cleanup.storage.bulk-delete-timer"

#: Number of blobs that failed to be deleted from the storage backend in a given bulk delete request
CLEANUP_STORAGE_DELETION_FAILURES_METRIC_NAME = "cleanup.storage.deletion-failures-count"


#
# ExecutedActionMetadata metrics
#

#: Time spent queued before being assigned to a worker
QUEUED_TIME_METRIC_NAME = 'action-queued-time'

#: Time spent in the worker (fetching inputs + executing + uploading outputs)
WORKER_HANDLING_TIME_METRIC_NAME = 'worker-handling-time'

#: Time spent fetching inputs before execution
INPUTS_FETCHING_TIME_METRIC_NAME = 'inputs-fetching-time'

#: Time spent waiting for executions to complete
EXECUTION_TIME_METRIC_NAME = 'execution-time'

#: Time spent uploading inputs after execution
OUTPUTS_UPLOADING_TIME_METRIC_NAME = 'outputs-uploading-time'

#: Total time spent servicing an execution request (time queued +fetching inputs +
# executing + uploading outputs)
TOTAL_HANDLING_TIME_METRIC_NAME = 'total-handling-time'


#
# Execution service metrics
#

#: Number of bots connected
BOT_COUNT_METRIC_NAME = 'bots-count'

#: Number of clients connected
CLIENT_COUNT_METRIC_NAME = 'clients-count'

#: Number of leases present in the scheduler
LEASE_COUNT_METRIC_NAME = 'lease-count'

#: Counter metric indicating lease stage transitions
LEASE_CHANGES_COUNTER_METRIC_NAME = 'lease-state-transitions-counter'

#: Number of active jobs in the scheduler
JOB_COUNT_METRIC_NAME = 'job-count'

#: Counter metric indicating job stage transitions
JOB_CHANGES_COUNTER_METRIC_NAME = 'job-stage-transitions-counter'

#: Average time that a job spends waiting to be executed
AVERAGE_QUEUE_TIME_METRIC_NAME = 'average-queue-time'

#: Number of ``Execute()`` requests received:
EXECUTE_REQUEST_COUNT_METRIC_NAME = 'execute-call-count'

#: Time spent servicing ``Execute()`` requests:
EXECUTE_SERVICER_TIME_METRIC_NAME = 'execute-servicing-time'

#: Number of ``WaitExecution()`` requests received:
WAIT_EXECUTION_REQUEST_COUNT_METRIC_NAME = 'wait-execution-call-count'

#: Time spent servicing ``WaitExecution()`` requests:
WAIT_EXECUTION_SERVICER_TIME_METRIC_NAME = 'wait-execution-servicing-time'

#
# LogStream service metrics
#

#: Time spent creating a LogStream
LOGSTREAM_CREATE_LOG_STREAM_TIME_METRIC_NAME = 'logstream.create-logstream-time'

#: Number of bytes in a committed logstream
LOGSTREAM_WRITE_UPLOADED_BYTES_COUNT = 'logstream.write.uploaded-bytes-count'

#
# Authentication Metrics
#

#: Number of invalid JWTs recieved:
INVALID_JWT_COUNT_METRIC_NAME = 'authentication.jwt.invalid-jwt-count'

#: Duration of JWK fetch request:
JWK_FETCH_TIME_METRIC_NAME = 'authentication.jwk.fetch-request-time'

#: Duration of JWT decoding:
JWT_DECODE_TIME_METRIC_NAME = 'authentication.jwt.decode-jwt-time'

#: Duration of JWT validation (can include fetching JWK):
JWT_VALIDATION_TIME_METRIC_NAME = 'authentication.jwt.validate-jwt-time'

#
# Bots service metrics
#

#: Time spent servicing ``CreateBotSession()`` requests
BOTS_CREATE_BOT_SESSION_TIME_METRIC_NAME = 'bots.create-bot-session-time'

#: Time spent servicing ``UpdateBotSession()`` requests
BOTS_UPDATE_BOT_SESSION_TIME_METRIC_NAME = 'bots.update-bot-session-time'

#: Time spent selecting an Action from the data store to create a lease for
BOTS_ASSIGN_JOB_LEASES_TIME_METRIC_NAME = 'bots.assign-job-leases-time'


#
# Scheduler metrics
#

#: Time taken to queue an Action
SCHEDULER_QUEUE_ACTION_TIME_METRIC_NAME = 'scheduler.queue-action-time'

#: Time taken to update a job's Lease
SCHEDULER_UPDATE_LEASE_TIME_METRIC_NAME = 'scheduler.update-lease-time'

#: Time taken to cancel an Operation
SCHEDULER_CANCEL_OPERATION_TIME_METRIC_NAME = 'scheduler.cancel-operation-time'


#
# Data Store (scheduler's backend) metrics
#
# Some of these seem like duplicates of the request-level timers
# at a glance, but measuring at the data store level allows us to
# see how much overhead our own code is adding to the calls.
#

#: Time taken to create a Job
DATA_STORE_CREATE_JOB_TIME_METRIC_NAME = 'datastore.all.create-job-time'

#: Time taken to enqueue a Job
DATA_STORE_QUEUE_JOB_TIME_METRIC_NAME = 'datastore.all.queue-job-time'

#: Time taken to update a Job
DATA_STORE_UPDATE_JOB_TIME_METRIC_NAME = 'datastore.all.update-job-time'

#: Time taken to create a Lease
DATA_STORE_CREATE_LEASE_TIME_METRIC_NAME = 'datastore.all.create-lease-time'

#: Time taken to update a Lease
DATA_STORE_UPDATE_LEASE_TIME_METRIC_NAME = 'datastore.all.update-lease-time'

#: Time taken to create an Operation
DATA_STORE_CREATE_OPERATION_TIME_METRIC_NAME = 'datastore.all.create-operation-time'

#: Time taken to update an Operation
DATA_STORE_UPDATE_OPERATION_TIME_METRIC_NAME = 'datastore.all.update-operation-time'

#: Time taken to get a list of Operations
DATA_STORE_LIST_OPERATIONS_TIME_METRIC_NAME = 'datastore.all.list-operations-time'

#: Time taken to get a Job by Action Digest
DATA_STORE_GET_JOB_BY_DIGEST_TIME_METRIC_NAME = 'datastore.all.get-job-by-digest-time'

#: Time taken to get a Job by name
DATA_STORE_GET_JOB_BY_NAME_TIME_METRIC_NAME = 'datastore.all.get-job-by-name-time'

#: Time taken to get a Job by Operation name
DATA_STORE_GET_JOB_BY_OPERATION_TIME_METRIC_NAME = 'datastore.all.get-job-by-operation-time'

#: Time taken to handle checking for a job update. When using
#  a database backend other than PostgreSQL, this will measure
#  how long it takes to check all watched jobs for updates once.
#  For PostgreSQL and the in-memory scheduler, this measures how
#  long it takes to handle a job update notification.
DATA_STORE_CHECK_FOR_UPDATE_TIME_METRIC_NAME = 'datastore.all.check-for-update-time'

# SQL-specific metrics

#: Time taken to store the ExecuteResponse
DATA_STORE_STORE_RESPONSE_TIME_METRIC_NAME = 'datastore.sql.store-response-time'

#: Number of rows deleted from the jobs table during each pruning
DATA_STORE_PRUNER_NUM_ROWS_DELETED_METRIC_NAME = "datastore.sql.pruner-num-rows-deleted"

#: Time taken per scheduler pruning invocation
DATA_STORE_PRUNER_DELETE_TIME_METRIC_NAME = 'datastore.sql.pruner-delete-time'


#
# Operations service metrics
#

#: Time taken to completely handle a ListOperations request
OPERATIONS_LIST_OPERATIONS_TIME_METRIC_NAME = 'operations.list-operations-time'

#: Time taken to completely handle a GetOperation request
OPERATIONS_GET_OPERATION_TIME_METRIC_NAME = 'operations.get-operation-time'

#: Time taken to completely handle a CancelOperation request
OPERATIONS_CANCEL_OPERATION_TIME_METRIC_NAME = 'operations.cancel-operation-time'

#: Time taken to completely handle a DeleteOperation request. BuildGrid
#  doesn't actually support DeleteOperation, but this metric will at
#  least provide insight into whether people are attempting to call it.
OPERATIONS_DELETE_OPERATION_TIME_METRIC_NAME = 'operations.delete-operation-time'
