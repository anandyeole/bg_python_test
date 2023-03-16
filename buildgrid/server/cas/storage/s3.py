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
S3Storage
==================

A storage provider that stores data in an Amazon S3 bucket.
"""

import io
import logging
from typing import Dict, List
from tempfile import TemporaryFile

import boto3
from botocore.exceptions import ClientError

from buildgrid._exceptions import StorageFullError
from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import Digest
from buildgrid._protos.google.rpc.status_pb2 import Status
from buildgrid._protos.google.rpc import code_pb2
from buildgrid.server.metrics_names import S3_DELETE_ERROR_CHECK_METRIC_NAME
from buildgrid.server.metrics_utils import DurationMetric
from buildgrid.server.s3 import s3utils
from buildgrid.settings import (
    HASH,
    MAX_IN_MEMORY_BLOB_SIZE_BYTES,
    S3_MAX_UPLOAD_SIZE
)
from .storage_abc import StorageABC


class S3Storage(StorageABC):

    def __init__(self, bucket, page_size=1000, **kwargs):
        self.__logger = logging.getLogger(__name__)

        self._bucket_template = bucket
        self._page_size = page_size

        # Boto logs can be very verbose, restrict to WARNING
        for boto_logger_name in [
                'boto3', 'botocore',
                's3transfer', 'urllib3'
        ]:
            boto_logger = logging.getLogger(boto_logger_name)
            boto_logger.setLevel(max(boto_logger.level, logging.WARNING))

        self._s3 = boto3.client('s3', **kwargs)

        self._instance_name = None

    def _get_bucket_name(self, digest):
        try:
            return self._bucket_template.format(digest=digest)
        except IndexError:
            self.__logger.error(f"Could not calculate bucket name for digest=[{digest}]. This "
                                "is either a misconfiguration in the BuildGrid S3 bucket "
                                "configuration, or a badly formed request.")
            raise

    def _construct_key(self, digest):
        return digest.hash + '_' + str(digest.size_bytes)

    def _get_s3object(self, digest):
        return s3utils.S3Object(self._get_bucket_name(digest.hash), self._construct_key(digest))

    def _deconstruct_key(self, key):
        parts = key.split('_')
        size_bytes = int(parts[-1])
        # This isn't as simple as just "the first part of the split" because
        # the hash part of the key itself might contain an underscore.
        digest_hash = '_'.join(parts[0:-1])
        return digest_hash, size_bytes

    def _multi_delete_blobs(self, bucket_name, digests):
        response = self._s3.delete_objects(Bucket=bucket_name, Delete={'Objects': digests})
        return_failed = []
        failed_deletions = response.get('Errors', [])
        with DurationMetric(S3_DELETE_ERROR_CHECK_METRIC_NAME,
                            self._instance_name,
                            instanced=True):
            for failed_key in failed_deletions:
                digest_hash, size_bytes = self._deconstruct_key(failed_key['Key'])
                return_failed.append(f'{digest_hash}/{size_bytes}')
        return return_failed

    def has_blob(self, digest):
        self.__logger.debug(f"Checking for blob: [{digest}]")
        try:
            s3utils.head_object(self._s3, self._get_s3object(digest))
        except ClientError as e:
            if e.response['Error']['Code'] not in ['404', 'NoSuchKey']:
                raise
            return False
        return True

    def get_blob(self, digest):
        self.__logger.debug(f"Getting blob: [{digest}]")
        try:
            s3object = self._get_s3object(digest)

            if digest.size_bytes > MAX_IN_MEMORY_BLOB_SIZE_BYTES:
                # To avoid storing the whole file in memory, download to a
                # temporary file.
                ret = TemporaryFile()  # pylint: disable=consider-using-with
            else:
                # But, to maximize performance, keep blobs that are small
                # enough in-memory.
                ret = io.BytesIO()

            s3object.fileobj = ret
            s3utils.get_object(self._s3, s3object)
            ret.seek(0)
            return ret
        except ClientError as e:
            if e.response['Error']['Code'] not in ['404', 'NoSuchKey']:
                raise
            return None

    def delete_blob(self, digest):
        self.__logger.debug(f"Deleting blob: [{digest}]")
        try:
            self._s3.delete_object(Bucket=self._get_bucket_name(digest.hash),
                                   Key=self._construct_key(digest))
        except ClientError as e:
            if e.response['Error']['Code'] not in ['404', 'NoSuchKey']:
                raise

    def bulk_delete(self, digests: List[Digest]) -> List[str]:
        self.__logger.debug(f"Deleting {len(digests)} digests from S3 storage: [{digests}]")
        buckets_to_digest_lists: Dict[str, List[Dict[str, str]]] = {}
        failed_deletions = []
        for digest in digests:
            bucket = self._get_bucket_name(digest.hash)
            if bucket in buckets_to_digest_lists:
                buckets_to_digest_lists[bucket].append({'Key': self._construct_key(digest)})
            else:
                buckets_to_digest_lists[bucket] = [{'Key': self._construct_key(digest)}]
            if len(buckets_to_digest_lists[bucket]) >= self._page_size:
                # delete items for this bucket, hit page limit
                failed_deletions += self._multi_delete_blobs(bucket,
                                                             buckets_to_digest_lists.pop(bucket))
        # flush remaining items
        for bucket, digest_list in buckets_to_digest_lists.items():
            failed_deletions += self._multi_delete_blobs(bucket, digest_list)
        return failed_deletions

    def begin_write(self, digest):
        if digest.size_bytes > MAX_IN_MEMORY_BLOB_SIZE_BYTES:
            # To avoid storing the whole file in memory, upload to a
            # temporary file.
            write_session = TemporaryFile()  # pylint: disable=consider-using-with
        else:
            # But, to maximize performance, keep blobs that are small
            # enough in-memory.
            write_session = io.BytesIO()
        return write_session

    def commit_write(self, digest, write_session):
        self.__logger.debug(f"Writing blob: [{digest}]")
        write_session.seek(0)
        try:
            s3object = self._get_s3object(digest)
            s3object.fileobj = write_session
            s3object.filesize = digest.size_bytes
            if digest.size_bytes <= S3_MAX_UPLOAD_SIZE:
                s3utils.put_object(self._s3, s3object)
            else:
                s3utils.multipart_upload(self._s3, s3object)
        except ClientError as error:
            if error.response['Error']['Code'] == 'QuotaExceededException':
                raise StorageFullError("S3 Quota Exceeded.") from error
            raise error
        finally:
            write_session.close()

    def is_cleanup_enabled(self):
        return True

    def missing_blobs(self, digests):
        result = []
        s3objects = []
        for digest in digests:
            s3object = self._get_s3object(digest)
            s3objects.append(s3object)
        s3utils.head_objects(self._s3, s3objects)
        for digest, s3object in zip(digests, s3objects):
            if s3object.error is not None:
                result.append(digest)
        return result

    def bulk_update_blobs(self, blobs):
        s3object_status_list = []
        s3objects = []
        for digest, data in blobs:
            if len(data) != digest.size_bytes or HASH(data).hexdigest() != digest.hash:
                status = Status(
                    code=code_pb2.INVALID_ARGUMENT,
                    message="Data doesn't match hash",
                )
                s3object_status_list.append((None, status))
            else:
                write_session = self.begin_write(digest)
                write_session.write(data)
                write_session.seek(0)
                s3object = self._get_s3object(digest)
                s3object.fileobj = write_session
                s3object.filesize = digest.size_bytes
                s3objects.append(s3object)
                s3object_status_list.append((s3object, None))

        s3utils.put_objects(self._s3, s3objects)

        result = []
        for s3object, status in s3object_status_list:
            if status is not None:
                # Failed check before S3 object creation
                result.append(status)
            elif s3object.error is None:
                # PUT was successful
                result.append(Status(code=code_pb2.OK))
            else:
                result.append(Status(code=code_pb2.UNKNOWN, message=str(s3object.error)))
        return result

    def bulk_read_blobs(self, digests):
        s3objects = []
        blobmap = {}
        for digest in digests:
            s3object = self._get_s3object(digest)
            s3object.fileobj = io.BytesIO()
            s3objects.append(s3object)

        s3utils.get_objects(self._s3, s3objects)

        for digest, s3object in zip(digests, s3objects):
            if s3object.error is None:
                s3object.fileobj.seek(0)
                blobmap[digest.hash] = s3object.fileobj
            elif s3object.status_code != 404:
                raise s3object.error
        return blobmap
