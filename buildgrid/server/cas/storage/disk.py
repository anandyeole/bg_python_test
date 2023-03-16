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
DiskStorage
==================

A CAS storage provider that stores files as blobs on disk.
"""

import errno
import logging
import os
import tempfile
import io

from buildgrid._exceptions import StorageFullError
from .storage_abc import StorageABC


class DiskStorage(StorageABC):

    def __init__(self, path):
        self.__logger = logging.getLogger(__name__)

        if not os.path.isabs(path):
            self.__root_path = os.path.abspath(path)
        else:
            self.__root_path = path
        self.__cas_path = os.path.join(self.__root_path, 'cas')

        self.objects_path = os.path.join(self.__cas_path, 'objects')
        self.temp_path = os.path.join(self.__root_path, 'tmp')

        os.makedirs(self.objects_path, exist_ok=True)
        os.makedirs(self.temp_path, exist_ok=True)

    def has_blob(self, digest):
        self.__logger.debug(f"Checking for blob: [{digest}]")
        return os.path.exists(self._get_object_path(digest))

    def get_blob(self, digest):
        self.__logger.debug(f"Getting blob: [{digest}]")
        try:
            # pylint: disable=consider-using-with
            f = open(self._get_object_path(digest), 'rb')
            return io.BufferedReader(f)
        except FileNotFoundError:
            return None

    def bulk_read_blobs(self, digests):
        self.__logger.debug(f"Getting {len(digests)} blobs")
        blobmap = {}
        for digest in digests:
            blob = self.get_blob(digest)
            if blob is not None:
                # Bulk read is for a potentially very large number of
                # small blobs. The total size of all blobs is limited by
                # the gRPC message size. Immediately read each blob to
                # avoid hitting open file limits.
                blobmap[digest.hash] = io.BytesIO(blob.read())
                blob.close()
        return blobmap

    def delete_blob(self, digest):
        self.__logger.debug(f"Deleting blob: [{digest}]")
        try:
            os.remove(self._get_object_path(digest))
        except OSError:
            pass

    def begin_write(self, digest):
        # pylint: disable=consider-using-with
        return tempfile.NamedTemporaryFile("wb", dir=self.temp_path)

    def commit_write(self, digest, write_session):
        self.__logger.debug(f"Writing blob: [{digest}]")
        object_path = self._get_object_path(digest)

        try:
            os.makedirs(os.path.dirname(object_path), exist_ok=True)
            os.link(write_session.name, object_path)
        except FileExistsError:
            # Object is already there!
            pass
        except OSError as e:
            # Not enough space error or file too large
            if e.errno in [errno.ENOSPC, errno.EFBIG]:
                raise StorageFullError(f"Disk Error: {e.errno}") from e
            raise e
        finally:
            write_session.close()

    def _get_object_path(self, digest):
        return os.path.join(self.objects_path, digest.hash[:2], digest.hash[2:])
