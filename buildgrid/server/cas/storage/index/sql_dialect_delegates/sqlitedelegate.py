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

"""
SQLiteDelegate
==================

Extra functionality for the SQL index when using a SQLite backend.

"""
from datetime import datetime
from typing import List, Optional, Tuple

from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm.session import Session as SessionType
from sqlalchemy.sql.functions import coalesce

from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import Digest
from buildgrid.server.persistence.sql.models import IndexEntry


class SQLiteDelegate:

    @staticmethod
    def _save_digests_to_index(digest_blob_pairs: List[Tuple[Digest, Optional[bytes]]],
                               session: SessionType, max_inline_blob_size: int) -> None:
        index_table = IndexEntry.__table__
        update_time = datetime.utcnow()
        new_rows = [{
            'digest_hash': digest.hash,
            'digest_size_bytes': digest.size_bytes,
            'accessed_timestamp': update_time,
            'inline_blob': (blob if digest.size_bytes <= max_inline_blob_size else None)
        } for (digest, blob) in digest_blob_pairs]

        base_insert_stmt = insert(index_table).values(new_rows)

        update_stmt = base_insert_stmt.on_conflict_do_update(
            index_elements=['digest_hash'],
            set_={
                'accessed_timestamp': update_time,
                'inline_blob': coalesce(base_insert_stmt.excluded.inline_blob, index_table.c.inline_blob)
            }
        )

        session.execute(update_stmt)
