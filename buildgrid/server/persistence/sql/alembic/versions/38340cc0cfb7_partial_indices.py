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

"""partial_indices

Revision ID: 38340cc0cfb7
Revises: 1553978c6e69
Create Date: 2020-10-20 08:56:42.559227

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '38340cc0cfb7'
down_revision = '1553978c6e69'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    dialect = op.get_context().bind.dialect.name
    if dialect == "postgresql":
        op.create_index('ix_worker_start_timestamp',
                        'jobs',
                        ['worker_start_timestamp'],
                        unique=False,
                        postgresql_where=sa.text('worker_start_timestamp IS NOT NULL'))
        op.create_index('ix_worker_completed_timestamp',
                        'jobs',
                        ['worker_completed_timestamp'],
                        unique=False,
                        postgresql_where=sa.text('worker_completed_timestamp IS NOT NULL'))
    elif dialect == "sqlite":
        op.create_index('ix_worker_start_timestamp',
                        'jobs',
                        ['worker_start_timestamp'],
                        unique=False,
                        sqlite_where=sa.text('worker_start_timestamp IS NOT NULL'))
        op.create_index('ix_worker_completed_timestamp',
                        'jobs',
                        ['worker_completed_timestamp'],
                        unique=False,
                        sqlite_where=sa.text('worker_completed_timestamp IS NOT NULL'))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index('ix_worker_start_timestamp', table_name='jobs')
    op.drop_index('ix_worker_completed_timestamp', table_name='jobs')
    # ### end Alembic commands ###