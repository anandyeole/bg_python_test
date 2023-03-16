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

"""Add stream resource names to jobs

Revision ID: 1553978c6e69
Revises: bbb77b202e31
Create Date: 2020-07-07 16:11:37.840129

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1553978c6e69'
down_revision = 'bbb77b202e31'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('jobs', sa.Column('stdout_stream_name', sa.String(), nullable=True))
    op.add_column('jobs', sa.Column('stdout_stream_write_name', sa.String(), nullable=True))
    op.add_column('jobs', sa.Column('stderr_stream_name', sa.String(), nullable=True))
    op.add_column('jobs', sa.Column('stderr_stream_write_name', sa.String(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('jobs', 'stdout_stream_write_name')
    op.drop_column('jobs', 'stdout_stream_name')
    op.drop_column('jobs', 'stderr_stream_write_name')
    op.drop_column('jobs', 'stderr_stream_name')
    # ### end Alembic commands ###
