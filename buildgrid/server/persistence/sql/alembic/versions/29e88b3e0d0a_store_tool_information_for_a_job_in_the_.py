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

"""Store tool information for a job in the database


Revision ID: 29e88b3e0d0a
Revises: 8d910c8de8b6
Create Date: 2020-10-16 14:35:49.304924

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '29e88b3e0d0a'
down_revision = '8d910c8de8b6'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('operations', sa.Column('correlated_invocations_id', sa.String(), nullable=True))
    op.add_column('operations', sa.Column('invocation_id', sa.String(), nullable=True))
    op.add_column('operations', sa.Column('tool_name', sa.String(), nullable=True))
    op.add_column('operations', sa.Column('tool_version', sa.String(), nullable=True))


def downgrade():
    op.drop_column('operations', 'tool_version')
    op.drop_column('operations', 'tool_name')
    op.drop_column('operations', 'invocation_id')
    op.drop_column('operations', 'correlated_invocations_id')
