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

"""Use LargeBinary to store Action message

Revision ID: 8d910c8de8b6
Revises: 2b49634f4459
Create Date: 2020-11-26 16:40:52.682830

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '8d910c8de8b6'
down_revision = '2b49634f4459'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('jobs') as batch_op:
        # Drop this column entirely rather than trying to migrate the content,
        # since anything there is likely to not be usefully recoverable.
        batch_op.drop_column('action')
        # `sa.LargeBinary` selects the actual column type based on the database
        # backend's preference for storing binary data.
        batch_op.add_column(sa.Column('action', sa.LargeBinary(), nullable=True))


def downgrade():
    with op.batch_alter_table('jobs') as batch_op:
        batch_op.drop_column('action')
        batch_op.add_column(sa.Column('action', sa.String(), nullable=True))
