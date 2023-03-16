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

"""Add a deleted flag for Indexed CAS entries

Revision ID: 69ceb5938c06
Revises: e287a533bbd7
Create Date: 2020-03-31 13:45:29.641272

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '69ceb5938c06'
down_revision = 'e287a533bbd7'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('index') as batch_op:
        batch_op.add_column(sa.Column('deleted', sa.Boolean(), nullable=False, server_default=sa.false()))


def downgrade():
    op.drop_column('index', 'deleted')
