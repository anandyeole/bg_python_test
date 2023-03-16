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

"""digest_size_bytes to bigint

Revision ID: c64c104b2c8b
Revises: 29e88b3e0d0a
Create Date: 2021-10-18 15:02:56.155398

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c64c104b2c8b'
down_revision = '29e88b3e0d0a'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('index') as batch_op:
        batch_op.alter_column('digest_size_bytes', type_=sa.BigInteger())


def downgrade():
    with op.batch_alter_table('index') as batch_op:
        batch_op.alter_column('digest_size_bytes', type_=sa.Integer())
