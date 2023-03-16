# Copyright (C) 2021 Bloomberg LP
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


from collections import namedtuple
from enum import Enum

from pika.exchange_type import ExchangeType  # type: ignore


Exchange = namedtuple('Exchange', ('name', 'type'))


class Exchanges(Enum):
    BOT_STATUS = Exchange(name='bot-status', type=ExchangeType.direct)
    JOB_CANCELLATION = Exchange(name='job-cancellation', type=ExchangeType.fanout)
    OPERATION_UPDATES = Exchange(name='operation-updates', type=ExchangeType.topic)
    JOBS = Exchange(name='jobs', type=ExchangeType.direct)
