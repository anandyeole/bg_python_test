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


import operator
from typing import Any, Callable, NamedTuple
from buildgrid._enums import OperationStage


OperationFilter = NamedTuple(
    "OperationFilter",
    [
        ("parameter", str),
        ("operator", Callable[[Any, Any], Any]),
        ("value", Any)
    ]
)


DEFAULT_OPERATION_FILTERS = [
    OperationFilter(
        parameter="stage",
        operator=operator.ne,
        value=OperationStage.COMPLETED.value
    )
]

SortKey = NamedTuple(
    "SortKey",
    [
        ("name", str),
        ("descending", bool)
    ]
)

DEFAULT_SORT_KEYS = [
    SortKey(
        name="queued_time",
        descending=False
    ),
    SortKey(
        name="name",
        descending=False
    )
]
