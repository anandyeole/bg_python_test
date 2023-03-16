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
from typing import List

from lark import Tree
from lark.visitors import Interpreter

from buildgrid._exceptions import InvalidArgumentError
from buildgrid.server.operations.filtering import OperationFilter
from buildgrid.server.operations.filtering.sanitizer import (
    DatetimeValueSanitizer,
    OperationStageValueSanitizer,
    RegexValueSanitizer,
    SortKeyValueSanitizer
)


# Valid operation filters mapped to regexes representing valid values.
VALID_OPERATION_FILTERS = {
    "stage": OperationStageValueSanitizer("stage"),

    # The operation name can technically be parsed as a UUID4, but this
    # parses it as an arbitrary string in case the naming scheme changes
    # in the future
    "name": RegexValueSanitizer("name", r"\S+"),

    # The request metadata is all arbitrary strings
    "invocation_id": RegexValueSanitizer("invocation_id", r".+"),
    "correlated_invocations_id": RegexValueSanitizer("correlated_invocations_id", r".+"),
    "tool_name": RegexValueSanitizer("tool_name", r".+"),
    "tool_version": RegexValueSanitizer("tool_version", r".+"),

    # Validate timestamps with a special sanitizer
    "queued_time": DatetimeValueSanitizer("queued_time"),
    "start_time": DatetimeValueSanitizer("start_time"),
    "completed_time": DatetimeValueSanitizer("completed_time"),

    # Backends determine what sort orders are acceptable
    "sort_order": SortKeyValueSanitizer("sort_order")
}


OPERATOR_MAP = {
    "=": operator.eq,
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
    "!=": operator.ne
}


class FilterTreeInterpreter(Interpreter):
    """ Interpreter for the parse tree.

    Calling FilterTreeInterpreter().visit(tree) walks the parse tree and
    outputs a list of OperationFilters. """
    def filter_phrase(self, tree: Tree) -> List[OperationFilter]:
        return self.visit_children(tree)

    def filter_elem(self, tree: Tree) -> OperationFilter:
        try:
            token_map = {token.type: str(token) for token in tree.children}  # type: ignore

            # Check that the parameter is valid
            parameter = token_map["PARAMETER"]
            if parameter not in VALID_OPERATION_FILTERS:
                raise InvalidArgumentError(f"Invalid filter parameter [{parameter}].")

            # Sanitize the value
            sanitizer = VALID_OPERATION_FILTERS[parameter]
            sanitized_value = sanitizer.sanitize(token_map["VALUE"])

            return OperationFilter(
                parameter=token_map["PARAMETER"],
                operator=OPERATOR_MAP[token_map["OPERATOR"]],
                value=sanitized_value
            )
        except KeyError as e:
            raise InvalidArgumentError(f"Invalid filter element. Token map: {token_map}") from e
