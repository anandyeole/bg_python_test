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


from typing import List

from lark import Lark
from lark.exceptions import LarkError

from buildgrid._exceptions import InvalidArgumentError

from buildgrid.server.operations.filtering import OperationFilter
from buildgrid.server.operations.filtering.interpreter import FilterTreeInterpreter


class FilterParser:
    """
    Utility class primarily used to parse a filter string and return a list a OperationFilters.
    """

    lark_parser = Lark.open("filter_grammar.lark", rel_to=__file__, start="filter_phrase")

    @staticmethod
    def parse_listoperations_filters(filter_string: str) -> List[OperationFilter]:
        """ Separate the lowercase filter string into individual components, and return a map containing
        the relevant filters to use.

        Filter strings take the following form: key1=value1&key2=value2,value3&key3=value4
        All spaces in the input are ignored.

        Filter options are separated with ampersands (&).
        If a key is repeated, the combined set of values are treated as a logical conjunction (AND).

        """
        operation_filters: List[OperationFilter] = []

        # Handle empty input separately to simplify grammar
        if not filter_string.strip():
            return operation_filters

        try:
            filter_phrase_tree = FilterParser.lark_parser.parse(filter_string)
        except LarkError as e:
            raise InvalidArgumentError(f"Error parsing filter string. See docs for correct filter string syntax. "
                                       f"Parser error follows: [{str(e)}]")

        try:
            operation_filters = FilterTreeInterpreter().visit(filter_phrase_tree)
        except InvalidArgumentError as e:
            raise InvalidArgumentError(
                f"Invalid filter string [{filter_string}]. "
                "See docs for correct filter string syntax and values.") from e

        return operation_filters
