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

""" Holds constants and utility functions for the SQL scheduler. """


from datetime import datetime
from distutils.util import strtobool
import operator
from typing import Any, List, Tuple

from sqlalchemy.sql.expression import and_, or_, ClauseElement

from buildgrid._exceptions import InvalidArgumentError
from buildgrid.server.operations.filtering import OperationFilter, SortKey
from buildgrid.server.persistence.sql.models import Job, Operation


DATETIME_FORMAT = "%Y-%m-%d-%H-%M-%S-%f"


LIST_OPERATIONS_PARAMETER_MODEL_MAP = {
    "stage": Job.stage,
    "name": Operation.name,
    "queued_time": Job.queued_timestamp,
    "start_time": Job.worker_start_timestamp,
    "completed_time": Job.worker_completed_timestamp,
    "invocation_id": Operation.invocation_id,
    "correlated_invocations_id": Operation.correlated_invocations_id,
    "tool_name": Operation.tool_name,
    "tool_version": Operation.tool_version
}


def parse_list_operations_sort_value(value: str, column) -> Any:
    """ Convert the string representation of a value to the proper Python type. """
    python_type = column.property.columns[0].type.python_type
    if python_type == datetime:
        return datetime.strptime(value, DATETIME_FORMAT)
    elif python_type == bool:
        # Using this distutils function to cover a few different bool representations
        return bool(strtobool(value))
    else:
        return python_type(value)


def dump_list_operations_token_value(token_value) -> str:
    """ Convert a value to a string for use in the page_token. """
    if isinstance(token_value, datetime):
        return datetime.strftime(token_value, DATETIME_FORMAT)
    else:
        return str(token_value)


def build_pagination_clause_for_sort_key(
        sort_value: Any, previous_sort_values: List[Any], sort_keys: List[SortKey]) -> ClauseElement:
    """ Build part of a filter clause to figure out the starting point of the page given
    by the page_token. See the docstring of build_page_filter for more details. """
    if len(sort_keys) <= len(previous_sort_values):
        raise ValueError("Not enough keys to unpack")

    filter_clause_list = []
    for i, previous_sort_value in enumerate(previous_sort_values):
        previous_sort_col = LIST_OPERATIONS_PARAMETER_MODEL_MAP[sort_keys[i].name]
        filter_clause_list.append(previous_sort_col == previous_sort_value)
    sort_key = sort_keys[len(previous_sort_values)]
    sort_col = LIST_OPERATIONS_PARAMETER_MODEL_MAP[sort_key.name]
    if sort_key.descending:
        filter_clause_list.append(sort_col < sort_value)
    else:
        filter_clause_list.append(sort_col > sort_value)
    return and_(*filter_clause_list)


def build_page_filter(page_token, sort_keys: List[SortKey]) -> ClauseElement:
    """ Build a filter to determine the starting point of the rows to fetch, based
    on the page_token.

    The page_token is directly related to the sort order, and in this way it acts as a
    "cursor." It is given in the format Xval|Yval|Zval|..., where each element is a value
    corresponding to an orderable column in the database. If the corresponding rows are
    X, Y, and Z, then X is the primary sort key, with Y breaking ties between X, and Z
    breaking ties between X and Y. The corresponding filter clause is then:

    (X > Xval) OR (X == XVal AND Y > Yval) OR (X == Xval AND Y == Yval AND Z > Zval) ...
    """
    # The page token is used as a "cursor" to determine the starting point
    # of the rows to fetch. It is derived from the sort keys.
    token_elements = page_token.split("|")
    if len(token_elements) != len(sort_keys):
        # It is possible that an extra "|" was in the input
        # TODO: Handle extra "|"s somehow? Or at least allow escaping them
        raise InvalidArgumentError(
            f"Wrong number of \"|\"-separated elements in page token [{page_token}]. "
            f"Expected {len(sort_keys)}, got {len(token_elements)}.")

    sort_key_clause_list = []
    previous_sort_values: List[Any] = []
    # Build the compound clause for each sort key in the token
    for i, sort_key in enumerate(sort_keys):
        col = LIST_OPERATIONS_PARAMETER_MODEL_MAP[sort_key.name]
        sort_value = parse_list_operations_sort_value(token_elements[i], col)
        filter_clause = build_pagination_clause_for_sort_key(
            sort_value, previous_sort_values, sort_keys)
        sort_key_clause_list.append(filter_clause)
        previous_sort_values.append(sort_value)

    return or_(*sort_key_clause_list)


def build_page_token(operation, sort_keys):
    """ Use the sort keys to build a page token from the given operation. """
    token_values = []
    for sort_key in sort_keys:
        col = LIST_OPERATIONS_PARAMETER_MODEL_MAP[sort_key.name]
        col_properties = col.property.columns[0]
        column_name = col_properties.name
        table_name = col_properties.table.name
        if table_name == "operations":
            token_value = getattr(operation, column_name)
        elif table_name == "jobs":
            token_value = getattr(operation.job, column_name)
        else:
            raise ValueError("Got invalid table f{table_name} for sort key {sort_key.name} while building page_token")

        token_values.append(dump_list_operations_token_value(token_value))

    next_page_token = "|".join(token_values)
    return next_page_token


def extract_sort_keys(operation_filters: List[OperationFilter]) -> Tuple[List[SortKey], List[OperationFilter]]:
    """ Splits the operation filters into sort keys and non-sort filters, returning both as
    separate lists.

    Sort keys are specified with the "sort_order" parameter in the filter string. Multiple
    "sort_order"s can appear in the filter string, and all are extracted and returned. """
    # pylint: disable=comparison-with-callable
    sort_keys = []
    non_sort_filters = []
    for op_filter in operation_filters:
        if op_filter.parameter == "sort_order":
            if op_filter.operator != operator.eq:
                raise InvalidArgumentError("sort_order must be specified with the \"=\" operator.")
            sort_keys.append(op_filter.value)
        else:
            non_sort_filters.append(op_filter)

    return sort_keys, non_sort_filters


def build_sort_column_list(sort_keys):
    """ Convert the list of sort keys into a list of columns that can be
    passed to an order_by.

    This function checks the sort keys to ensure that they are in the
    parameter-model map and raises an InvalidArgumentError if they are not. """
    sort_columns = []
    for sort_key in sort_keys:
        try:
            col = LIST_OPERATIONS_PARAMETER_MODEL_MAP[sort_key.name]
            if sort_key.descending:
                sort_columns.append(col.desc())
            else:
                sort_columns.append(col.asc())
        except KeyError:
            raise InvalidArgumentError(f"[{sort_key.name}] is not a valid sort key.")
    return sort_columns


def convert_filter_to_sql_filter(operation_filter: OperationFilter) -> ClauseElement:
    """ Convert the internal representation of a filter to a representation that SQLAlchemy
    can understand. The return type is a "ColumnElement," per the end of this section in
    the SQLAlchemy docs: https://docs.sqlalchemy.org/en/13/core/tutorial.html#selecting-specific-columns

    This function assumes that the parser has appropriately converted the filter
    value to a Python type that can be compared to the parameter. """
    try:
        param = LIST_OPERATIONS_PARAMETER_MODEL_MAP[operation_filter.parameter]
    except KeyError:
        raise InvalidArgumentError(f"Invalid parameter: [{operation_filter.parameter}]")

    return operation_filter.operator(param, operation_filter.value)


def build_custom_filters(operation_filters: List[OperationFilter]) -> List[ClauseElement]:
    return [
        convert_filter_to_sql_filter(operation_filter)
        for operation_filter in operation_filters
    ]
