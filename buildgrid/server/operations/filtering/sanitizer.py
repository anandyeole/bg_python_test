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


from abc import ABC
from datetime import datetime
import re

import dateutil.parser
from dateutil.tz import tzutc

import buildgrid._enums as enums
from buildgrid._exceptions import InvalidArgumentError
from buildgrid.server.operations.filtering import SortKey


class ValueSanitizer(ABC):
    """ Base sanitizer class. """
    def sanitize(self, value_string: str):
        """ Method that takes an input string, validates that input string,
        and transforms it to a value of another type if necessary.

        Raises InvalidArgumentError if the sanitization fails. Returns a
        value of an arbitrary type if it succeeds. """
        return NotImplementedError()


class RegexValueSanitizer(ValueSanitizer):
    """ Sanitizer for regexable patterns. """
    def __init__(self, filter_name, regex_pattern):
        self.filter_name = filter_name
        self.regex = re.compile(f"^({regex_pattern})$")

    def sanitize(self, value_string: str) -> str:
        if not self.regex.fullmatch(value_string):
            raise InvalidArgumentError(f"[{value_string}] is not a valid value for {self.filter_name}.")
        return value_string


class DatetimeValueSanitizer(ValueSanitizer):
    """ Sanitizer for ISO 8601 datetime strings. """
    def __init__(self, filter_name):
        self.filter_name = filter_name

    def sanitize(self, value_string: str) -> datetime:
        try:
            dt = dateutil.parser.isoparse(value_string)
            # Convert to UTC and remove timezone awareness
            if dt.tzinfo:
                dt = dt.astimezone(tz=tzutc()).replace(tzinfo=None)
            return dt
        except ValueError:
            raise InvalidArgumentError(f"[{value_string}] is not a valid value for {self.filter_name}.")


class OperationStageValueSanitizer(ValueSanitizer):
    """ Sanitizer for the OperationStage type.

    Matches valid OperationStage values and converts to the
    numeric representation of that stage. """
    def __init__(self, filter_name):
        self.filter_name = filter_name

    def sanitize(self, value_string: str) -> int:
        try:
            stage = value_string.upper()
            return enums.OperationStage[stage].value
        except KeyError:
            raise InvalidArgumentError(f"[{value_string}] is not a valid value for {self.filter_name}.")


class SortKeyValueSanitizer(ValueSanitizer):
    """ Sanitizer for sort orders.

    Produces a SortKey tuple, which specifies both a key name and a boolean
    indicating ascending/descending order. """
    def __init__(self, filter_name):
        self.filter_name = filter_name

    def sanitize(self, value_string: str) -> SortKey:
        desc_key = "(desc)"
        asc_key = "(asc)"

        key_name = value_string.lower().strip()
        if not key_name:
            raise InvalidArgumentError(f"Invalid sort key [{value_string}].")
        descending = False

        if value_string.endswith(desc_key):
            descending = True
            key_name = key_name[:-len(desc_key)].strip()
            if not key_name:
                raise InvalidArgumentError(f"Invalid sort key [{value_string}].")

        elif value_string.endswith(asc_key):
            key_name = key_name[:-len(asc_key)].strip()
            if not key_name:
                raise InvalidArgumentError(f"Invalid sort key [{value_string}].")

        return SortKey(name=key_name, descending=descending)
