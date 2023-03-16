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

from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import RequestMetadata, ToolDetails
from buildgrid.settings import REQUEST_METADATA_HEADER_NAME


def printable_request_metadata(metadata_entries) -> str:
    """Given a metadata object, return a human-readable representation
    of its `RequestMetadata` entry.

    Args:
        metadata_entries: tuple of entries obtained from a gRPC context
            with, for example, `context.invocation_metadata()`.

    Returns:
        A string with the metadata contents.
    """
    metadata = extract_request_metadata(metadata_entries)
    return request_metadata_to_string(metadata)


def extract_request_metadata(metadata_entries) -> RequestMetadata:
    """Given a list of string tuples, extract the RequestMetadata
    header values if they are present. If they were not provided,
    returns an empty message.

    Args:
        metadata_entries: tuple of entries obtained from a gRPC context
            with, for example, `context.invocation_metadata()`.

    Returns:
        A `RequestMetadata` proto. If the metadata is not defined in the
        request, the message will be empty.
    """
    request_metadata_entry = next((entry for entry in metadata_entries
                                   if entry.key == REQUEST_METADATA_HEADER_NAME),
                                  None)

    request_metadata = RequestMetadata()
    if request_metadata_entry:
        request_metadata.ParseFromString(request_metadata_entry.value)
    return request_metadata


def request_metadata_to_string(request_metadata):
    if request_metadata.tool_details:
        tool_name = request_metadata.tool_details.tool_name
        tool_version = request_metadata.tool_details.tool_version
    else:
        tool_name = tool_version = ''

    return (
        f'tool_name="{tool_name}", tool_version="{tool_version}", '
        f'action_id="{request_metadata.action_id}", '
        f'tool_invocation_id="{request_metadata.tool_invocation_id}", '
        f'correlated_invocations_id="{request_metadata.correlated_invocations_id}"'
    )


def request_metadata_from_scheduler_dict(scheduler_request_metadata: dict) -> RequestMetadata:
    tool_details = ToolDetails()
    tool_details.tool_name = scheduler_request_metadata['tool-name']
    tool_details.tool_version = scheduler_request_metadata['tool-version']

    request_metadata = RequestMetadata()
    request_metadata.tool_details.CopyFrom(tool_details)
    request_metadata.tool_invocation_id = scheduler_request_metadata['invocation-id']
    request_metadata.correlated_invocations_id = scheduler_request_metadata['correlated-invocations-id']

    return request_metadata
