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


import itertools

from typing import Iterator

from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import Command


def command_output_paths(command: Command) -> Iterator[str]:
    """Given a remote_execution_pb2.Command returns an iterator to
    its list of output paths.
    """
    # According to the REAPI v2.1:
    # "If `output_paths` is used, `output_files` and `output_directories`
    # will be ignored!"
    if command.output_paths:
        return iter(command.output_paths)

    return itertools.chain(command.output_files, command.output_directories)
