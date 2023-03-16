# Copyright (C) 2018 Bloomberg LP
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


import random
import time

from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid.utils import get_hostname


def work_dummy(lease, context, event):
    """ Just returns lease after some random time
    """
    action_result = remote_execution_pb2.ActionResult()

    lease.result.Clear()

    action_result.execution_metadata.worker = get_hostname()

    # Simulation input-downloading phase:
    action_result.execution_metadata.input_fetch_start_timestamp.GetCurrentTime()
    time.sleep(random.random())
    action_result.execution_metadata.input_fetch_completed_timestamp.GetCurrentTime()

    # Simulation execution phase:
    action_result.execution_metadata.execution_start_timestamp.GetCurrentTime()
    time.sleep(random.random())
    action_result.execution_metadata.execution_completed_timestamp.GetCurrentTime()

    # Simulation output-uploading phase:
    action_result.execution_metadata.output_upload_start_timestamp.GetCurrentTime()
    time.sleep(random.random())
    action_result.execution_metadata.output_upload_completed_timestamp.GetCurrentTime()

    lease.result.Pack(action_result)

    return lease
