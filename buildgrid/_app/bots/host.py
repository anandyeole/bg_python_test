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


import io
import logging
import os
import subprocess
import tempfile

from buildgrid.client.cas import download, upload
from buildgrid._exceptions import InvalidArgumentError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid.settings import MAX_REQUEST_SIZE
from buildgrid.utils import get_hostname, output_file_maker, output_directory_maker
from buildgrid._app.bots.utils import command_output_paths


def work_host_tools(lease, context, event):
    """Executes a lease for a build action, using host tools.
    """
    instance_name = context.instance_name

    logger = logging.getLogger(__name__)

    # The lease may contain an `Action` or its `Digest`.
    action = remote_execution_pb2.Action()
    action_digest = remote_execution_pb2.Digest()
    payload_is_digest = False
    if lease.payload.Unpack(action):
        logger.debug('Lease contains an Action payload')
    elif lease.payload.Unpack(action_digest):
        logger.debug('Lease contains a Digest payload')
        payload_is_digest = True
    else:
        raise InvalidArgumentError('Unexpected payload type in Lease (not Action or Digest)')

    lease.result.Clear()

    action_result = remote_execution_pb2.ActionResult()
    action_result.execution_metadata.worker = get_hostname()

    with tempfile.TemporaryDirectory() as temp_directory:
        with download(context.cas_channel, instance=instance_name) as downloader:
            if payload_is_digest:  # We have a Digest, fetching the Action proto:
                logger.debug(f'Fetching Action [{action_digest}]')
                action = downloader.get_message(action_digest,
                                                remote_execution_pb2.Action())

            assert action.command_digest.hash

            command = downloader.get_message(action.command_digest,
                                             remote_execution_pb2.Command())

            action_result.execution_metadata.input_fetch_start_timestamp.GetCurrentTime()

            downloader.download_directory(action.input_root_digest, temp_directory)

        logger.debug(f"Command digest: [{action.command_digest.hash}/{action.command_digest.size_bytes}]")
        logger.debug(
            f"Input root digest: [{action.input_root_digest.hash}/{action.input_root_digest.size_bytes}]")

        action_result.execution_metadata.input_fetch_completed_timestamp.GetCurrentTime()

        environment = os.environ.copy()
        for variable in command.environment_variables:
            if variable.name not in ['PATH', 'PWD']:
                environment[variable.name] = variable.value

        command_line = []
        for argument in command.arguments:
            command_line.append(argument.strip())

        working_directory = None
        if command.working_directory:
            working_directory = os.path.join(temp_directory,
                                             command.working_directory)
            os.makedirs(working_directory, exist_ok=True)
        else:
            working_directory = temp_directory

        # Ensure that output files and directories structure exists:
        for output_path in command_output_paths(command):
            parent_path = os.path.join(working_directory,
                                       os.path.dirname(output_path))
            os.makedirs(parent_path, exist_ok=True)

        logger.info(f"Starting execution: [{command.arguments[0]}...]")

        action_result.execution_metadata.execution_start_timestamp.GetCurrentTime()

        return_code, stdout, stderr = _execute_command(command_line, environment,
                                                       working_directory)

        action_result.execution_metadata.execution_completed_timestamp.GetCurrentTime()

        action_result.exit_code = return_code

        logger.info(f"Execution finished with code: [{return_code}]")

        action_result.execution_metadata.output_upload_start_timestamp.GetCurrentTime()

        with upload(context.cas_channel, instance=instance_name) as uploader:
            output_files, output_directories = [], []

            for output_path in command_output_paths(command):
                path = os.path.join(working_directory, output_path)
                if os.path.isfile(path):
                    file_digest = uploader.upload_file(path, queue=True)
                    output_file = output_file_maker(path, working_directory, file_digest)
                    output_files.append(output_file)

                    logger.debug(f"Output file digest: [{file_digest.hash}/{file_digest.size_bytes}]")

                elif os.path.isdir(path):
                    tree_digest = uploader.upload_tree(path, queue=True)
                    output_directory = output_directory_maker(path, working_directory, tree_digest)
                    output_directories.append(output_directory)

                    logger.debug(f"Output tree digest: [{tree_digest.hash}/{tree_digest.size_bytes}]")

            action_result.output_files.extend(output_files)
            action_result.output_directories.extend(output_directories)

            if action_result.ByteSize() + len(stdout) > MAX_REQUEST_SIZE:
                stdout_digest = uploader.put_blob(io.BytesIO(stdout), length=len(stdout))
                action_result.stdout_digest.CopyFrom(stdout_digest)

            else:
                action_result.stdout_raw = stdout

            if action_result.ByteSize() + len(stderr) > MAX_REQUEST_SIZE:
                stderr_digest = uploader.put_blob(io.BytesIO(stderr), length=len(stderr))
                action_result.stderr_digest.CopyFrom(stderr_digest)

            else:
                action_result.stderr_raw = stderr

        action_result.execution_metadata.output_upload_completed_timestamp.GetCurrentTime()

        lease.result.Pack(action_result)

    return lease


def _execute_command(command_line, environment, working_directory):
    # pylint: disable=consider-using-with
    process = subprocess.Popen(command_line,
                               cwd=working_directory,
                               env=environment,
                               stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()  # Waits for command to finish
    return process.returncode, stdout, stderr
