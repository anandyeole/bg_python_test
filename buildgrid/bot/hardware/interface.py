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


from buildgrid._exceptions import FailedPreconditionError


class HardwareInterface:
    """Class to configure hardware and check requirements of a :class:`Lease`.

    In the future this could also be used to request and display
    the status of hardware.
    """

    def __init__(self, worker):
        """Initialises a new :class:`HardwareInstance`.

        Args:
            worker (:class:`Worker`)
        """
        self._worker = worker

    def configure_hardware(self, requirements):
        """Configure's hardware and checks to see if the requirements can
        be met.

        Args:
            requirements (:class:`Worker`): Requirements for the job.
        """
        worker = self._worker

        for config_requirement in requirements.configs:
            if config_requirement.key not in worker.configs:
                raise FailedPreconditionError("Requirements can't be met. "
                                              f"Requirements=[{config_requirement}]")

    def get_worker_pb2(self):
        """Returns the protobuffer representation of the :class:`Worker`."""
        return self._worker.get_pb2()
