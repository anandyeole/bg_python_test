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


import uuid

from buildgrid._protos.google.devtools.remoteworkers.v1test2 import worker_pb2


class Device:
    """ Describes a device.

    A device available to the :class:`Worker`. The first device is known as
    the Primary Device which is running a bot and responsible for actually
    executing commands. All other devices are known as Attatched Devices and
    must be controlled by the Primary Device.
    """

    def __init__(self, properties=None):
        """ Initialises a :class:`Device` instances.

        Property keys supported: `os`, `has-docker`.

        Args:
        properties (dict(string : list(string))) : Properties of device. Keys may have
        multiple values.
        """

        self.__properties = {}
        self.__property_keys = ['OSFamily', 'has-docker','image']
        self.__name = str(uuid.uuid4())

        if properties:
            for k, l in properties.items():
                for v in l:
                    self._add_property(k, v)

    @property
    def name(self):
        """Returns the name of the device which is in the form of a `uuid4`."""
        return self.__name

    @property
    def properties(self):
        """Returns the device properties."""
        return self.__properties

    def get_pb2(self):
        """Returns the protobuffer representation of the :class:`Device`."""
        device = worker_pb2.Device(handle=self.__name)
        for k, v in self.__properties.items():
            for prop in v:
                property_message = worker_pb2.Device.Property()
                property_message.key = k
                property_message.value = prop
                device.properties.extend([property_message])
        return device

    def _add_property(self, key, value):
        """Adds a property to the :class:`Device` instance."""
        if key in self.__property_keys:
            prop = self.__properties.get(key)
            if not prop:
                self.__properties[key] = [value]
            else:
                prop.append(value)

        else:
            raise KeyError(f'Key not supported: [{key}]')
