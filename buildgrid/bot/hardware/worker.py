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


from buildgrid._protos.google.devtools.remoteworkers.v1test2 import worker_pb2


class Worker:
    """ Describes a worker, which is a list of one of more devices and the
    connextions between them. A device could be a computer, a phone or a GPU.

    The first device is known as the Primary Device which is responsible for
    executing commands. All other devices are known as Attatched Devices and
    must be controlled by the Primary Device.
    """

    def __init__(self, properties=None, configs=None):
        """ Create an instance of a :class:`Worker`.

        Property keys supported: `pool`.
        Config keys supported: `DockerImage`.

        Args:
            properties (dict(string : list(string))) : Properties of worker. Keys may have
        multiple values.
            configs (dict(string : list(string))) : Configurations of a worker. Keys may have
        multiple values.

        """

        self._devices = []
        self.__configs = {}
        self.__properties = {}
        self.__property_keys = ['pool']
        self.__config_keys = ['DockerImage']

        if properties:
            for k, l in properties.items():
                for v in l:
                    self._add_property(k, v)

        if configs:
            for k, l in configs.items():
                for v in l:
                    self._add_config(k, v)

    @property
    def configs(self):
        """Returns configurations supported by :class:`Worker`."""
        return self.__configs

    @property
    def properties(self):
        """Returns properties supported by :class:`Worker`."""
        return self.__properties

    def add_device(self, device):
        """ Adds a class:`Device` to this instance. First device added should be the Primary Device."""
        self._devices.append(device)

    def get_pb2(self):
        """Returns the protobuffer representation of a :class:`Device`."""
        devices = [device.get_pb2() for device in self._devices]
        worker = worker_pb2.Worker(devices=devices)

        for k, v in self.__properties.items():
            for prop in v:
                property_message = worker_pb2.Worker.Property()
                property_message.key = k
                property_message.value = prop
                worker.properties.extend([property_message])

        for k, v in self.__configs.items():
            for cfg in v:
                config_message = worker_pb2.Worker.Config()
                config_message.key = k
                config_message.value = cfg
                worker.configs.extend([config_message])

        return worker

    def _add_config(self, key, value):
        """Adds a configuration. Will raise KeyError if not supported."""
        if key in self.__config_keys:
            cfg = self.__configs.get(key)
            if not cfg:
                self.__configs[key] = [value]
            else:
                cfg.append(value)

        else:
            raise KeyError(f'Key not supported: [{key}]')

    def _add_property(self, key, value):
        """Adds a property. Will raise KeyError if not supported."""
        if key in self.__property_keys:
            prop = self.__properties.get(key)
            if not prop:
                self.__properties[key] = [value]
            else:
                prop.append(value)

        else:
            raise KeyError(f'Key not supported: [{key}]')
