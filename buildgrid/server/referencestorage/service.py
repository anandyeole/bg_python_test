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


import logging

import grpc

from buildgrid._exceptions import InvalidArgumentError, NotFoundError
from buildgrid._protos.buildstream.v2 import buildstream_pb2
from buildgrid._protos.buildstream.v2 import buildstream_pb2_grpc
from buildgrid.server._authentication import AuthContext, authorize


class ReferenceStorageService(buildstream_pb2_grpc.ReferenceStorageServicer):

    def __init__(self, server):
        self.__logger = logging.getLogger(__name__)

        self._instances = {}

        buildstream_pb2_grpc.add_ReferenceStorageServicer_to_server(self, server)

    # --- Public API ---

    def add_instance(self, name, instance):
        self._instances[name] = instance

    # --- Public API: Servicer ---

    @authorize(AuthContext)
    def GetReference(self, request, context):
        self.__logger.debug(f"GetReference request from [{context.peer()}]")

        try:
            instance = self._get_instance(request.instance_name)
            digest = instance.get_digest_reference(request.key)
            response = buildstream_pb2.GetReferenceResponse()
            response.digest.CopyFrom(digest)
            return response

        except InvalidArgumentError as e:
            self.__logger.info(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        except NotFoundError as e:
            self.__logger.debug(e)
            context.set_code(grpc.StatusCode.NOT_FOUND)

        except Exception:
            self.__logger.exception(
                f"Unexpected error in GetReference; request=[{request}]"
            )
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)

        return buildstream_pb2.GetReferenceResponse()

    @authorize(AuthContext)
    def UpdateReference(self, request, context):
        self.__logger.debug(f"UpdateReference request from [{context.peer()}]")

        try:
            instance = self._get_instance(request.instance_name)
            digest = request.digest

            for key in request.keys:
                instance.update_reference(key, digest)

        except InvalidArgumentError as e:
            self.__logger.info(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        except NotImplementedError as e:
            self.__logger.info(e)
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)

        except Exception:
            self.__logger.exception(
                f"Unexpected error in UpdateReference; request=[{request}]"
            )
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)

        return buildstream_pb2.UpdateReferenceResponse()

    @authorize(AuthContext)
    def Status(self, request, context):
        self.__logger.debug(f"Status request from [{context.peer()}]")

        try:
            instance = self._get_instance(request.instance_name)
            allow_updates = instance.allow_updates
            return buildstream_pb2.StatusResponse(allow_updates=allow_updates)

        except InvalidArgumentError as e:
            self.__logger.info(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        except Exception:
            self.__logger.exception(
                f"Unexpected error in Status; request=[{request}]"
            )
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)

        return buildstream_pb2.StatusResponse()

    # --- Private API ---

    def _get_instance(self, instance_name):
        try:
            return self._instances[instance_name]

        except KeyError:
            raise InvalidArgumentError(f"Invalid instance name: [{instance_name}]")
