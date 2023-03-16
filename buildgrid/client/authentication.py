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


import base64
from collections import namedtuple
import os
from typing import Optional

import grpc
from grpc import aio  # type: ignore

from buildgrid._exceptions import InvalidArgumentError
from buildgrid.utils import read_file


def load_tls_channel_credentials(client_key=None, client_cert=None, server_cert=None):
    """Looks-up and loads TLS gRPC client channel credentials.

    Args:
        client_key(str, optional): Client certificate chain file path.
        client_cert(str, optional): Client private key file path.
        server_cert(str, optional): Serve root certificate file path.

    Returns:
        ChannelCredentials: Credentials to be used for a TLS-encrypted gRPC
            client channel.
    """
    if server_cert and os.path.exists(server_cert):
        server_cert_pem = read_file(server_cert)
    else:
        server_cert_pem = None
        server_cert = None

    if client_key and os.path.exists(client_key):
        client_key_pem = read_file(client_key)
    else:
        client_key_pem = None
        client_key = None

    if client_key_pem and client_cert and os.path.exists(client_cert):
        client_cert_pem = read_file(client_cert)
    else:
        client_cert_pem = None
        client_cert = None

    credentials = grpc.ssl_channel_credentials(root_certificates=server_cert_pem,
                                               private_key=client_key_pem,
                                               certificate_chain=client_cert_pem)

    return credentials, (client_key, client_cert, server_cert,)


def load_channel_authorization_token(auth_token=None):
    """Looks-up and loads client authorization token.

    Args:
        auth_token (str, optional): Token file path.

    Returns:
        str: Encoded token string.
    """
    if auth_token and os.path.exists(auth_token):
        return read_file(auth_token).decode()

    # TODO: Try loading the token from a default location?

    return None


class AuthMetadataClientInterceptorBase:

    def __init__(self, auth_token: Optional[str]=None, auth_secret: Optional[bytes]=None):
        """Initialises a new :class:`AuthMetadataClientInterceptorBase`.

        Important:
            One of `auth_token` or `auth_secret` must be provided.

        Args:
            auth_token (str, optional): Authorization token as a string.
            auth_secret (bytes, optional): Authorization secret as bytes.

        Raises:
            InvalidArgumentError: If neither `auth_token` or `auth_secret` are
                provided.
        """
        if auth_token:
            self.__secret = auth_token.strip()

        elif auth_secret:
            self.__secret = base64.b64encode(auth_secret.strip()).decode()

        else:
            raise InvalidArgumentError("A secret or token must be provided")

        self.__header_field_name = 'authorization'
        self.__header_field_value = f'Bearer {self.__secret}'

    def _amend_call_details(self, client_call_details, grpc_call_details_class):
        """Appends an authorization field to given client call details."""
        if client_call_details.metadata is not None:
            new_metadata = list(client_call_details.metadata)
        else:
            new_metadata = []

        new_metadata.append((self.__header_field_name, self.__header_field_value,))

        class _ClientCallDetails(
                namedtuple('_ClientCallDetails',
                           ('method', 'timeout', 'credentials', 'metadata', 'wait_for_ready',)),
                grpc_call_details_class):
            pass

        return _ClientCallDetails(client_call_details.method,
                                  client_call_details.timeout,
                                  client_call_details.credentials,
                                  new_metadata,
                                  client_call_details.wait_for_ready)


class AuthMetadataClientInterceptor(AuthMetadataClientInterceptorBase,
                                    grpc.UnaryUnaryClientInterceptor,
                                    grpc.UnaryStreamClientInterceptor,
                                    grpc.StreamUnaryClientInterceptor,
                                    grpc.StreamStreamClientInterceptor):

    def __init__(self, auth_token: Optional[str]=None, auth_secret: Optional[bytes]=None):
        AuthMetadataClientInterceptorBase.__init__(self, auth_token, auth_secret)

    def intercept_unary_unary(self, continuation, client_call_details, request):
        new_details = self._amend_call_details(client_call_details, grpc.ClientCallDetails)

        return continuation(new_details, request)

    def intercept_unary_stream(self, continuation, client_call_details, request):
        new_details = self._amend_call_details(client_call_details, grpc.ClientCallDetails)

        return continuation(new_details, request)

    def intercept_stream_unary(self, continuation, client_call_details, request_iterator):
        new_details = self._amend_call_details(client_call_details, grpc.ClientCallDetails)

        return continuation(new_details, request_iterator)

    def intercept_stream_stream(self, continuation, client_call_details, request_iterator):
        new_details = self._amend_call_details(client_call_details, grpc.ClientCallDetails)

        return continuation(new_details, request_iterator)


class AsyncAuthMetadataClientInterceptor(AuthMetadataClientInterceptorBase,
                                         aio.UnaryUnaryClientInterceptor,
                                         aio.UnaryStreamClientInterceptor,
                                         aio.StreamUnaryClientInterceptor,
                                         aio.StreamStreamClientInterceptor):

    def __init__(self, auth_token: Optional[str]=None, auth_secret: Optional[bytes]=None):
        AuthMetadataClientInterceptorBase.__init__(self, auth_token, auth_secret)

    async def intercept_unary_unary(self, continuation, client_call_details, request):
        new_details = self._amend_call_details(client_call_details, aio.ClientCallDetails)

        return await continuation(new_details, request)

    async def intercept_unary_stream(self, continuation, client_call_details, request):
        new_details = self._amend_call_details(client_call_details, aio.ClientCallDetails)

        return await continuation(new_details, request)

    async def intercept_stream_unary(self, continuation, client_call_details, request_iterator):
        new_details = self._amend_call_details(client_call_details, aio.ClientCallDetails)

        return await continuation(new_details, request_iterator)

    async def intercept_stream_stream(self, continuation, client_call_details, request_iterator):
        new_details = self._amend_call_details(client_call_details, aio.ClientCallDetails)

        return await continuation(new_details, request_iterator)
