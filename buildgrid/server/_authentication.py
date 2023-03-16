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

from collections import namedtuple, OrderedDict
from datetime import datetime, timedelta, timezone
from enum import Enum
import functools
import json
import logging
import sys
import threading

import grpc

from buildgrid._exceptions import InvalidArgumentError
from buildgrid.settings import AUTH_CACHE_SIZE
from buildgrid.server.metrics_utils import (
    ExceptionCounter,
    DurationMetric,
)
from buildgrid.server.metrics_names import (
    INVALID_JWT_COUNT_METRIC_NAME,
    JWK_FETCH_TIME_METRIC_NAME,
    JWT_DECODE_TIME_METRIC_NAME,
    JWT_VALIDATION_TIME_METRIC_NAME,
)

# Since jwt authentication is not required, make it optional.
# If used, but module not imported/found, will raise an exception.
try:
    import jwt
    import requests

    # Algorithm classes defined in: https://github.com/jpadilla/pyjwt/blob/master/jwt/algorithms.py
    ALGORITHM_TO_PYJWT_CLASS = {
        "RSA": jwt.algorithms.RSAAlgorithm,
        "EC": jwt.algorithms.ECAlgorithm,
        "oct": jwt.algorithms.HMACAlgorithm,
    }

except ImportError:
    pass


class AuthMetadataMethod(Enum):
    # No authentication:
    NONE = 'none'
    # JWT based authentication:
    JWT = 'jwt'


class AuthMetadataAlgorithm(Enum):
    # No encryption involved:
    UNSPECIFIED = 'unspecified'
    # JWT related algorithms:
    JWT_ES256 = 'es256'  # ECDSA signature algorithm using SHA-256 hash algorithm
    JWT_ES384 = 'es384'  # ECDSA signature algorithm using SHA-384 hash algorithm
    JWT_ES512 = 'es512'  # ECDSA signature algorithm using SHA-512 hash algorithm
    JWT_HS256 = 'hs256'  # HMAC using SHA-256 hash algorithm
    JWT_HS384 = 'hs384'  # HMAC using SHA-384 hash algorithm
    JWT_HS512 = 'hs512'  # HMAC using SHA-512 hash algorithm
    JWT_PS256 = 'ps256'  # RSASSA-PSS using SHA-256 and MGF1 padding with SHA-256
    JWT_PS384 = 'ps384'  # RSASSA-PSS signature using SHA-384 and MGF1 padding with SHA-384
    JWT_PS512 = 'ps512'  # RSASSA-PSS signature using SHA-512 and MGF1 padding with SHA-512
    JWT_RS256 = 'rs256'  # RSASSA-PKCS1-v1_5 signature algorithm using SHA-256 hash algorithm
    JWT_RS384 = 'rs384'  # RSASSA-PKCS1-v1_5 signature algorithm using SHA-384 hash algorithm
    JWT_RS512 = 'rs512'  # RSASSA-PKCS1-v1_5 signature algorithm using SHA-512 hash algorithm


class AuthContext:

    interceptor = None


class _InvalidTokenError(Exception):
    pass


class _ExpiredTokenError(Exception):
    pass


class _UnboundedTokenError(Exception):
    pass


def authorize(auth_context):
    """RPC method decorator for authorization validations.

    This decorator is design to be used together with an :class:`AuthContext`
    authorization context holder::

        @authorize(AuthContext)
        def Execute(self, request, context):

    By default, any request is accepted. Authorization validation can be
    activated by setting up a :class:`grpc.ServerInterceptor`::

        AuthContext.interceptor = AuthMetadataServerInterceptor()

    Args:
        auth_context(AuthContext): Authorization context holder.
    """
    def __authorize_decorator(behavior):
        """RPC authorization method decorator."""
        _HandlerCallDetails = namedtuple('_HandlerCallDetails', (
            'invocation_metadata',
            'method',
        ))

        @functools.wraps(behavior)
        def __authorize_wrapper(self, request, context):
            """RPC authorization method wrapper."""
            if auth_context.interceptor is None:
                return behavior(self, request, context)

            authorized = False

            def __continuator(handler_call_details):
                nonlocal authorized
                authorized = True

            details = _HandlerCallDetails(context.invocation_metadata(),
                                          behavior.__name__)

            auth_context.interceptor.intercept_service(__continuator, details)

            if authorized:
                return behavior(self, request, context)
            else:
                request_args = str(request).replace("\n", "")
                logging.getLogger(__name__).info(
                    "Authentication failed for request=["
                    f"{behavior.__name__}({request_args})], "
                    f"peer=[{context.peer()}]")

            context.abort(grpc.StatusCode.UNAUTHENTICATED,
                          "No valid authorization or authentication provided")

            return None

        return __authorize_wrapper

    return __authorize_decorator


class AuthMetadataServerInterceptor(grpc.ServerInterceptor):

    __auth_errors = {
        'missing-bearer': "Missing authentication header field",
        'invalid-bearer': "Invalid authentication header field",
        'invalid-token': "Invalid authentication token",
        'expired-token': "Expired authentication token",
        'unbounded-token': "Unbounded authentication token",
    }

    def __init__(self,
                 method,
                 secret=None,
                 algorithm=AuthMetadataAlgorithm.UNSPECIFIED,
                 jwks_url=None,
                 audience=None,
                 jwks_fetch_minutes=60):
        """Initializes a new :class:`AuthMetadataServerInterceptor`.

        Args:
            method (AuthMetadataMethod): Type of authorization method.
            secret (str): The secret or key to be used for validating request,
                depending on `method`. Defaults to ``None``.
            algorithm (AuthMetadataAlgorithm): The crytographic algorithm used
                to encode `secret`. Defaults to ``UNSPECIFIED``.
            jwks_url (str): The url to fetch the JWKs. Either secret or
                this field must be specified if the authentication method is JWT.
                Defaults to ``None``.
            audience (str): The audience used to validate jwt tokens against.
                The tokens must have an audience field.
            jwks_fetch_minutes (int): The number of minutes to wait before
                refreshing the jwks set. Default: 60 minutes.

        Raises:
            InvalidArgumentError: If `method` is not supported or if `algorithm`
                is not supported for the given `method`.
        """
        self.__logger = logging.getLogger(__name__)
        self.__bearer_cache = OrderedDict()
        self.__terminators = {}
        self.__validator = None
        self.__secret = secret
        self.__jwk_update_lock = threading.Lock()

        self._audience = audience
        self._jwks_url = jwks_url
        self._public_keys = {}
        self._jwks_fetch_minutes = jwks_fetch_minutes
        self._last_fetch_time = 0
        self._method = method
        self._algorithm = algorithm

        if self._method == AuthMetadataMethod.JWT:
            if self.__secret and self._jwks_url:
                raise RuntimeError(
                    "Only allowed to set secret or jwks-url. Not both.")

            if self._jwks_url:
                # Fetch jwk and store
                self._get_and_parse_jwks_from_url()

            self._check_jwt_support(self._algorithm)
            self.__validator = self._validate_jwt_token

        for code, message in self.__auth_errors.items():
            self.__terminators[code] = _unary_unary_rpc_terminator(message)

    def _error_message_for_call(self, call_details, auth_error_type, exception_details=""):
        return (
            f"Authentication error. Rejecting '{str(call_details.method)}' request: "
            f"Reason=[{self.__auth_errors[auth_error_type]}], "
            f"{exception_details}")

    # --- Public API ---

    @property
    def method(self):
        return self._method

    @property
    def algorithm(self):
        return self._algorithm

    def intercept_service(self, continuation, handler_call_details):
        try:
            # Reject requests not carrying a token:
            bearer = dict(
                handler_call_details.invocation_metadata)['authorization']

        except KeyError:
            self.__logger.info(
                self._error_message_for_call(handler_call_details,
                                             'missing-bearer'))
            return self.__terminators['missing-bearer']

        # Reject requests with malformated bearer:
        if not bearer.startswith('Bearer '):
            self.__logger.info(
                self._error_message_for_call(handler_call_details,
                                             'invalid-bearer'))
            return self.__terminators['invalid-bearer']

        try:
            # Hit the cache for already validated token:
            expiration_time = self.__bearer_cache[bearer]

            # Accept request if cached token hasn't expired yet:
            if expiration_time >= datetime.utcnow():
                return continuation(handler_call_details)  # Accepted

            else:
                del self.__bearer_cache[bearer]

            # Cached token has expired, reject the request:
            self.__logger.info(
                self._error_message_for_call(handler_call_details,
                                             'expired-token'))
            # TODO: Use grpc.Status.details to inform the client of the expiry?
            return self.__terminators['expired-token']

        except KeyError:
            pass

        assert self.__validator is not None

        try:
            # Decode and validate the new token:
            expiration_time = self.__validator(bearer[7:])

        except _InvalidTokenError as e:
            self.__logger.info(
                self._error_message_for_call(handler_call_details,
                                             'invalid-token', str(e)))
            return self.__terminators['invalid-token']

        except _ExpiredTokenError as e:
            self.__logger.info(
                self._error_message_for_call(handler_call_details,
                                             'expired-token', str(e)))
            return self.__terminators['expired-token']

        except _UnboundedTokenError as e:
            self.__logger.info(
                self._error_message_for_call(handler_call_details,
                                             'unbounded-token', str(e)))
            return self.__terminators['unbounded-token']

        # Cache the validated token and store expiration time:
        self.__bearer_cache[bearer] = expiration_time
        if len(self.__bearer_cache) > AUTH_CACHE_SIZE:
            self.__bearer_cache.popitem(last=False)

        return continuation(handler_call_details)  # Accepted

    # --- Private API: JWT ---

    def _check_jwt_support(self, algorithm=AuthMetadataAlgorithm.UNSPECIFIED):
        """Ensures JWT and possible dependencies are available."""
        if 'jwt' not in sys.modules:
            raise InvalidArgumentError(
                "JWT authorization method requires PyJWT")

        try:
            if algorithm != AuthMetadataAlgorithm.UNSPECIFIED:
                jwt.register_algorithm(algorithm.value.upper(), None)

        except TypeError:
            raise InvalidArgumentError(
                f"Algorithm not supported for JWT decoding: [{self._algorithm}]"
            )

        except ValueError:
            pass

    jwt_invalid_exceptions = (_ExpiredTokenError, _InvalidTokenError,
                              _UnboundedTokenError)

    @ExceptionCounter(INVALID_JWT_COUNT_METRIC_NAME,
                      exceptions=jwt_invalid_exceptions)
    @DurationMetric(JWT_VALIDATION_TIME_METRIC_NAME)
    def _validate_jwt_token(self, token):
        """Validates a JWT token and returns its expiry date."""
        if self._algorithm != AuthMetadataAlgorithm.UNSPECIFIED:
            algorithms = [self._algorithm.value.upper()]
        else:
            algorithms = None

        try:
            if self.__secret:
                with DurationMetric(JWT_DECODE_TIME_METRIC_NAME):
                    payload = jwt.decode(token,
                                         self.__secret,
                                         algorithms=algorithms)
            if self._jwks_url:
                self.__logger.debug(
                    f"Validating token with JWKS fetched from url: [{self._jwks_url}]"
                )
                # Refetch the jwks if the current time
                # is greater than the last fetch time plus the specified delta.
                # The first thread that is able to acquire the lock will be the one that updates the set.
                # pylint: disable=consider-using-with
                if (self._last_fetch_time +
                        timedelta(minutes=self._jwks_fetch_minutes) <=
                        datetime.now(tz=timezone.utc)
                    ) and self.__jwk_update_lock.acquire(False):
                    try:
                        self._get_and_parse_jwks_from_url()
                    except Exception:
                        self.__logger.exception(
                            "Exception thrown while fetching jwk. \
                            Continuing with request using previously cached keys."
                        )
                        # Continue if an exception occurred.
                    finally:
                        self.__jwk_update_lock.release()

                kid = jwt.get_unverified_header(token).get('kid')
                if kid is None:
                    raise RuntimeError("JWT token is missing kid.")
                key = self._public_keys.get(kid)
                if key is None:
                    # Try to update JWKs, if unable to grab lock (currently ongoing refresh process)
                    # then block until we can obtain and try again (see "else" block).

                    # pylint: disable=consider-using-with
                    if self.__jwk_update_lock.acquire(False):
                        try:
                            self._get_and_parse_jwks_from_url()
                        except Exception:
                            self.__logger.exception(
                                "Exception thrown while fetching jwk. \
                                Continuing with request using previously cached keys."
                            )
                            # Continue if an exception occurred.
                        finally:
                            self.__jwk_update_lock.release()
                    else:
                        # Wait until lock can be acquired (update has completed).
                        with self.__jwk_update_lock:
                            pass
                    key = self._public_keys.get(kid)
                    if key is None:
                        raise _InvalidTokenError(
                            f"No public key found for token with kid: {kid}")

                with DurationMetric(JWT_DECODE_TIME_METRIC_NAME):
                    payload = jwt.decode(token,
                                         key,
                                         algorithms=algorithms,
                                         audience=self._audience)
                self.__logger.debug(
                    f"JWT validated from JWK set fetched from: [{self._jwks_url}]"
                )

        except jwt.exceptions.ExpiredSignatureError as e:
            raise _ExpiredTokenError(e)

        except jwt.exceptions.InvalidTokenError as e:
            raise _InvalidTokenError(e)

        if 'exp' not in payload or not isinstance(payload['exp'], int):
            raise _UnboundedTokenError("Missing 'exp' in payload")

        return datetime.utcfromtimestamp(payload['exp'])

    @DurationMetric(JWK_FETCH_TIME_METRIC_NAME)
    def _get_and_parse_jwks_from_url(self):
        """ Get JWKs from url, and parse JSON web key set. """
        # pyJWT 2.0 will support these operations, once merged:
        # https://github.com/jpadilla/pyjwt/pull/470/files
        #
        # jwks_client = PyJWKClient(self._jwks_url)
        # signing_key = jwks_client.get_signing_key_from_jwt(token)
        # payload = jwt.decode(token, signing_key.key, algorithms=algorithms)
        try:
            self.__logger.info(
                f"Sending request to fetch JWKs from provided url: [{self._jwks_url}]"
            )
            data = requests.get(self._jwks_url)
        except requests.exceptions.RequestException as e_thrown:
            self.__logger.exception(
                f"Error sending request to: [{self._jwks_url}]")
            raise e_thrown

        try:
            jwks = data.json()
            temp_keys = {}
            for jwk in jwks.get('keys'):
                kid = jwk.get('kid')
                kty = jwk.get('kty')
                if kid is None or kty is None:
                    raise RuntimeError(
                        f"A key in the JWKs fetched from [{self._jwks_url}], \
                                      doesn't include one of the required properties: kid, or kty."
                    )
                alg_class = ALGORITHM_TO_PYJWT_CLASS.get(kty)
                if alg_class is None:
                    raise RuntimeError(
                        f"Unsupported algorithm type provided by \
                                       JWKs: [{kty}], fetched from [{self._jwks_url}]"
                    )
                temp_keys[kid] = alg_class.from_jwk(json.dumps(jwk))
        except (AttributeError, ValueError) as e_thrown:
            self.__logger.exception(f"Error parsing input: [{jwks}], \
                                     fetched from [{self._jwks_url}]")
            raise e_thrown

        if not temp_keys:
            self.__logger.error(
                f"No public keys returned from url: [{self._jwks_url}]")
            # If there are no public keys, raise an exception.
            if not self._public_keys:
                raise RuntimeError(
                    "Error fetching public keys, non-existing public keys.")

            self.__logger.info(
                f"Unable to fetch proper JWKs from [{self._jwks_url}], \
                                leaving existing set last fetched at [{self._last_fetch_time}] unchanged."
            )
            return

        # Set _last_fetch_time, this will be used to check
        # whether to refetch the token after a certain amount of time.
        self._last_fetch_time = datetime.now(tz=timezone.utc)

        self.__logger.info(
            f"Replacing existing JWKs set, with one fetched at time: \
                            [{self._last_fetch_time}] from url: [{self._jwks_url}"
        )

        # Set the class member variable to the new keys.
        self._public_keys = temp_keys


def _unary_unary_rpc_terminator(details):
    def terminate(ignored_request, context):
        context.abort(grpc.StatusCode.UNAUTHENTICATED, details)

    return grpc.unary_unary_rpc_method_handler(terminate)
