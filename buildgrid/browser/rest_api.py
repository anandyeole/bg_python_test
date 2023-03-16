# Copyright (C) 2021 Bloomberg LP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  <http://www.apache.org/licenses/LICENSE-2.0>
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License' is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
from base64 import b64encode
from collections import namedtuple
import logging
import os
import shutil
import tarfile
import tempfile
from typing import Any, Callable, Dict, Optional, Sequence, Tuple, Type

import aiofiles
from aiohttp import web, WSMsgType
from grpc import RpcError, StatusCode
from grpc.aio import Call  # type: ignore

from buildgrid._app.cli import Context
from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import (
    ActionResult,
    Digest,
    Directory,
    GetActionResultRequest
)
from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2_grpc import ActionCacheStub
from buildgrid._protos.buildgrid.v2.query_build_events_pb2 import QueryEventStreamsRequest
from buildgrid._protos.buildgrid.v2.query_build_events_pb2_grpc import QueryBuildEventsStub
from buildgrid._protos.google.bytestream.bytestream_pb2_grpc import ByteStreamStub
from buildgrid._protos.google.bytestream.bytestream_pb2 import ReadRequest
from buildgrid._protos.google.longrunning.operations_pb2_grpc import OperationsStub
from buildgrid._protos.google.longrunning import operations_pb2
from buildgrid.browser.utils import TARBALL_DIRECTORY_PREFIX, ResponseCache, get_cors_headers
from buildgrid.server.request_metadata_utils import extract_request_metadata
from buildgrid.settings import BROWSER_MAX_CACHE_ENTRY_SIZE, BROWSER_OPERATION_CACHE_MAX_LENGTH


LOGGER = logging.getLogger(__name__)


def query_build_events_handler(context: Context) -> Callable:
    """Factory function which returns a handler for QueryEventStreams.

    The returned handler uses ``context.channel`` to send a QueryEventStreams
    request constructed based on the provided URL query parameters. Currently
    only querying by build_id (equivalent to correlated invocations ID) is
    supported.

    The handler returns a serialised QueryEventStreamsResponse, and raises a
    500 error in the case of some RPC error.

    Args:
        context (Context): The context to use to send the gRPC request.

    """
    async def _query_build_events_handler(request: web.Request) -> web.Response:
        build_id = request.rel_url.query.get('build_id', '.*')

        stub = QueryBuildEventsStub(context.channel)

        grpc_request = QueryEventStreamsRequest(
            build_id_pattern=build_id
        )

        try:
            grpc_response = await stub.QueryEventStreams(grpc_request)
        except RpcError as e:
            LOGGER.warning(e.details())
            raise web.HTTPInternalServerError()

        serialized_response = grpc_response.SerializeToString()
        return web.Response(body=serialized_response)
    return _query_build_events_handler


def list_operations_handler(context: Context, cache: ResponseCache) -> Callable:
    """Factory function which returns a handler for ListOperations.

    The returned handler uses ``context.channel`` and ``context.instance_name``
    to send a ListOperations request constructed based on the provided URL
    query parameters.

    The handler returns a serialised ListOperationsResponse, raises a 400
    error in the case of a bad filter or other invalid argument, or raises
    a 500 error in the case of some other RPC error.

    Args:
        context (Context): The context to use to send the gRPC request.

    """
    async def _list_operations_handler(request: web.Request) -> web.Response:
        filter_string = request.rel_url.query.get('q', '')
        page_token = request.rel_url.query.get('page_token')
        page_size_str = request.rel_url.query.get('page_size')
        page_size = None
        if page_size_str is not None:
            page_size = int(page_size_str)

        LOGGER.info(f'Received ListOperations request, filter_string="{filter_string}" '
                    f'page_token="{page_token}" page_size="{page_size}"')
        stub = OperationsStub(context.operations_channel)
        grpc_request = operations_pb2.ListOperationsRequest(
            name=context.instance_name,
            page_token=page_token,
            page_size=page_size,
            filter=filter_string)

        try:
            grpc_response = await stub.ListOperations(grpc_request)
        except RpcError as e:
            LOGGER.warning(e.details())
            if e.code() == StatusCode.INVALID_ARGUMENT:
                raise web.HTTPBadRequest()
            raise web.HTTPInternalServerError()

        if grpc_response.operations:
            for i, operation in enumerate(grpc_response.operations):
                if i > BROWSER_OPERATION_CACHE_MAX_LENGTH:
                    break
                await cache.store_operation(operation.name, operation)

        serialised_response = grpc_response.SerializeToString()
        return web.Response(body=serialised_response)
    return _list_operations_handler


async def _get_operation(context: Context, request: web.Request) -> Tuple[operations_pb2.Operation, Call]:
    operation_name = f"{context.instance_name}/{request.match_info['name']}"

    stub = OperationsStub(context.operations_channel)
    grpc_request = operations_pb2.GetOperationRequest(name=operation_name)

    try:
        call = stub.GetOperation(grpc_request)
        operation = await call
    except RpcError as e:
        LOGGER.warning(f"Error fetching operation: {e.details()}")
        if e.code() == StatusCode.INVALID_ARGUMENT:
            raise web.HTTPNotFound()
        raise web.HTTPInternalServerError()

    return operation, call


def get_operation_handler(context: Context, cache: ResponseCache) -> Callable:
    """Factory function which returns a handler for GetOperation.

    The returned handler uses ``context.channel`` and ``context.instance_name``
    to send a GetOperation request constructed based on the path component of
    the URL.

    The handler returns a serialised Operation message, raises a 400 error in
    the case of an invalid operation name, or raises a 500 error in the case
    of some other RPC error.

    Args:
        context (Context): The context to use to send the gRPC request.

    """
    async def _get_operation_handler(request: web.Request) -> web.Response:
        name = request.match_info["name"]
        LOGGER.info(f'Received GetOperation request for "{name}"')

        # Check the cache to see if we already fetched this operation recently
        operation = await cache.get_operation(name)

        # Fall back to sending an actual gRPC request
        if operation is None:
            operation, _ = await _get_operation(context, request)
            await cache.store_operation(name, operation)

        serialised_response = operation.SerializeToString()
        return web.Response(body=serialised_response)
    return _get_operation_handler


def get_operation_request_metadata_handler(context: Context) -> Callable:
    """Factory function which returns a handler to get RequestMetadata.

    The returned handler uses ``context.channel`` and ``context.instance_name``
    to send a GetOperation request constructed based on the path component of
    the URL.

    The handler returns a serialised RequestMetadata proto message, retrieved
    from the trailing metadata of the GetOperation response. In the event of
    an invalid operation name it raises a 404 error, and raises a 500 error in
    the case of some other RPC error.

    Args:
        context (Context): The context to use to send the gRPC request.

    """
    async def _get_operation_request_metadata_handler(request: web.Request) -> web.Response:
        LOGGER.info(f'Received request for RequestMetadata for "{request.match_info["name"]}')
        _, call = await _get_operation(context, request)
        metadata = await call.trailing_metadata()

        def extract_metadata(m):
            # `m` contains a list of tuples, but `extract_request_metadata()`
            # expects a `key` and `value` attributes.
            MetadataTuple = namedtuple('MetadataTuple', ['key', 'value'])
            m = [MetadataTuple(entry[0], entry[1]) for entry in m]
            return extract_request_metadata(m)

        request_metadata = extract_metadata(metadata)
        return web.Response(body=request_metadata.SerializeToString())
    return _get_operation_request_metadata_handler


def cancel_operation_handler(context: Context) -> Callable:
    """Factory function which returns a handler for CancelOperation.

    The returned handler uses ``context.channel`` and ``context.instance_name``
    to send a CancelOperation request constructed based on the path component of
    the URL.

    The handler raises a 404 error in the case of an invalid operation name,
    or a 500 error in the case of some other RPC error.

    On success, the response is empty.

    Args:
        context (Context): The context to use to send the gRPC request.

    """
    async def _cancel_operation_handler(request: web.Request) -> web.Response:
        LOGGER.info(f'Received CancelOperation request for "{request.match_info["name"]}"')
        operation_name = f"{context.instance_name}/{request.match_info['name']}"

        stub = OperationsStub(context.operations_channel)
        grpc_request = operations_pb2.CancelOperationRequest(name=operation_name)

        try:
            await stub.CancelOperation(grpc_request)
            return web.Response()
        except RpcError as e:
            LOGGER.warning(e.details())
            if e.code() == StatusCode.INVALID_ARGUMENT:
                raise web.HTTPNotFound()
            raise web.HTTPInternalServerError()

    return _cancel_operation_handler


async def _fetch_action_result(
    context: Context,
    request: web.Request,
    cache: ResponseCache,
    cache_key: str
) -> ActionResult:
    stub = ActionCacheStub(context.cache_channel)
    digest = Digest(
        hash=request.match_info["hash"],
        size_bytes=int(request.match_info["size_bytes"])
    )
    grpc_request = GetActionResultRequest(
        action_digest=digest, instance_name=context.instance_name)

    try:
        result = await stub.GetActionResult(grpc_request)
    except RpcError as e:
        LOGGER.warning(f"Failed to fetch ActionResult: [{e.details()}]")
        if e.code() == StatusCode.NOT_FOUND:
            raise web.HTTPNotFound()
        raise web.HTTPInternalServerError()

    await cache.store_action_result(cache_key, result)

    return result


def get_action_result_handler(context: Context, cache: ResponseCache, cache_capacity: int=512) -> Callable:
    """Factory function which returns a handler for GetActionResult.

    The returned handler uses ``context.channel`` and ``context.instance_name``
    to send a GetActionResult request constructed based on the path components
    of the URL.

    The handler returns a serialised ActionResult message, raises a 404 error
    if there's no result cached, or raises a 500 error in the case of some
    other RPC error.

    Args:
        context (Context): The context to use to send the gRPC request.
        cache_capacity (int): The number of ActionResults to cache in memory
            to avoid hitting the actual ActionCache.

    """
    class FetchSpec:
        """Simple class used to store information about a GetActionResult request.

        A class is used here rather than a namedtuple since we need this state
        to be mutable.

        """
        def __init__(self, *,
                     error: Optional[Exception],
                     event: asyncio.Event,
                     result: Optional[ActionResult],
                     refcount: int):
            self.error = error
            self.event = event
            self.result = result
            self.refcount = refcount

    in_flight_fetches: Dict[str, FetchSpec] = {}
    fetch_lock = asyncio.Lock()

    async def _get_action_result_handler(request: web.Request) -> web.Response:
        LOGGER.info(
            'Received GetActionResult request for '
            f'"{request.match_info["hash"]}/{request.match_info["size_bytes"]}"'
        )

        cache_key = f'{request.match_info["hash"]}/{request.match_info["size_bytes"]}'
        result = await cache.get_action_result(cache_key)

        if result is None:
            try:
                duplicate_request = False
                spec = None
                async with fetch_lock:
                    if cache_key in in_flight_fetches:
                        LOGGER.info(f'Deduplicating GetActionResult request for [{cache_key}]')
                        spec = in_flight_fetches[cache_key]
                        spec.refcount += 1
                        duplicate_request = True
                    else:
                        spec = FetchSpec(
                            error=None,
                            event=asyncio.Event(),
                            result=None,
                            refcount=1
                        )
                        in_flight_fetches[cache_key] = spec

                if duplicate_request and spec:
                    # If we're a duplicate of an existing request, then wait for the
                    # existing request to finish fetching from the ActionCache.
                    await spec.event.wait()
                    if spec.error is not None:
                        raise spec.error
                    if spec.result is None:
                        # Note: this should be impossible, but lets guard against accidentally setting
                        # the event before the result is populated
                        LOGGER.info(f'Result not set in deduplicated request for [{cache_key}]')
                        raise web.HTTPInternalServerError()
                    result = spec.result
                else:
                    try:
                        result = await _fetch_action_result(context, request, cache, cache_key)
                    except Exception as e:
                        async with fetch_lock:
                            if spec is not None:
                                spec.error = e
                        raise e

                    async with fetch_lock:
                        if spec is not None:
                            spec.result = result
                            spec.event.set()

            finally:
                async with fetch_lock:
                    # Decrement refcount now we're done with the result. If we're the
                    # last request interested in the result then remove it from the
                    # `in_flight_fetches` dictionary.
                    spec = in_flight_fetches.get(cache_key)
                    if spec is not None:
                        spec.refcount -= 1
                        if spec.refcount <= 0:
                            in_flight_fetches.pop(cache_key)

        return web.Response(body=result.SerializeToString())
    return _get_action_result_handler


def get_blob_handler(
    context: Context,
    cache: ResponseCache,
    allow_all: bool=False,
    allowed_origins: Sequence=()
) -> Callable:
    async def _get_blob_handler(request: web.Request) -> web.StreamResponse:
        digest = Digest(
            hash=request.match_info['hash'],
            size_bytes=int(request.match_info['size_bytes'])
        )
        try:
            offset = int(request.rel_url.query.get('offset', '0'))
            limit = int(request.rel_url.query.get('limit', '0'))
        except ValueError:
            raise web.HTTPBadRequest()

        response = web.StreamResponse()

        # We need to explicitly set CORS headers here, because the middleware that
        # normally handles this only adds the header when the response is returned
        # from this function. However, when using a StreamResponse with chunked
        # encoding enabled, the client begins to receive the response when we call
        # `response.write()`. This leads to the request being disallowed due to the
        # missing reponse header for clients executing in browsers.
        cors_headers = get_cors_headers(
            request.headers.get('Origin'), allowed_origins, allow_all)
        response.headers.update(cors_headers)

        if request.rel_url.query.get('raw', '') == '1':
            response.headers['Content-type'] = 'text/plain; charset=utf-8'
        else:
            # Setting the Content-Disposition header so that when
            # downloading a blob with a browser the default name uniquely
            # identifies its contents:
            filename = f"{request.match_info['hash']}_{request.match_info['size_bytes']}"

            # For partial reads also include the indices to prevent file mix-ups:
            if offset != 0 or limit != 0:
                filename += f"_chunk_{offset}-"
                if limit != 0:
                    filename += f"{offset + limit}"

            response.headers['Content-Disposition'] = f'Attachment;filename={filename}'

        prepared = False

        async def _callback(data):
            nonlocal prepared
            if not prepared:
                # Prepare for chunked encoding when the callback is first called,
                # so that we're sure we actually have some data before doing so.
                response.enable_chunked_encoding()
                await response.prepare(request)
                prepared = True
            await response.write(data)
        await _fetch_blob(context, cache, digest, callback=_callback, offset=offset, limit=limit)

        return response
    return _get_blob_handler


def _create_tarball(directory: str, name: str) -> bool:
    """Makes a tarball from a given directory.

    Returns True if the tarball was successfully created, and False if not.

    Args:
        directory (str): The directory to tar up.
        name (str): The name of the tarball to be produced.

    """
    try:
        with tarfile.open(name, 'w:gz') as tarball:
            tarball.add(directory, arcname="")
    except Exception:
        return False
    return True


async def _fetch_blob(
    context: Context,
    cache: ResponseCache,
    digest: Digest,
    message_class: Optional[Type]=None,
    callback: Optional[Callable]=None,
    offset: int=0,
    limit: int=0
) -> Any:
    """Fetch a blob from CAS.

    This function sends a ByteStream Read request for the given digest. If ``callback``
    is set then the callback is called with the data in each ReadResponse message and
    this function returns an empty bytes object once the response is finished. If
    ``message_class`` is set with no ``callback`` set then this function calls
    ``message_class.FromString`` on the fetched blob and returns the result.

    If neither ``callback`` or ``message_class`` are set then this function just returns
    the raw blob that was fetched from CAS.

    Args:
        context (Context): The context to use to send the gRPC request.
        cache (ResponseCache): The response cache to check/update with the fetched blob.
        digest (Digest): The Digest of the blob to fetch from CAS.
        message_class (type): A class which implements a ``FromString`` class method.
            The method should take a bytes object expected to contain the blob fetched
            from CAS.
        callback (callable): A function or other callable to act on a subset of the
            blob contents.
        offset (int): Read offset to start reading the blob at. Defaults to 0, the start
            of the blob.
        limit (int): Maximum number of bytes to read from the blob. Defaults to 0, no
            limit.

    """
    cacheable = digest.size_bytes <= BROWSER_MAX_CACHE_ENTRY_SIZE
    resource_name = f"{context.instance_name}/blobs/{digest.hash}/{digest.size_bytes}"
    blob = None
    if cacheable:
        blob = await cache.get_blob(resource_name)
        if blob is not None and callback is not None:
            if limit > 0:
                try:
                    blob = blob[offset:offset + limit]
                except IndexError:
                    raise web.HTTPBadRequest()
            await callback(blob)

    if blob is None:
        stub = ByteStreamStub(context.cas_channel)
        grpc_request = ReadRequest(
            resource_name=resource_name,
            read_offset=offset,
            read_limit=limit
        )

        blob = b''
        try:
            async for grpc_response in stub.Read(grpc_request):
                if grpc_response.data:
                    if callback is not None:
                        await callback(grpc_response.data)

                    if callback is None or cacheable:
                        blob += grpc_response.data
        except RpcError as e:
            LOGGER.warning(e.details())
            if e.code() == StatusCode.NOT_FOUND:
                raise web.HTTPNotFound()
            raise web.HTTPInternalServerError()

        if cacheable:
            await cache.store_blob(resource_name, blob)  # type: ignore

    if message_class is not None and callback is None:
        return message_class.FromString(blob)
    return blob


async def _download_directory(
    context: Context,
    cache: ResponseCache,
    base: str,
    path: str,
    directory: Directory
) -> None:
    """Download the contents of a directory from CAS.

    This function takes a Directory message and downloads the directory
    contents defined in the message from CAS. Raises a 400 error if the
    directory contains a symlink which points to a location outside the
    initial directory.

    The directory is downloaded recursively depth-first.

    Args:
        context (Context): The context to use for making gRPC requests.
        cache (ResponseCache): The response cache to use when downloading the
            directory contents.
        base (str): The initial directory path, used to check symlinks don't
            escape into the wider filesystem.
        path (str): The path to download the directory into.
        directory (Directory): The Directory message to fetch the contents of.

    """
    for directory_node in directory.directories:
        dir_path = os.path.join(path, directory_node.name)
        os.mkdir(dir_path)
        child = await _fetch_blob(context, cache, directory_node.digest, message_class=Directory)
        await _download_directory(context, cache, base, dir_path, child)

    for file_node in directory.files:
        file_path = os.path.join(path, file_node.name)
        async with aiofiles.open(file_path, 'wb') as f:
            await _fetch_blob(context, cache, file_node.digest, callback=f.write)
        if file_node.is_executable:
            os.chmod(file_path, 0o755)

    for link_node in directory.symlinks:
        link_path = os.path.join(path, link_node.name)
        target_path = os.path.realpath(link_node.target)
        target_relpath = os.path.relpath(base, target_path)
        if target_relpath.startswith(os.pardir):
            raise web.HTTPBadRequest(
                reason="Requested directory contains a symlink targeting a location outside the tarball")

        os.symlink(link_node.target, link_path)


async def _tarball_from_directory(context: Context, cache: ResponseCache, directory: Directory, tmp_dir: str) -> str:
    """Construct a tarball of a directory stored in CAS.

    This function fetches the contents of the given directory message into a
    temporary directory, and then constructs a tarball of the directory. The
    path to this tarball is returned when construction is complete.

    Args:
        context (Context): The context to use to send the gRPC requests.
        cache (ResponseCache): The response cache to use when fetching the
            tarball contents.
        directory (Directory): The Directory message for the directory we're
            making a tarball of.
        tmp_dir (str): Path to a temporary directory to use for storing the
            directory contents and its tarball.

    """
    tarball_dir = tempfile.mkdtemp(dir=tmp_dir)
    tarball_path = os.path.join(tmp_dir, 'directory.tar.gz')
    loop = asyncio.get_event_loop()

    # Fetch the contents of the directory into a temporary directory
    await _download_directory(context, cache, tarball_dir, tarball_dir, directory)

    # Make a tarball from that temporary directory
    # NOTE: We do this using loop.run_in_executor to avoid the
    # synchronous and blocking tarball construction
    tarball_result = await loop.run_in_executor(
        None, _create_tarball, tarball_dir, tarball_path)
    if not tarball_result:
        raise web.HTTPInternalServerError()
    return tarball_path


def get_tarball_handler(context: Context, cache: ResponseCache, allow_all: bool=False,
                        allowed_origins: Sequence=(), tarball_dir: Optional[str]=None) -> Callable:
    """Factory function which returns a handler for tarball requests.

    This function also takes care of cleaning up old incomplete tarball constructions
    when given a named directory to do the construction in.

    The returned handler takes the hash and size_bytes of a Digest of a Directory
    message and constructs a tarball of the directory defined by the message.

    Args:
        context (Context): The context to use to send the gRPC requests.
        allow_all (bool): Whether or not to allow all CORS origins.
        allowed_origins (list): List of valid CORS origins.
        tarball_dir (str): Base directory to use for tarball construction.

    """
    class FetchSpec:
        """Simple class used to store information about a tarball request.

        A class is used here rather than a namedtuple since we need this state
        to be mutable.

        """
        def __init__(self, *,
                     error: Optional[Exception],
                     event: Optional[asyncio.Event],
                     path: str,
                     refcount: int):
            self.error = error
            self.event = event
            self.path = path
            self.refcount = refcount

    in_flight_fetches: Dict[str, FetchSpec] = {}
    fetch_lock = asyncio.Lock()

    # If we have a tarball directory to use, empty all existing tarball constructions from it
    # to provide some form of cleanup after a crash.
    if tarball_dir is not None:
        for path in os.listdir(tarball_dir):
            if path.startswith(TARBALL_DIRECTORY_PREFIX):
                shutil.rmtree(os.path.join(tarball_dir, path))

    async def _get_tarball_handler(request: web.Request) -> web.StreamResponse:
        digest_str = f'{request.match_info["hash"]}/{request.match_info["size_bytes"]}'
        LOGGER.info(f'Received request for a tarball from CAS for blob with digest [{digest_str}]')

        digest = Digest(
            hash=request.match_info['hash'],
            size_bytes=int(request.match_info['size_bytes'])
        )

        tmp_dir = tempfile.mkdtemp(prefix=TARBALL_DIRECTORY_PREFIX, dir=tarball_dir)

        try:
            duplicate_request = False
            event = None
            async with fetch_lock:
                if digest_str in in_flight_fetches:
                    LOGGER.info(f'Deduplicating request for tarball of [{digest_str}]')
                    spec = in_flight_fetches[digest_str]
                    spec.refcount += 1
                    event = spec.event
                    duplicate_request = True
                else:
                    event = asyncio.Event()
                    in_flight_fetches[digest_str] = FetchSpec(
                        error=None,
                        event=event,
                        path='',
                        refcount=1
                    )

            if duplicate_request and event:
                # If we're a duplicate of an existing request, then wait for the
                # existing request to finish tarball creation before reading the
                # path from the cache.
                await event.wait()
                spec = in_flight_fetches[digest_str]
                if spec.error is not None:
                    raise spec.error
                tarball_path = in_flight_fetches[digest_str].path
            else:
                try:
                    directory = await _fetch_blob(context, cache, digest, message_class=Directory)
                    tarball_path = await _tarball_from_directory(context, cache, directory, tmp_dir)
                except web.HTTPError as e:
                    in_flight_fetches[digest_str].error = e
                    if event:
                        event.set()
                    raise e
                except Exception as e:
                    LOGGER.debug("Unexpected error constructing tarball", exc_info=True)
                    in_flight_fetches[digest_str].error = e
                    if event:
                        event.set()
                    raise web.HTTPInternalServerError()

                # Update path in deduplication cache, and set event to notify
                # duplicate requests that the tarball is ready
                async with fetch_lock:
                    if event:
                        in_flight_fetches[digest_str].path = tarball_path
                        event.set()

            response = web.StreamResponse()

            # We need to explicitly set CORS headers here, because the middleware that
            # normally handles this only adds the header when the response is returned
            # from this function. However, when using a StreamResponse with chunked
            # encoding enabled, the client begins to receive the response when we call
            # `response.write()`. This leads to the request being disallowed due to the
            # missing reponse header for clients executing in browsers.
            cors_headers = get_cors_headers(
                request.headers.get('Origin'), allowed_origins, allow_all)
            response.headers.update(cors_headers)

            response.enable_chunked_encoding()
            await response.prepare(request)

            async with aiofiles.open(tarball_path, 'rb') as tarball:
                await tarball.seek(0)
                chunk = await tarball.read(1024)
                while chunk:
                    await response.write(chunk)
                    chunk = await tarball.read(1024)
            return response

        except RpcError as e:
            LOGGER.warning(e.details())
            if e.code() == StatusCode.NOT_FOUND:
                raise web.HTTPNotFound()
            raise web.HTTPInternalServerError()

        finally:
            cleanup = False
            async with fetch_lock:
                # Decrement refcount now we're done with the tarball. If we're the
                # last request interested in the tarball then remove it along with
                # its construction directory.
                spec = in_flight_fetches[digest_str]
                spec.refcount -= 1
                if spec.refcount <= 0:
                    cleanup = True
                    in_flight_fetches.pop(digest_str)
            if cleanup:
                shutil.rmtree(tmp_dir)
    return _get_tarball_handler


def logstream_handler(context: Context) -> Callable:
    async def _logstream_handler(request):
        LOGGER.info('Receieved request for a LogStream websocket')
        stub = ByteStreamStub(context.logstream_channel)
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        async for msg in ws:
            if msg.type == WSMsgType.BINARY:
                read_request = ReadRequest()
                read_request.ParseFromString(msg.data)

                read_request.resource_name = f'{context.instance_name}/{read_request.resource_name}'
                try:
                    async for response in stub.Read(read_request):
                        serialized_response = response.SerializeToString()
                        if serialized_response:
                            ws_response = {
                                "resource_name": read_request.resource_name,
                                "data": b64encode(serialized_response).decode('utf-8'),
                                "complete": False
                            }
                            await ws.send_json(ws_response)
                    ws_response = {
                        "resource_name": read_request.resource_name,
                        "data": "",
                        "complete": True
                    }
                    await ws.send_json(ws_response)
                except RpcError as e:
                    LOGGER.warning(e.details())
                    if e.code() == StatusCode.NOT_FOUND:
                        ws_response = {
                            "resource_name": read_request.resource_name,
                            "data": "NOT_FOUND",
                            "complete": True
                        }
                        await ws.send_json(ws_response)
                    ws_response = {
                        "resource_name": read_request.resource_name,
                        "data": "INTERNAL",
                        "complete": True
                    }
                    await ws.send_json(ws_response)
    return _logstream_handler
