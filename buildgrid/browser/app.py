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

import os
import re

from aiohttp import web
from aiohttp_middlewares import cors_middleware
from aiohttp_middlewares.annotations import Handler, Middleware, UrlCollection

from buildgrid.browser import rest_api, utils


def serve_client(base_path):
    async def _serve_client(request):
        if not request.match_info['path']:
            path = os.path.join(base_path, 'index.html')
        else:
            path = os.path.join(base_path, request.match_info['path'])
            if not os.path.exists(path):
                path = os.path.join(base_path, 'index.html')
        return web.FileResponse(path)
    return _serve_client


def cors_middleware_with_error(
    allow_all: bool=False,
    origins: UrlCollection=None,
    urls: UrlCollection=None
) -> Middleware:
    middleware = cors_middleware(allow_all=allow_all, origins=origins, urls=urls)

    @web.middleware
    async def _middleware(request: web.Request, handler: Handler) -> web.StreamResponse:
        origin = request.headers.get("Origin")
        headers = utils.get_cors_headers(origin, origins, allow_all)
        try:
            return await middleware(request, handler)

        except web.HTTPNotFound:
            raise web.HTTPNotFound(headers=headers)

        except web.HTTPBadRequest:
            raise web.HTTPBadRequest(headers=headers)

        except web.HTTPInternalServerError:
            raise web.HTTPInternalServerError(headers=headers)
    return _middleware


def create_app(context, cache, cors_origins, static_path=None, allow_cancelling_operations=False, tarball_dir=None):
    allow_all = len(cors_origins) == 0
    app = web.Application(
        middlewares=(
            cors_middleware_with_error(allow_all=allow_all,
                                       origins=cors_origins,
                                       urls=[re.compile(r'^\/api')]),
        )
    )
    routes = [
        web.get('/api/v1/build_events', rest_api.query_build_events_handler(context)),
        web.get('/api/v1/operations', rest_api.list_operations_handler(context, cache)),
        web.get('/api/v1/operations/{name}', rest_api.get_operation_handler(context, cache)),
        web.get(
            '/api/v1/operations/{name}/request_metadata',
            rest_api.get_operation_request_metadata_handler(context)
        ),
        web.get('/api/v1/action_results/{hash}/{size_bytes}', rest_api.get_action_result_handler(context, cache)),
        web.get(
            '/api/v1/blobs/{hash}/{size_bytes}',
            rest_api.get_blob_handler(context, cache, allow_all=allow_all, allowed_origins=cors_origins)
        ),
        web.get(
            '/api/v1/tarballs/{hash}_{size_bytes}.tar.gz',
            rest_api.get_tarball_handler(
                context,
                cache,
                allow_all=allow_all,
                allowed_origins=cors_origins,
                tarball_dir=tarball_dir
            )
        ),
        web.get('/ws/logstream', rest_api.logstream_handler(context))
    ]

    if allow_cancelling_operations:
        routes.append(web.delete('/api/v1/operations/{name}', rest_api.cancel_operation_handler(context)))

    if static_path is not None:
        routes.append(web.get('/{path:.*}', serve_client(static_path)))
    app.add_routes(routes)
    return app
