import json

from aiohttp import web

from lib.util.util import safe_json_dumps
from lib.webserver.handlers import Handlers


async def websocket_index(request):
    return web.json_response()


def setup_socketio_handlers(sio):
    @sio.event
    async def connect(sid, environ):
        print("connect ", sid)

    @sio.event
    async def disconnect(sid):
        print('disconnect ', sid)

    @sio.event
    async def get_all_series(sid, regex_filter):
        regex_filter = regex_filter if regex_filter else None
        resp = await Handlers.resp_get_monitored_series(regex_filter)
        return safe_json_dumps(resp)
