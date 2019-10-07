import functools

from aiohttp import web
from aiohttp.web_response import Response

from lib.util.util import safe_json_dumps
from lib.webserver.auth import EnodoAuth
from lib.webserver.basehandler import BaseHandler


def socketio_auth_wrapper(handler):
    @functools.wraps(handler)
    async def wrapper(*args):
        resp = await handler(*args)
        if isinstance(resp, Response):
            if resp.status == 401:
                raise ConnectionRefusedError('authentication failed')
        return resp

    return wrapper


class SocketIoHandler:

    @classmethod
    @socketio_auth_wrapper
    @EnodoAuth.auth.required
    async def connect(cls, sid, environ, request):
        print("connect ", sid, environ.get('aiohttp.request'))

    @classmethod
    async def disconnect(cls, sid):
        print('disconnect ', sid)

    @classmethod
    async def get_all_series(cls, sid, regex_filter, event):
        regex_filter = regex_filter if regex_filter else None
        resp = await BaseHandler.resp_get_monitored_series(regex_filter)
        print(resp)
        return safe_json_dumps(resp)
