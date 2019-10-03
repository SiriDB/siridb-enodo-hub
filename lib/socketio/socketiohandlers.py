from aiohttp import web

from lib.util.util import safe_json_dumps
from lib.webserver.basehandler import BaseHandler


class SocketIoHandler:

    @classmethod
    async def connect(cls, sid, environ):
        print("connect ", sid)

    @classmethod
    async def disconnect(cls, sid):
        print('disconnect ', sid)

    @classmethod
    async def get_all_series(cls, sid, regex_filter):
        regex_filter = regex_filter if regex_filter else None
        resp = await BaseHandler.resp_get_monitored_series(regex_filter)
        return safe_json_dumps(resp)
