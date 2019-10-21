import functools

from aiohttp.web_response import Response

from lib.socketio.subscriptionmanager import SubscriptionManager
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
    _sio = None

    @classmethod
    async def prepare(cls, sio):
        cls._sio = sio

    @classmethod
    @socketio_auth_wrapper
    @EnodoAuth.auth.required
    async def connect(cls, sid, environ, request):
        pass  # TODO verbose logging

    @classmethod
    async def disconnect(cls, sid):
        await SubscriptionManager.remove_subscriber(sid)
        pass  # TODO verbose logging

    @classmethod
    async def get_all_series(cls, sid, regex_filter, event):
        regex_filter = regex_filter if regex_filter else None
        resp = await BaseHandler.resp_get_monitored_series(regex_filter)
        return safe_json_dumps(resp)

    @classmethod
    async def subscribe_series(cls, sid, data, event):
        if cls._sio is not None:
            cls._sio.enter_room(sid, 'series_updates')
            await cls._sio.emit('series_updates', {'data': await cls.get_all_series(None, None, None)}, room=sid)

    @classmethod
    async def unsubscribe_series(cls, sid, data, event):
        if cls._sio is not None:
            cls._sio.leave_room(sid, 'series_updates')

    @classmethod
    async def subscribe_filtered_series(cls, sid, data, event):
        if isinstance(data, dict):
            regex = data.get('filter')
        else:
            regex = None

        if cls._sio is not None and regex is not None:
            await SubscriptionManager.add_subscriber(sid, regex)
            await cls._sio.emit('series_updates', {'data': await cls.get_all_series(None, regex, None)}, room=sid)
        else:
            await cls._sio.emit('series_updates', {'error': 'incorrect regex'}, room=sid)

    @classmethod
    async def unsubscribe_filtered_series(cls, sid, data, event):
        if cls._sio is not None:
            cls._sio.leave_room(sid, 'series_updates')

    @classmethod
    async def internal_updates_series_subscribers(cls):
        if cls._sio is not None:
            await cls._sio.emit('series_updates', {'data': await cls.get_all_series(None, None, None)},
                                room='series_updates')

        filtered_subs = await SubscriptionManager.get_all_filtered_subscriptions()
        for sub in filtered_subs:
            print(sub)
            await cls._sio.emit('series_updates', {'data': await cls.get_all_series(None, sub.get('regex'), None)},
                                room=sub.get('sid'))
