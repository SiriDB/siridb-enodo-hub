import functools

from lib.socketio.subscriptionmanager import SubscriptionManager
from lib.util.util import safe_json_dumps
from lib.webserver.auth import EnodoAuth
from lib.webserver.basehandler import BaseHandler


def socketio_auth_required(handler):
    @functools.wraps(handler)
    async def wrapper(cls, sid, data, event):
        async with cls._sio.session(sid) as session:
            if not session['auth']:
                raise ConnectionRefusedError('unauthorized')
            resp = await handler(cls, sid, data, event)
        return resp

    return wrapper


class SocketIoHandler:
    _sio = None

    @classmethod
    async def prepare(cls, sio):
        cls._sio = sio

    @classmethod
    async def connect(cls, sid, environ):
        await cls._sio.save_session(sid, {'auth': False})
        pass  # TODO verbose logging

    @classmethod
    async def authenticate(cls, sid, data):
        user = data.get('username')
        password = data.get('password')

        if not await EnodoAuth.auth.check_credentials(user, password):
            raise ConnectionRefusedError('authentication failed')

        async with cls._sio.session(sid) as session:
            session['auth'] = True

    @classmethod
    async def disconnect(cls, sid):
        await SubscriptionManager.remove_subscriber(sid)
        pass  # TODO verbose logging

    @classmethod
    @socketio_auth_required
    async def get_all_series(cls, sid, regex_filter, event):
        return await cls._get_all_series(sid, regex_filter, event)

    @classmethod
    async def _get_all_series(cls, sid, regex_filter, event):
        regex_filter = regex_filter if regex_filter else None
        resp = await BaseHandler.resp_get_monitored_series(regex_filter)
        return safe_json_dumps(resp)

    @classmethod
    @socketio_auth_required
    async def get_serie_details(cls, sid, data, event):
        serie_name = data.get('serie_name')
        resp = await BaseHandler.resp_get_monitored_serie_details(serie_name, include_points=True)
        return safe_json_dumps(resp)

    @classmethod
    @socketio_auth_required
    async def create_series(cls, sid, data, event):
        if not isinstance(data, dict):
            resp = {'error': 'Incorrect data'}
        else:
            series_name = data.get('name')
            series_model = data.get('model')
            if series_name is not None and series_model is not None:
                resp, status = await BaseHandler.resp_add_serie(data)
            else:
                resp = {'error': 'Missing required field(s)'}
        return safe_json_dumps(resp)

    @classmethod
    @socketio_auth_required
    async def remove_series(cls, sid, data, event):
        if not isinstance(data, dict):
            resp = {'error': 'Incorrect data'}
        else:
            series_name = data.get('name')
            if series_name is not None:
                resp = {'status': await BaseHandler.resp_remove_serie(series_name)}
            else:
                resp = {'error': 'Missing required field(s)'}
        return safe_json_dumps(resp)

    @classmethod
    @socketio_auth_required
    async def get_enodo_models(cls, sid, data, event=None):
        return await BaseHandler.resp_get_possible_analyser_models()

    @classmethod
    @socketio_auth_required
    async def subscribe_series(cls, sid, data, event=None):
        print(cls, sid, data, event)
        if cls._sio is not None:
            cls._sio.enter_room(sid, 'series_updates')
            return await cls._get_all_series(None, None, None)

    @classmethod
    @socketio_auth_required
    async def unsubscribe_series(cls, sid, data, event):
        if cls._sio is not None:
            cls._sio.leave_room(sid, 'series_updates')

    @classmethod
    @socketio_auth_required
    async def subscribe_filtered_series(cls, sid, data, event):
        if isinstance(data, dict):
            regex = data.get('filter')
        else:
            regex = None

        if cls._sio is not None and regex is not None:
            await SubscriptionManager.add_subscriber(sid, regex)
            return await cls._get_all_series(None, regex, None)
        else:
            return {'error': 'incorrect regex'}

    @classmethod
    @socketio_auth_required
    async def unsubscribe_filtered_series(cls, sid, data, event):
        if cls._sio is not None:
            cls._sio.leave_room(sid, 'series_updates')

    @classmethod
    async def internal_updates_series_subscribers(cls, change_type, series_name, series_data):
        if cls._sio is not None:
            await cls._sio.emit('series_updates', {
                'change_type': change_type,
                'series_name': series_name,
                'series_data': series_data
            }, room='series_updates')

        filtered_subs = await SubscriptionManager.get_subscriptions_for_serie_name(series_name)
        for sub in filtered_subs:
            await cls._sio.emit('series_updates', {
                'change_type': change_type,
                'series_name': series_name,
                'series_data': series_data
            }, room=sub.get('sid'))
