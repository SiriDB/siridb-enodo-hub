import functools

from lib.socketio.subscriptionmanager import SubscriptionManager
from lib.util import safe_json_dumps
from lib.webserver.auth import EnodoAuth
from lib.webserver.basehandler import BaseHandler
from lib.socketio import SUBSCRIPTION_CHANGE_TYPE_ADD, SUBSCRIPTION_CHANGE_TYPE_DELETE
from lib.series.seriesmanager import SeriesManager


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

        if not await EnodoAuth.auth.check_credentials(user, password, None):
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
    async def get_series_details(cls, sid, data, event):
        series_name = data.get('series_name')
        resp = await BaseHandler.resp_get_monitored_series_details(series_name, include_points=True)
        return safe_json_dumps(resp)

    @classmethod
    @socketio_auth_required
    async def create_series(cls, sid, data, event):
        if not isinstance(data, dict):
            resp = {'error': 'Incorrect data'}
        else:
            series_name = data.get('name')
            if series_name is not None:
                resp, status = await BaseHandler.resp_add_series(data)
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
                resp = {'status': await BaseHandler.resp_remove_series(series_name)}
            else:
                resp = {'error': 'Missing required field(s)'}
        return safe_json_dumps(resp)

    @classmethod
    @socketio_auth_required
    async def update_series(cls, sid, data, event):
        if not isinstance(data, dict):
            resp = {'error': 'Incorrect data'}
        else:
            series_name = data.get('name')
            series_data = data.get('data')
            if series_name is not None:
                resp, status = await BaseHandler.resp_update_series(series_name, series_data)
            else:
                resp = {'error': 'Missing required field(s)'}
        return safe_json_dumps(resp)

    @classmethod
    @socketio_auth_required
    async def get_enodo_models(cls, sid, data, event=None):
        return await BaseHandler.resp_get_possible_analyser_models()

    @classmethod
    @socketio_auth_required
    async def get_event_outputs(cls, sid, data, event=None):
        return await BaseHandler.resp_get_event_outputs()

    @classmethod
    @socketio_auth_required
    async def add_event_output(cls, sid, data, event=None):
        output_type = data.get('output_type')
        output_data = data.get('data')
        return await BaseHandler.resp_add_event_output(output_type, output_data)

    @classmethod
    @socketio_auth_required
    async def update_event_output(cls, sid, data, event=None):
        output_id = data.get('id')
        output_data = data.get('data')
        return await BaseHandler.resp_update_event_output(output_id, output_data)

    @classmethod
    @socketio_auth_required
    async def remove_event_output(cls, sid, data, event=None):
        output_id = data.get('output_id')
        return await BaseHandler.resp_remove_event_output(output_id)

    @classmethod
    @socketio_auth_required
    async def subscribe_queue(cls, sid, data, event=None):
        if cls._sio is not None:
            cls._sio.enter_room(sid, 'job_updates')
        return safe_json_dumps(await BaseHandler.resp_get_jobs_queue())

    @classmethod
    @socketio_auth_required
    async def unsubscribe_queue(cls, sid, data, event):
        if cls._sio is not None:
            cls._sio.leave_room(sid, 'job_updates')

    @classmethod
    @socketio_auth_required
    async def subscribe_series(cls, sid, data, event=None):
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
    @socketio_auth_required
    async def subscribe_enodo_models(cls, sid, data, event):
        if cls._sio is not None:
            cls._sio.enter_room(sid, 'enodo_model_updates')
            return await BaseHandler.resp_get_possible_analyser_models()

    @classmethod
    @socketio_auth_required
    async def subscribe_event_output(cls, sid, data, event):
        if cls._sio is not None:
            cls._sio.enter_room(sid, 'event_output_updates')
            return await BaseHandler.resp_get_event_outputs()

    @classmethod
    @socketio_auth_required
    async def unsubscribe_enodo_models(cls, sid, data, event):
        if cls._sio is not None:
            cls._sio.leave_room(sid, 'enodo_model_updates')

    @classmethod
    @socketio_auth_required
    async def get_enodo_hub_status(cls, sid, data, event):
        resp = await BaseHandler.resp_get_enodo_hub_status()
        return resp

    @classmethod
    async def internal_updates_queue_subscribers(cls, change_type, job):
        if cls._sio is not None:
            await cls._sio.emit('update', {
                'resource': 'job',
                'updateType': change_type,
                'resourceData': job
            }, room='job_updates')

    @classmethod
    async def internal_updates_series_subscribers(cls, change_type, series, name=None):
        if cls._sio is not None:
            await cls._sio.emit('update', {
                'resource': 'series',
                'updateType': change_type,
                'resourceData': series
            }, room='series_updates')

        filtered_subs = await SubscriptionManager.get_subscriptions_for_series_name(name)
        for sub in filtered_subs:
            await cls._sio.emit('update', {
                'resource': 'series',
                'updateType': change_type,
                'resourceData': series
            }, room=sub.get('sid'))

    @classmethod
    async def internal_updates_enodo_models_subscribers(cls, change_type, model_data):
        if cls._sio is not None:
            await cls._sio.emit('update', {
                'resource': 'enodo_model',
                'updateType': change_type,
                'resourceData': model_data
            }, room='enodo_model_updates')
