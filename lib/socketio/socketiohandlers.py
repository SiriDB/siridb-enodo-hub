import functools

from lib.socketio.subscriptionmanager import SubscriptionManager
from lib.util import safe_json_dumps
from lib.webserver.auth import EnodoAuth
from lib.webserver.basehandler import BaseHandler
from lib.serverstate import ServerState


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
        # TODO verbose logging

    @classmethod
    async def authenticate(cls, sid, data):
        user = data.get('username')
        password = data.get('password')

        if not await EnodoAuth.auth.check_credentials(user, password, None):
            return False

        async with cls._sio.session(sid) as session:
            session['auth'] = True
        
        return True

    @classmethod
    async def disconnect(cls, sid):
        await SubscriptionManager.remove_subscriber(sid)
        # TODO verbose logging

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
        resp = await BaseHandler.resp_get_single_monitored_series(
            series_name)
        return safe_json_dumps(resp)

    @classmethod
    @socketio_auth_required
    async def get_series_forecasts(cls, sid, data, event):
        series_name = data.get('series_name')
        resp = await BaseHandler.resp_get_series_forecasts(
            series_name)
        return safe_json_dumps(resp)

    @classmethod
    @socketio_auth_required
    async def get_series_anomalies(cls, sid, data, event):
        series_name = data.get('series_name')
        resp = await BaseHandler.resp_get_series_anomalies(
            series_name)
        return safe_json_dumps(resp)

    @classmethod
    @socketio_auth_required
    async def get_series_static_rules_hits(cls, sid, data, event):
        series_name = data.get('series_name')
        resp = await BaseHandler.resp_get_series_static_rules_hits(
            series_name)
        return safe_json_dumps(resp)

    @classmethod
    @socketio_auth_required
    async def run_siridb_query(cls, sid, data, event):
        query = data.get('query')
        resp, status = await BaseHandler.resp_run_siridb_query(
            query)
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
                pass
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
                resp = {'status':
                        await BaseHandler.resp_remove_series(series_name)}
            else:
                resp = {'error': 'Missing required field(s)'}
        return safe_json_dumps(resp)

    @classmethod
    @socketio_auth_required
    async def update_series(cls, sid, data, event):
        if not isinstance(data, dict):
            return safe_json_dumps({'error': 'Incorrect data'})
        series_name = data.get('name')
        series_data = data.get('data')
        if series_name is not None:
            resp, status = await BaseHandler.resp_update_series(
                series_name, series_data)
        else:
            resp = {'error': 'Missing required field(s)'}
        return safe_json_dumps(resp)

    @classmethod
    @socketio_auth_required
    async def get_enodo_modules(cls, sid, data, event=None):
        return await BaseHandler.resp_get_possible_analyser_modules()

    @classmethod
    @socketio_auth_required
    async def get_open_jobs(cls, sid, data, event=None):
        return await BaseHandler.resp_get_open_jobs()

    @classmethod
    @socketio_auth_required
    async def get_active_jobs(cls, sid, data, event=None):
        return await BaseHandler.resp_get_active_jobs()

    @classmethod
    @socketio_auth_required
    async def get_failed_jobs(cls, sid, data, event=None):
        return await BaseHandler.resp_get_failed_jobs()

    @classmethod
    @socketio_auth_required
    async def resolve_failed_job(cls, sid, data, event=None):
        return await BaseHandler.resp_resolve_failed_job(
            data.get('series_name'))

    @classmethod
    @socketio_auth_required
    async def get_event_outputs(cls, sid, data, event=None):
        return await BaseHandler.resp_get_event_outputs()

    @classmethod
    @socketio_auth_required
    async def add_event_output(cls, sid, data, event=None):
        output_type = data.get('output_type')
        output_data = data.get('data')
        return await BaseHandler.resp_add_event_output(
            output_type, output_data)

    @classmethod
    @socketio_auth_required
    async def update_event_output(cls, sid, data, event=None):
        output_id = data.get('id')
        output_data = data.get('data')
        return await BaseHandler.resp_update_event_output(
            output_id, output_data)

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
    async def subscribe_enodo_modules(cls, sid, data, event):
        if cls._sio is not None:
            cls._sio.enter_room(sid, 'enodo_module_updates')
            return await BaseHandler.resp_get_possible_analyser_modules()

    @classmethod
    @socketio_auth_required
    async def subscribe_event_output(cls, sid, data, event):
        if cls._sio is not None:
            cls._sio.enter_room(sid, 'event_output_updates')
            return await BaseHandler.resp_get_event_outputs()

    @classmethod
    @socketio_auth_required
    async def unsubscribe_enodo_modules(cls, sid, data, event):
        if cls._sio is not None:
            cls._sio.leave_room(sid, 'enodo_module_updates')

    @classmethod
    @socketio_auth_required
    async def get_enodo_hub_status(cls, sid, data, event):
        resp = await BaseHandler.resp_get_enodo_hub_status()
        return resp

    @classmethod
    @socketio_auth_required
    async def get_enodo_settings(cls, sid, data, event):
        resp = await BaseHandler.resp_get_enodo_config()
        return resp

    @classmethod
    @socketio_auth_required
    async def update_enodo_hub_settings(cls, sid, data, event):
        resp = await BaseHandler.resp_set_config(data)
        return resp

    @classmethod
    @socketio_auth_required
    async def get_enodo_stats(cls, sid, data, event):
        resp = await BaseHandler.resp_get_enodo_stats()
        return resp

    @classmethod
    @socketio_auth_required
    async def subscribe_siridb_status(cls, sid, data, event):
        if cls._sio is not None:
            cls._sio.enter_room(sid, 'siridb_status_updates')
            return ServerState.siridb_conn_status

    @classmethod
    @socketio_auth_required
    async def unsubscribe_siridb_status(cls, sid, data, event):
        if cls._sio is not None:
            cls._sio.leave_room(sid, 'siridb_status_updates')

    @classmethod
    @socketio_auth_required
    async def get_enodo_labels(cls, sid, data, event):
        return await BaseHandler.resp_get_enodo_labels()

    @classmethod
    @socketio_auth_required
    async def add_enodo_label(cls, sid, data, event):
        return await BaseHandler.resp_add_enodo_label(data)

    @classmethod
    @socketio_auth_required
    async def remove_enodo_label(cls, sid, data, event):
        return await BaseHandler.resp_remove_enodo_label(data)

    @classmethod
    async def internal_updates_queue_subscribers(cls, change_type, job):
        if cls._sio is not None:
            await cls._sio.emit('update', {
                'resource': 'job',
                'updateType': change_type,
                'resourceData': job
            }, room='job_updates')

    @classmethod
    async def internal_updates_series_subscribers(cls, change_type,
                                                  series, name=None):
        if cls._sio is not None:
            await cls._sio.emit('update', {
                'resource': 'series',
                'updateType': change_type,
                'resourceData': series
            }, room='series_updates')

        filtered_subs = \
            await SubscriptionManager.get_subscriptions_for_series_name(name)
        for sub in filtered_subs:
            await cls._sio.emit('update', {
                'resource': 'series',
                'updateType': change_type,
                'resourceData': series
            }, room=sub.get('sid'))

    @classmethod
    async def internal_updates_enodo_modules_subscribers(cls, change_type,
                                                        module_data):
        if cls._sio is not None:
            await cls._sio.emit('update', {
                'resource': 'enodo_module',
                'updateType': change_type,
                'resourceData': module_data
            }, room='enodo_module_updates')
