import datetime
import json

import psutil as psutil
from aiohttp import web

from lib.analyser.model.arimamodel import ARIMAModel
from lib.config.config import Config
from lib.logging.eventlogger import EventLogger
from lib.serie.seriemanager import SerieManager
from lib.siridb.siridb import SiriDB


class Handlers:
    _socket_server = None

    @classmethod
    async def prepare(cls, socket_server):
        cls._socket_server = socket_server

    @classmethod
    async def get_monitored_series(cls, request):
        """
        Returns a list of monitored series
        :param request:
        :return:
        """
        return web.json_response(data={'data': list(await SerieManager.get_series_to_dict())})

    @classmethod
    async def get_monitored_serie_details(cls, request):
        """
        Returns all details and data points of a specific serie.
        :param request:
        :return:
        """
        serie = await SerieManager.get_serie(request.match_info['serie_name'])

        if serie is None:
            return web.json_response(data={'data': ''}, status=404)

        serie_data = await serie.to_dict()
        _siridb_client = SiriDB()
        serie_points = await _siridb_client.query_serie_data(await serie.get_name(), "*")
        serie_data['points'] = serie_points.get(await serie.get_name())
        if await serie.get_analysed():
            saved_analysis = ARIMAModel.load(await serie.get_name())
            serie_data['forecast_points'] = saved_analysis.forecast_values
        else:
            serie_data['forecast_points'] = []

        return web.json_response(data={'data': serie_data})

    @classmethod
    async def add_serie(cls, request):
        """
        Add new serie to the application.
        :param request:
        :return:
        """
        required_fields = ['m', 'name', 'unit']
        data = await request.json()

        if all(required_field in data for required_field in required_fields):
            serie_data = {
                'serie_name': data.get('name'),
                'parameters': {
                    'm': data.get('m', 12),
                    'd': data.get('d', None),
                    'D': data.get('D', None)}
            }

            # Config.db.insert(serie_data)
            Config.names_enabled_series_for_analysis.append(data.get('name'))
            Config.enabled_series_for_analysis[data.get('name')] = serie_data
            await SerieManager.check_for_config_changes()

            return web.json_response(data={'data': list(await SerieManager.get_series_to_dict())}, status=201)

        return web.json_response(data={'error': 'missing required fields'}, status=400)

    @classmethod
    async def remove_serie(cls, request):
        """
        Remove serie by it's name
        :param request:
        :return:
        """
        if await SerieManager.remove_serie(request.match_info['serie_name']):
            return web.json_response(data={}, status=200)
        return web.json_response(data={}, status=404)

    @classmethod
    async def get_siridb_status(cls, request):
        """
        Get siridb connection status
        :param request:
        :return:
        """

        return web.json_response(data={'data': {
            'connected': SiriDB.siridb_connected,
            'status': SiriDB.siridb_status
        }}, status=200)

    @classmethod
    async def get_siridb_enodo_status(cls, request):
        """
        Get status of this analyser instance
        :param request:
        :return:
        """

        cpu_usage = psutil.cpu_percent()
        return web.json_response(data={'data': {
            'cpu_usage': cpu_usage
        }}, status=200)

    @classmethod
    async def get_event_log(cls, request):
        """
        Returns event log
        :param request:
        :return:
        """
        log = EventLogger.get()
        return web.json_response(data={'data': log}, status=200)

    @classmethod
    async def get_settings(cls, request):
        settings = await cls.build_settings_dict()

        return web.json_response(data={'data': settings}, status=200)

    @classmethod
    async def build_settings_dict(cls):
        settings = {}
        fields = ['pipe_path', 'min_data_points', 'analysis_save_path', 'siridb_host', 'siridb_port', 'siridb_user',
                  'siridb_password', 'siridb_database']
        for field in fields:
            settings[field] = getattr(Config, field)
        return settings

    @classmethod
    async def set_settings(cls, request):
        """
        Override settings.
        :param request:
        :return:
        """
        data = await request.json()

        fields = ['pipe_path', 'min_data_points', 'analysis_save_path', 'siridb_host', 'siridb_port', 'siridb_user',
                  'siridb_password', 'siridb_database']

        for field in fields:
            if field in data:
                setattr(Config, field, data[field])

        await Config.save_config()

        settings = await cls.build_settings_dict()

        return web.json_response(data={'data': settings}, status=200)

    @classmethod
    async def get_connected_clients(cls, request):
        """
        Return connected listeners and workers
        :param request:
        :return:
        """
        return web.json_response(data={
            'data': {'listeners': await cls._socket_server.get_connected_clients('listeners'),
                     'workers': await cls._socket_server.get_connected_clients('workers')}}, status=200,
            dumps=cls._safe_json_dumps)

    @classmethod
    def _safe_json_dumps(cls, data):
        return json.dumps(data, default=cls._json_datetime_serializer)

    @classmethod
    def _json_datetime_serializer(cls, o):
        if isinstance(o, datetime.datetime):
            return o.__str__()
