import psutil as psutil
import urllib.parse
from aiohttp import web

from aiohttp_apispec import (
    docs,
    request_schema,
    response_schema)
from aiohttp_basicauth import BasicAuthMiddleware

from lib.config.config import Config
from lib.logging.eventlogger import EventLogger
from lib.siridb.siridb import SiriDB
from lib.socket.clientmanager import ClientManager
from lib.util.util import safe_json_dumps
from lib.api.apischemas import SchemaResponseSeries, SchemaResponseError, SchemaResponseSeriesDetails, \
    SchemaRequestCreateSeries, SchemaSeries, SchemaResponseModels
from lib.webserver.auth import EnodoAuth
from lib.webserver.basehandler import BaseHandler

auth = BasicAuthMiddleware(username=None, password=None, force=False)


class ApiHandlers:

    @classmethod
    async def prepare(cls):
        EnodoAuth.auth.username = Config.basic_auth_username
        EnodoAuth.auth.password = Config.basic_auth_password

    @classmethod
    @docs(
        tags=["public_api"],
        summary="Get list of active series",
        description="Get list of active series with general info. You can filter with a regex on the series names.",
        parameters=[{
            'in': 'query',
            'name': 'filter',
            'description': 'regex string for filtering on series names. Should be URL encoded!',
            'schema': {'type': 'string', 'format': 'regex'}
        }]
    )
    @response_schema(SchemaResponseSeries(), 200)
    @response_schema(SchemaResponseError(), 400)
    @EnodoAuth.auth.required
    async def get_monitored_series(cls, request):
        """
        Returns a list of monitored series
        :param request:
        :return:
        """
        regex_filter = urllib.parse.unquote(request.rel_url.query['filter']) if 'filter' in request.rel_url.query else None
        # TODO implement filter

        return web.json_response(data=await BaseHandler.resp_get_monitored_series(regex_filter),
                                 dumps=safe_json_dumps)

    @classmethod
    @docs(
        tags=["public_api"],
        summary="Get details of a certain series",
        description="Get details of a certain series, including forecasted datapoints",
        parameters=[{
            'in': 'query',
            'name': 'include_points',
            'description': 'include (original) data points of serie in response, default=false',
            'schema': {'type': 'boolean'}
        }]
    )
    @response_schema(SchemaResponseSeriesDetails(), 200)
    @response_schema(SchemaResponseError(), 400)
    @EnodoAuth.auth.required
    async def get_monitored_serie_details(cls, request):
        """
        Returns all details and data points of a specific serie.
        :param request:
        :return:
        """
        include_points = True if 'include_points' in request.rel_url.query and request.rel_url.query[
            'include_points'] == 'true' else False

        return web.json_response(
            data=await BaseHandler.resp_get_monitored_serie_details(request.match_info['serie_name'], include_points),
            dumps=safe_json_dumps)

    @classmethod
    @docs(
        tags=["public_api"],
        summary="Add a new series",
        description="Add a new series with specified model and optional parameters",
        parameters=[]
    )
    @request_schema(SchemaRequestCreateSeries())
    @response_schema(SchemaSeries(), 201)
    @response_schema(SchemaResponseError(), 400)
    @EnodoAuth.auth.required
    async def add_serie(cls, request):
        """
        Add new serie to the application.
        :param request:
        :return:
        """
        data = await request.json()

        return web.json_response(data=await BaseHandler.resp_add_serie(data), status=201)

    @classmethod
    @docs(
        tags=["public_api"],
        summary="Remove a new series",
        description="Remove a certain series by its name",
        parameters=[]
    )
    @response_schema(SchemaResponseError(), 400)
    @EnodoAuth.auth.required
    async def remove_serie(cls, request):
        """
        Remove series with certain name
        :param request:
        :return:
        """
        serie_name = request.match_info['serie_name']
        return web.json_response(data={}, status=await BaseHandler.resp_remove_serie(serie_name))

    @classmethod
    @docs(
        tags=["public_api"],
        summary="Get a list of possible models",
        description="Get a list of possible models to use for adding a series",
        parameters=[]
    )
    @response_schema(SchemaResponseModels(), 200)
    @response_schema(SchemaResponseError(), 400)
    @EnodoAuth.auth.required
    async def get_possible_analyser_models(cls, request):
        """
        Returns list of possible models with corresponding parameters
        :param request:
        :return:
        """

        return web.json_response(data=await BaseHandler.resp_get_possible_analyser_models(), status=200)

    @classmethod
    @EnodoAuth.auth.required
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
    @EnodoAuth.auth.required
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
    @EnodoAuth.auth.required
    async def get_event_log(cls, request):
        """
        Returns event log
        :param request:
        :return:
        """
        log = EventLogger.get()
        return web.json_response(data={'data': log}, status=200)

    @classmethod
    @EnodoAuth.auth.required
    async def get_settings(cls, request):
        settings = await cls.build_settings_dict()

        return web.json_response(data={'data': settings}, status=200)

    @classmethod
    @EnodoAuth.auth.required
    async def build_settings_dict(cls):
        settings = {}
        fields = ['min_data_points', 'analysis_save_path', 'siridb_host', 'siridb_port', 'siridb_user',
                  'siridb_password', 'siridb_database']
        for field in fields:
            settings[field] = getattr(Config, field)
        return settings

    @classmethod
    @EnodoAuth.auth.required
    async def set_settings(cls, request):
        """
        Override settings.
        :param request:
        :return:
        """
        data = await request.json()

        fields = ['min_data_points', 'analysis_save_path', 'siridb_host', 'siridb_port', 'siridb_user',
                  'siridb_password', 'siridb_database']

        for field in fields:
            if field in data:
                setattr(Config, field, data[field])

        await Config.save_config()

        settings = await cls.build_settings_dict()

        return web.json_response(data={'data': settings}, status=200)

    @classmethod
    @EnodoAuth.auth.required
    async def get_connected_clients(cls, request):
        """
        Return connected listeners and workers
        :param request:
        :return:
        """
        return web.json_response(data={
            'data': {'listeners': [l.to_dict() for l in ClientManager.listeners.values()],
                     'workers': [w.to_dict() for w in ClientManager.workers.values()]}}, status=200,
            dumps=safe_json_dumps)
