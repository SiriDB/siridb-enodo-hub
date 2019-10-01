import psutil as psutil
from aiohttp import web

from aiohttp_apispec import (
    docs,
    request_schema,
    setup_aiohttp_apispec,
    response_schema)

# from lib.analyser.model.arimamodel import ARIMAModel
from lib.analyser.analyserwrapper import MODEL_NAMES, MODEL_PARAMETERS
from lib.config.config import Config
from lib.logging.eventlogger import EventLogger
from lib.serie.seriemanager import SerieManager
from lib.siridb.siridb import SiriDB
from lib.socket.clientmanager import ClientManager
from lib.util.util import safe_json_dumps
from lib.webserver.apischemas import SchemaResponseSeries, SchemaResponseError, SchemaResponseSeriesDetails, \
    SchemaRequestCreateSeries, SchemaSeries, SchemaResponseModels


class Handlers:
    _socket_server = None

    @classmethod
    async def prepare(cls, socket_server):
        cls._socket_server = socket_server

    @classmethod
    async def resp_get_monitored_series(cls, regex_filter):
        return {'data': list(await SerieManager.get_series_to_dict())}

    @classmethod
    @docs(
        tags=["public_api"],
        summary="Get list of active series",
        description="Get list of active series with general info. You can filter with a regex on the series names.",
        parameters=[{
            'in': 'query',
            'name': 'filter',
            'description': 'regex string for filtering on series names',
            'schema': {'type': 'string', 'format': 'regex'}
        }]
    )
    @response_schema(SchemaResponseSeries(), 200)
    @response_schema(SchemaResponseError(), 400)
    async def get_monitored_series(cls, request):
        """
        Returns a list of monitored series
        :param request:
        :return:
        """
        regex_filter = request.rel_url.query['filter'] if 'filter' in request.rel_url.query else None
        # TODO implement filter

        return web.json_response(data=await cls._get_monitored_series(regex_filter),
                                 dumps=safe_json_dumps)

    @classmethod
    async def resp_get_monitored_serie_details(cls, serie_name, include_points=False):
        serie = await SerieManager.get_serie(serie_name)

        if serie is None:
            return web.json_response(data={'data': ''}, status=404)

        serie_data = await serie.to_dict()
        if include_points:
            _siridb_client = SiriDB(username=Config.siridb_user,
                                    password=Config.siridb_password,
                                    dbname=Config.siridb_database,
                                    hostlist=[(Config.siridb_host, Config.siridb_port)])
            serie_points = await _siridb_client.query_serie_data(await serie.get_name(), "mean(1h)")
            serie_data['points'] = serie_points.get(await serie.get_name())
        if await serie.is_forecasted():
            serie_data['analysed'] = True
            serie_data['forecast_points'] = await SerieManager.get_serie_forecast(await serie.get_name())
        else:
            serie_data['forecast_points'] = []

        return {'data': serie_data}

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
    async def get_monitored_serie_details(cls, request):
        """
        Returns all details and data points of a specific serie.
        :param request:
        :return:
        """
        include_points = True if 'include_points' in request.rel_url.query and request.rel_url.query[
            'include_points'] == 'true' else False

        return web.json_response(
            data=await cls._get_monitored_serie_details(request.match_info['serie_name'], include_points),
            dumps=safe_json_dumps)

    @classmethod
    async def resp_add_serie(cls, data):
        required_fields = ['name', 'model']
        model = data.get('model')
        model_parameters = data.get('model_parameters')

        if model not in MODEL_NAMES.keys():
            return web.json_response(data={'error': 'Unknown model'}, status=400)

        if model_parameters is None and len(MODEL_PARAMETERS.get(model, [])) > 0:
            return web.json_response(data={'error': 'Missing required fields'}, status=400)
        for key in MODEL_PARAMETERS.get(model, {}):
            if key not in model_parameters.keys():
                return web.json_response(data={'error': 'Missing required fields'}, status=400)

        if all(required_field in data for required_field in required_fields):
            await SerieManager.add_serie(data)

        return {'data': list(await SerieManager.get_series_to_dict())}

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
    async def add_serie(cls, request):
        """
        Add new serie to the application.
        :param request:
        :return:
        """
        data = await request.json()

        return web.json_response(data=await cls._add_serie(data), status=201)

    @classmethod
    async def resp_remove_serie(cls, serie_name):
        if await SerieManager.remove_serie(serie_name):
            return 200
        return 404

    @classmethod
    @docs(
        tags=["public_api"],
        summary="Remove a new series",
        description="Remove a certain series by its name",
        parameters=[]
    )
    @response_schema(SchemaResponseError(), 400)
    async def remove_serie(cls, request):
        """
        Remove series with certain name
        :param request:
        :return:
        """
        serie_name = request.match_info['serie_name']
        return web.json_response(data={}, status=await cls._remove_serie(serie_name))

    @classmethod
    async def resp_get_possible_analyser_models(cls):
        data = {
            'models': MODEL_NAMES,
            'parameters': MODEL_PARAMETERS
        }
        return {'data': data}

    @classmethod
    @docs(
        tags=["public_api"],
        summary="Get a list of possible models",
        description="Get a list of possible models to use for adding a series",
        parameters=[]
    )
    @response_schema(SchemaResponseModels(), 200)
    @response_schema(SchemaResponseError(), 400)
    async def get_possible_analyser_models(cls, request):
        """
        Returns list of possible models with corresponding parameters
        :param request:
        :return:
        """

        return web.json_response(data=await cls._get_possible_analyser_models(), status=200)

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
        fields = ['min_data_points', 'analysis_save_path', 'siridb_host', 'siridb_port', 'siridb_user',
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

        fields = ['min_data_points', 'analysis_save_path', 'siridb_host', 'siridb_port', 'siridb_user',
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
            'data': {'listeners': [l.to_dict() for l in ClientManager.listeners.values()],
                     'workers': [w.to_dict() for w in ClientManager.workers.values()]}}, status=200,
            dumps=safe_json_dumps)
