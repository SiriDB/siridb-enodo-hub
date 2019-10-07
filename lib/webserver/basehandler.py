from aiohttp import web
from lib.analyser.analyserwrapper import MODEL_NAMES, MODEL_PARAMETERS
from lib.config.config import Config
from lib.serie.seriemanager import SerieManager
from lib.siridb.siridb import SiriDB


class BaseHandler:

    @classmethod
    async def resp_get_monitored_series(cls, regex_filter):
        return {'data': list(await SerieManager.get_series_to_dict())}

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
    async def resp_remove_serie(cls, serie_name):
        if await SerieManager.remove_serie(serie_name):
            return 200
        return 404

    @classmethod
    async def resp_get_possible_analyser_models(cls):
        data = {
            'models': MODEL_NAMES,
            'parameters': MODEL_PARAMETERS
        }
        return {'data': data}