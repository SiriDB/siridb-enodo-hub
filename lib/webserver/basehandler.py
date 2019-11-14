from aiohttp import web
from lib.analyser.analyserwrapper import MODEL_NAMES, MODEL_PARAMETERS, setup_default_model_arguments
from lib.events import EnodoEventManager
from lib.serie.seriemanager import SerieManager
from lib.serie import DETECT_ANOMALIES_STATUS_DONE
from lib.serverstate import ServerState
from lib.siridb.siridb import query_serie_data
from lib.util.util import regex_valid


class BaseHandler:

    @classmethod
    async def resp_trigger_detect_anomalies(cls, serie_name):
        pass

    @classmethod
    async def resp_get_monitored_series(cls, regex_filter=None):
        if regex_filter is not None:
            if not regex_valid(regex_filter):
                return {'data': []}
        return {'data': list(await SerieManager.get_series_to_dict(regex_filter))}

    @classmethod
    async def resp_get_monitored_serie_details(cls, serie_name, include_points=False):
        serie = await SerieManager.get_serie(serie_name)

        if serie is None:
            return web.json_response(data={'data': ''}, status=404)

        serie_data = await serie.to_dict()
        if include_points:
            serie_points = await query_serie_data(ServerState.siridb_data_client, serie.name, "*")
            serie_data['points'] = serie_points.get(serie.name)
        if await serie.is_forecasted():
            serie_data['analysed'] = True
            serie_data['forecast_points'] = await SerieManager.get_serie_forecast(serie.name)
        else:
            serie_data['forecast_points'] = []

        serie_data['anomalies'] = []
        if await serie.get_detect_anomalies_status() is DETECT_ANOMALIES_STATUS_DONE:
            anomalies = await SerieManager.get_serie_anomalies(serie.name)
            serie_data['anomalies'] = anomalies

        return {'data': serie_data}

    @classmethod
    async def resp_add_event_output(cls, output_type, data):
        await EnodoEventManager.create_event_output(output_type, data)
        return {'data': [await output.to_dict() for output in EnodoEventManager.outputs]}, 201

    @classmethod
    async def resp_remove_event_output(cls, output_id):
        await EnodoEventManager.remove_event_output(output_id)
        return {'data': None}, 200

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

        data['model_parameters'] = await setup_default_model_arguments(model_parameters)

        if all(required_field in data for required_field in required_fields):
            if not await SerieManager.add_serie(data):
                return {'error': 'Something went wrong when adding the serie. Are you sure the serie exists?'}

        return {'data': list(await SerieManager.get_series_to_dict())}

    @classmethod
    async def resp_remove_serie(cls, serie_name):
        # TODO: REMOVE JOBS, EVENTS ETC
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
