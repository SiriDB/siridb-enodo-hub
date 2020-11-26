from aiohttp import web
from lib.analyser.analyserwrapper import setup_default_model_arguments
from enodo import EnodoModel
from enodo.model.config.series import SeriesConfigModel
from lib.analyser.model import EnodoModelManager
from lib.events import EnodoEventManager
from lib.series.seriesmanager import SeriesManager
from lib.series import DETECT_ANOMALIES_STATUS_DONE
from lib.serverstate import ServerState
from lib.siridb.siridb import query_series_data
from lib.util import regex_valid
from version import VERSION
from lib.enodojobmanager import EnodoJobManager
from enodo.jobs import JOB_TYPE_FORECAST_SERIES, JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES, JOB_STATUS_DONE


class BaseHandler:

    @classmethod
    async def resp_trigger_detect_anomalies(cls, series_name):
        pass

    @classmethod
    async def resp_get_monitored_series(cls, regex_filter=None):
        if regex_filter is not None:
            if not regex_valid(regex_filter):
                return {'data': []}
        return {'data': list(await SeriesManager.get_series_to_dict(regex_filter))}

    @classmethod
    async def resp_get_monitored_series_details(cls, series_name, include_points=False):
        series = await SeriesManager.get_series(series_name)

        if series is None:
            return web.json_response(data={'data': ''}, status=404)

        series_data = await series.to_dict()
        if include_points:
            series_points = await query_series_data(ServerState.siridb_data_client, series.name, "*")
            series_data['points'] = series_points.get(series.name)
        if await series.get_job_status(JOB_TYPE_FORECAST_SERIES) == JOB_STATUS_DONE:
            series_data['forcasted'] = True
            series_data['forecast_points'] = await SeriesManager.get_series_forecast(series.name)
        else:
            series_data['forecast_points'] = []

        series_data['anomalies'] = []
        if await series.get_job_status(JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES) == JOB_STATUS_DONE:
            anomalies = await SeriesManager.get_series_anomalies(series.name)
            series_data['anomalies'] = anomalies

        return {'data': series_data}

    @classmethod
    async def resp_get_event_outputs(cls):
        return {'data': [await output.to_dict() for output in EnodoEventManager.outputs]}, 200

    @classmethod
    async def resp_add_event_output(cls, output_type, data):
        await EnodoEventManager.create_event_output(output_type, data)
        return {'data': [await output.to_dict() for output in EnodoEventManager.outputs]}, 201

    @classmethod
    async def resp_remove_event_output(cls, output_id):
        await EnodoEventManager.remove_event_output(output_id)
        return {'data': None}, 200

    @classmethod
    async def resp_add_series(cls, data):
        required_fields = ['name', 'config']
        series_config = SeriesConfigModel.from_dict(data.get('config'))
        for model_name in list(series_config.job_models.values()):
            model_parameters = series_config.model_params

            model = await EnodoModelManager.get_model(model_name)
            if model is None:
                return {'error': 'Unknown model'}, 400
            if model_parameters is None and len(model.model_arguments.keys()) > 0:
                return {'error': 'Missing required fields'}, 400
            for key in model.model_arguments:
                if key not in model_parameters.keys():
                    return {'error': f'Missing required field {key}'}, 400

        # data['model_parameters'] = await setup_default_model_arguments(model_parameters)

        if all(required_field in data for required_field in required_fields):
            if not await SeriesManager.add_series(data):
                return {'error': 'Something went wrong when adding the series. Are you sure the series exists?'}, 400

        return {'data': list(await SeriesManager.get_series_to_dict())}, 201

    @classmethod
    async def resp_remove_series(cls, series_name):
        # TODO: REMOVE JOBS, EVENTS ETC
        if await SeriesManager.remove_series(series_name):
            await EnodoJobManager.cancel_jobs_for_series(series_name)
            await EnodoJobManager.remove_failed_jobs_for_series(series_name)
            return 200
        return 404

    @classmethod
    async def resp_get_jobs_queue(cls):
        return {'data': await EnodoJobManager.get_open_queue()}

    @classmethod
    async def resp_get_possible_analyser_models(cls):
        data = {
            'models': [await EnodoModel.to_dict(model) for model in EnodoModelManager.models]
        }
        return {'data': data}

    @classmethod
    async def resp_add_model(cls, data):
        try:
            await EnodoModelManager.add_model(data['model_name'],
                                              data['model_arguments'])
            return {'data': [await EnodoModel.to_dict(model) for model in EnodoModelManager.models]}, 201
        except Exception:
            return {'error': 'Incorrect model data'}, 400

    @classmethod
    async def resp_get_enodo_hub_status(cls):
        data = {'version': VERSION}
        return {'data': data}
