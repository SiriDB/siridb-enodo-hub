from aiohttp import web

from lib.analyser.model.arimamodel import ARIMAModel
from lib.config.config import Config
from lib.serie.seriemanager import SerieManager
from lib.siridb.siridb import SiriDB


async def get_monitored_series(request):
    """
    Returns a list of monitored series
    :param request:
    :return:
    """
    return web.json_response(data={'data': list(await SerieManager.get_series_to_dict())})


async def get_monitored_serie_details(request):
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


async def add_serie(request):
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
            'm': data.get('m', 12),
            'd': data.get('d', None),
            'D': data.get('D', None)
        }

        # Config.db.insert(serie_data)
        Config.names_enabled_series_for_analysis.append(data.get('name'))
        Config.enabled_series_for_analysis[data.get('name')] = serie_data
        await SerieManager.check_for_config_changes()

        return web.json_response(data={'data': list(await SerieManager.get_series_to_dict())}, status=201)

    return web.json_response(data={'error': 'missing required fields'}, status=400)


async def remove_serie(request):
    """
    Remove serie by it's name
    :param request:
    :return:
    """
    if await SerieManager.remove_serie(request.match_info['serie_name']):
        return web.json_response(data={}, status=200)
    return web.json_response(data={}, status=404)


async def get_settings(request):
    settings = await build_settings_dict()

    return web.json_response(data={'data': settings}, status=200)


async def build_settings_dict():
    settings = {}
    fields = ['pipe_path', 'min_data_points', 'analysis_save_path']
    for field in fields:
        settings[field] = getattr(Config, field)
    return settings


async def set_settings(request):
    """
    Override settings.
    :param request:
    :return:
    """
    data = await request.json()

    fields = ['pipe_path', 'min_data_points', 'analysis_save_path']

    for field in fields:
        if field in data:
            setattr(Config, field, data[field])

    settings = await build_settings_dict()

    return web.json_response(data={'data': settings}, status=200)
