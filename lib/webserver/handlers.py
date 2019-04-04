from aiohttp import web

from lib.config.config import Config
from lib.serie.seriemanager import SerieManager


async def test_webserver(request):
    return web.json_response(data={'data': ["hello", ", ", "world", "!"]})


async def get_monitored_series(request):
    return web.json_response(data={'data': list(await SerieManager.get_series_to_dict())})


async def add_serie(request):
    required_fields = ['m', 'name', 'unit']
    data = await request.json()

    if all(required_field in data for required_field in required_fields):
        serie_data = {
            'serie_name': data.get('name'),
            'm': data.get('m', 12),
            'd': data.get('d', None),
            'D': data.get('D', None)
        }

        Config.db.insert(serie_data)
        Config.names_enabled_series_for_analysis.append(data.get('name'))
        Config.enabled_series_for_analysis[data.get('name')] = serie_data
        await SerieManager.check_for_config_changes()

        return web.json_response(data={'data': ''}, status=201)

    return web.json_response(data={'error': 'missing required fields'}, status=400)
