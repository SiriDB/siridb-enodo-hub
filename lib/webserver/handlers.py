from aiohttp import web

from lib.serie.seriemanager import SerieManager


async def test_webserver(request):
    return web.json_response(data={'data': ["hello", ", ", "world", "!"]})


async def get_monitored_series(request):
    return web.json_response(data={'data': list(await SerieManager.get_series_to_dict())})
