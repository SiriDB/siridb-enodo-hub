import argparse
import asyncio
import os

from aiohttp import web
from aiohttp.web_middlewares import middleware

from lib.config.config import Config
from lib.logging.eventlogger import EventLogger
from server import Server


@middleware
async def middleware(request, handler):
    if request.path.startswith('/api') and not request.path.startswith('/api/docs'):
        return web.json_response(status=404)
    else:
        return await handler(request)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process config')
    parser.add_argument('--create_config', help='Create standard config file', action='store_true', default=False)
    if parser.parse_args().create_config:
        Config.create_standard_config_file(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'default.conf'))
        exit()

    parser.add_argument('--config', help='Config path', required=True)
    parser.add_argument('--log_level', help='Log level, error/warning/info/verbose, default: info', required=False,
                        default='info')
    parser.add_argument('--docs_only', help='Host docs only', action='store_true', default=False)

    docs_only = parser.parse_args().docs_only

    loop = asyncio.get_event_loop()
    # loop = asyncio.new_event_loop()
    if docs_only:
        app = web.Application(middlewares=[middleware])
    else:
        app = web.Application()

    server = Server(loop, app, parser.parse_args().config, parser.parse_args().log_level, docs_only)
    server.start_server()
