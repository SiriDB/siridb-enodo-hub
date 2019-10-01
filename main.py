import argparse
import asyncio

from aiohttp import web
from aiohttp.web_middlewares import middleware

from server import Server


def str2bool(v, default):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        return default


@middleware
async def middleware(request, handler):
    if request.path.startswith('/api/docs'):
        return await handler(request)
    else:
        return web.json_response(status=404)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process config')
    parser.add_argument('--config', help='Config path', required=True)
    parser.add_argument('--docs_only', help='Host docs only', required=False, default=False)

    docs_only = str2bool(parser.parse_args().docs_only, False)

    loop = asyncio.get_event_loop()
    # loop = asyncio.new_event_loop()
    if docs_only:
        app = web.Application(middlewares=[middleware])
    else:
        app = web.Application()

    server = Server(loop, app, parser.parse_args().config, docs_only)
    server.start_server()
