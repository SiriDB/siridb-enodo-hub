import argparse
import asyncio

from aiohttp import web

from server import Server

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process config')
    parser.add_argument('--config', help='Config path', required=True)

    loop = asyncio.get_event_loop()
    # loop = asyncio.new_event_loop()
    app = web.Application()

    server = Server(loop, app, parser.parse_args().config)
    server.start_server()
