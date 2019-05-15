import asyncio

from aiohttp import web

from server import Server

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    app = web.Application()

    server = Server(loop, app)
    server.start_server()
