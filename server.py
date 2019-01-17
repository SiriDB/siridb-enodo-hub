import asyncio
import sys
from threading import Thread

import aiohttp_cors
import os

from aiohttp import web

from lib.analyser.analyser import Analyser, start_analyser_worker, watch_queue
from lib.config.config import Config
from lib.serie.seriemanager import SerieManager
from lib.siridb.pipeserver import PipeServer
from lib.siridb.siridb import SiriDB
from lib.webserver import handlers

loop = asyncio.get_event_loop()
worker_loop = None
analyser_queue = list()


async def start_up():
    await Config.read_config()

    if os.path.exists(Config.pipe_path):
        os.unlink(Config.pipe_path)

    siridb_client = SiriDB()
    await SerieManager.prepare(siridb_client)

    asyncio.ensure_future(Analyser.prepare(analyser_queue), loop=worker_loop)
    asyncio.run_coroutine_threadsafe(watch_queue(), worker_loop)


def on_data(data):
    asyncio.ensure_future(handle_data(data))


async def watch_series():
    while True:
        for serie_name in await SerieManager.get_series():
            serie = await SerieManager.get_serie(serie_name)
            serie_in_queue = serie_name in analyser_queue
            if serie is not None and not await serie.get_analysed() and not serie_in_queue:
                if await serie.get_datapoints_count() >= Config.min_data_points:
                    print(f"Adding serie: {serie_name} to the Analyser queue")
                    analyser_queue.append(serie_name)

        await asyncio.sleep(Config.watcher_interval)


async def handle_data(data):
    for serie_name, values in data.items():
        should_be_handled = serie_name in Config.names_enabled_series_for_analysis

        if should_be_handled:
            await SerieManager.add_to_datapoint_counter(serie_name, len(values))
            print(f"Handling new data for serie: {serie_name}")


async def start_siridb_pipeserver():
    pipe_server = PipeServer(Config.pipe_path, on_data)
    await pipe_server.create()


if __name__ == '__main__':
    try:
        print('starting...')

        # Create the new loop and worker thread
        worker_loop = asyncio.new_event_loop()
        worker = Thread(target=start_analyser_worker, args=(worker_loop,))
        # Start the thread
        worker.start()

        app = web.Application()
        cors = aiohttp_cors.setup(app, defaults={
            "*": aiohttp_cors.ResourceOptions(
                allow_credentials=True,
                expose_headers="*",
                allow_headers="*",
            )
        })

        app.router.add_get("/test", handlers.test_webserver)
        app.router.add_get("/series", handlers.get_monitored_series)
        app.router.add_post("/series", handlers.add_serie)

        # Configure CORS on all routes.
        for route in list(app.router.routes()):
            cors.add(route)

        loop.run_until_complete(start_up())
        loop.run_until_complete(start_siridb_pipeserver())

        web.run_app(app)
        loop.run_forever()
    except (KeyboardInterrupt, SystemExit):
        # cleanup_stop_thread()
        loop.close()
        worker_loop.close()
        asyncio.gather(*asyncio.Task.all_tasks()).cancel()
        sys.exit()
