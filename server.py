import asyncio
import functools
import signal
from threading import Thread

import aiohttp_cors
import os

from aiohttp import web

from lib.analyser.analyser import Analyser, start_analyser_worker
from lib.config.config import Config
from lib.serie.seriemanager import SerieManager
from lib.siridb.pipeserver import PipeServer
from lib.webserver import handlers


class Server:

    def __init__(self, loop, app):
        self.loop = loop
        self.app = app
        self.worker_loop = None
        self.analyser_queue = list()  # Queue should only be read/processed by one thread only
        self.watch_series_task = None

    async def start_up(self):
        await Config.read_config()

        if os.path.exists(Config.pipe_path):
            os.unlink(Config.pipe_path)

        await SerieManager.prepare()
        asyncio.ensure_future(Analyser.prepare(self.analyser_queue), loop=self.worker_loop)
        asyncio.run_coroutine_threadsafe(Analyser.watch_queue(), self.worker_loop)
        self.watch_series_task = self.loop.create_task(self.watch_series())

    async def clean_up(self):
        """
        Cleans up before shutdown
        :return:
        """
        self.watch_series_task.cancel()
        await self.watch_series_task
        await Analyser.shutdown()

    def on_data(self, data):
        """
        Forwards incoming data to a async handler
        :param data:
        :return:
        """
        asyncio.ensure_future(self.handle_data(data))

    async def watch_series(self):
        while True:
            for serie_name in await SerieManager.get_series():
                serie = await SerieManager.get_serie(serie_name)
                serie_in_queue = serie_name in self.analyser_queue
                if serie is not None and not await serie.get_analysed() and not serie_in_queue:
                    if await serie.get_datapoints_count() >= Config.min_data_points:
                        print(f"Adding serie: {serie_name} to the Analyser queue")
                        self.analyser_queue.append(serie_name)

            await asyncio.sleep(Config.watcher_interval)

    async def handle_data(self, data):
        """
        Handles incoming data, when not relevant, it will be ignored
        :param data:
        :return:
        """
        for serie_name, values in data.items():
            should_be_handled = serie_name in Config.names_enabled_series_for_analysis

            if should_be_handled:
                await SerieManager.add_to_datapoint_counter(serie_name, len(values))
                print(f"Handling new data for serie: {serie_name}")

    async def start_siridb_pipeserver(self):
        pipe_server = PipeServer(Config.pipe_path, self.on_data)
        await pipe_server.create()

    def stop_server(self):
        print('Stopping analyser server...')
        self.loop.run_until_complete(self.clean_up())
        asyncio.gather(*asyncio.Task.all_tasks()).cancel()
        self.worker_loop.close()
        self.loop.close()
        print('bye!')

    def start_server(self):
        print('starting...')

        for signame in ('SIGINT', 'SIGTERM'):
            self.loop.add_signal_handler(getattr(signal, signame),
                                         functools.partial(self.stop_server, self.loop, signame))

        # Create the new loop and worker thread, Only one worker thread should be made.
        self.worker_loop = asyncio.new_event_loop()
        worker = Thread(target=start_analyser_worker, args=(self.worker_loop,))
        # Start the thread
        worker.start()

        cors = aiohttp_cors.setup(self.app, defaults={
            "*": aiohttp_cors.ResourceOptions(
                allow_credentials=True,
                expose_headers="*",
                allow_headers="*",
            )
        })

        # Add rest api routes
        self.app.router.add_get("/series", handlers.get_monitored_series)
        self.app.router.add_post("/series", handlers.add_serie)
        self.app.router.add_get("/series/{serie_name}", handlers.get_monitored_serie_details)
        self.app.router.add_delete("/series/{serie_name}", handlers.remove_serie)
        self.app.router.add_get("/settings", handlers.get_settings)
        self.app.router.add_post("/settings", handlers.set_settings)

        # Configure CORS on all routes.
        for route in list(self.app.router.routes()):
            cors.add(route)

        self.loop.run_until_complete(self.start_up())
        self.loop.run_until_complete(self.start_siridb_pipeserver())

        web.run_app(self.app)
        self.loop.run_forever()

        for signame in ('SIGINT', 'SIGTERM'):
            self.loop.remove_signal_handler(getattr(signal, signame))
