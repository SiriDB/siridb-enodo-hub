import asyncio
import functools
import signal
from threading import Thread

import aiohttp_cors
import os

from aiohttp import web

from lib.analyser.analyser import Analyser, start_analyser_worker
from lib.config.config import Config
from lib.logging.eventlogger import EventLogger
from lib.serie.seriemanager import SerieManager
from lib.siridb.pipeserver import PipeServer
from lib.siridb.siridb import SiriDB
from lib.webserver import handlers


class Server:

    def __init__(self, loop, app):
        self.run = True
        self.loop = loop
        self.app = app
        self.worker_loop = None
        self.analyser_queue = list()  # Queue should only be read/processed by one thread only
        self.watch_series_task = None

    async def start_up(self):
        await Config.read_config()
        await EventLogger.prepare(Config.log_path)

        if os.path.exists(Config.pipe_path):
            os.unlink(Config.pipe_path)

        _siridb = SiriDB()
        status, connected = await _siridb.test_connection()
        SiriDB.siridb_status = status
        SiriDB.siridb_connected = connected

        await SerieManager.prepare()
        asyncio.ensure_future(Analyser.prepare(self.analyser_queue), loop=self.worker_loop)
        asyncio.run_coroutine_threadsafe(Analyser.watch_queue(), self.worker_loop)
        self.watch_series_task = self.loop.create_task(self.watch_series())
        self.watch_series_task = self.loop.create_task(self.check_siridb_connection())

    async def clean_up(self):
        """
        Cleans up before shutdown
        :return:
        """
        # self.watch_series_task.cancel()
        await self.watch_series_task
        await Analyser.shutdown()

    def on_data(self, data):
        """
        Forwards incoming data to a async handler
        :param data:
        :return:
        """
        asyncio.ensure_future(self.handle_data(data))

    async def check_siridb_connection(self):
        while self.run:
            print(f"Checking SiriDB Connection")
            EventLogger.log("Checking SiriDB Connection", "verbose")
            _siridb = SiriDB()
            status, connected = await _siridb.test_connection()
            if not SiriDB.siridb_connected and connected:
                await SerieManager.check_for_config_changes()
            SiriDB.siridb_status = status
            SiriDB.siridb_connected = connected
            await asyncio.sleep(Config.siridb_connection_check_interval)

    async def watch_series(self):
        while self.run:
            for serie_name in await SerieManager.get_series():
                serie = await SerieManager.get_serie(serie_name)
                serie_in_queue = serie_name in self.analyser_queue
                if serie is not None and not await serie.get_analysed() and not serie_in_queue:
                    if await serie.get_datapoints_count() >= Config.min_data_points:
                        print(f"Adding serie: {serie_name} to the Analyser queue")
                        EventLogger.log(f"Adding serie: {serie_name} to the Analyser queue", "info",
                                              "serie_add_queue")
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

    async def start_siridb_pipeserver(self):
        pipe_server = PipeServer(Config.pipe_path, self.on_data)
        await pipe_server.create()

    def stop_server(self):
        EventLogger.log('Stopping analyser server...', "info")
        EventLogger.save_to_disk()
        self.run = False
        self.loop.run_until_complete(self.clean_up())
        # asyncio.gather(*asyncio.Task.all_tasks()).cancel()

        pending = asyncio.Task.all_tasks(self.loop)
        for task in pending:
            task.cancel()
        self.loop.run_until_complete(asyncio.gather(*pending, loop=self.loop))

        self.worker_loop.stop()
        self.loop.stop()
        print('Bye!')

    def start_server(self):
        print('Starting...')

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
        self.app.router.add_get("/siridb/status", handlers.get_siridb_status)
        self.app.router.add_get("/enodo/status", handlers.get_siridb_enodo_status)
        self.app.router.add_get("/enodo/log", handlers.get_event_log)

        # Configure CORS on all routes.
        for route in list(self.app.router.routes()):
            cors.add(route)

        self.loop.run_until_complete(self.start_up())
        self.loop.run_until_complete(self.start_siridb_pipeserver())

        for signame in ('SIGINT', 'SIGTERM'):
            # self.loop.add_signal_handler(getattr(signal, signame),
            #                              functools.partial(self.loop.stop, self.loop, signame))
            # self.worker_loop.add_signal_handler(getattr(signal, signame),
            #                                     functools.partial(self.worker_loop.stop, self.loop, signame))
            print("hi1")
            # self.stop_server()
            self.loop.add_signal_handler(getattr(signal, signame), self.worker_loop.stop)
            self.loop.add_signal_handler(getattr(signal, signame), self.loop.stop)

        web.run_app(self.app)

        # self.loop.run_forever()

        print("hi")

        self.stop_server()

        # cleanup signal handlers
        for signame in ('SIGINT', 'SIGTERM'):
            self.loop.remove_signal_handler(getattr(signal, signame))
            self.worker_loop.remove_signal_handler(getattr(signal, signame))
