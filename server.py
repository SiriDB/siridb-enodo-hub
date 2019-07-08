import asyncio
import datetime
import signal
import aiohttp_cors
from aiohttp import web

from lib.config.config import Config
from lib.logging.eventlogger import EventLogger
from lib.serie.seriemanager import SerieManager
from lib.siridb.siridb import SiriDB
from lib.socket.clientmanager import ClientManager
from lib.socket.handler import update_serie_count, receive_worker_status_update, send_forecast_request
from lib.socket.package import LISTENER_ADD_SERIE_COUNT, WORKER_UPDATE_BUSY
from lib.socket.socketserver import SocketServer
from lib.webserver.handlers import Handlers


class Server:

    def __init__(self, loop, app, config_path):
        self.run = True
        self.loop = loop
        self.app = app
        self.config_path = config_path
        self.socket_server = None
        self.watch_series_task = None
        self.check_siridb_connection_task = None
        self.socket_server_task = None
        self.save_to_disk_task = None
        self._send_for_analyse = []

    async def start_up(self):
        await Config.read_config(self.config_path)
        await EventLogger.prepare(Config.log_path)

        # TODO MOVE TO LISTENER
        # if os.path.exists(Config.pipe_path):
        #     os.unlink(Config.pipe_path)

        _siridb = SiriDB()
        status, connected = await _siridb.test_connection()
        SiriDB.siridb_status = status
        SiriDB.siridb_connected = connected

        self.socket_server = SocketServer(Config.socket_server_host, Config.socket_server_port,
                                          {LISTENER_ADD_SERIE_COUNT: update_serie_count,
                                           WORKER_UPDATE_BUSY: receive_worker_status_update})
        await Handlers.prepare(self.socket_server)

        await SerieManager.prepare()
        await SerieManager.read_from_disk()
        self.watch_series_task = self.loop.create_task(self.watch_series())
        self.check_siridb_connection_task = self.loop.create_task(self.check_siridb_connection())
        self.socket_server_task = self.loop.create_task(self.socket_server.create())
        self.save_to_disk_task = self.loop.create_task(self.save_to_disk())

    async def clean_up(self):
        """
        Cleans up before shutdown
        :return:
        """
        await self.watch_series_task
        await self.check_siridb_connection_task
        await self.socket_server_task
        await self.save_to_disk_task

    async def check_siridb_connection(self):
        while self.run:
            print(f"Checking SiriDB Connection")
            EventLogger.log("Checking SiriDB Connection", "verbose")
            _siridb = SiriDB()
            status, connected = await _siridb.test_connection()
            # if not SiriDB.siridb_connected and connected:
            #     await SerieManager.check_for_config_changes()
            SiriDB.siridb_status = status
            SiriDB.siridb_connected = connected
            await asyncio.sleep(Config.siridb_connection_check_interval)

    async def watch_series(self):
        while self.run:
            await ClientManager.check_clients_alive(Config.client_max_timeout)

            for serie_name in await SerieManager.get_series():
                serie = await SerieManager.get_serie(serie_name)
                if serie is not None and (not await serie.is_forecasted or (
                        serie.new_forecast_at is not None and serie.new_forecast_at < datetime.datetime.now())):
                    # Should be forecasted if not forecasted yet or new forecast should be made

                    if await serie.get_datapoints_count() >= Config.min_data_points:
                        print(f"Adding serie: {serie_name} to the Analyser queue")
                        EventLogger.log(f"Adding serie: {serie_name} to the Analyse queue", "info",
                                        "serie_add_queue")
                        worker = await ClientManager.get_free_worker()
                        if worker is not None:
                            await serie.set_pending_forecast(True)
                            await send_forecast_request(worker, serie)

            await asyncio.sleep(Config.watcher_interval)

    async def save_to_disk(self):
        while self.run:
            await asyncio.sleep(Config.save_to_disk_interval)
            EventLogger.log('Saving seriemanager state to disk', "verbose")
            await SerieManager.save_to_disk()

    def stop_server(self):
        EventLogger.log('Stopping analyser server...', "info")
        self.loop.run_until_complete(self.save_to_disk())
        EventLogger.save_to_disk()
        self.run = False
        self.loop.run_until_complete(self.clean_up())
        # asyncio.gather(*asyncio.Task.all_tasks()).cancel()

        pending = asyncio.Task.all_tasks(self.loop)
        for task in pending:
            task.cancel()
        self.loop.run_until_complete(asyncio.gather(*pending, loop=self.loop))

        # self.worker_loop.stop()
        self.loop.stop()
        print('Bye!')

    def start_server(self):
        print('Starting...')

        cors = aiohttp_cors.setup(self.app, defaults={
            "*": aiohttp_cors.ResourceOptions(
                allow_credentials=True,
                expose_headers="*",
                allow_headers="*",
            )
        })

        # Add rest api routes
        self.app.router.add_get("/series", Handlers.get_monitored_series)
        self.app.router.add_post("/series", Handlers.add_serie)
        self.app.router.add_get("/series/{serie_name}", Handlers.get_monitored_serie_details)
        self.app.router.add_delete("/series/{serie_name}", Handlers.remove_serie)
        self.app.router.add_get("/settings", Handlers.get_settings)
        self.app.router.add_post("/settings", Handlers.set_settings)
        self.app.router.add_get("/siridb/status", Handlers.get_siridb_status)
        self.app.router.add_get("/enodo/status", Handlers.get_siridb_enodo_status)
        self.app.router.add_get("/enodo/log", Handlers.get_event_log)
        self.app.router.add_get("/enodo/clients", Handlers.get_connected_clients)

        # Configure CORS on all routes.
        for route in list(self.app.router.routes()):
            cors.add(route)

        self.loop.run_until_complete(self.start_up())

        for signame in ('SIGINT', 'SIGTERM'):
            # self.stop_server()
            # self.loop.add_signal_handler(getattr(signal, signame), self.worker_loop.stop)
            self.loop.add_signal_handler(getattr(signal, signame), self.loop.stop)

        web.run_app(self.app)

        # self.loop.run_forever()

        self.stop_server()

        # cleanup signal handlers
        for signame in ('SIGINT', 'SIGTERM'):
            self.loop.remove_signal_handler(getattr(signal, signame))
