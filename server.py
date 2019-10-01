import asyncio
import datetime
import aiohttp_cors
import socketio
from aiohttp import web
from aiohttp_apispec import setup_aiohttp_apispec

from lib.config.config import Config
from lib.logging.eventlogger import EventLogger
from lib.serie.seriemanager import SerieManager
from lib.siridb.siridb import SiriDB
from lib.socket.clientmanager import ClientManager
from lib.socket.handler import update_serie_count, receive_worker_status_update, send_forecast_request, \
    receive_worker_result, received_worker_refused
from lib.socket.package import LISTENER_ADD_SERIE_COUNT, WORKER_UPDATE_BUSY, WORKER_RESULT, WORKER_REFUSED
from lib.socket.socketserver import SocketServer
from lib.socketio.handlers import setup_socketio_handlers, websocket_index
from lib.webserver.handlers import Handlers


class Server:

    def __init__(self, loop, app, config_path, docs_only=False):
        self.run = True
        self.loop = loop
        self.app = app
        self.config_path = config_path
        self._docs_only = docs_only
        self.socket_server = None
        self.watch_series_task = None
        self.check_siridb_connection_task = None
        self.socket_server_task = None
        self.save_to_disk_task = None
        self._send_for_analyse = []

    async def start_up(self, sio):
        await Config.read_config(self.config_path)
        await EventLogger.prepare(Config.log_path)

        _siridb = SiriDB(username=Config.siridb_user,
                         password=Config.siridb_password,
                         dbname=Config.siridb_database,
                         hostlist=[(Config.siridb_host, Config.siridb_port)])
        status, connected = await _siridb.test_connection()
        SiriDB.siridb_status = status
        SiriDB.siridb_connected = connected

        self.socket_server = SocketServer(Config.socket_server_host, Config.socket_server_port,
                                          {LISTENER_ADD_SERIE_COUNT: update_serie_count,
                                           WORKER_RESULT: receive_worker_result,
                                           WORKER_UPDATE_BUSY: receive_worker_status_update,
                                           WORKER_REFUSED: received_worker_refused})
        await Handlers.prepare(self.socket_server)
        setup_socketio_handlers(sio)
        await SerieManager.prepare()
        await SerieManager.read_from_disk()
        await ClientManager.setup(SerieManager)
        self.watch_series_task = self.loop.create_task(self.watch_series())
        self.check_siridb_connection_task = self.loop.create_task(self.check_siridb_connection())
        self.save_to_disk_task = self.loop.create_task(self.save_to_disk())
        await self.socket_server.create()

    async def clean_up(self):
        """
        Cleans up before shutdown
        :return:
        """
        self.watch_series_task.cancel()
        self.check_siridb_connection_task.cancel()
        await self.socket_server.stop()
        await self.save_to_disk_task

    async def check_siridb_connection(self):
        while self.run:
            EventLogger.log("Checking SiriDB Connection", "verbose")
            _siridb = SiriDB(username=Config.siridb_user,
                             password=Config.siridb_password,
                             dbname=Config.siridb_database,
                             hostlist=[(Config.siridb_host, Config.siridb_port)])
            status, connected = await _siridb.test_connection()
            SiriDB.siridb_status = status
            SiriDB.siridb_connected = connected
            await asyncio.sleep(Config.siridb_connection_check_interval)

    async def watch_series(self):
        while self.run:
            await ClientManager.check_clients_alive(Config.client_max_timeout)

            for serie_name in await SerieManager.get_series():
                serie = await SerieManager.get_serie(serie_name)
                if serie is not None \
                        and not await serie.ignored() \
                        and not await serie.pending_forecast() \
                        and (not await serie.is_forecasted() or (
                        serie.new_forecast_at is not None and serie.new_forecast_at < datetime.datetime.now())):
                    # Should be forecasted if not forecasted yet or new forecast should be made

                    if await serie.get_datapoints_count() >= Config.min_data_points:
                        worker = await ClientManager.get_free_worker()
                        if worker is not None:
                            EventLogger.log(f"Adding serie: sending {serie_name} to Worker", "info",
                                            "serie_add_queue")
                            await serie.set_pending_forecast(True)
                            await send_forecast_request(worker, serie)
                            worker.is_going_busy = True

            await asyncio.sleep(Config.watcher_interval)

    @staticmethod
    async def save_to_disk():
        await SerieManager.save_to_disk()

    async def save_to_disk_on_interval(self):
        while self.run:
            await asyncio.sleep(Config.save_to_disk_interval)
            EventLogger.log('Saving seriemanager state to disk', "verbose")
            await self.save_to_disk()

    async def stop_server(self):
        EventLogger.log('Stopping analyser server...', "info")
        EventLogger.log('...Saving log to disk', "info")
        await self.save_to_disk()
        EventLogger.save_to_disk()
        self.run = False
        EventLogger.log('...Doing clean up', "info")
        await self.clean_up()
        # asyncio.gather(*asyncio.Task.all_tasks()).cancel()

        EventLogger.log('...Stopping all running tasks', "info")

        await asyncio.sleep(2)

        for task in asyncio.Task.all_tasks():
            try:
                task.cancel()
                await asyncio.wait([task])
            except asyncio.CancelledError as e:
                pass

        self.loop.stop()
        print('Bye!')

    async def _stop_server_from_aiohttp_cleanup(self, *args, **kwargs):
        await self.stop_server()

    def start_server(self):
        print('Starting...')

        cors = aiohttp_cors.setup(self.app, defaults={
            "*": aiohttp_cors.ResourceOptions(
                allow_credentials=True,
                expose_headers="*",
                allow_headers="*",
            )
        })

        self.app.router.add_get("/", websocket_index, allow_head=False)

        # Add rest api routes
        self.app.router.add_get("/api/series", Handlers.get_monitored_series, allow_head=False)
        self.app.router.add_post("/api/series", Handlers.add_serie)
        self.app.router.add_get("/api/series/{serie_name}", Handlers.get_monitored_serie_details, allow_head=False)
        self.app.router.add_delete("/api/series/{serie_name}", Handlers.remove_serie)
        self.app.router.add_get("/api/enodo/models", Handlers.get_possible_analyser_models, allow_head=False)

        # Add internal api routes
        self.app.router.add_get("/api/settings", Handlers.get_settings, allow_head=False)
        self.app.router.add_post("/api/settings", Handlers.set_settings)
        self.app.router.add_get("/api/siridb/status", Handlers.get_siridb_status, allow_head=False)
        self.app.router.add_get("/api/enodo/status", Handlers.get_siridb_enodo_status, allow_head=False)
        self.app.router.add_get("/api/enodo/log", Handlers.get_event_log, allow_head=False)
        self.app.router.add_get("/api/enodo/clients", Handlers.get_connected_clients, allow_head=False)

        # init docs with all parameters, usual for ApiSpec
        setup_aiohttp_apispec(
            app=self.app,
            title="API references",
            version="v1",
            url="/api/docs/swagger.json",
            swagger_path="/api/docs",
        )

        if self._docs_only:
            web.run_app(self.app)
        else:
            # Configure CORS on all routes.
            for route in list(self.app.router.routes()):
                cors.add(route)

            sio = socketio.AsyncServer(async_mode='aiohttp')
            sio.attach(self.app)

            self.loop.run_until_complete(self.start_up(sio))
            self.app.on_cleanup.append(self._stop_server_from_aiohttp_cleanup)
            web.run_app(self.app)
