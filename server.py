import asyncio
import datetime
import aiohttp_cors
import socketio
from aiohttp import web
from aiohttp.web_middlewares import middleware
from aiohttp_apispec import setup_aiohttp_apispec

from lib.api.apihandlers import ApiHandlers, auth
from lib.config.config import Config
from lib.jobmanager.enodojobmanager import EnodoJobManager, EnodoJob, JOB_TYPE_FORECAST_SERIE, \
    JOB_TYPE_DETECT_ANOMALIES_FOR_SERIE
from lib.logging.eventlogger import EventLogger
from lib.serie.seriemanager import SerieManager
from lib.serie.series import DETECT_ANOMALIES_STATUS_REQUESTED
from lib.serverstate import ServerState
from lib.siridb.siridb import SiriDB
from lib.socket.clientmanager import ClientManager
from lib.socket.handler import update_serie_count, receive_worker_status_update, \
    receive_worker_result, received_worker_refused
from lib.socket.package import LISTENER_ADD_SERIE_COUNT, WORKER_UPDATE_BUSY, WORKER_RESULT, WORKER_REFUSED
from lib.socket.socketserver import SocketServer
from lib.socketio.socketiohandlers import SocketIoHandler

from lib.socketio.socketiorouter import SocketIoRouter
from lib.util.util import print_custom_aiohttp_startup_message


class Server:

    def __init__(self, loop, port, config_path, log_level='info', docs_only=False):
        self.loop = loop
        self.port = port
        self.app = None
        self.sio = None
        self.auth = None
        self.config_path = config_path
        self._log_level = log_level
        self._docs_only = docs_only
        self.socket_server = None
        self.watch_series_task = None
        self.check_siridb_connection_task = None
        self.socket_server_task = None
        self.save_to_disk_task = None
        self.check_jobs_task = None
        # self._send_for_analyse = []

    async def start_up(self):
        await ServerState.async_setup()
        _siridb = SiriDB(username=Config.siridb_user,
                         password=Config.siridb_password,
                         dbname=Config.siridb_database,
                         hostlist=[(Config.siridb_host, Config.siridb_port)])
        status, connected = await _siridb.test_connection()
        SiriDB.siridb_status = status
        SiriDB.siridb_connected = connected

        EventLogger.log('Setting up internal communications token...', "info")
        Config.setup_internal_security_token()

        self.socket_server = SocketServer(Config.socket_server_host, Config.socket_server_port,
                                          Config.internal_security_token,
                                          {LISTENER_ADD_SERIE_COUNT: update_serie_count,
                                           WORKER_RESULT: receive_worker_result,
                                           WORKER_UPDATE_BUSY: receive_worker_status_update,
                                           WORKER_REFUSED: received_worker_refused})
        await ApiHandlers.prepare()

        if self.sio is not None:
            await SocketIoHandler.prepare(self.sio)
            SocketIoRouter(self.sio)

        await SerieManager.prepare(SocketIoHandler.internal_updates_series_subscribers)
        await SerieManager.read_from_disk()
        await ClientManager.setup(SerieManager)
        await EnodoJobManager.async_setup()
        self.watch_series_task = self.loop.create_task(self.watch_series())
        self.check_siridb_connection_task = self.loop.create_task(self.check_siridb_connection())
        self.save_to_disk_task = self.loop.create_task(self.save_to_disk())
        self.check_jobs_task = self.loop.create_task(EnodoJobManager.check_for_jobs())
        await self.socket_server.create()

    async def clean_up(self):
        """
        Cleans up before shutdown
        :return:
        """
        self.watch_series_task.cancel()
        self.check_jobs_task.cancel()
        self.check_siridb_connection_task.cancel()
        await self.socket_server.stop()
        await self.save_to_disk_task

    async def check_siridb_connection(self):
        while ServerState.running:
            _siridb = SiriDB(username=Config.siridb_user,
                             password=Config.siridb_password,
                             dbname=Config.siridb_database,
                             hostlist=[(Config.siridb_host, Config.siridb_port)])
            status, connected = await _siridb.test_connection()
            SiriDB.siridb_status = status
            SiriDB.siridb_connected = connected
            EventLogger.log(f"Checking SiriDB Connection, connected: {connected}", "verbose")
            await asyncio.sleep(Config.siridb_connection_check_interval)

    async def watch_series(self):
        while ServerState.running:
            await ClientManager.check_clients_alive(Config.client_max_timeout)

            for serie_name in await SerieManager.get_series():
                serie = await SerieManager.get_serie(serie_name)
                if serie is not None \
                        and not await serie.ignored():

                    if not await serie.pending_forecast() and (not await serie.is_forecasted() or (
                            serie.new_forecast_at is not None and serie.new_forecast_at < datetime.datetime.now())):
                        # Should be forecasted if not forecasted yet or new forecast should be made
                        if await serie.get_datapoints_count() >= Config.min_data_points:
                            await EnodoJobManager.add_job(EnodoJob(JOB_TYPE_FORECAST_SERIE, serie_name))
                    elif await serie.get_detect_anomalies_status() is DETECT_ANOMALIES_STATUS_REQUESTED:
                        await EnodoJobManager.add_job(EnodoJob(JOB_TYPE_DETECT_ANOMALIES_FOR_SERIE, serie_name))

            await asyncio.sleep(Config.watcher_interval)

    @staticmethod
    async def save_to_disk():
        await SerieManager.save_to_disk()

    async def save_to_disk_on_interval(self):
        while ServerState.running:
            await asyncio.sleep(Config.save_to_disk_interval)
            EventLogger.log('Saving seriemanager state to disk', "verbose")
            await self.save_to_disk()

    async def stop_server(self):

        if self.sio is not None:
            clients = []
            if '/' in self.sio.manager.rooms and None in self.sio.manager.rooms['/']:
                clients = [client for client in self.sio.manager.rooms['/'][None]]

            # await self.sio.emit(event='disconnect', room=None)

            for sid in clients:
                if sid is not None:
                    if sid in self.sio.eio.sockets:
                        try:
                            socket = self.sio.eio.sockets[sid]
                        except KeyError:  # pragma: no cover
                            # the socket was already closed or gone
                            pass
                        else:
                            await socket.close()
                            del self.sio.eio.sockets[sid]

            await asyncio.sleep(1)
            del self.sio
            self.sio = None

        EventLogger.log('Stopping analyser server...', "info")
        EventLogger.log('...Saving log to disk', "info")
        await self.save_to_disk()
        EventLogger.save_to_disk()
        ServerState.running = False
        EventLogger.log('...Doing clean up', "info")
        await self.clean_up()

        EventLogger.log('...Stopping all running tasks', "info")
        await asyncio.sleep(1)
        # asyncio.gather(*asyncio.Task.all_tasks()).cancel()
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

    @middleware
    async def _docs_only_middleware(self, request, handler):
        if request.path.startswith('/api') and not request.path.startswith('/api/docs'):
            return web.json_response(status=404)
        else:
            return await handler(request)

    def start_server(self):
        Config.read_config(self.config_path)
        EventLogger.prepare(Config.log_path, self._log_level)
        EventLogger.log('Starting...', "info")
        EventLogger.log('Loaded Config...', "info")

        if self._docs_only:
            EventLogger.log('Running in docs_only mode...', "info")
            self.app = web.Application(middlewares=[self._docs_only_middleware])
        else:
            EventLogger.log('Running in secure mode...', "info")
            self.app = web.Application(middlewares=[auth])

        if Config.enable_rest_api:
            EventLogger.log('REST API enabled', "info")
            cors = aiohttp_cors.setup(self.app, defaults={
                "*": aiohttp_cors.ResourceOptions(
                    allow_credentials=True,
                    expose_headers="*",
                    allow_headers="*",
                )
            })

            # self.app.router.add_get("/", websocket_index, allow_head=False)

            # Add rest api routes
            self.app.router.add_get("/api/series", ApiHandlers.get_monitored_series, allow_head=False)
            self.app.router.add_post("/api/series", ApiHandlers.add_serie)
            self.app.router.add_get("/api/series/{serie_name}", ApiHandlers.get_monitored_serie_details,
                                    allow_head=False)
            self.app.router.add_delete("/api/series/{serie_name}", ApiHandlers.remove_serie)
            self.app.router.add_get("/api/enodo/models", ApiHandlers.get_possible_analyser_models, allow_head=False)

            # Add internal api routes
            self.app.router.add_get("/api/settings", ApiHandlers.get_settings, allow_head=False)
            self.app.router.add_post("/api/settings", ApiHandlers.set_settings)
            self.app.router.add_get("/api/siridb/status", ApiHandlers.get_siridb_status, allow_head=False)
            self.app.router.add_get("/api/enodo/status", ApiHandlers.get_siridb_enodo_status, allow_head=False)
            self.app.router.add_get("/api/enodo/log", ApiHandlers.get_event_log, allow_head=False)
            self.app.router.add_get("/api/enodo/clients", ApiHandlers.get_connected_clients, allow_head=False)

            # init docs with all parameters, usual for ApiSpec
            setup_aiohttp_apispec(
                app=self.app,
                title="API references",
                version="v1",
                url="/api/docs/swagger.json",
                swagger_path="/api/docs",
                securityDefinitions={
                    "user": {"type": "basic", "name": "Authorization", "in": "header"}
                }
            )

            # Configure CORS on all routes.
            for route in list(self.app.router.routes()):
                cors.add(route)

        self.sio = None
        if Config.enable_socket_io_api:
            EventLogger.log('Socket.io API enabled', "info")
            self.sio = socketio.AsyncServer(async_mode='aiohttp',
                                            ping_timeout=60,
                                            ping_interval=25,
                                            cookie=None)
            self.sio.attach(self.app)

        self.app.on_shutdown.append(self._stop_server_from_aiohttp_cleanup)
        self.loop.run_until_complete(self.start_up())
        try:
            web.run_app(self.app, print=print_custom_aiohttp_startup_message, port=self.port)
        except asyncio.CancelledError:
            pass
