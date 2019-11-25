import asyncio
import datetime
import logging

import aiohttp_cors
import socketio
from aiohttp import web
from aiohttp.web_middlewares import middleware
from aiohttp_apispec import setup_aiohttp_apispec

from enodo.jobs import EnodoForecastJobDataModel
from lib.analyser.model import EnodoModelManager
from lib.api.apihandlers import ApiHandlers, auth
from lib.config.config import Config
from lib.events.enodoeventmanager import EnodoEventManager
from lib.jobmanager.enodojobmanager import EnodoJobManager, JOB_TYPE_FORECAST_SERIE
from lib.logging import prepare_logger
from lib.serie.seriemanager import SerieManager
from lib.serverstate import ServerState
from lib.socket.clientmanager import ClientManager
from lib.socket.handler import receive_new_series_points, receive_worker_status_update, \
    received_worker_refused
from lib.socket.package import LISTENER_NEW_SERIES_POINTS, WORKER_UPDATE_BUSY, WORKER_JOB_RESULT, WORKER_REFUSED, \
    WORKER_JOB_CANCELLED
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
        self.socket_server_task = None
        self.save_to_disk_task = None
        self.check_jobs_task = None
        # self._send_for_analyse = []

    async def start_up(self):
        await ServerState.async_setup(siridb_data_username=Config.siridb_user,
                                      siridb_data_password=Config.siridb_password,
                                      siridb_data_dbname=Config.siridb_database,
                                      siridb_data_hostlist=[(Config.siridb_host, Config.siridb_port)],
                                      siridb_forecast_username=Config.siridb_forecast_user,
                                      siridb_forecast_password=Config.siridb_forecast_password,
                                      siridb_forecast_dbname=Config.siridb_forecast_database,
                                      siridb_forecast_hostlist=[
                                          (Config.siridb_forecast_host, Config.siridb_forecast_port)]
                                      )

        logging.info('Setting up internal communications token...')
        Config.setup_internal_security_token()

        self.socket_server = SocketServer(Config.socket_server_host, Config.socket_server_port,
                                          Config.internal_security_token,
                                          {LISTENER_NEW_SERIES_POINTS: receive_new_series_points,
                                           WORKER_JOB_RESULT: EnodoJobManager.receive_job_result,
                                           WORKER_UPDATE_BUSY: receive_worker_status_update,
                                           WORKER_REFUSED: received_worker_refused,
                                           WORKER_JOB_CANCELLED: EnodoJobManager.receive_worker_cancelled_job})
        await ApiHandlers.prepare()

        if self.sio is not None:
            await SocketIoHandler.prepare(self.sio)
            SocketIoRouter(self.sio)

        await SerieManager.prepare(SocketIoHandler.internal_updates_series_subscribers)
        await SerieManager.read_from_disk()
        await ClientManager.setup(SerieManager)
        await EnodoJobManager.async_setup()
        await EnodoJobManager.load_from_disk()
        await EnodoEventManager.async_setup()
        await EnodoEventManager.load_from_disk()
        await EnodoModelManager.async_setup()
        await EnodoModelManager.load_from_disk()
        self.watch_series_task = self.loop.create_task(self.watch_series())
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
        self.save_to_disk_task.cancel()
        await self.socket_server.stop()

    async def watch_series(self):
        while ServerState.running:
            await ClientManager.check_clients_alive(Config.client_max_timeout)

            for serie_name in await SerieManager.get_series():
                serie = await SerieManager.get_serie(serie_name)
                if serie is not None \
                        and not await serie.ignored():
                    if not len(await EnodoJobManager.get_failed_jobs_for_series(serie_name)):
                        if not await serie.pending_forecast() and (not await serie.is_forecasted() or (
                                serie.new_forecast_at is not None and serie.new_forecast_at < datetime.datetime.now())):
                            # Should be forecasted if not forecasted yet or new forecast should be made
                            if await serie.get_datapoints_count() >= Config.min_data_points:
                                await EnodoJobManager.create_job(JOB_TYPE_FORECAST_SERIE, serie_name, EnodoForecastJobDataModel())
                                await serie.set_pending_forecast(True)
                        # elif await serie.get_detect_anomalies_status() is DETECT_ANOMALIES_STATUS_REQUESTED:
                        #     await EnodoJobManager.create_job(JOB_TYPE_DETECT_ANOMALIES_FOR_SERIE, serie_name)
                        #     await serie.set_detect_anomalies_status(DETECT_ANOMALIES_STATUS_PENDING)

            await asyncio.sleep(Config.watcher_interval)

    @staticmethod
    async def _save_to_disk():
        await SerieManager.save_to_disk()
        await EnodoJobManager.save_to_disk()
        await EnodoEventManager.save_to_disk()
        await EnodoModelManager.save_to_disk()

    async def save_to_disk(self):
        while ServerState.running:
            await asyncio.sleep(Config.save_to_disk_interval)
            logging.debug('Saving seriemanager state to disk')
            await self._save_to_disk()

    async def stop_server(self):

        if self.sio is not None:
            clients = []
            if '/' in self.sio.manager.rooms and None in self.sio.manager.rooms['/']:
                clients = [client for client in self.sio.manager.rooms['/'][None]]

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

        logging.info('Stopping analyser server...')
        logging.info('...Saving data to disk')
        await self._save_to_disk()
        ServerState.running = False
        logging.info('...Doing clean up')
        await self.clean_up()

        logging.info('...Stopping all running tasks')
        logging.info('...Going down in 1')
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
        prepare_logger(self._log_level)
        Config.read_config(self.config_path)
        logging.info('Starting...')
        logging.info('Loaded Config...')

        if self._docs_only:
            logging.info('Running in docs_only mode...')
            self.app = web.Application(middlewares=[self._docs_only_middleware])
        else:
            logging.info('Running in secure mode...')
            self.app = web.Application(middlewares=[auth])

        if Config.enable_rest_api:
            logging.info('REST API enabled')
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
            self.app.router.add_get("/api/enodo/model", ApiHandlers.get_possible_analyser_models, allow_head=False)
            self.app.router.add_post("/api/enodo/model", ApiHandlers.add_analyser_models)
            self.app.router.add_delete("/api/enodo/event/output/{output_id}", ApiHandlers.remove_enodo_event_output)
            self.app.router.add_post("/api/enodo/event/output", ApiHandlers.add_enodo_event_output)

            # Add internal api routes
            self.app.router.add_get("/api/settings", ApiHandlers.get_settings, allow_head=False)
            self.app.router.add_post("/api/settings", ApiHandlers.set_settings)
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
            logging.info('Socket.io API enabled')
            self.sio = socketio.AsyncServer(async_mode='aiohttp',
                                            ping_timeout=60,
                                            ping_interval=25,
                                            cookie=None,
                                            cors_allowed_origins='*',
                                            logger=False)
            self.sio.attach(self.app)

            logging.getLogger('socketio').setLevel(logging.ERROR)
            logging.getLogger('engineio').setLevel(logging.ERROR)

        self.app.on_shutdown.append(self._stop_server_from_aiohttp_cleanup)
        self.loop.run_until_complete(self.start_up())
        try:
            web.run_app(self.app, print=print_custom_aiohttp_startup_message, port=self.port)
        except asyncio.CancelledError:
            pass
