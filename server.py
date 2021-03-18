import datetime

import asyncio
import logging

import aiohttp_cors
import socketio
from aiohttp import web
from enodo.protocol.packagedata import *

from lib.analyser.model import EnodoModelManager
from lib.api.apihandlers import ApiHandlers, auth
from lib.config import Config
from lib.events.enodoeventmanager import EnodoEventManager
from lib.enodojobmanager import EnodoJobManager
from enodo.jobs import JOB_TYPE_BASE_SERIES_ANALYSIS, JOB_TYPE_FORECAST_SERIES, JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES, JOB_TYPE_STATIC_RULES, JOB_STATUS_NONE, JOB_STATUS_DONE
from lib.logging import prepare_logger
from lib.series.seriesmanager import SeriesManager
from lib.serverstate import ServerState
from lib.socket import ClientManager
from lib.socket.handler import receive_new_series_points, receive_worker_status_update, \
    received_worker_refused
from lib.socket.package import LISTENER_NEW_SERIES_POINTS, WORKER_UPDATE_BUSY, WORKER_JOB_RESULT, WORKER_REFUSED, \
    WORKER_JOB_CANCELLED
from lib.socket.socketserver import SocketServer
from lib.socketio.socketiohandlers import SocketIoHandler
from lib.socketio.socketiorouter import SocketIoRouter
from lib.util import print_custom_aiohttp_startup_message


class Server:

    def __init__(self, loop, port, config_path, log_level='info'):
        self.loop = loop
        self.port = port
        self.app = None
        self.sio = None
        self.auth = None

        self._config_path = config_path
        self.backend_socket = None
        self._log_level = log_level

        self._watch_series_task = None
        self._save_to_disk_task = None
        self._check_jobs_task = None
        self._connection_management_task = None

        self._watch_tasks_task = None

    async def start_up(self):
        # Setup server state object
        await ServerState.async_setup(sio=self.sio)

        # Setup internal security token for authenticating backend socket connections
        logging.info('Setting up internal communications token...')
        Config.setup_internal_security_token()

        # Setup backend socket connection
        self.backend_socket = SocketServer(Config.socket_server_host, Config.socket_server_port,
                                           Config.internal_security_token,
                                           {LISTENER_NEW_SERIES_POINTS: receive_new_series_points,
                                            WORKER_JOB_RESULT: EnodoJobManager.receive_job_result,
                                            WORKER_UPDATE_BUSY: receive_worker_status_update,
                                            WORKER_REFUSED: received_worker_refused,
                                            WORKER_JOB_CANCELLED: EnodoJobManager.receive_worker_cancelled_job})

        # Setup REST API handlers
        await ApiHandlers.prepare()

        # Setup websocket handlers and routes
        if self.sio is not None:
            await SocketIoHandler.prepare(self.sio)
            SocketIoRouter(self.sio)

        # Setup internal managers for handling and managing series, clients, jobs, events and models
        await SeriesManager.prepare(SocketIoHandler.internal_updates_series_subscribers)
        await SeriesManager.read_from_disk()
        await ClientManager.setup(SeriesManager)
        await EnodoJobManager.async_setup(SocketIoHandler.internal_updates_queue_subscribers)
        await EnodoJobManager.load_from_disk()
        await EnodoEventManager.async_setup()
        await EnodoEventManager.load_from_disk()
        await EnodoModelManager.async_setup(SocketIoHandler.internal_updates_enodo_models_subscribers)
        await EnodoModelManager.load_from_disk()

        # Setup background tasks
        self._watch_series_task = self.loop.create_task(self.watch_series())
        self._save_to_disk_task = self.loop.create_task(self.save_to_disk())
        self._check_jobs_task = self.loop.create_task(
            EnodoJobManager.check_for_jobs())
        self._connection_management_task = self.loop.create_task(
            self._manage_connections())
        self._watch_tasks_task = self.loop.create_task(self.watch_tasks())

        # Open backend socket connection
        await self.backend_socket.create()

    async def clean_up(self):
        """
        Cleans up before shutdown
        :return:
        """
        self._watch_series_task.cancel()
        self._check_jobs_task.cancel()
        self._save_to_disk_task.cancel()
        self._connection_management_task.cancel()
        self._watch_tasks_task.cancel()
        await self.backend_socket.stop()

    async def _manage_connections(self):
        while ServerState.running:
            await asyncio.sleep(Config.save_to_disk_interval)
            ServerState.tasks_last_runs['manage_connections'] = datetime.datetime.now()
            logging.debug('Cleaning-up clients')
            await ClientManager.check_clients_alive(Config.client_max_timeout)
            logging.debug('Refreshing SiriDB connection status')
            await ServerState.refresh_siridb_status()

    async def watch_tasks(self):
        while ServerState.running:
            await asyncio.sleep(Config.save_to_disk_interval * 2)
            logging.debug('Watching background tasks')
            try:
                for task in ServerState.tasks_last_runs:
                    if ServerState.tasks_last_runs[task] is not None and \
                        (datetime.datetime.now() - ServerState.tasks_last_runs[task]).total_seconds() > 60:
                        logging.error(f"Background task \"{task}\" is unresponsive!")
            except Exception as e:
                logging.error('Watching background tasks failed')

    async def watch_series(self):
        while ServerState.running:
            ServerState.tasks_last_runs['watch_series'] = datetime.datetime.now()
            series_names = SeriesManager.get_all_series()
            for series_name in series_names:
                series = await SeriesManager.get_series(series_name)
                
                # Check if series is valid and not ignored
                if series is not None \
                        and not series.is_ignored():

                    # Check if requirement of min amount of datapoints is met
                    if await series.get_datapoints_count() >= Config.min_data_points or (
                        series.series_config.min_data_points is not None and series.get_datapoints_count() >= series.series_config.min_data_points):
                        try:
                            # Check if series does not have any failed jobs
                            if not len(EnodoJobManager.get_failed_jobs_for_series(series_name)):

                                if series.job_activated(JOB_TYPE_BASE_SERIES_ANALYSIS) \
                                    and await series.get_job_status(JOB_TYPE_BASE_SERIES_ANALYSIS) == JOB_STATUS_NONE:
                                    await EnodoJobManager.create_job(JOB_TYPE_BASE_SERIES_ANALYSIS, series_name)

                                # If forecast job is not pending and job is due
                                if series.job_activated(JOB_TYPE_FORECAST_SERIES) \
                                    and (await series.get_job_status(JOB_TYPE_FORECAST_SERIES) in [JOB_STATUS_NONE, JOB_STATUS_DONE]) and \
                                    await series.is_job_due(JOB_TYPE_FORECAST_SERIES):

                                    await EnodoJobManager.create_job(JOB_TYPE_FORECAST_SERIES, series_name)
                                    continue
                                
                                # If anomaly detect job is not pending and job is due
                                if series.job_activated(JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES) \
                                    and (await series.get_job_status(JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES) in [JOB_STATUS_NONE, JOB_STATUS_DONE]) and \
                                    await series.is_job_due(JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES):
                                    
                                    await EnodoJobManager.create_job(JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES, series_name)
                                    continue

                                # If anomaly detect job is not pending and job is due
                                if series.job_activated(JOB_TYPE_STATIC_RULES) \
                                    and (await series.get_job_status(JOB_TYPE_STATIC_RULES) in [JOB_STATUS_NONE, JOB_STATUS_DONE]) and \
                                    await series.is_job_due(JOB_TYPE_STATIC_RULES):
                                    
                                    await EnodoJobManager.create_job(JOB_TYPE_STATIC_RULES, series_name)
                                    continue
                        except Exception as e:
                            logging.error(f"Something went wrong when trying to create new job")
                            logging.debug(f"Corresponding error: {e}")

            await asyncio.sleep(Config.watcher_interval)

    @staticmethod
    async def _save_to_disk():
        await SeriesManager.save_to_disk()
        await EnodoJobManager.save_to_disk()
        await EnodoEventManager.save_to_disk()
        await EnodoModelManager.save_to_disk()

    async def save_to_disk(self):
        while ServerState.running:
            await asyncio.sleep(Config.save_to_disk_interval)
            ServerState.tasks_last_runs['save_to_disk'] = datetime.datetime.now()
            logging.debug('Saving seriesmanager state to disk')
            await self._save_to_disk()

    async def stop_server(self):
        logging.info('Stopping Hub...')
        if self.sio is not None:
            clients = []
            if '/' in self.sio.manager.rooms and None in self.sio.manager.rooms['/']:
                clients = self.sio.manager.rooms['/'][None]
            for sid in clients:
                if sid is not None:
                    if sid in self.sio.eio.sockets:
                        try:
                            socket = self.sio.eio.sockets[sid]
                        except KeyError:  # pragma: no cover
                            # the socket was already closed or gone
                            pass
                        else:
                            await socket.close(wait=False)
                            del self.sio.eio.sockets[sid]

            await asyncio.sleep(1)
            del self.sio
            self.sio = None
        logging.info('...Saving data to disk')
        await self._save_to_disk()
        ServerState.running = False
        ServerState.stop()
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
            except asyncio.CancelledError as _:
                pass

        self.loop.stop()
        print('Bye!')

    async def _stop_server_from_aiohttp_cleanup(self, *args, **kwargs):
        await self.stop_server()

    def start_server(self):
        prepare_logger(self._log_level)
        Config.read_config(self._config_path)
        logging.info('Starting...')
        logging.info('Loaded Config...')

        logging.info('Running API\'s in secure mode...')
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

            # Add rest api routes
            self.app.router.add_get(
                "/api/series", ApiHandlers.get_monitored_series, allow_head=False)
            self.app.router.add_post("/api/series", ApiHandlers.add_series) 
            self.app.router.add_get("/api/series/{series_name}", ApiHandlers.get_monitored_series_details,
                                    allow_head=False)
            self.app.router.add_delete(
                "/api/series/{series_name}", ApiHandlers.remove_series)
            self.app.router.add_get(
                "/api/enodo/model", ApiHandlers.get_possible_analyser_models, allow_head=False)
            self.app.router.add_post(
                "/api/enodo/model", ApiHandlers.add_analyser_models)
            self.app.router.add_get(
                "/api/enodo/event/output", ApiHandlers.get_enodo_event_outputs)
            self.app.router.add_delete(
                "/api/enodo/event/output/{output_id}", ApiHandlers.remove_enodo_event_output)
            self.app.router.add_post(
                "/api/enodo/event/output", ApiHandlers.add_enodo_event_output)
            self.app.router.add_get(
                "/api/enodo/stats", ApiHandlers.get_enodo_stats)

            # Add internal api routes
            self.app.router.add_get(
                "/api/settings", ApiHandlers.get_settings, allow_head=False)
            self.app.router.add_post(
                "/api/settings", ApiHandlers.update_settings)
            self.app.router.add_get(
                "/api/enodo/status", ApiHandlers.get_siridb_enodo_status, allow_head=False)
            self.app.router.add_get(
                "/api/enodo/log", ApiHandlers.get_event_log, allow_head=False)
            self.app.router.add_get(
                "/api/enodo/clients", ApiHandlers.get_connected_clients, allow_head=False)

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

            logging.getLogger('aiohttp').setLevel(logging.ERROR)
            logging.getLogger('socketio').setLevel(logging.ERROR)
            logging.getLogger('engineio').setLevel(logging.ERROR)

        self.app.on_shutdown.append(self._stop_server_from_aiohttp_cleanup)
        self.loop.run_until_complete(self.start_up())
        try:
            web.run_app(
                self.app, print=print_custom_aiohttp_startup_message, port=self.port)
        except (asyncio.CancelledError, RuntimeError):
            pass
