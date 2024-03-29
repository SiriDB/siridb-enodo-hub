import datetime
from time import time

import asyncio
import logging
import signal

import aiohttp_cors
import socketio
from aiohttp import web
from aiojobs.aiohttp import setup
from enodo.protocol.packagedata import *
from enodo.protocol.package import LISTENER_NEW_SERIES_POINTS, \
    WORKER_UPDATE_BUSY, WORKER_JOB_RESULT, WORKER_REFUSED, \
    WORKER_JOB_CANCELLED
from lib.exceptions.enodoexception import EnodoScheduleException
from lib.series.seriestemplate import SeriesConfigTemplate
from lib.state.thingsdbstorage import ThingsDBStorage
from lib.util.upgrade import UpgradeUtil

from lib.webserver.apihandlers import ApiHandlers, auth
from lib.config import Config
from lib.eventmanager import EnodoEventManager
from lib.jobmanager import EnodoJobManager

from lib.logging import prepare_logger
from lib.series.series import Series
from lib.series.seriesmanager import SeriesManager
from lib.serverstate import ServerState
from lib.socket import ClientManager
from lib.socket.handler import receive_new_series_points, \
    receive_worker_status_update, received_worker_refused
from lib.socket.socketserver import SocketServer
from lib.socketio.socketiohandlers import SocketIoHandler
from lib.socketio.socketiorouter import SocketIoRouter
from lib.util import print_custom_aiohttp_startup_message
from lib.webserver.routes import setup_routes
from lib.state.resource import ResourceManager, resource_manager_index
from version import VERSION


class Server:

    def __init__(self, port, config_path, log_level='info'):
        self.loop = None
        self.port = port
        self.app = None
        self.sio = None
        self.auth = None

        self._config_path = config_path
        self.backend_socket = None
        self._log_level = log_level

        self._watch_series_task = None
        self._check_jobs_task = None
        self._connection_management_task = None

        self._watch_tasks_task = None
        self._shutdown_trigger = False
        self._force_shutdown = False

    async def start_up(self):
        """All connections and classes will be prepared
        """

        self.loop = asyncio.get_running_loop()

        for signame in ('SIGINT', 'SIGTERM'):
            self.loop.add_signal_handler(
                getattr(signal, signame),
                lambda: asyncio.ensure_future(self.stop_server()))

        storage = ThingsDBStorage()
        try:
            await storage.startup()
        except Exception as e:
            logging.error(
                "Cannot startup, not able to connect to ThingsDB")
            logging.debug(f"Corresponding error: {e}")
            raise e

        # Setup server state object
        await ServerState.async_setup(sio=self.sio,
                                      storage=storage)
        try:
            await UpgradeUtil.upgrade_thingsdb()
        except Exception as e:
            logging.error("Upgrade of thingsdb collection failed")
            logging.debug(f"Corresponding error: {e}")
            raise e

        await Config.read_settings(ServerState.storage.client)

        # Setup internal security token for authenticating
        # backend socket connections
        logging.info('Setting up internal communications token...')
        Config.setup_internal_security_token()

        # Setup backend socket connection
        self.backend_socket = SocketServer(
            Config.socket_server_host, Config.socket_server_port,
            Config.internal_security_token,
            {LISTENER_NEW_SERIES_POINTS: receive_new_series_points,
             WORKER_JOB_RESULT: EnodoJobManager.receive_job_result,
             WORKER_UPDATE_BUSY: receive_worker_status_update,
             WORKER_REFUSED: received_worker_refused,
             WORKER_JOB_CANCELLED: EnodoJobManager.
             receive_worker_cancelled_job})

        # Setup REST API handlers
        ApiHandlers.prepare()

        # Setup websocket handlers and routes
        if self.sio is not None:
            SocketIoHandler.prepare(self.sio)
            SocketIoRouter(self.sio)

        ServerState.series_config_template_rm = ResourceManager(
            'series_config_templates', SeriesConfigTemplate, cache_only=True)
        await ServerState.series_config_template_rm.load()
        ServerState.series_rm = ResourceManager(
            'series', Series, extra_index_field="name",
            keep_in_memory=-1)
        await ServerState.series_rm.load()

        # Setup internal managers for handling and managing series,
        # clients, jobs, events and modules
        await SeriesManager.prepare(
            SocketIoHandler.internal_updates_series_subscribers)
        await ClientManager.setup(SeriesManager)
        EnodoJobManager.setup(
            SocketIoHandler.internal_updates_queue_subscribers)
        await EnodoJobManager.load_from_disk()
        await EnodoEventManager.async_setup()
        await ClientManager.load_from_disk()

        scheduler = ServerState.scheduler
        self._watch_series_task = await scheduler.spawn(self.watch_series())
        self._check_jobs_task = await scheduler.spawn(
            EnodoJobManager.check_for_jobs())
        self._connection_management_task = await scheduler.spawn(
            self._manage_connections())
        self._watch_tasks_task = await scheduler.spawn(self.watch_tasks())
        self._cleanup_resource_managers = await scheduler.spawn(
            self.clean_resource_manager())

        # Open backend socket connection
        await self.backend_socket.create()
        ServerState.readiness = True

    async def clean_up(self):
        """Cleans up before shutdown
        """
        await ServerState.storage.close()
        await ServerState.scheduler.close()
        await self.backend_socket.stop()

    async def clean_resource_manager(self):
        while ServerState.running:
            logging.debug("Cleaning resource manager caches")
            for manager in resource_manager_index.values():
                manager.cleanup()
            await asyncio.sleep(10)

    async def wait_for_queue(self):
        """Wait for queue to be empty or until max amount of seconds
        """
        logging.info("...Start waiting for queue to be empty")
        start_ts = time()
        current_ts = start_ts
        queues_empty = EnodoJobManager.get_open_jobs_count() == 0 and \
            EnodoJobManager.get_active_jobs_count() == 0
        while (current_ts - start_ts) < 60 and not queues_empty and \
                not self._force_shutdown:
            current_ts = time()
            queues_empty = EnodoJobManager.get_open_jobs_count(
            ) == 0 and EnodoJobManager.get_active_jobs_count() == 0
            await asyncio.sleep(1)

        if not queues_empty:
            if self._force_shutdown:
                logging.info(
                    "...Queue is not empty, but shutdown is forced, "
                    "canceling jobs")
            else:
                logging.info(
                    "...Queue is not empty, but hit max time limit, "
                    "canceling jobs")
            await EnodoJobManager.clear_jobs()
        else:
            logging.info(
                "...Queue is empty, closing task to work queue")
        ServerState.work_queue = False

    async def _manage_connections(self):
        """Background task to check if all connections are up
        """
        while ServerState.running:
            await asyncio.sleep(Config.save_to_disk_interval)
            ServerState.tasks_last_runs['manage_connections'] = \
                datetime.datetime.now()
            logging.debug('Cleaning-up clients')
            await ClientManager.check_clients_alive(
                Config.client_max_timeout)
            logging.debug('Refreshing SiriDB connection status')
            await ServerState.refresh_siridb_status()

    async def watch_tasks(self):
        """Background task to check if other background tasks are running
        """
        while ServerState.running:
            await asyncio.sleep(Config.save_to_disk_interval * 2)
            logging.debug('Watching background tasks')
            try:
                for task in ServerState.tasks_last_runs:
                    last_run = ServerState.tasks_last_runs[task]
                    now = datetime.datetime.now()
                    if last_run is not None and \
                            (now - last_run).total_seconds() > 60:
                        logging.error(
                            f"Background task \"{task}\" is unresponsive!")
            except Exception as _:
                logging.error('Watching background tasks failed')

    async def _check_for_jobs(self, series, state, series_name,
                              base_only=False):
        """Private function to check if jobs need to be created

        Args:
            series (Series): Series instance
            series_name (string): name of series
        """

        if base_only:
            # Check if base analysis needs to run
            if state.is_job_due(
                    series.base_analysis_job.config_name, series):
                await EnodoJobManager.create_job(
                    series.base_analysis_job.config_name, series_name)
                return
            raise EnodoScheduleException(
                "No base job created",
                job_config_name=series.base_analysis_job.config_name)

        # loop through scheduled jobs:
        jobs_created = 0
        job_schedules = state.get_all_job_schedules()
        for job_config_name in series.config.job_config:
            if job_config_name in job_schedules and \
                    state.is_job_due(job_config_name, series):
                await EnodoJobManager.create_job(job_config_name, series_name)
                jobs_created += 1

        if jobs_created == 0:
            raise EnodoScheduleException("No job created")

    async def _handle_low_datapoints(self, series, state):
        dp_ok, _ = series.check_datapoint_count(state)
        if dp_ok:
            return False
        await self._check_for_jobs(series, state, series.name,
                                   base_only=True)
        return True

    async def work_schedule_queue(self):
        current_time = time()
        while len(ServerState.job_schedule_index.items) > 0:
            async with ServerState.job_schedule_index.seek_and_hold() as \
                    (series_name, next_ts):
                if next_ts > current_time:
                    break
                series, state = \
                    await SeriesManager.get_series_read_only(series_name)
                # Check if series is valid and not ignored
                if series is None or series.is_ignored():
                    continue
                # Check if requirement of min amount of datapoints is met
                if state.get_datapoints_count() is None:
                    continue
            series_name, next_ts = \
                await ServerState.job_schedule_index.pop()

            try:
                if await self._handle_low_datapoints(series, state):
                    series.schedule_jobs(state, delay=60)
                    continue
                await self._check_for_jobs(series, state, series_name)
            except EnodoScheduleException as e:
                series.schedule_jobs(state, delay=60)
                logging.debug(
                    "Job could not be created, "
                    f"rescheduling {series_name}...")
            except Exception as e:
                logging.error(
                    "Something went wrong when trying to create new job")
                logging.debug(
                    f"Corresponding error: {e}, "
                    f'exception class: {e.__class__.__name__}')

            await asyncio.sleep(0.1)

    async def watch_series(self):
        """Background task to check each series if
        jobs need to be managed
        """
        while ServerState.running:
            ServerState.tasks_last_runs['watch_series'] = \
                datetime.datetime.now()

            try:
                await self.work_schedule_queue()
            except Exception as e:
                logging.error("Something is wrong with the watch series job")
                logging.debug(f"Corresponding error: {e}")
                import traceback
                traceback.format_exc()

            await asyncio.sleep(Config.watcher_interval)

    async def stop_server(self):
        """Stop all parts of the server for a clean shutdown
        """
        if self._shutdown_trigger:
            self._force_shutdown = True
            return
        self._shutdown_trigger = True
        logging.info('Stopping Hub...')
        ServerState.readiness = False
        if self.sio is not None:
            clients = []
            if '/' in self.sio.manager.rooms and \
                    None in self.sio.manager.rooms['/']:
                clients = [sid
                           for sid in self.sio.manager.rooms['/']
                           [None]]
            for sid in clients:
                await self.sio.disconnect(sid)
            rooms = self.sio.manager.rooms
            self.sio.manager.set_server(None)
            for room in rooms:
                print("closing room: ", room)
                await self.sio.close_room(room)

            await asyncio.sleep(1)
            del self.sio
            self.sio = None

        await SeriesManager.close()
        ServerState.running = False
        await self.wait_for_queue()
        logging.info('...Doing clean up')
        await self.clean_up()

        if ServerState.storage is not None:
            await ServerState.storage.close()
        ServerState.stop()

        logging.info('...Stopping all running tasks')
        logging.info('...Going down in 1')

        await asyncio.sleep(1)
        tasks = [task for task in asyncio.all_tasks() if task is not
                 asyncio.current_task()]
        for task in tasks:
            try:
                task.cancel()
                await asyncio.wait([task])
            except asyncio.CancelledError as _:
                pass
        await asyncio.gather(*tasks, return_exceptions=True)

        self.loop.stop()
        print('Bye!')

    async def _stop_server_from_aiohttp_cleanup(self, *args, **kwargs):
        """Stop server when aiohttp can exit
        """
        await self.stop_server()

    async def _start_server_from_aiohttp_startup(self, *args, **kwargs):
        """Start server when aiohttp starts
        """
        await self.start_up()

    def start_server(self):
        """Start server by loading config and calling other startup functions
        """
        prepare_logger(self._log_level)
        Config.read_config(self._config_path)
        logging.info(f'Starting Hub V{VERSION}...')
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
            setup_routes(self.app, cors)

        self.sio = None
        if Config.enable_socket_io_api:
            logging.info('Socket.io API enabled')
            self.sio = socketio.AsyncServer(async_mode='aiohttp',
                                            # ping_timeout=60,
                                            # ping_interval=25,
                                            # cookie=None,
                                            cors_allowed_origins='*')
            self.sio.attach(self.app)
            logging.getLogger('socketio').setLevel(logging.ERROR)
            logging.getLogger('engineio').setLevel(logging.ERROR)
        logging.getLogger('aiohttp').setLevel(logging.ERROR)
        logging.getLogger('siridb.connector').setLevel(logging.ERROR)

        self.app.on_cleanup.append(
            self._stop_server_from_aiohttp_cleanup)
        self.app.on_startup.append(
            self._start_server_from_aiohttp_startup)

        # Setup aiojobs
        setup(self.app)

        try:
            web.run_app(
                self.app, print=print_custom_aiohttp_startup_message,
                port=self.port)
        except (asyncio.CancelledError, RuntimeError):
            pass
