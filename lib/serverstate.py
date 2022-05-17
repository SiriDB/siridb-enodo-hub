from asyncio import Lock
import logging

from aiojobs import create_scheduler
from siridb.connector import SiriDBClient
from lib.config import Config
from lib.siridb.siridb import query_time_unit
from lib.socketio import SUBSCRIPTION_CHANGE_TYPE_INITIAL


class ServerState:
    running = None
    work_queue = None
    sio = None
    siridb_data_client = None
    siridb_output_client = None
    siridb_data_client_lock = None
    siridb_output_client_lock = None
    tasks_last_runs = {}
    siridb_conn_status = {}
    siridb_ts_unit = None
    readiness = None
    scheduler = None

    @classmethod
    async def async_setup(cls, sio):
        cls.running = True
        cls.work_queue = True
        cls.readiness = False
        cls.sio = sio
        cls.siridb_data_client_lock = Lock()
        cls.siridb_output_client_lock = Lock()

        await cls.setup_siridb_data_connection()
        await cls.setup_siridb_output_connection()

        cls.tasks_last_runs = {
            'watch_series': None,
            'save_to_disk': None,
            'check_jobs': None,
            'manage_connections': None
        }

        cls.siridb_conn_status = {
            'data_conn': False,
            'analysis_conn': False
        }

        cls.scheduler = await create_scheduler()

        await cls.refresh_siridb_status()

    @classmethod
    def get_readiness(cls):
        return cls.readiness

    @classmethod
    def _siridb_config_equal(cls, a, b):
        if a.get('user') != b.get('user'):
            return False
        if a.get('password') != b.get('password'):
            return False
        if a.get('database') != b.get('database'):
            return False
        if a.get('host') != b.get('host'):
            return False

        return True

    @classmethod
    async def setup_siridb_data_connection(cls):
        data_config, output_config = Config.get_siridb_settings()

        logging.info('Setting up SiriDB data Connection')
        async with cls.siridb_data_client_lock:
            if cls.siridb_data_client is not None:
                cls.siridb_data_client.close()

            cls.siridb_data_client = SiriDBClient(
                **data_config,
                keepalive=True)
            await cls.siridb_data_client.connect()

        await cls.refresh_siridb_status()

    @classmethod
    async def setup_siridb_output_connection(cls):
        data_config, output_config = Config.get_siridb_settings()

        logging.info('Setting up SiriDB output Connection')
        async with cls.siridb_output_client_lock:
            if cls.siridb_output_client is not None:
                cls.siridb_output_client.close()
                cls.siridb_output_client = None

            if not cls._siridb_config_equal(data_config, output_config):
                cls.siridb_output_client = SiriDBClient(
                    **output_config,
                    keepalive=True)
                await cls.siridb_output_client.connect()

        await cls.refresh_siridb_status()

    @classmethod
    def get_siridb_data_conn(cls):
        return cls.siridb_data_client

    @classmethod
    def get_siridb_output_conn(cls):
        if cls.siridb_output_client is None:
            return cls.siridb_data_client
        return cls.siridb_output_client

    @classmethod
    def get_siridb_data_conn_status(cls):
        return cls.siridb_data_client.connected

    @classmethod
    def get_siridb_output_conn_status(cls):
        if cls.siridb_output_client is None:
            return cls.siridb_data_client.connected
        return cls.siridb_output_client.connected

    @classmethod
    async def refresh_siridb_status(cls):
        status = {}
        status['data_conn'] = cls.get_siridb_data_conn_status()
        if status['data_conn']:
            cls.siridb_ts_unit = await query_time_unit(cls.siridb_data_client)
        status['analysis_conn'] = cls.get_siridb_output_conn_status()

        if status != cls.siridb_conn_status:
            cls.siridb_conn_status = status
            if cls.sio is not None:
                await cls.sio.emit('update', {
                    'resource': 'siridb_status',
                    'updateType': SUBSCRIPTION_CHANGE_TYPE_INITIAL,
                    'resourceData': cls.siridb_conn_status
                }, room='siridb_status_updates')

    @classmethod
    def stop(cls):
        if cls.siridb_data_client is not None:
            cls.siridb_data_client.close()
        if cls.siridb_output_client is not None:
            cls.siridb_output_client.close()
