from asyncio import Lock, ensure_future
import logging
import time

from enodo.jobs import JOB_STATUS_FAILED, JOB_STATUS_OPEN, JOB_STATUS_PENDING
from thingsdb.client import Client

from aiojobs import create_scheduler
from siridb.connector import SiriDBClient
from lib.config import Config
from lib.siridb.siridb import query_time_unit
from lib.state.priorityqueue import SeriesPriorityQueue


class ServerState:
    running = None
    siridb_data_client = None
    siridb_output_client = None
    siridb_data_client_lock = None
    siridb_output_client_lock = None
    tasks_last_runs = {}
    siridb_conn_status = {}
    siridb_ts_unit = {}
    readiness = None
    scheduler = None
    job_schedule_index = None

    series_rm = None
    series_config_rm = None
    job_config_template_rm = None
    thingsdb_client = None

    @classmethod
    async def async_setup(cls):
        cls.running = True
        cls.job_schedule_index = SeriesPriorityQueue()
        cls.readiness = False
        cls.siridb_data_client_lock = Lock()
        cls.siridb_output_client_lock = Lock()

        try:
            await cls._setup_storage()
        except Exception:
            logging.error("Cannot connect with thingsdb...")
            exit()

        cls.tasks_last_runs = {
            'check_jobs': None,
            'manage_connections': None
        }

        cls.siridb_conn_status = {
            'data_conn': False,
            'analysis_conn': False
        }

        cls.siridb_ts_unit = {
            'data': 'ms',
            'analysis': 'ms'
        }

        await cls.setup_siridb_data_connection()
        await cls.setup_siridb_output_connection()
        cls.scheduler = await create_scheduler()
        await cls.refresh_siridb_status()

    @classmethod
    async def _setup_storage(cls):
        cls.thingsdb_client = Client()
        await cls.thingsdb_client.connect(Config.thingsdb_host,
                                          port=Config.thingsdb_port)
        await cls.thingsdb_client.authenticate(Config.thingsdb_auth_token)
        cls.thingsdb_client.set_default_scope(Config.thingsdb_scope)

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

        if cls.siridb_conn_status['data_conn']:
            cls.siridb_ts_unit['data'] = await query_time_unit(
                cls.siridb_data_client)

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

        if cls.siridb_conn_status['output_conn']:
            cls.siridb_ts_unit['output'] = await query_time_unit(
                cls.get_siridb_output_conn())

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
    def index_series_schedules(cls, series, state):
        if series.is_ignored():
            return False
        job_schedules = state.get_all_job_schedules()
        earliest = None
        for job_config_name in series.config.job_config:
            schedule = job_schedules.get(job_config_name)
            next_ts = None
            if state.get_job_status(job_config_name) in [
                    JOB_STATUS_FAILED, JOB_STATUS_OPEN, JOB_STATUS_PENDING]:
                continue
            if schedule is None:
                next_ts = int(time.time())
            else:
                if schedule["type"] == "N":
                    if state.interval is not None:
                        points_left = int(schedule["value"] -
                                          state.get_datapoints_count())
                        if points_left < 0:
                            points_left = 0
                        next_ts = int(
                            time.time() + int(state.interval) *
                            points_left)
                    else:
                        next_ts = int(time.time())
                        # Base job

                elif schedule["type"] == "TS":
                    next_ts = int(schedule["value"])
            if next_ts is not None:
                if earliest is None or next_ts < earliest:
                    earliest = next_ts
        if earliest is not None:
            return ensure_future(
                cls.job_schedule_index.insert_schedule(
                    series.name, earliest))

    @classmethod
    async def refresh_siridb_status(cls):
        status = {}
        status['data_conn'] = cls.get_siridb_data_conn_status()
        status['output_conn'] = cls.get_siridb_output_conn_status()

        if status != cls.siridb_conn_status:
            cls.siridb_conn_status = status

    @classmethod
    def stop(cls):
        if cls.siridb_data_client is not None:
            cls.siridb_data_client.close()
        if cls.siridb_output_client is not None:
            cls.siridb_output_client.close()
