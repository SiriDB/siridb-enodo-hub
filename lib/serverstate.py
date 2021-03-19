from siridb.connector import SiriDBClient
from lib.config import Config
from lib.socketio import SUBSCRIPTION_CHANGE_TYPE_INITIAL


class ServerState:
    running = None
    sio = None
    siridb_data_client = None
    siridb_forecast_client = None
    tasks_last_runs = {}
    siridb_conn_status = {}

    @classmethod
    async def async_setup(cls, sio):
        cls.running = True
        cls.sio = sio

        await cls.setup_siridb_connection()

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

        await cls.refresh_siridb_status()

    @classmethod
    def _siridb_config_equal(cls, a, b):
        if a.get('username') != b.get('username'):
            return False
        if a.get('password') != b.get('password'):
            return False
        if a.get('dbname') != b.get('dbname'):
            return False
        if a.get('hostlist')[0] != b.get('hostlist')[0]:
            return False

        return True

    @classmethod
    async def setup_siridb_connection(cls):
        siridb_data_config, siridb_forecast_config = Config.get_siridb_settings()

        if cls.siridb_data_client is not None:
            cls.stop()
        
        cls.siridb_data_client = SiriDBClient(
            **siridb_data_config,
            keepalive=True)
        await cls.siridb_data_client.connect()
        if not cls._siridb_config_equal(siridb_data_config, siridb_forecast_config):
            if cls.siridb_forecast_client is not None:
                cls.siridb_forecast_client.close()
            cls.siridb_forecast_client = SiriDBClient(
                **siridb_forecast_config,
                keepalive=True)
            await cls.siridb_forecast_client.connect()
        elif cls.siridb_forecast_client is not None:
            cls.siridb_forecast_client.close()
            cls.siridb_forecast_client = None

        await cls.refresh_siridb_status()

    @classmethod
    def get_siridb_data_conn(cls):
        return cls.siridb_data_client

    @classmethod
    def get_siridb_forecast_conn(cls):
        if cls.siridb_forecast_client is None:
            return cls.siridb_data_client
        return cls.siridb_forecast_client


    @classmethod
    def get_siridb_data_conn_status(cls):
        return cls.siridb_data_client.connected

    @classmethod
    def get_siridb_forecast_conn_status(cls):
        if cls.siridb_forecast_client is None:
            return cls.siridb_data_client.connected
        return cls.siridb_forecast_client.connected

    @classmethod
    async def refresh_siridb_status(cls):
        status = {}
        status['data_conn'] = cls.get_siridb_data_conn_status()
        status['analysis_conn'] = cls.get_siridb_forecast_conn_status()

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
        if cls.siridb_forecast_client is not None:
            cls.siridb_forecast_client.close()
