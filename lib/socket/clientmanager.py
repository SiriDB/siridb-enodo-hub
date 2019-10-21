import datetime

from lib.logging.eventlogger import EventLogger
from lib.serie.series import DETECT_ANOMALIES_STATUS_PENDING, DETECT_ANOMALIES_STATUS_REQUESTED


class Client:

    def __init__(self, client_id, ip_address, writer, version="unknown", last_seen=None, busy=None):
        self.client_id = client_id
        self.ip_address = ip_address
        self.writer = writer
        self.busy = busy
        self.is_going_busy = False
        self.last_seen = last_seen
        self.version = version
        self.pending_series = []
        if last_seen is None:
            self.last_seen = datetime.datetime.now()

    def to_dict(self):
        return {'client_id': self.client_id,
                'ip_address': self.ip_address,
                'writer': self.writer,
                'busy': self.busy,
                'last_seen': self.last_seen,
                'version': self.version}


class ClientManager:
    listeners = {}
    workers = {}
    serie_manager = None

    @classmethod
    async def setup(cls, serie_manager):
        cls.serie_manager = serie_manager

    @classmethod
    async def add_listener(cls, client):
        cls.listeners[client.client_id] = client

    @classmethod
    async def add_worker(cls, client):
        cls.workers[client.client_id] = client

    @classmethod
    async def get_listener_by_id(cls, client_id):
        if client_id in cls.listeners:
            return cls.listeners.get(client_id)
        return None

    @classmethod
    async def get_worker_by_id(cls, client_id):
        if client_id in cls.workers:
            return cls.workers.get(client_id)
        return None

    @classmethod
    async def get_free_worker(cls):
        for worker in cls.workers:
            if not cls.workers.get(worker).busy and not cls.workers.get(worker).is_going_busy:
                return cls.workers.get(worker)
        return None

    @classmethod
    async def check_clients_alive(cls, max_timeout):
        clients_to_remove = []
        for client in cls.listeners:
            if (datetime.datetime.now() - cls.listeners.get(client).last_seen) \
                    .total_seconds() > max_timeout:
                print(f'Not alive: {client}')
                clients_to_remove.append(client)

        for client in clients_to_remove:
            del cls.listeners[client]
        clients_to_remove = []

        for client in cls.workers:
            if (datetime.datetime.now() - cls.workers.get(client).last_seen) \
                    .total_seconds() > max_timeout:
                print(f'Not alive: {client}')
                clients_to_remove.append(client)

        for client in clients_to_remove:
            await cls.check_for_pending_series(cls.workers[client])
            del cls.workers[client]

    @classmethod
    async def remove_worker(cls, client_id):
        await cls.check_for_pending_series(cls.workers[client_id])
        del cls.workers[client_id]

    @classmethod
    async def remove_listener(cls, client_id):
        del cls.listeners[client_id]

    @classmethod
    async def check_for_pending_series(cls, client):
        if len(client.pending_series):
            for serie_name in client.pending_series:
                serie = await cls.serie_manager.get_serie(serie_name)
                if await serie.pending_forecast():
                    EventLogger.log(f'Setting for serie {serie_name} pending to false...', "info")
                    await serie.set_pending_forecast(False)
                if await serie.get_detect_anomalies_status() is DETECT_ANOMALIES_STATUS_PENDING:
                    EventLogger.log(f'Setting for serie {serie_name} anomaly detection pending to false...', "info")
                    await serie.set_detect_anomalies_status(DETECT_ANOMALIES_STATUS_REQUESTED)
