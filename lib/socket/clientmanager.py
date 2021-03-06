import datetime
import logging

from enodo import WorkerConfigModel
from enodo.model.config.worker import WORKER_MODE_GLOBAL, WORKER_MODE_DEDICATED_JOB_TYPE, \
    WORKER_MODE_DEDICATED_SERIES
from enodo.jobs import JOB_STATUS_OPEN

from lib.analyser.model import EnodoModelManager
from lib.events.enodoeventmanager import EnodoEvent, EnodoEventManager, \
    ENODO_EVENT_LOST_CLIENT_WITHOUT_GOODBYE


class EnodoClient:

    def __init__(self, client_id, ip_address, writer, version="unknown", last_seen=None, online=True):
        self.client_id = client_id
        self.ip_address = ip_address
        self.writer = writer
        self.last_seen = last_seen
        self.version = version
        self.online = online
        if last_seen is None:
            self.last_seen = datetime.datetime.now()

    async def reconnected(self, ip_address, writer):
        self.online = True
        self.last_seen = datetime.datetime.now()
        self.ip_address = ip_address
        self.writer = writer

    def to_dict(self):
        return {'client_id': self.client_id,
                'ip_address': self.ip_address,
                'writer': self.writer,
                'last_seen': self.last_seen,
                'version': self.version}


class ListenerClient(EnodoClient):
    def __init__(self, client_id, ip_address, writer, version="unknown", last_seen=None):
        super().__init__(client_id, ip_address, writer, version, last_seen)


class WorkerClient(EnodoClient):
    def __init__(self, client_id, ip_address, writer, supported_jobs_and_models, version="unknown", last_seen=None, busy=False, worker_config=None):
        super().__init__(client_id, ip_address, writer, version, last_seen)
        self.busy = busy
        self.is_going_busy = False
        self.supported_jobs_and_models = supported_jobs_and_models

        if worker_config is None:
            worker_config = WorkerConfigModel(WORKER_MODE_GLOBAL, dedicated_job_type=None, dedicated_series_name=None)
        self.worker_config = worker_config

    def support_model_for_job(self, job_type, model_name):
        if job_type in self.supported_jobs_and_models.keys():
            for model in self.supported_jobs_and_models[job_type]:
                if model.get('model_name') == model_name:
                    return True
        return False

    def set_config(self, worker_config):
        if isinstance(worker_config, WorkerConfigModel):
            self.worker_config = worker_config

    def get_config(self):
        return self.worker_config

    def to_dict(self):
        base_dict = super().to_dict()
        extra_dict = {
            'busy': self.busy,
            'jobs_and_models': self.supported_jobs_and_models,
            'worker_config': self.worker_config.to_dict()
        }
        return {**base_dict, **extra_dict}


class ClientManager:
    listeners = {}
    workers = {}
    series_manager = None

    _dedicated_for_series = {}
    _dedicated_for_job_type = {}

    @classmethod
    async def setup(cls, series_manager):
        cls.series_manager = series_manager
        await cls._refresh_dedicated_cache()

    @classmethod
    def get_listener_count(cls):
        return len(cls.listeners.keys())

    @classmethod
    def get_worker_count(cls):
        return len(cls.workers.keys())

    @classmethod
    def get_busy_worker_count(cls):
        busy_workers = [cls.workers[worker_id] for worker_id in cls.workers if cls.workers[worker_id].busy is True]
        return len(busy_workers)

    @classmethod
    async def _refresh_dedicated_cache(cls):
        cls._dedicated_for_series = {}
        cls._dedicated_for_job_type = {}
        for worker_id in cls.workers:
            w = cls.workers.get(worker_id)
            if not w.online:
                continue
            if w.worker_config.mode == WORKER_MODE_DEDICATED_SERIES:
                if cls._dedicated_for_series[w.worker_config.series] is None:
                    cls._dedicated_for_series[w.worker_config.series] = [worker_id]
                else:
                    cls._dedicated_for_series[w.worker_config.series].append(worker_id)
            elif w.worker_config.mode == WORKER_MODE_DEDICATED_JOB_TYPE:
                if cls._dedicated_for_job_type[w.worker_config.job_type] is None:
                    cls._dedicated_for_job_type[w.worker_config.job_type] = [worker_id]
                else:
                    cls._dedicated_for_job_type[w.worker_config.job_type].append(worker_id)

    @classmethod
    async def listener_connected(cls, peername, writer, client_data):
        client_id = client_data.get('client_id')
        if client_id not in cls.listeners:
            client = ListenerClient(client_id, peername, writer,
                                    client_data.get('version', None))
            await cls.add_client(client)
        else:
            await cls.listeners.get(client_id).reconnected(peername, writer)

    @classmethod
    async def worker_connected(cls, peername, writer, client_data):
        client_id = client_data.get('client_id')
        if client_id not in cls.workers:
            client = WorkerClient(client_id, peername, writer,
                                    client_data.get('jobs_and_models'),
                                    client_data.get('version', None),
                                    busy=client_data.get('busy', None))
            await cls.add_client(client)
        else:
            await cls.workers.get(client_id).reconnected(peername, writer)

    @classmethod
    async def add_client(cls, client):
        if isinstance(client, ListenerClient):
            cls.listeners[client.client_id] = client
        elif isinstance(client, WorkerClient):
            for job in client.supported_jobs_and_models:
                for model in client.supported_jobs_and_models[job]:
                    await EnodoModelManager.add_model_from_dict(model)
            cls.workers[client.client_id] = client
            await cls._refresh_dedicated_cache()

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
    async def get_dedicated_series_workers(cls):
        return cls._dedicated_for_series

    @classmethod
    async def get_dedicated_job_type_workers(cls):
        return cls._dedicated_for_job_type

    @classmethod
    async def get_free_worker(cls, series_name, job_type, model_name):
        # Check if there is a worker free that's dedicated for the series
        if cls._dedicated_for_series.get(series_name) is not None:
            for worker_id in cls._dedicated_for_series[series_name]:
                worker = cls.workers.get(worker_id)
                if not worker.busy and not worker.is_going_busy:
                    if worker.support_model_for_job(job_type, model_name):
                        return worker

        # Check if there is a worker free that's dedicated for the job_type
        if cls._dedicated_for_job_type.get(job_type) is not None:
            for worker_id in cls._dedicated_for_series[job_type]:
                worker = cls.workers.get(worker_id)
                if not worker.busy and not worker.is_going_busy:
                    if worker.support_model_for_job(job_type, model_name):
                        return worker

        for worker_id in cls.workers: 
            worker = cls.workers.get(worker_id)
            if worker.worker_config.mode == WORKER_MODE_GLOBAL and not worker.busy and not worker.is_going_busy:
                if worker.support_model_for_job(job_type, model_name):
                    return worker

        return None

    @classmethod
    async def check_clients_alive(cls, max_timeout):
        for client in cls.listeners:
            listener = cls.listeners.get(client)
            if listener.online and (datetime.datetime.now() - listener.last_seen) \
                    .total_seconds() > max_timeout:
                logging.info(f'Lost connection to listener: {client}')
                listener.online = False

        for client in cls.workers:
            worker = cls.workers.get(client)
            if worker.online and (datetime.datetime.now() - worker.last_seen) \
                    .total_seconds() > max_timeout:
                logging.info(f'Lost connection to worker: {client}')
                worker.online = False

    @classmethod
    async def set_worker_offline(cls, client_id):
        await cls.check_for_pending_series(cls.workers[client_id])
        cls.workers[client_id].online = False
        await cls._refresh_dedicated_cache()

    @classmethod
    async def set_listener_offline(cls, client_id):
        cls.listeners[client_id].online = False

    @classmethod
    async def assert_if_client_is_offline(cls, client_id):
        client = None
        if client_id in cls.listeners:
            client = cls.listeners.get(client_id)
        elif client_id in cls.workers:
            client = cls.workers.get(client_id)

        if client is None:
            return

        if client.online:
            logging.error(f'Client {client_id} went offline without goodbye')
            client.online = False
            event = EnodoEvent('Lost client', f'Client {client_id} went offline without goodbye',
                               ENODO_EVENT_LOST_CLIENT_WITHOUT_GOODBYE)
            await EnodoEventManager.handle_event(event)

    @classmethod
    async def check_for_pending_series(cls, client):
        from ..enodojobmanager import EnodoJobManager ## To stop circular import
        pending_jobs = EnodoJobManager.get_active_jobs_by_worker(client.client_id)
        if len(pending_jobs) > 0:
            for job in pending_jobs:
                await EnodoJobManager.cancel_job(job)
                series = await cls.series_manager.get_series(job.series_name)
                await series.set_job_status(job.job_type, JOB_STATUS_OPEN)
                logging.info(f'Setting for series job status pending to false...')