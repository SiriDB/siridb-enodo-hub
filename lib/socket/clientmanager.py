from codecs import StreamWriter
import datetime
import logging
from typing import Any

from enodo import WorkerConfigModel
from enodo.model.config.worker import WORKER_MODE_GLOBAL, \
    WORKER_MODE_DEDICATED_JOB_TYPE, \
    WORKER_MODE_DEDICATED_SERIES
from enodo.jobs import JOB_STATUS_OPEN
from enodo import EnodoModule
from enodo.protocol.package import UPDATE_SERIES, create_header
import qpack

from lib.eventmanager import EnodoEvent, EnodoEventManager, \
    ENODO_EVENT_LOST_CLIENT_WITHOUT_GOODBYE


class EnodoClient:

    def __init__(self, client_id: str, ip_address: str,
                 writer: StreamWriter, version="unknown",
                 last_seen=None, online=True):
        self.client_id = client_id
        self.ip_address = ip_address
        self.writer = writer
        self.last_seen = last_seen
        self.version = version
        self.online = online
        if last_seen is None:
            self.last_seen = datetime.datetime.now()

    async def reconnected(self, ip_address: str, writer: StreamWriter):
        self.online = True
        self.last_seen = datetime.datetime.now()
        self.ip_address = ip_address
        self.writer = writer

    def to_dict(self) -> dict:
        return {'client_id': self.client_id,
                'ip_address': self.ip_address,
                'writer': self.writer,
                'last_seen': self.last_seen,
                'version': self.version,
                'online': self.online}


class ListenerClient(EnodoClient):
    def __init__(self, client_id: str, ip_address: str,
                 writer: StreamWriter, version="unknown",
                 last_seen=None):
        super().__init__(client_id, ip_address, writer, version, last_seen)


class WorkerClient(EnodoClient):
    def __init__(self, client_id: str, ip_address: str,
                 writer: StreamWriter, supported_modules: list,
                 version="unknown", last_seen=None, busy=False,
                 worker_config=None):
        super().__init__(client_id, ip_address, writer, version, last_seen)
        self.busy = busy
        self.is_going_busy = False
        supported_modules = [
            EnodoModule(**module_data)
            for module_data in supported_modules]
        self.supported_modules = {
            module.name: module
            for module in supported_modules}

        if worker_config is None:
            worker_config = WorkerConfigModel(
                WORKER_MODE_GLOBAL, dedicated_job_type=None,
                dedicated_series_name=None)
        self.worker_config = worker_config

    def support_module_for_job(
            self, job_type: str, module_name: str) -> bool:
        module = self.supported_modules.get(module_name)
        if module is not None and module.support_job_type(job_type):
            return True
        return False

    def conform_params(self, module_name: str, job_type: str,
                       params: dict) -> bool:
        module = self.supported_modules.get(module_name)
        if module is not None:
            return module.conform_to_params(job_type, params)
        return False

    def set_config(self, worker_config: WorkerConfigModel):
        if isinstance(worker_config, WorkerConfigModel):
            self.worker_config = worker_config

    def get_config(self) -> WorkerConfigModel:
        return self.worker_config

    async def reconnected(self, ip_address: str, writer: StreamWriter):
        await super().reconnected(ip_address, writer)
        self.busy = False
        self.is_going_busy = False

    def to_dict(self) -> dict:
        base_dict = super().to_dict()
        extra_dict = {
            'busy': self.busy,
            'jobs_and_modules': self.supported_modules,
            'worker_config': self.worker_config
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
    @property
    def modules(cls) -> dict:
        m_index = {}
        for worker in cls.workers.values():
            m_index = m_index | worker.supported_modules
        return m_index

    @classmethod
    def get_module(cls, name: str) -> EnodoModule:
        return cls.modules.get(name)

    @classmethod
    def get_listener_count(cls) -> int:
        return len(cls.listeners.keys())

    @classmethod
    def get_worker_count(cls) -> int:
        return len(cls.workers.keys())

    @classmethod
    def get_busy_worker_count(cls) -> int:
        busy_workers = [cls.workers[worker_id]
                        for worker_id in cls.workers
                        if cls.workers[worker_id].busy is True]
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
                    cls._dedicated_for_series[w.worker_config.series] = [
                        worker_id]
                else:
                    cls._dedicated_for_series[w.worker_config.series].append(
                        worker_id)
            elif w.worker_config.mode == WORKER_MODE_DEDICATED_JOB_TYPE:
                if cls._dedicated_for_job_type[
                        w.worker_config.job_type] is None:
                    cls._dedicated_for_job_type[w.worker_config.job_type] = [
                        worker_id]
                else:
                    cls._dedicated_for_job_type[
                        w.worker_config.job_type].append(worker_id)

    @classmethod
    async def listener_connected(cls, peername: str, writer: StreamWriter,
                                 client_data: Any):
        client_id = client_data.get('client_id')
        if client_id not in cls.listeners:
            client = ListenerClient(client_id, peername, writer,
                                    client_data.get('version', None))
            await cls.add_client(client)
        else:
            await cls.listeners.get(client_id).reconnected(peername, writer)

    @classmethod
    async def worker_connected(cls, peername: str, writer: StreamWriter,
                               client_data: Any):
        client_id = client_data.get('client_id')
        if client_id not in cls.workers:
            client = WorkerClient(client_id, peername, writer,
                                  client_data.get('modules'),
                                  client_data.get('version', None),
                                  busy=client_data.get('busy', None))
            await cls.add_client(client)
        else:
            await cls.workers.get(client_id).reconnected(peername, writer)

    @classmethod
    async def add_client(cls, client: EnodoClient):
        if isinstance(client, ListenerClient):
            cls.listeners[client.client_id] = client
        elif isinstance(client, WorkerClient):
            cls.workers[client.client_id] = client
            await cls._refresh_dedicated_cache()

    @classmethod
    async def get_listener_by_id(cls, client_id) -> ListenerClient:
        if client_id in cls.listeners:
            return cls.listeners.get(client_id)
        return None

    @classmethod
    async def get_worker_by_id(cls, client_id) -> WorkerClient:
        if client_id in cls.workers:
            return cls.workers.get(client_id)
        return None

    @classmethod
    async def get_dedicated_series_workers(cls) -> dict:
        return cls._dedicated_for_series

    @classmethod
    async def get_dedicated_job_type_workers(cls) -> dict:
        return cls._dedicated_for_job_type

    @classmethod
    async def get_free_worker(cls, series_name: str, job_type: str,
                              module_name: str) -> WorkerClient:
        # Check if there is a worker free that's dedicated for the series
        if cls._dedicated_for_series.get(series_name) is not None:
            for worker_id in cls._dedicated_for_series[series_name]:
                worker = cls.workers.get(worker_id)
                if not worker.busy and not worker.is_going_busy:
                    if worker.support_module_for_job(
                            job_type, module_name):
                        return worker

        # Check if there is a worker free that's dedicated for the job_type
        if cls._dedicated_for_job_type.get(job_type) is not None:
            for worker_id in cls._dedicated_for_series[job_type]:
                worker = cls.workers.get(worker_id)
                if not worker.busy and not worker.is_going_busy:
                    if worker.support_module_for_job(
                            job_type, module_name):
                        return worker

        for worker_id in cls.workers:
            worker = cls.workers.get(worker_id)
            if worker.worker_config.mode == WORKER_MODE_GLOBAL and \
                    not worker.busy and not worker.is_going_busy:
                if worker.support_module_for_job(job_type, module_name):
                    return worker

        return None

    @classmethod
    def update_listeners(cls, data: Any):
        for client in cls.listeners:
            listener = cls.listeners.get(client)
            if listener.online:
                cls.update_listener(listener, data)

    @classmethod
    def update_listener(cls, listener: ListenerClient, data: Any):
        update = qpack.packb(data)
        series_update = create_header(len(update), UPDATE_SERIES, 1)
        listener.writer.write(series_update + update)

    @classmethod
    async def check_clients_alive(cls, max_timeout: int):
        for client in cls.listeners:
            listener = cls.listeners.get(client)
            if listener.online and \
                (datetime.datetime.now() - listener.last_seen) \
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
    async def set_worker_offline(cls, client_id: str):
        await cls.check_for_pending_series(cls.workers[client_id])
        cls.workers[client_id].online = False
        await cls._refresh_dedicated_cache()

    @classmethod
    async def set_listener_offline(cls, client_id: str):
        cls.listeners[client_id].online = False

    @classmethod
    async def assert_if_client_is_offline(cls, client_id: str):
        client = None
        if client_id in cls.listeners:
            client = cls.listeners.get(client_id)
        elif client_id in cls.workers:
            client = cls.workers.get(client_id)

        if client is None:
            return

        if client.online:
            logging.error(
                f'Client {client_id} went offline without goodbye')
            client.online = False
            event = EnodoEvent(
                'Lost client',
                f'Client {client_id} went offline without goodbye',
                ENODO_EVENT_LOST_CLIENT_WITHOUT_GOODBYE)
            await EnodoEventManager.handle_event(event)

    @classmethod
    async def check_for_pending_series(cls, client: WorkerClient):
        # To stop circular import
        from ..jobmanager import EnodoJobManager
        pending_jobs = EnodoJobManager.get_active_jobs_by_worker(
            client.client_id)
        if len(pending_jobs) > 0:
            for job in pending_jobs:
                await EnodoJobManager.cancel_job(job)
                series = await cls.series_manager.get_series(job.series_name)
                await series.set_job_status(
                    job.job_config.config_name, JOB_STATUS_OPEN)
                logging.info(
                    f'Setting for series job status pending to false...')
