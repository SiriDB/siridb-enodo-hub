import logging
import time
from asyncio import StreamWriter
from typing import Any, Optional, Union

import qpack
from enodo import EnodoModule, WorkerConfigModel
from enodo.model.config.worker import (WORKER_MODE_DEDICATED_JOB_TYPE,
                                       WORKER_MODE_DEDICATED_SERIES,
                                       WORKER_MODE_GLOBAL)
from enodo.protocol.package import UPDATE_SERIES, create_header
from enodo.jobs import JOB_STATUS_OPEN
from lib.serverstate import ServerState
from lib.state.resource import StoredResource
from lib.eventmanager import (ENODO_EVENT_LOST_CLIENT_WITHOUT_GOODBYE,
                              EnodoEvent, EnodoEventManager)


class EnodoClient(StoredResource):

    def __init__(
            self, client_id: str, ip_address: str, writer: StreamWriter,
            version: Optional[str] = "unknown",
            last_seen: Optional[Union[str, Union[float, int]]] = None,
            online=True):
        self.client_id = client_id
        self.ip_address = ip_address
        self.writer = writer
        self.version = version
        self.online = online
        self._last_seen = int(time.time()) if last_seen is None \
            else int(last_seen)

    @property
    def last_seen(self) -> int:
        return self._last_seen

    @last_seen.setter
    def last_seen(self, val: Union[str, Union[float, int]]):
        self._last_seen = int(val)

    async def reconnected(self, ip_address: str, writer: StreamWriter):
        self.online = True
        self.last_seen = time.time()
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
    def __init__(self,
                 client_id: str,
                 ip_address: str,
                 writer: StreamWriter,
                 version: Optional[str] = "unknown",
                 last_seen: Optional[int] = None):
        super().__init__(client_id, ip_address, writer, version, last_seen)

    @property
    def should_be_stored(self):
        return False


class WorkerClient(EnodoClient):
    def __init__(self,
                 client_id: str,
                 ip_address: str,
                 writer: StreamWriter,
                 module: dict,
                 lib_version: Optional[str] = "unknown",
                 last_seen: Optional[int] = None,
                 busy=False,
                 worker_config: Optional[
                     Union[dict, WorkerConfigModel]] = None,
                 online=False):
        super().__init__(client_id, ip_address,
                         writer, lib_version, last_seen, online)
        self.rid = client_id
        self.busy = busy
        self.is_going_busy = False
        self.module = EnodoModule(**module)
        if worker_config is None:
            worker_config = WorkerConfigModel(
                WORKER_MODE_GLOBAL, dedicated_job_type=None,
                dedicated_series_name=None)
        elif isinstance(worker_config, dict):
            worker_config = WorkerConfigModel(**worker_config)
        self.worker_config = worker_config

    def support_module_for_job(
            self, job_type: str, module_name: str) -> bool:
        return self.module.name == module_name and \
            self.module.support_job_type(job_type)

    def conform_params(self, module_name: str, job_type: str,
                       params: dict) -> bool:
        if self.module.name == module_name:
            return self.module.conform_to_params(job_type, params)
        return False

    @StoredResource.changed
    def set_config(self, worker_config: WorkerConfigModel):
        if isinstance(worker_config, WorkerConfigModel):
            self.worker_config = worker_config

    def get_config(self) -> WorkerConfigModel:
        return self.worker_config

    @property
    def resource_type(self):
        return "workers"

    @StoredResource.async_changed
    async def reconnected(self, ip_address: str, writer: StreamWriter,
                          module: dict):
        await super().reconnected(ip_address, writer)
        self.busy = False
        self.is_going_busy = False
        self.module = EnodoModule(**module)

    def to_dict(self) -> dict:
        return {
            'client_id': self.client_id,
            'ip_address': self.ip_address,
            'writer': None,
            'module': self.module,
            'lib_version': self.version,
            'last_seen': self.last_seen,
            'busy': self.busy,
            'worker_config': self.worker_config
        }


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
            d_val = {worker.module.name: worker.module}
            m_index = m_index | d_val
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
            logging.info(f'New worker with id: {client_id} connected')
            client = WorkerClient(client_id, peername, writer,
                                  client_data.get('module'),
                                  client_data.get('lib_version', None),
                                  busy=client_data.get('busy', None),
                                  online=True)
            await cls.add_client(client)
        else:
            logging.info(f'Known worker with id: {client_id} connected')
            await cls.workers.get(client_id).reconnected(
                peername,
                writer,
                client_data.get('module'))

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
            for worker in cls._dedicated_for_series[series_name].values():
                if not worker.online:
                    continue
                if not worker.busy and not worker.is_going_busy:
                    if worker.support_module_for_job(
                            job_type, module_name):
                        return worker

        # Check if there is a worker free that's dedicated for the job_type
        if cls._dedicated_for_job_type.get(job_type) is not None:
            for worker in cls._dedicated_for_series[job_type].values():
                if not worker.online:
                    continue
                if not worker.busy and not worker.is_going_busy:
                    if worker.support_module_for_job(
                            job_type, module_name):
                        return worker

        for worker in cls.workers.values():
            if not worker.online:
                continue
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
                    (time.time() - listener.last_seen) > max_timeout:
                logging.info(f'Lost connection to listener: {client}')
                listener.online = False

        for client in cls.workers:
            worker = cls.workers.get(client)
            if worker.online and \
                    (time.time() - worker.last_seen) > max_timeout:
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
                series.set_job_status(
                    job.job_config.config_name, JOB_STATUS_OPEN)
                await series.set_job_status(
                    job.job_config.config_name, JOB_STATUS_OPEN)
                logging.info(
                    f'Setting for series job status pending to false...')

    @ classmethod
    async def load_from_disk(cls):
        workers = await ServerState.storage.load_by_type("workers")
        for w in workers:
            try:
                worker = WorkerClient(**w)
                worker.online = False
                await cls.add_client(worker)
            except Exception as e:
                logging.warning(
                    "Tried loading invalid data when loading worker")
                logging.debug(
                    f"Corresponding error: {e}, "
                    f'exception class: {e.__class__.__name__}')
