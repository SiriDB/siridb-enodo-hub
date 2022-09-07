import logging
import time
from asyncio import StreamWriter
from typing import Any, Optional, Union

import qpack
from enodo import WorkerConfigModel
from enodo.protocol.package import UPDATE_SERIES, create_header
from enodo.jobs import JOB_TYPES, JOB_TYPE_FORECAST_SERIES
from lib.serverstate import ServerState
from lib.socket.socketclient import WorkerSocketClient
from lib.state.resource import ResourceManager, StoredResource, from_thing
from lib.eventmanager import (ENODO_EVENT_LOST_CLIENT_WITHOUT_GOODBYE,
                              EnodoEvent, EnodoEventManager)
from lib.util.util import generate_worker_lookup, get_worker_for_series


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


class WorkerClient(StoredResource):
    def __init__(self,
                 hostname: str,
                 port: int,
                 worker_config: dict,
                 rid: Optional[str] = None):
        self.rid = rid
        self.hostname = hostname
        self.port = port
        self.worker_config = WorkerConfigModel(**worker_config)
        self._client = WorkerSocketClient(hostname, port, worker_config)

    @StoredResource.changed
    def set_config(self, worker_config: WorkerConfigModel):
        if isinstance(worker_config, WorkerConfigModel):
            self.worker_config = worker_config

    def get_config(self) -> WorkerConfigModel:
        return self.worker_config

    @classmethod
    @property
    def resource_type(self):
        return "workers"

    @property
    def to_store_data(self):
        data = self.to_dict()
        if "_client" in data:
            del data["_client"]
        if "rid" in data:
            del data["rid"]
        return data

    def to_dict(self) -> dict:
        return {
            'rid': self.rid,
            'hostname': self.hostname,
            'port': self.port,
            'worker_config': self.worker_config
        }


class ClientManager:
    listeners = {}
    workers = {}
    workers_indexed = {}
    series_manager = None

    _crm = None
    _worker_lookup = {}

    @classmethod
    async def setup(cls, series_manager):
        cls.series_manager = series_manager
        cls._crm = ResourceManager("workers", WorkerClient)
        await cls._crm.load()
        cls.update_worker_lookup()

        # await cls.add_worker({
        #     "hostname": "localhost",
        #     "port": 9105,
        #     "worker_config": {
        #         "supported_job_types": [JOB_TYPE_FORECAST_SERIES],
        #         "config": {}
        #     }
        # })

    @classmethod
    def update_worker_lookup(cls):
        cls.workers_indexed = {}
        for worker in cls.workers.values():
            pass
        cls._worker_lookup = {}
        for job_type in JOB_TYPES:
            count = 0
            for worker in cls.workers.values():
                if job_type in worker.worker_config.supported_job_types:
                    count += 1
            if count > 1:
                cls._worker_lookup[job_type] = generate_worker_lookup(count)

    @classmethod
    def load_worker(cls, worker):
        cls.workers[worker.rid] = worker

    @classmethod
    async def add_worker(cls, worker: dict):
        worker = await cls._crm.create_and_return(worker)
        cls.load_worker(worker)
        cls.update_worker_lookup()

    @classmethod
    @property
    def modules(cls) -> dict:
        m_index = {}
        for worker in cls.workers.values():
            d_val = {worker.module.name: worker.module}
            m_index = m_index | d_val
        return m_index

    @classmethod
    def get_listener_count(cls) -> int:
        return len(cls.listeners.keys())

    @classmethod
    def get_worker_count(cls) -> int:
        return len(cls.workers.keys())

    @classmethod
    async def listener_connected(cls, peername: str, writer: StreamWriter,
                                 client_data: Any):
        client_id = client_data.get('client_id')
        if client_id not in cls.listeners:
            client = ListenerClient(client_id, peername, writer,
                                    client_data.get('version', None))
            await cls.add_listener(client)
        else:
            await cls.listeners.get(client_id).reconnected(peername, writer)

    @classmethod
    async def add_listener(cls, client: ListenerClient):
        cls.listeners[client.client_id] = client

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
    async def get_worker(cls,
                         series_name: str,
                         job_type: str) -> WorkerClient:
        if job_type in cls._worker_lookup:
            return get_worker_for_series(
                cls._worker_lookup[job_type], series_name)
        return 0

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

    @classmethod
    async def set_listener_offline(cls, client_id: str):
        cls.listeners[client_id].online = False

    @classmethod
    async def assert_if_client_is_offline(cls, client_id: str):
        if client_id not in cls.listeners:
            return
        client = cls.listeners.get(client_id)

        if client.online:
            logging.error(
                f'Client {client_id} went offline without goodbye')
            client.online = False
            event = EnodoEvent(
                'Lost client',
                f'Client {client_id} went offline without goodbye',
                ENODO_EVENT_LOST_CLIENT_WITHOUT_GOODBYE)
            await EnodoEventManager.handle_event(event)

    @ classmethod
    async def load_from_disk(cls):
        workers = await ServerState.storage.load_by_type("workers")
        for w in workers:
            try:
                from_thing(w)
                cls.load_worker(WorkerClient(**w))
            except Exception as e:
                logging.warning(
                    "Tried loading invalid data when loading worker")
                logging.debug(
                    f"Corresponding error: {e}, "
                    f'exception class: {e.__class__.__name__}')
        cls.update_worker_lookup()
