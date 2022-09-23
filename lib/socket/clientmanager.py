import asyncio
import logging
import time
from asyncio import Future, StreamWriter
from typing import Any, Optional
from uuid import uuid4

import qpack
from enodo import WorkerConfigModel
from enodo.protocol.package import UPDATE_SERIES, create_header, WORKER_QUERY
from enodo.protocol.packagedata import EnodoJobRequestDataModel
from enodo.jobs import (JOB_TYPE_FORECAST_SERIES,
                        JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES)
from lib.serverstate import ServerState
from lib.socket.queryhandler import QueryHandler
from lib.socket.socketclient import WorkerSocketClient
from lib.state.resource import ResourceManager, StoredResource, from_thing
from lib.util.util import generate_worker_lookup, get_worker_for_series


class ListenerClient:
    def __init__(self,
                 client_id: str,
                 ip_address: str,
                 writer: StreamWriter,
                 version: Optional[str] = "unknown",
                 last_seen: Optional[int] = None):
        super().__init__(client_id, ip_address, writer, version, last_seen)
        self.client_id = client_id
        self.ip_address = ip_address
        self.writer = writer
        self.version = version
        self.last_seen = last_seen

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


class WorkerClient(StoredResource):
    def __init__(self,
                 hostname: str,
                 port: int,
                 worker_config: dict,
                 increment_id: int,
                 rid: Optional[str] = None):
        self.rid = rid
        self.increment_id = increment_id
        self.hostname = hostname
        self.port = port
        self.worker_config = WorkerConfigModel(**worker_config)
        self.client = WorkerSocketClient(hostname, port, worker_config)

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
        if "client" in data:
            del data["client"]
        if "rid" in data:
            del data["rid"]
        return data

    def to_dict(self) -> dict:
        return {
            'rid': self.rid,
            'increment_id': self.increment_id,
            'hostname': self.hostname,
            'port': self.port,
            'worker_config': self.worker_config
        }


class WorkerPool:
    __slots__ = ('pool_id', '_workers', '_pool', '_lookups')

    def __init__(self, pool_id, workers):
        self.pool_id = pool_id
        self._pool = {}
        self._lookups = {}
        self._build(workers)

    def _build(self, workers):
        self._pool = {
            JOB_TYPE_FORECAST_SERIES: {},
            JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES: {}
        }
        for worker in workers:
            self._pool[
                worker.worker_config.job_type][worker.increment_id] = worker
        for job_type in self._pool:
            self._lookups[job_type] = generate_worker_lookup(
                len(self._pool[job_type]))

    def add(self, worker):
        job_type = worker.worker_config.job_type
        increment_id = len(self._pool[job_type])
        worker.increment_id = increment_id
        self._pool[job_type][increment_id] = worker
        self._lookups = generate_worker_lookup(len(self._pool[
            worker.worker_config.job_type]))

    def get_worker(self, job_type, series_name):
        idx = get_worker_for_series(self._lookups[job_type], series_name)
        return self._pool[job_type][idx]


class ClientManager:
    listeners = {}
    _worker_pools = {}
    series_manager = None
    _crm = None
    _query_handler = None

    @classmethod
    async def setup(cls, series_manager):
        cls.series_manager = series_manager
        cls._crm = ResourceManager("workers", WorkerClient)
        await cls._crm.load()
        cls._query_handler = QueryHandler()

    @classmethod
    async def query_series_state(cls, series_name, job_type):
        worker = cls._worker_pools[0].get_worker(job_type, series_name)
        return await cls._query_handler.do_query(worker, series_name)

    @classmethod
    async def query_series_state_from_worker(cls,
                                             series_name, job_type, worker):
        worker = cls._worker_pools[0].get_worker(job_type, series_name)
        await cls._query_handler.do_query(worker, series_name,
                                          origin=worker)

    @classmethod
    async def add_worker(cls, worker: dict):
        worker = await cls._crm.create_and_return(worker)
        cls._worker_pools[0].add(worker)

    @classmethod
    def update_worker_pools(cls, workers):
        cls._worker_pools[0] = WorkerPool(0, workers)

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
        return cls._worker_pools[0].get_worker(job_type, series_name)

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
            # TODO: emit alert

    @classmethod
    async def load_from_disk(cls):
        workers = await ServerState.storage.load_by_type("workers")
        loaded_workers = []
        for w in workers:
            try:
                from_thing(w)
                loaded_workers.append(WorkerClient(**w))
            except Exception as e:
                logging.warning(
                    "Tried loading invalid data when loading worker")
                logging.debug(
                    f"Corresponding error: {e}, "
                    f'exception class: {e.__class__.__name__}')
        cls.update_worker_pools(loaded_workers)

        # await cls.add_worker({
        #     "hostname": "localhost",
        #     "port": 9104,
        #     "worker_config": {
        #         "job_type": JOB_TYPE_FORECAST_SERIES,
        #         "config": {}
        #     },
        #     "increment_id": 0
        # })
        # await cls.add_worker({
        #     "hostname": "localhost",
        #     "port": 9105,
        #     "worker_config": {
        #         "job_type": JOB_TYPE_FORECAST_SERIES,
        #         "config": {}
        #     },
        #     "increment_id": 1
        # })
