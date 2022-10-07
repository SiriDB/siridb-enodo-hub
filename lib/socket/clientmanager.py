import logging
import struct
import time
from asyncio import StreamWriter
from typing import Any, Optional

import qpack
from enodo import WorkerConfigModel
from enodo.protocol.package import (
    UPDATE_SERIES, create_header, WORKER_REQUEST, WORKER_REQUEST_RESULT)
from enodo.protocol.packagedata import EnodoRequest
from lib.serverstate import ServerState
from lib.socket.queryhandler import QueryHandler
from lib.socket.socketclient import WorkerSocketClient
from lib.state.resource import ResourceManager, StoredResource, from_thing
from lib.util.util import (
    gen_pool_idx, generate_worker_lookup, get_worker_for_series)


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
                 worker_idx: int,
                 rid: Optional[str] = None):
        self.rid = rid
        self.worker_idx = worker_idx
        self._idx_bytes = worker_idx.to_bytes(12, byteorder="big")
        self.hostname = hostname
        self.port = port
        self.worker_config = WorkerConfigModel(**worker_config)
        from lib.jobmanager import EnodoJobManager
        self.client = WorkerSocketClient(
            hostname, port, worker_config,
            cbs={WORKER_REQUEST: self._handle_worker_request,
                 WORKER_REQUEST_RESULT: EnodoJobManager.handle_job_result})

    @property
    def pool_idx(self):
        return int.from_bytes(self._idx_bytes[0:8], 'big')

    @property
    def pool_id(self):
        return int.from_bytes(self._idx_bytes[:4], 'big')

    @property
    def job_type_id(self):
        return int.from_bytes(self._idx_bytes[4:8], 'big')

    @property
    def worker_id(self):
        return struct.unpack('>I', self._idx_bytes[8:12])

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

    async def _handle_worker_request(self, data, *args):
        data = qpack.unpackb(data)
        data['pool_id'] = self.pool_id
        data['worker_id'] = self.increment_id
        request = EnodoRequest(**data)
        await ClientManager.fetch_request_response_from_worker(
            self.pool_id, request.series_name, request)

    async def _handle_worker_response(self, data, pool_id, worker_id):
        header = create_header(len(data), WORKER_REQUEST_RESULT) + \
            pool_id.to_bytes(4, byteorder='big') + \
            worker_id.to_bytes(1, byteorder='big')
        await self.client.send_data(header + data)

    def to_dict(self) -> dict:
        return {
            'rid': self.rid,
            'worker_idx': self.worker_idx,
            'hostname': self.hostname,
            'port': self.port,
            'worker_config': self.worker_config
        }


class ClientManager:
    listeners = {}
    _workers = {}
    series_manager = None
    _crm = None
    _query_handler = None
    _sd_lookups = {}

    @classmethod
    async def setup(cls, series_manager):
        cls.series_manager = series_manager
        cls._crm = ResourceManager("workers", WorkerClient)
        await cls._crm.load()
        cls._query_handler = QueryHandler()

    @classmethod
    def get_worker(cls, pool_idx, series_name):
        if pool_idx not in cls._sd_lookups:
            return None
        wid = get_worker_for_series(cls._sd_lookups[pool_idx], series_name)
        worker_idx = int.from_bytes(
            pool_idx.to_bytes(8, 'big') + wid.to_bytes(4, 'big'), 'big')
        return cls._workers.get(worker_idx)

    @classmethod
    async def query_series_state(cls, pool_id, job_type_id, series_name):
        worker = cls.get_worker((pool_id << 8) | job_type_id, series_name)
        return await cls._query_handler.do_query(worker, series_name)

    @classmethod
    async def fetch_request_response_from_worker(
            cls, pool_id, series_name, request: EnodoRequest):
        if request.config is None:
            return
        worker = cls.get_worker(
            (pool_id << 8) | request.config.job_type_id, series_name)
        await worker.client.send_message(request, WORKER_REQUEST)

    @classmethod
    async def add_worker(cls, pool_id: int, worker: dict):
        pool_idx = gen_pool_idx(
            pool_id, worker['worker_config']['job_type_id'])
        current_num = len(
            [w.pool_idx == pool_idx for w in cls._workers.values()])
        bpool_idx = pool_idx.to_bytes(8, 'big')
        worker['worker_idx'] = int.from_bytes(
            bpool_idx + current_num.to_bytes(4, 'big'), 'big')
        cls._sd_lookups[pool_idx] = generate_worker_lookup(current_num + 1)
        cls._workers[worker['worker_idx']] = await cls._crm.create_and_return(
            worker)

    @classmethod
    def update_worker_pools(cls, workers):
        _sd_lookups = {}
        for w in workers:
            if w.pool_idx not in _sd_lookups:
                _sd_lookups[w.pool_idx] = 0
            _sd_lookups[w.pool_idx] += 1
            cls._workers[w.worker_idx] = w
        for pool_idx, num_workers in _sd_lookups.items():
            cls._sd_lookups[pool_idx] = generate_worker_lookup(num_workers)

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
    async def get_worker_by_idx(cls, idx: int) -> WorkerClient:
        return cls.workers.get(idx)

    @classmethod
    def update_listeners(cls, data: Any):
        for client in cls.listeners:
            listener = cls.listeners.get(client)
            if listener.online:
                cls.update_listener(listener, data)

    @classmethod
    def update_listener(cls, listener: ListenerClient, data: Any):
        update = qpack.packb(data)
        series_update = create_header(len(update), UPDATE_SERIES)
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

        # from enodo.jobs import JOB_TYPE_FORECAST_SERIES, JOB_TYPE_IDS
        # forecast_id = JOB_TYPE_IDS[JOB_TYPE_FORECAST_SERIES]
        # await cls.add_worker(0, {
        #     "hostname": "localhost",
        #     "port": 9104,
        #     "worker_config": {
        #         "job_type_id": forecast_id,
        #         "config": {}
        #     }
        # })
        # await cls.add_worker(0, {
        #     "hostname": "localhost",
        #     "port": 9105,
        #     "worker_config": {
        #         "job_type_id": forecast_id,
        #         "config": {}
        #     }
        # })
