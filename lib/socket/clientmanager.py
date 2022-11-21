import asyncio
import logging
import struct
import time
from asyncio import StreamWriter
from typing import Any, Optional

from enodo import WorkerConfigModel
from enodo.net import Package, PROTO_REQ_HANDSHAKE, PROTO_REQ_WORKER_REQUEST
from enodo.protocol.packagedata import EnodoRequest, EnodoQuery
from enodo.model.enodoevent import EnodoEvent

from lib.outputmanager import EnodoOutputManager
from lib.config import Config
from lib.serverstate import ServerState
from lib.socket.queryhandler import QueryHandler
from lib.socket.protocol import EnodoProtocol
from lib.util.util import (
    gen_pool_idx, generate_worker_lookup, get_worker_for_series,
    gen_worker_idx)


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


class WorkerClient:
    MAX_RETRY_STEP = 60

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

        self._connecting = False
        self._auth = False
        self._protocol = None
        self._retry_next = 0
        self._retry_step = 1

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
        return int.from_bytes(self._idx_bytes[8:12], 'big')

    def close(self):
        if self._protocol and self._protocol.transport:
            self._protocol.transport.close()
        self._protocol = None

    def set_protocol(self, protocol: EnodoProtocol):
        self._protocol = protocol
        protocol.set_connection_lost(self.connection_lost)

    def set_status(self, status: int):
        self.status = status

    def is_connected(self) -> bool:
        return \
            self._protocol is not None and self._protocol.is_connected() and \
            self._auth

    def is_connecting(self) -> bool:
        return self._connecting

    def next_retry(self) -> int:
        return self._retry_next

    def set_next_retry(self, counter: int):
        self._retry_step = min(self._retry_step * 2, self.MAX_RETRY_STEP)
        self._retry_next = counter + self._retry_step

    def connection_lost(self):
        self._retry_next = 0
        self._retry_step = 1

    async def _connect(self):
        loop = asyncio.get_event_loop()
        conn = loop.create_connection(
            lambda: EnodoProtocol(self, self.connection_lost),
            host=self.hostname,
            port=int(self.port)
        )
        self._auth = False

        try:
            _, self._protocol = await asyncio.wait_for(conn, timeout=10)
        except Exception as e:
            logging.error(f'connecting to node {self.hostname} failed: {e}')
        else:
            pkg = Package.make(
                PROTO_REQ_HANDSHAKE,
                data={
                    'worker_config': self.worker_config,
                    'hub_id': Config.hub_id
                }
            )
            if self._protocol and self._protocol.transport:
                try:
                    self._protocol.write(pkg)
                except Exception as e:
                    logging.error(
                        f'auth request to node {self.hostname} failed: {e}')
                else:
                    self._auth = True
        finally:
            self._connecting = False

    def send_message(self, data, pt):
        if data is None:
            data = ""
        pkg = Package.make(
            pt,
            data=data
        )
        self._protocol.write(pkg)

    def connect(self) -> asyncio.Future:
        self._connecting = True
        return self._connect()

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

    async def handle_worker_request(self, data, *args):
        data['pool_id'] = self.pool_id
        data['worker_id'] = self.worker_id
        request = EnodoRequest(**data)
        await ClientManager.fetch_request_response_from_worker(
            self.pool_id, request.series_name, request)

    async def handle_event(self, event: EnodoEvent):
        await EnodoOutputManager.handle_event(event)

    async def redirect_response(self, data, worker_id):
        pass

    def handle_query_resp(self, query: EnodoQuery):
        ClientManager._query_handler.set_query_result(
            query.query_id, query.result)

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
    _ws = None
    _query_handler = None
    _sd_lookups = {}
    _lock = None

    @classmethod
    async def setup(cls):
        from lib.state.stores import WorkerStore  # Circular import
        cls._ws = await WorkerStore.setup(ServerState.thingsdb_client,
                                          cls.update_worker_pools)
        cls._query_handler = QueryHandler()
        cls._lock = asyncio.Lock()

    @classmethod
    def get_worker(cls, pool_idx, series_name):
        if pool_idx not in cls._sd_lookups:
            logging.debug(f"Unknown pool_idx: {pool_idx}")
            return None
        wid = get_worker_for_series(cls._sd_lookups[pool_idx], series_name)
        worker_idx = int.from_bytes(
            pool_idx.to_bytes(8, 'big') + wid.to_bytes(4, 'big'), 'big')
        worker = cls._ws.lookup_workers.get(worker_idx)
        if worker is None:
            logging.debug(f"Unknown worker with idx: {worker_idx}")
        return worker

    @classmethod
    def get_pool_idxs(cls):
        return list(cls._sd_lookups.keys())

    @classmethod
    def get_workers_in_pool(cls, pool_id):
        return [w for w in cls._ws.workers.values() if w.pool_id == pool_id]

    @classmethod
    async def query_series_state(cls, pool_id, job_type_id, series_name):
        worker = cls.get_worker((pool_id << 8) | job_type_id, series_name)
        return await cls._query_handler.do_query(worker, series_name)

    @classmethod
    async def query_client_stats(cls, pool_id):
        return await cls._query_handler.do_query_stats(
            cls.get_workers_in_pool(pool_id))

    @classmethod
    async def fetch_request_response_from_worker(
            cls, pool_id, series_name, request: EnodoRequest):
        if request.config is None:
            return
        worker = cls.get_worker(
            (pool_id << 8) | request.config.job_type_id, series_name)
        await worker.send_message(request, PROTO_REQ_WORKER_REQUEST)

    @classmethod
    async def add_worker(cls, pool_id: int, worker: dict):
        async with cls._lock:
            pool_idx = gen_pool_idx(
                pool_id, worker['worker_config']['job_type_id'])
            current_num = len([w.pool_idx == pool_idx
                               for w in cls._ws.lookup_workers.values()])
            bpool_idx = pool_idx.to_bytes(8, 'big')
            worker['worker_idx'] = int.from_bytes(
                bpool_idx + current_num.to_bytes(4, 'big'), 'big')
            cls._sd_lookups[pool_idx] = generate_worker_lookup(current_num + 1)
            await cls._ws.create(worker)

    @classmethod
    async def delete_worker(cls, pool_id: int, job_type_id):
        async with cls._lock:
            pool_idx = gen_pool_idx(pool_id, job_type_id)
            workers_in_pool = len([w.pool_idx == pool_idx
                                   for w in cls._ws.lookup_workers.values()])
            if workers_in_pool == 0:
                logging.error("Cannot delete worker as there "
                              "are no workers in pool")
                return False
            worker_idx = gen_worker_idx(
                pool_id, job_type_id, workers_in_pool - 1)
            worker = await cls.get_worker_by_idx(worker_idx)

            if worker is None:
                return False
            if worker.pool_id != pool_id:
                return False
            try:
                await cls._ws.delete_worker(worker.rid)
            except Exception:
                logging.debug("ThingsDB Error while trying to delete worker")
            return True

    @classmethod
    def update_worker_pools(cls, workers):
        _sd_lookups = {}
        for w in workers:
            if w.pool_idx not in _sd_lookups:
                _sd_lookups[w.pool_idx] = 0
            _sd_lookups[w.pool_idx] += 1
            cls._ws.lookup_workers[w.worker_idx] = w
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
        return cls._ws.lookup_workers.get(idx)

    @classmethod
    def update_listeners(cls, data: Any):
        for client in cls.listeners:
            listener = cls.listeners.get(client)
            if listener.online:
                cls.update_listener(listener, data)

    @classmethod
    def update_listener(cls, listener: ListenerClient, data: Any):
        pass
        # update = qpack.packb(data)
        # series_update = create_header(len(update), UPDATE_SERIES)
        # listener.writer.write(series_update + update)

    @classmethod
    def get_workers(cls):
        return cls._ws.workers.values()

    @classmethod
    async def check_clients_alive(cls, max_timeout: int):
        for client in cls.listeners:
            listener = cls.listeners.get(client)
            if listener.online and \
                    (time.time() - listener.last_seen) > max_timeout:
                logging.info(f'Lost connection to listener: {client}')
                listener.online = False

    @classmethod
    async def connect_loop(cls):
        logging.info(f'Connect loop started')
        counter = 0
        while True:
            counter += 1
            for worker in cls._ws.workers.values():
                if (
                    worker.is_connected() or
                    worker.is_connecting() or
                    worker.next_retry() > counter
                ):
                    continue
                asyncio.ensure_future(worker.connect())
                worker.set_next_retry(counter)

            await asyncio.sleep(1)

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
        workers = cls._ws.workers.values()
        loaded_workers = []
        for w in workers:
            try:
                await w.connect()
                loaded_workers.append(w)
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
