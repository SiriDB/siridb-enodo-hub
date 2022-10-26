from abc import abstractmethod
from asyncio import ensure_future
from functools import partial
from typing import Any
from thingsdb.room import Room, event

from lib.outputmanager import EnodoEventOutputWebhook, EnodoResultOutputWebhook
from lib.socket.clientmanager import WorkerClient


class BaseStore(Room):

    @classmethod
    def to_rc(cls, data: dict, rc: object, deep=5) -> object:
        data['rid'] = data['#']

        def clean_id(dict_data, current_deep=0):
            if current_deep > deep:
                return
            if '#' in dict_data:
                del dict_data['#']
            for key in dict_data:
                if isinstance(dict_data[key], dict):
                    clean_id(dict_data[key], current_deep+1)

        clean_id(data)
        return rc(**data)

    @abstractmethod
    async def setup(cls):
        pass


class EventOutputStore(BaseStore):

    def on_init(self):
        self.outputs = {}
        self.add_output = partial(self.client.run, 'add_event_output')
        self.delete_output = partial(self.client.run, 'delete_event_output')

    @classmethod
    async def setup(cls, client):
        room_id = await client.query("""//ti
                    .event_output_store.ev.id();
                """)
        s = cls(room_id)
        await s.join(client)
        return s

    async def create(self, data: dict) -> EnodoEventOutputWebhook:
        data = await self.add_output(data)
        return self.to_rc(data, EnodoEventOutputWebhook)

    async def on_join(self):
        _outputs = await self.client.query("""//ti
            .event_output_store.outputs;
        """)
        self.outputs = {e['#']: self.to_rc(
            e, EnodoEventOutputWebhook) for e in _outputs}

    @event('add-event-output')
    def on_add_output(self, output):
        output = self.to_rc(output, EnodoEventOutputWebhook)
        self.outputs[output.rid] = output

    @event('delete-event-output')
    def on_delete_output(self, output):
        output = self.to_rc(output, EnodoEventOutputWebhook)
        if output.rid in self.outputs:
            del self.outputs[output.rid]


class ResultOutputStore(BaseStore):

    def on_init(self):
        self.outputs = {}
        self.add_output = partial(self.client.run, 'add_result_output')
        self.delete_output = partial(self.client.run, 'delete_result_output')

    @classmethod
    async def setup(cls, client):
        room_id = await client.query("""//ti
                    .result_output_store.ev.id();
                """)
        s = cls(room_id)
        await s.join(client)
        return s

    async def create(self, data: dict) -> EnodoResultOutputWebhook:
        data = await self.add_output(data)
        return self.to_rc(data, EnodoResultOutputWebhook)

    async def on_join(self):
        _outputs = await self.client.query("""//ti
            .result_output_store.outputs;
        """)
        self.outputs = {e['#']: self.to_rc(
            e, EnodoResultOutputWebhook) for e in _outputs}

    @event('add-result-output')
    def on_add_output(self, output):
        output = self.to_rc(output, EnodoResultOutputWebhook)
        self.outputs[output.rid] = output

    @event('delete-result-output')
    def on_delete_output(self, output):
        output = self.to_rc(output, EnodoResultOutputWebhook)
        if output.rid in self.outputs:
            del self.outputs[output.rid]


class WorkerStore(BaseStore):
    worker_update_cb = None

    def on_init(self):
        self.workers = {}
        self.lookup_workers = {}
        self.add_worker = partial(self.client.run, 'add_worker')
        self.delete_worker = partial(self.client.run, 'delete_worker')

    @classmethod
    async def setup(cls, client, update_cb):
        cls.worker_update_cb = update_cb
        room_id = await client.query("""//ti
                    .worker_store.ev.id();
                """)
        s = cls(room_id)
        await s.join(client)
        return s

    async def create(self, data: dict) -> WorkerClient:
        data = await self.add_worker(data)
        return self.to_rc(data, WorkerClient)

    async def on_join(self):
        _workers = await self.client.query("""//ti
            .worker_store.workers;
        """)
        self.workers = {e['#']: self.to_rc(
            e, WorkerClient) for e in _workers}

    @event('add-worker')
    def on_add_worker(self, worker):
        worker = self.to_rc(worker, WorkerClient)
        self.workers[worker.rid] = worker
        self.lookup_workers[worker.worker_idx] = worker
        ensure_future(worker.connect())
        if WorkerStore.worker_update_cb is not None:
            WorkerStore.worker_update_cb(self.workers.values())

    @event('delete-worker')
    def on_delete_worker(self, worker):
        worker = self.to_rc(worker, WorkerClient)
        if worker.rid in self.workers:
            del self.workers[worker.rid]
            del self.lookup_workers[worker.worker_idx]
        if WorkerStore.worker_update_cb is not None:
            WorkerStore.worker_update_cb(self.workers.values())


class SettingStore(BaseStore):

    def on_init(self):
        self.settings = {}
        self.update_setting = partial(self.client.run, 'update_setting')

    @classmethod
    async def setup(cls, client):
        room_id = await client.query("""//ti
                    .setting_store.ev.id();
                """)
        s = cls(room_id)
        await s.join(client)
        return s

    async def update(self, key: str, value: Any):
        await self.update_setting(key, value)

    async def on_join(self):
        _settings = await self.client.query("""//ti
            .setting_store.settings;
        """)
        del _settings['#']
        self.settings = _settings

    @event('update-setting')
    def on_update_setting(self, key, value):
        self.settings[key] = value
