from asyncio import ensure_future
import contextlib
from typing import Any
from abc import abstractproperty
import functools
from lib.serverstate import ServerState
from lib.state.lruqueue import LRUQueue


resource_manager_index = {}


class StoredResource:

    _is_deleted = None
    _last_accessed = None

    @staticmethod
    def async_changed(func):
        @functools.wraps(func)
        async def wrapped(self, *args, **kwargs):
            resp = await func(self, *args, **kwargs)
            if self.should_be_stored and not self._is_deleted:
                await ServerState.storage.store(self)
            return resp
        return wrapped

    @staticmethod
    def changed(func):
        @functools.wraps(func)
        def wrapped(self, *args, **kwargs):
            resp = func(self, *args, **kwargs)
            ensure_future(self.store())
            return resp
        return wrapped

    async def store(self):
        if self.should_be_stored and not self._is_deleted:
            resource_manager_index[self.resource_type].set_cache(self)
            await ServerState.storage.store(self)

    @classmethod
    async def create(cls, data):
        rm = resource_manager_index[cls.resource_type]
        async with rm.create_resource(data) as rc:
            return rc

    async def delete(self):
        if self.should_be_stored:
            self._is_deleted = True
            await ServerState.storage.delete(self)

    @property
    def should_be_stored(self):
        return True

    @classmethod
    @abstractproperty
    def resource_type(cls):
        pass

    def keep_in_memory(self):
        return False

    @property
    def to_store_data(self):
        return self.to_dict()


class ResourceIterator:
    def __init__(self, resource_manager):
        self._resource_manager = resource_manager
        self._rids = list(resource_manager.get_resource_rids())

    def __iter__(self):
        self.i = 0
        return self

    def __next__(self):
        if self.i >= len(self._rids):
            raise StopIteration
        resp = self._resource_manager.get_resource(self.rids[self.i])
        self.i += 1
        return resp


class ResourceManager:

    def __new__(cls, resource_type, *args, **kwargs):
        if resource_type in resource_manager_index:
            return resource_manager_index[resource_type]
        return super().__new__(cls)

    def __init__(
            self, resource_type: str, resource_class: Any,
            keep_in_memory=120, cache_only=False):
        self._resource_type = resource_type
        self._resource_class = resource_class
        self._resources = {}
        self._lruqueue = LRUQueue(keep_in_memory)
        resource_manager_index[resource_type] = self
        self._loaded = False
        self._cache_only = cache_only

    def cleanup(self):
        pass

    async def load(self):
        if not self._loaded:
            rids = await ServerState.storage.get_all_rids_for_type(
                self._resource_type)
            self._resources = {rid: None for rid in rids}

            if self._cache_only:
                for rid in rids:
                    resp = await self.get_resource(rid)
                    self._lruqueue.put(rid, resp)
                    
            self._loaded = True

    @contextlib.asynccontextmanager
    async def create_resource(self, resource: dict):
        rc = self._resource_class(**resource)
        yield rc
        await rc.store()
        self._resources[rc.rid] = None
        if self._resource_type == "series":
            ServerState.index_series_schedules(rc)

    async def delete_resource(self, resource: StoredResource):
        del self._resources[resource.rid]
        await resource.delete()

    def set_cache(self, resource):
        self._lruqueue.put(resource.rid, resource)

    def get_cached_resource(self, rid: str) -> StoredResource:
        return self._lruqueue.get(rid)

    def get_cached_resources(self) -> list:
        return list(self._lruqueue.all())

    async def get_resource(self, rid: str) -> StoredResource:
        if rid not in self._resources:
            return None
        resp = self._lruqueue.get(rid)
        if resp is None:
            resp = \
                self._resource_class(**(
                    await ServerState.storage.load_by_type_and_rid(
                        self._resource_type, rid)))
            self._lruqueue.put(rid, resp)
        return resp

    def get_resource_rids(self) -> list:
        return list(self._resources.keys())

    def rid_exists(self, rid: str) -> bool:
        return rid in self._resources

    def get_resource_count(self) -> int:
        return len(self._resources)

    async def itter(self):
        rids = self.get_resource_rids()
        for rid in rids:
            resp = await self.get_resource(rid)
            yield resp
