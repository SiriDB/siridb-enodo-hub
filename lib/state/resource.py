from abc import abstractproperty
from collections import namedtuple
import functools
from typing import Any, Coroutine, Iterator
from lib.serverstate import ServerState


class StoredResource:

    _is_deleted = None

    @staticmethod
    def async_changed(func):
        @functools.wraps(func)
        async def wrapped(self, *args, **kwargs):
            resp = await func(self, *args, **kwargs)
            if self.should_be_stored and not self._is_deleted:
                ServerState.storage.store(self)
            return resp
        return wrapped

    @staticmethod
    def changed(func):
        @functools.wraps(func)
        def wrapped(self, *args, **kwargs):
            resp = func(self, *args, **kwargs)
            if self.should_be_stored and not self._is_deleted:
                ServerState.storage.store(self)
            return resp
        return wrapped

    def created(self):
        if self.should_be_stored and not self._is_deleted:
            ServerState.storage.store(self)

    def delete(self):
        if self.should_be_stored:
            self._is_deleted = True
            ServerState.storage.delete(self)

    @property
    def should_be_stored(self):
        return True

    @abstractproperty
    def resource_type(self):
        pass

    def keep_in_memory(self):
        return False

    @property
    def to_store_data(self):
        return self.to_dict()


class InMemoryResource(StoredResource):

    def keep_in_memory(self):
        return True


ProxyResource = namedtuple('ProxyResource', ['rid'])


class ResourceCollectionIterator:
    def __init__(self, collection: 'ResourceCollection'):
        self._resources = dict(collection.resources)
        self._collection = collection

    def __iter__(self):
        self.i = 0
        return self

    def __next__(self) -> Coroutine:
        if self.i >= len(self._resources.values()) - 1:
            raise StopIteration
        resp = self._resources.values()[self.i]
        self.i += 1
        return self._collection.get_resource(resp)


class InMemoryResourceCollectionIterator(ResourceCollectionIterator):
    def __next__(self):
        if self.i >= len(self._resources.values()) - 1:
            raise StopIteration
        resp = self._resources.values()[self.i]
        self.i += 1
        return resp


class ResourceCollection:

    def __init__(self, resource_type: str, resource_class: Any):
        self.resources = {}
        self._resource_type = resource_type
        self._resource_class = resource_class

    async def load_resources(self):
        rids = await ServerState.storage.get_all_rids_for_type(
            self._resource_type)
        self.resources = {rid: ProxyResource(rid)
                          for rid in rids}

    async def add(self, rid):
        if rid not in self.resources:
            self.resources[rid] = ProxyResource(rid)

    async def delete(self, rid):
        if rid in self.resources:
            del self.resources[rid]

    def get_rids(self):
        return list(self.resources.keys())

    def get_count(self) -> int:
        return len(self.resources.keys())

    def get_itter(self) -> Iterator:
        return ResourceCollectionIterator(self)

    async def get_resource(self, rid: str):
        data = await ServerState.storage.load_by_type_and_rid(
            self._resource_type, rid)

        return self._resource_class(
            **data) if data is not False else None

    async def get(self, rid: str) -> Coroutine:
        proxy = self.resources.get(rid)
        if proxy is None:
            return proxy
        return await self.get_resource(proxy.rid)

    async def get_resources(self):
        return [(await self.get_resource(rid))
                for rid in self.resources.keys()]


class InMemoryResourceCollection(ResourceCollection):

    async def load_resources(self):
        resources = await ServerState.storage.load_by_type(self._resource_type)
        self.resources = {
            resource.get('rid'): self._resource_class(**resource)
            for resource in resources}

    async def add(self, resource):
        if resource not in self.resources:
            self.resources[resource.rid] = resource

    async def get(self, rid: str) -> StoredResource:
        return self.resources.get(rid)

    async def get_resources(self):
        return list(self.resources.values())

    def get_itter(self) -> Iterator:
        return InMemoryResourceCollectionIterator(self)
