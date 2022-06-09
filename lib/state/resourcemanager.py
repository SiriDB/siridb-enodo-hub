from typing import Any
from lib.state.resource import (
    InMemoryResourceCollection, ResourceCollection, StoredResource)

resource_manager_index = {}


class ResourceManager:

    def __new__(cls, resource_type, *args, **kwargs):
        if resource_type in resource_manager_index:
            return resource_manager_index[resource_type]
        return super().__new__(cls)

    def __init__(
            self, resource_type: str, resource_class: Any,
            keep_in_memory=False):
        self._resource_type = resource_type
        self._resource_class = resource_class
        self._keep_in_memory = keep_in_memory
        if self._keep_in_memory:
            self._collection = InMemoryResourceCollection(
                self._resource_type, resource_class)
        else:
            self._collection = ResourceCollection(
                self._resource_type, resource_class)
        resource_manager_index[resource_type] = self

    async def load(self):
        await self._collection.load_resources()

    async def create_resource(self, resource: dict):
        rc = self._resource_class(**resource)
        rc.created()
        if self._keep_in_memory:
            await self._collection.add(rc)
        else:
            await self._collection.add(rc.rid)
        return rc

    async def delete_resource(self, resource: StoredResource):
        resource.delete()
        await self._collection.delete(resource.rid)

    async def get_resource(self, rid: str) -> StoredResource:
        if self._keep_in_memory:
            return self._collection.get(rid)
        resp = await self._collection.get(rid)
        return resp

    async def get_resources(self) -> StoredResource:
        return await self._collection.get_resources()

    def get_resource_rids(self) -> list:
        return self._collection.get_rids()

    def rid_exists(self, rid: str) -> bool:
        return rid in self._collection.resources

    def get_resource_count(self) -> int:
        return self._collection.get_count()

    def itter(self):
        return self._collection.get_itter()
