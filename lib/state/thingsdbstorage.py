import logging
from typing import Any
from lib.config import Config
from lib.state import StoredResource, StorageBase

from thingsdb.client import Client


class ThingsDBStorage(StorageBase):

    def __init__(self):
        self.load_as_needed = False
        self.client = Client()
        self._scope = Config.thingsdb_scope

    async def startup(self):
        await self.client.connect(Config.thingsdb_host,
                                  port=Config.thingsdb_port)
        await self.client.authenticate(Config.thingsdb_auth_token)

    async def delete(self, resource: StoredResource):
        rid = resource.rid
        resource_type = resource.resource_type
        logging.debug(f"Removing data of {rid} of type {resource_type}")
        q = f".{resource_type}.has('{rid}') && .{resource_type}.del('{rid}')"
        await self.client.query(q, self._scope)

    async def store(self, resource: StoredResource):
        data = resource.to_store_data
        rid = resource.rid
        resource_type = resource.resource_type
        logging.debug(
            f"Saving data of {rid} of type {resource_type}")
        resp = await self.client.query(f"""//ti
            .{resource_type}.set('{rid}', object);
        """, object=data, scope=self._scope)
        return resp['#'] if '#' in resp else resp

    async def load_by_type(self, resource_type: str) -> list:
        resp = await self.client.query(f".{resource_type}"
                                       ".map(|_, item| item.copy(10))",
                                       self._scope)
        return resp

    async def load_by_type_and_rid(
            self, resource_type: str, rid: Any) -> dict:
        q = (f".{resource_type}.get('{rid}').copy(10)")
        resp = await self.client.query(q, self._scope)
        return resp

    async def get_all_rids_for_type(self, resource_type: str,
                                    with_storage_id: bool = False) -> list:
        if with_storage_id:
            q = f".{resource_type}.map(|rid, s| [rid, s.id()])"
            resp = await self.client.query(q, self._scope)
            return resp
        q = f".{resource_type}.map(|rid, _| rid)"
        resp = await self.client.query(q, self._scope)
        return resp

    async def close(self):
        self.client.close()
        await self.client.wait_closed()
