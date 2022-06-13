import logging
from typing import Any
from lib.config import Config
from lib.state import StoredResource, StorageBase

from thingsdb.client import Client


class ThingsDBStorage(StorageBase):

    def __init__(self):
        self.load_as_needed = False
        self.client = Client()

    async def startup(self):
        await self.client.connect(Config.thingsdb_host,
                                  port=Config.thingsdb_port)
        await self.client.authenticate(Config.thingsdb_user,
                                       Config.thingsdb_password)

    async def delete(self, resource: StoredResource):
        rid = resource.rid
        resource_type = resource.resource_type
        logging.debug(f"Removing data of {rid} of type {resource_type}")
        q = f".{resource_type}.remove(|item| item.rid == '{rid}')"
        await self.client.query(q, "//enodo")

    async def store(self, resource: StoredResource):
        data = resource.to_store_data
        rid = resource.rid
        resource_type = resource.resource_type
        logging.debug(
            f"Saving data of {rid} of type {resource_type}")
        await self.client.query(f"""//ti
            .{resource_type}.push(object);
        """, object=data, scope="//enodo")

    async def load_by_type(self, resource_type: str) -> list:
        resp = await self.client.query(f".{resource_type}"
                                       ".map(|item| item.copy(10))", "//enodo")
        return resp

    async def load_by_type_and_rid(
            self, resource_type: str, rid: Any) -> dict:
        q = (f".{resource_type}.filter(|item| item.rid == "
             f"'{rid}').map(|item| item.copy(10))")
        resp = await self.client.query(q, "//enodo")
        return resp[0] if len(resp) else None

    async def get_all_rids_for_type(self, resource_type: str) -> list:
        q = f".{resource_type}.map(|item| item.rid)"
        resp = await self.client.query(q, "//enodo")
        return resp

    async def close(self):
        self.client.close()
        await self.client.wait_closed()
