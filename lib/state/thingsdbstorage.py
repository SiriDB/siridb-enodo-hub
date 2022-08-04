import logging
from typing import Any
from lib.config import Config
from lib.state import StoredResource

from thingsdb.client import Client


class ThingsDBStorage:

    def __init__(self):
        self.load_as_needed = False
        self.client = Client()
        self._scope = Config.thingsdb_scope

    async def startup(self):
        await self.client.connect(Config.thingsdb_host,
                                  port=Config.thingsdb_port)
        await self.client.authenticate(Config.thingsdb_auth_token)
        self.client.set_default_scope(self._scope)

    async def get_registered_hub_version(self):
        try:
            return await self.client.query(".hub_version")
        except Exception:
            return None

    async def delete(self, resource: StoredResource):
        rid = resource.rid
        resource_type = resource.resource_type
        logging.debug(f"Removing data of {rid} of type {resource_type}")
        q = ".get(resource_type).remove(|item| item.id()==rid)"
        await self.client.query(q, resource_type=resource_type, rid=int(rid))

    async def create(self, resource: StoredResource):
        data = resource.to_store_data
        resource_type = resource.resource_type

        logging.debug(
            f"Creating resource of type {resource_type}")
        if "rid" in data:
            del data["rid"]
        resp = await self.client.query("""//ti
            .get(resource_type).push(object);
            str(object.id());
        """, object=data, resource_type=resource_type)
        resource.rid = resp
        return resource

    async def store(self, resource: StoredResource):
        data = resource.to_store_data
        rid = resource.rid
        resource_type = resource.resource_type
        if "rid" in data:
            del data["rid"]
        logging.debug(
            f"Saving data of {rid} of type {resource_type}")
        resp = await self.client.query("""//ti
            a = thing(rid);
            a.assign(data);
        """, data=data, rid=int(rid))
        return resource

    async def load_by_type(self, resource_type: str) -> list:
        resp = await self.client.query(
            """.get(resource_type)""",
            resource_type=resource_type)
        return resp

    async def load_by_type_and_key(self, resource_type: str, key: Any,
                                   value: Any) -> dict:
        resp = await self.client.query(
            """.get(resource_type).filter(|item| item.get(key) == value)""",
            resource_type=resource_type, key=key, value=value)
        if len(resp) < 1:
            return None
        return resp[0]

    async def load_by_type_and_rid(
            self, resource_type: str, rid: Any) -> dict:
        resp = await self.client.query(
            """//ti
            t = thing(rid);
            """,
            rid=int(rid))
        return resp

    async def get_all_rids_for_type(self, resource_type: str) -> list:
        q = ".get(resource_type).map(|item| str(item.id()))"
        resp = await self.client.query(q, resource_type=resource_type)
        return resp

    async def run_code(self, code):
        return await self.client.query(code)

    async def close(self):
        self.client.close()
        await self.client.wait_closed()
