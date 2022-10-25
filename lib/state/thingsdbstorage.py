from typing import Any
from lib.config import Config

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

    async def run_code(self, code):
        return await self.client.query(code)

    async def close(self):
        self.client.close()
        await self.client.wait_closed()
