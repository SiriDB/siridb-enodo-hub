import asyncio
from siridb.connector.lib.connection import SiriDBAsyncConnection

from .protocol import SiriDBServerProtocol


class PipeServer(SiriDBAsyncConnection):
    def __init__(self, pipe_name, on_data):
        self._pipe_name = pipe_name
        self._protocol = None
        self._server = None
        self._on_data_cb = on_data

    async def create(self, loop=None):
        loop = loop or asyncio.get_event_loop()

        self._server = await loop.create_unix_server(
            path=self._pipe_name,
            protocol_factory=lambda: SiriDBServerProtocol(self._on_data))

    def _on_data(self, data):
        """
        series names are returned as c strings (0 terminated)
        """
        data = {k.rstrip('\x00'): v for k, v in data.items()}
        self._on_data_cb(data)
