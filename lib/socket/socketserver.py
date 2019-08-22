import asyncio
import datetime
import json

from lib.serie.seriemanager import SerieManager
from lib.socket.clientmanager import ClientManager, Client
from lib.socket.package import *


class SocketServer:
    def __init__(self, hostname, port, cbs=None):
        self._hostname = hostname
        self._port = port
        self._cbs = cbs or {}
        self._server = None
        self._server_coro = None
        self._server_running = False

    async def create(self, loop=None):
        loop = loop or asyncio.get_event_loop()
        self._server_running = True
        coro = asyncio.start_server(self._handle_client_connection, self._hostname, self._port,
                                    loop=loop)
        self._server = loop.create_task(coro)
        self._server_coro = coro

    async def stop(self):
        self._server_running = False
        await self._server
        await self._server_coro
        self._server.cancel()

    async def _handle_client_connection(self, reader, writer):
        connected = True
        saved_client_id = None

        while connected and self._server_running:
            packet_type, packet_id, data = await read_packet(reader)

            addr = writer.get_extra_info('peername')
            print("Received %r from %r" % (packet_id, addr))
            if packet_id == 0:
                connected = False

            if packet_type == HANDSHAKE:
                client_data = json.loads(data.decode("utf-8"))
                client_id = client_data.get('client_id')

                if 'client_type' in client_data:
                    client = Client(client_id, writer.get_extra_info('peername'), writer,
                                    client_data.get('version', None))
                    if client_data.get('client_type') == 'listener':
                        await ClientManager.add_listener(client)
                        print(f'New listener with id: {client_id}')
                    elif client_data.get('client_type') == 'worker':
                        await ClientManager.add_worker(client)
                        print(f'New worker with id: {client_id}')

                    response = create_header(0, HANDSHAKE_OK, packet_id)
                    writer.write(response)

                    if client_data.get('client_type') == 'listener':
                        update = json.dumps(await SerieManager.get_series())
                        series_update = create_header(len(update), UPDATE_SERIES, packet_id)
                        writer.write(series_update + update.encode("utf-8"))
                else:
                    response = create_header(0, HANDSHAKE_FAIL, packet_id)
                    writer.write(response)
                saved_client_id = client_id

            elif packet_type == HEARTBEAT:
                client_id = data.decode("utf-8")
                l_client = await ClientManager.get_listener_by_id(client_id)
                w_client = await ClientManager.get_worker_by_id(client_id)
                if l_client is not None:
                    print(f'Heartbeat from listener with id: {client_id}')
                    l_client.last_seen = datetime.datetime.now()
                    response = create_header(0, HEARTBEAT, packet_id)
                    writer.write(response)
                elif w_client is not None:
                    print(f'Heartbeat from worker with id: {client_id}')
                    w_client.last_seen = datetime.datetime.now()
                    response = create_header(0, HEARTBEAT, packet_id)
                    writer.write(response)
                else:
                    response = create_header(0, UNKNOW_CLIENT, packet_id)
                    writer.write(response)

            else:
                if packet_type in self._cbs:
                    await self._cbs.get(packet_type)(writer, packet_type, packet_id, data, saved_client_id)
                else:
                    print("NOT IMPLEMENTED")

            await writer.drain()

        print("Close the client socket")
        writer.close()
