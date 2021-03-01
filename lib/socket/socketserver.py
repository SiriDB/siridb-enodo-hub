import asyncio
import datetime
import json
import logging

import qpack

from lib.series.seriesmanager import SeriesManager
from . import ClientManager, ListenerClient, WorkerClient
from .package import *


class SocketServer:
    def __init__(self, hostname, port, token, cbs=None):
        self._hostname = hostname
        self._port = port
        self._token = token
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
        # await self._server_coro
        self._server.cancel()

    async def _handle_client_connection(self, reader, writer):
        connected = True
        saved_client_id = None

        while connected and self._server_running:
            packet_type, packet_id, data = await read_packet(reader)
            if len(data):
                data = qpack.unpackb(data, decode='utf-8')

            addr = writer.get_extra_info('peername')
            logging.debug("Received %r from %r" % (packet_id, addr))
            if packet_id == 0:
                connected = False

            if packet_type == HANDSHAKE:
                client_data = data
                client_id = client_data.get('client_id')
                client_token = client_data.get('token')

                if self._token is not None and (client_token is None or client_token != self._token):
                    response = create_header(0, HANDSHAKE_FAIL, packet_id)
                    writer.write(response)
                    connected = False
                else:
                    if 'client_type' in client_data:
                        if client_data.get('client_type') == 'listener':
                            await ClientManager.listener_connected(writer.get_extra_info('peername'), writer,
                                                    client_data.get('version', None))
                            logging.info(f'New listener with id: {client_id}')
                        elif client_data.get('client_type') == 'worker':
                            supported_jobs_and_models = client_data.get('jobs_and_models')
                            if supported_jobs_and_models is None or len(supported_jobs_and_models) < 1:
                                response = create_header(0, HANDSHAKE_FAIL, packet_id)
                                writer.write(response)
                                connected = False
                            else:
                                await ClientManager.worker_connected(writer.get_extra_info('peername'), writer, client_data)
                                logging.info(f'New worker with id: {client_id}')

                        response = create_header(0, HANDSHAKE_OK, packet_id)
                        writer.write(response)

                        if client_data.get('client_type') == 'listener':
                            update = qpack.packb(SeriesManager.get_all_series())
                            series_update = create_header(len(update), UPDATE_SERIES, packet_id)
                            writer.write(series_update + update)
                    else:
                        response = create_header(0, HANDSHAKE_FAIL, packet_id)
                        writer.write(response)
                saved_client_id = client_id
            elif packet_type == HEARTBEAT:
                client_id = data
                l_client = await ClientManager.get_listener_by_id(client_id)
                w_client = await ClientManager.get_worker_by_id(client_id)
                if l_client is not None:
                    logging.debug(f'Heartbeat from listener with id: {client_id}')
                    l_client.last_seen = datetime.datetime.now()
                    response = create_header(0, HEARTBEAT, packet_id)
                    writer.write(response)
                elif w_client is not None:
                    logging.debug(f'Heartbeat from worker with id: {client_id}')
                    w_client.last_seen = datetime.datetime.now()
                    response = create_header(0, HEARTBEAT, packet_id)
                    writer.write(response)
                else:
                    response = create_header(0, UNKNOWN_CLIENT, packet_id)
                    writer.write(response)
            elif packet_type == CLIENT_SHUTDOWN:
                client_id = saved_client_id
                l_client = await ClientManager.get_listener_by_id(client_id)
                w_client = await ClientManager.get_worker_by_id(client_id)
                if l_client is not None:
                    logging.info(f'Listener {client_id} is going down, removing from client list...')
                    await ClientManager.set_listener_offline(client_id)
                elif w_client is not None:
                    logging.info(f'Worker {client_id} is going down, removing from client list...')
                    await ClientManager.set_worker_offline(client_id)

                connected = False
            else:
                if packet_type in self._cbs:
                    await self._cbs.get(packet_type)(writer, packet_type, packet_id, data, saved_client_id)
                else:
                    logging.debug(f'Package type {packet_type} not implemented')

            await writer.drain()

        logging.info(f'Closing socket with client {saved_client_id}')
        await ClientManager.assert_if_client_is_offline(saved_client_id)
        writer.close()
