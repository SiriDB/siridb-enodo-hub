import asyncio
import datetime
import logging
import time
from packaging import version

import qpack
from enodo.protocol.package import create_header, read_packet, HEARTBEAT, \
    HANDSHAKE, HANDSHAKE_FAIL, UNKNOWN_CLIENT, CLIENT_SHUTDOWN, \
    HANDSHAKE_OK, UPDATE_SERIES

from lib.series.seriesmanager import SeriesManager
from lib.serverstate import ServerState
from . import ClientManager

ENODO_HUB_WORKER_MIN_VERSION = "0.1.0-beta3.0"


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
        self._server = await ServerState.scheduler.spawn(asyncio.start_server(
            self._handle_client_connection, self._hostname, self._port))

    async def stop(self):
        self._server_running = False
        await self._server.close()

    async def _handle_client_connection(self, reader, writer):
        connected = True
        saved_client_id = None

        while connected and self._server_running:
            packet_type, packet_id, data = await read_packet(reader)
            if data is False:
                connected = False
                continue
            if len(data):
                data = qpack.unpackb(data, decode='utf-8')

            addr = writer.get_extra_info('peername')
            logging.debug("Received %r from %r" % (packet_id, addr))
            if packet_id == 0:
                connected = False

            if packet_type == HANDSHAKE:
                saved_client_id, connected = await self._handle_handshake(
                    writer, packet_id, data)
            elif packet_type == HEARTBEAT:
                await self._handle_heartbeat(writer, packet_id, data)
            elif packet_type == CLIENT_SHUTDOWN:
                await self._handle_client_shutdown(saved_client_id)
                connected = False
            else:
                if packet_type in self._cbs:
                    await self._cbs.get(packet_type)(writer, packet_type,
                                                     packet_id, data,
                                                     saved_client_id)
                else:
                    logging.debug(
                        f'Package type {packet_type} not implemented')

            await writer.drain()

        logging.info(f'Closing socket with client {saved_client_id}')
        await ClientManager.assert_if_client_is_offline(saved_client_id)
        writer.close()

    async def _handle_client_shutdown(self, saved_client_id):
        client_id = saved_client_id
        l_client = await ClientManager.get_listener_by_id(client_id)
        w_client = await ClientManager.get_worker_by_id(client_id)
        if l_client is not None:
            logging.info(
                f'Listener {client_id} is going down'
                ' removing from client list...')
            await ClientManager.set_listener_offline(client_id)
        elif w_client is not None:
            logging.info(
                f'Worker {client_id} is going down'
                ' removing from client list...')
            await ClientManager.set_worker_offline(client_id)

    async def _handle_heartbeat(self, writer, packet_id, data):
        client_id = data
        l_client = await ClientManager.get_listener_by_id(client_id)
        w_client = await ClientManager.get_worker_by_id(client_id)
        if l_client is not None:
            logging.debug(
                f'Heartbeat from listener with id: {client_id}')
            l_client.last_seen = int(time.time())
            response = create_header(0, HEARTBEAT, packet_id)
            writer.write(response)
        elif w_client is not None:
            logging.debug(
                f'Heartbeat from worker with id: {client_id}')
            w_client.last_seen = int(time.time())
            response = create_header(0, HEARTBEAT, packet_id)
            writer.write(response)
        else:
            response = create_header(
                0, UNKNOWN_CLIENT, packet_id)
            writer.write(response)

    async def _handle_handshake(self, writer, packet_id, data):
        client_data = data
        client_id = client_data.get('client_id')
        client_token = client_data.get('token')

        if self._token is not None and (
                client_token
                is None or client_token
                != self._token):
            response = create_header(0, HANDSHAKE_FAIL, packet_id)
            writer.write(response)
            return client_id, False

        if 'client_type' not in client_data:
            response = create_header(0, HANDSHAKE_FAIL, packet_id)
            writer.write(response)
            return client_id, False

        if client_data.get('client_type') == 'listener':
            await ClientManager.listener_connected(
                writer.get_extra_info('peername'),
                writer,
                client_data)
            logging.info(
                f'New listener with id: {client_id}')
        elif client_data.get('client_type') == 'worker':
            supported_modules = client_data.get('module')
            if supported_modules is None:
                logging.warning(
                    f"New worker connected with id : {client_id}"
                    ", but has no installed modules")

            if version.parse(
                    client_data.get('lib_version')) < version.parse(
                    ENODO_HUB_WORKER_MIN_VERSION):
                logging.warning(
                    f"Worker with id : {client_id} tried to connect,"
                    "but has incompatible version")
                response = create_header(0, HANDSHAKE_FAIL, packet_id)
                writer.write(response)
                return client_id, False

            await ClientManager.worker_connected(
                writer.get_extra_info('peername'), writer, client_data)
            response = create_header(0, HANDSHAKE_OK, packet_id)
            writer.write(response)
            return client_id, True

        if client_data.get('client_type') == 'listener':
            update = qpack.packb(
                SeriesManager.get_listener_series_info())
            header = create_header(
                len(update),
                UPDATE_SERIES, packet_id)
            writer.write(header + update)
            return client_id, True
