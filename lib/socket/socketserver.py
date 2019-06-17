import asyncio
import datetime
import json

from lib.socket.package import *


class SocketServer:
    def __init__(self, hostname, port, cbs=None):
        self._hostname = hostname
        self._port = port
        self._cbs = cbs or {}
        self._server = None
        self._connected_listeners = {}
        self._connected_workers = {}
        self._series = ('hub_test1', 'hub_test2')

    async def create(self, loop=None):
        loop = loop or asyncio.get_event_loop()
        coro = asyncio.start_server(self._handle_client_connection, self._hostname, self._port,
                                    loop=loop)
        self._server = loop.run_until_complete(coro)

    async def _handle_client_connection(self, reader, writer):
        connected = True

        while connected:
            packet_type, packet_id, data = await read_packet(reader)

            addr = writer.get_extra_info('peername')
            print("Received %r from %r" % (packet_id, addr))
            if packet_id == 0:
                connected = False

            if packet_type == HANDSHAKE:
                client_data = json.loads(data.decode("utf-8"))
                client_id = client_data.get('client_id')
                if 'client_type' in client_data:
                    if client_data.get('client_type') == 'listener':
                        self._connected_listeners[client_id] = {'last_seen': datetime.datetime.now()}
                    elif client_data.get('client_type') == 'worker':
                        self._connected_workers[client_id] = {'last_seen': datetime.datetime.now()}

                    print(f'New listener with id: {client_id}')
                    response = create_header(0, HANDSHAKE_OK, packet_id)
                    writer.write(response)

                    update = json.dumps(self._series)
                    series_update = create_header(len(update), UPDATE_SERIES, packet_id)
                    writer.write(series_update + update.encode("utf-8"))
                else:
                    response = create_header(0, HANDSHAKE_FAIL, packet_id)
                    writer.write(response)

            elif packet_type == HEARTBEAT:
                client_id = data.decode("utf-8")
                if client_id in self._connected_listeners:
                    print(f'Heartbeat from listener with id: {client_id}')
                    self._connected_listeners[client_id]['last_seen'] = datetime.datetime.now()
                    response = create_header(0, HEARTBEAT, packet_id)
                    writer.write(response)
                elif client_id in self._connected_workers:
                    print(f'Heartbeat from worker with id: {client_id}')
                    self._connected_workers[client_id]['last_seen'] = datetime.datetime.now()
                    response = create_header(0, HEARTBEAT, packet_id)
                    writer.write(response)
                else:
                    response = create_header(0, UNKNOW_CLIENT, packet_id)
                    writer.write(response)

            # elif packet_type == LISTENER_ADD_SERIE_COUNT:
            #     data = json.loads(data.decode("utf-8"))
            #     print(f'Update from listener with id: {client_id}')
            #     print(data)
            #     response = create_header(0, REPONSE_OK, packet_id)
            #     writer.write(response)

            else:
                if packet_type in self._cbs:
                    await self._cbs.get(packet_type)(writer, packet_type, packet_id, data)
                else:
                    print("NOT IMPLEMENTED")

            await writer.drain()

        print("Close the client socket")
        writer.close()

    async def get_connected_clients(self, client_type='listeners'):
        if client_type == 'listeners':
            return self._connected_listeners
        elif client_type == 'workers':
            return self._connected_workers

    async def check_clients_alive(self, max_timeout):
        clients_to_remove = []
        for client in self._connected_listeners:
            if (datetime.datetime.now() - self._connected_listeners.get(client)['last_seen']) \
                    .total_seconds() > max_timeout:
                print(f'Not alive: {client}')
                clients_to_remove.append(client)

        for client in clients_to_remove:
            del self._connected_listeners[client]
        clients_to_remove = []

        for client in self._connected_workers:
            if (datetime.datetime.now() - self._connected_workers.get(client)['last_seen']) \
                    .total_seconds() > max_timeout:
                print(f'Not alive: {client}')
                clients_to_remove.append(client)

        for client in clients_to_remove:
            del self._connected_workers[client]
