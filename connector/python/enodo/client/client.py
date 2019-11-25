import asyncio
import datetime
import errno
import fcntl
import os
import uuid
import qpack

from enodo.exceptions import EnodoConnectionError
from .package import *
from ..version import __version__ as VERSION


class Client:

    def __init__(self, loop, hostname, port, client_type, token, heartbeat_interval=5):
        self.loop = loop
        self._hostname = hostname
        self._port = port
        self._heartbeat_interval = heartbeat_interval

        self._client_type = client_type
        self._id = uuid.uuid4().hex
        self._token = token
        self._messages = {}
        self._current_message_id = 1
        self._current_message_id_locked = False

        self._last_heartbeat_send = datetime.datetime.now()
        self._updates_on_heartbeat = []
        self._cbs = None
        self._handshake_data_cb = None
        self._sock = None
        self._running = True

    async def setup(self, cbs=None, handshake_cb=None):
        await self._connect()

        self._cbs = cbs
        if cbs is None:
            self._cbs = {}
        if handshake_cb is not None:
            self._handshake_data_cb = handshake_cb

        await self._handshake()

    async def _connect(self):
        connected = False
        while not connected and self._running:
            print("Trying to connect")
            try:
                self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self._sock.connect((self._hostname, self._port))
            except Exception as e:
                print("Cannot connect, ", str(e))
                print("Retrying in 5")
                await asyncio.sleep(5)
            else:
                print("Connected")
                connected = True
                fcntl.fcntl(self._sock, fcntl.F_SETFL, os.O_NONBLOCK)

    async def run(self):
        while self._running:
            if (datetime.datetime.now() - self._last_heartbeat_send).total_seconds() > int(
                    self._heartbeat_interval):
                await self._send_heartbeat()

            await self._read_from_socket()
            await asyncio.sleep(1)

    async def close(self):
        print('Close the socket')
        self._running = False
        self._sock.close()

    async def _read_from_socket(self):
        try:
            header = self._sock.recv(PACKET_HEADER_LEN)
        except socket.error as e:
            err = e.args[0]
            if err == errno.EAGAIN or err == errno.EWOULDBLOCK:
                pass
            else:
                raise EnodoConnectionError
        else:
            await self._read_message(header)

    async def _read_message(self, header):
        packet_type, packet_id, data = await read_packet(self._sock, header)

        if len(data):
            data = qpack.unpackb(data, decode='utf-8')

        if packet_type == 0:
            print("Connection lost, trying to reconnect")
            try:
                await self.setup(self._cbs)
            except Exception as e:
                print(e)
                await asyncio.sleep(5)
        elif packet_type == HANDSHAKE_OK:
            print(f'Hands shaked with hub')
        elif packet_type == HANDSHAKE_FAIL:
            print(f'Hub does not want to shake hands')
        elif packet_type == HEARTBEAT:
            print(f'Heartbeat back from hub')
        elif packet_type == REPONSE_OK:
            print(f'Hub received update correctly')
        elif packet_type == UNKNOWN_CLIENT:
            print(f'Hub does not recognize us')
            await self._handshake()
        else:
            if packet_type in self._cbs.keys():
                await self._cbs.get(packet_type)(data)
            else:
                print(f'Message type not implemented: {packet_type}')

    async def _send_message(self, length, message_type, data):
        if self._current_message_id_locked:
            while self._current_message_id_locked:
                await asyncio.sleep(0.1)

        self._current_message_id_locked = True
        header = create_header(length, message_type, self._current_message_id)
        self._current_message_id += 1
        self._current_message_id_locked = False

        print("SENDING TYPE: ", message_type)
        self._sock.send(header + data)

    async def send_message(self, body, message_type, use_qpack=True):
        if use_qpack:
            body = qpack.packb(body)
        await self._send_message(len(body), message_type, body)

    async def _handshake(self):
        data = {'client_id': str(self._id), 'client_type': self._client_type, 'token': self._token, 'version': VERSION}
        if self._handshake_data_cb is not None:
            handshake_data = await self._handshake_data_cb()
            data = {**data, **handshake_data}
        data = qpack.packb(data)
        await self._send_message(len(data), HANDSHAKE, data)
        self._last_heartbeat_send = datetime.datetime.now()

    async def _send_heartbeat(self):
        print('Sending heartbeat to hub')
        id_encoded = qpack.packb(self._id)
        await self._send_message(len(id_encoded), HEARTBEAT, id_encoded)
        self._last_heartbeat_send = datetime.datetime.now()
