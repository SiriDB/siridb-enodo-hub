import asyncio
import logging
import time

import qpack

from enodo.protocol.package import (
    create_header, read_packet, HANDSHAKE, HANDSHAKE_OK, HANDSHAKE_FAIL,
    HEARTBEAT, RESPONSE_OK, UNKNOWN_CLIENT, WORKER_QUERY_RESULT, WORKER_QUERY,
    WORKER_REQUEST)
from enodo.model.config.series import SeriesJobConfigModel

from lib.config import Config
from lib.socket.queryhandler import QueryHandler
from lib.jobmanager import EnodoJob, EnodoJobManager
from lib.socket.clientmanager import ClientManager


class WorkerSocketClient:

    def __init__(self, hostname, port, config, heartbeat_interval=25):
        self._hostname = hostname
        self._port = port
        self._heartbeat_interval = heartbeat_interval
        self._config = config

        self._last_heartbeat_send = time.time()
        self._last_heartbeat_received = time.time()
        self._handshake_data_cb = None

        self._reader = None
        self._writer = None
        self._running = True
        self._connected = False
        self._handsshaked = False
        self._read_task = None
        self._connection_task = asyncio.ensure_future(self._run())

    async def _connect(self):
        while not self._connected:
            logging.info("Trying to connect")
            try:
                self._reader, self._writer = await asyncio.open_connection(
                    self._hostname,
                    self._port)
            except Exception as e:
                logging.warning(f"Cannot connect, {str(e)}")
                logging.info("Retrying in 5")
                await asyncio.sleep(4)
            else:
                logging.info("Connected")
                self._connected = True
                await self._handshake()

    def connection_lost(self):
        logging.warning('Connection lost')
        self._connected = False
        self._writer.close()
        self._writer = None
        self._read_task.cancel()

    async def _run(self):
        while self._running:
            if not self._connected:
                if self._handsshaked:
                    self._handsshaked = False
                if self._writer and not self._writer.is_closing():
                    self._writer.close()
                    await self._writer.wait_closed()
                await self._connect()
            elif self._handsshaked:
                diff = time.time() - self._last_heartbeat_send
                if diff > int(
                        self._heartbeat_interval):
                    await self._send_heartbeat()
                diff = time.time() - self._last_heartbeat_received
                if diff > int(
                        2*self._heartbeat_interval):
                    logging.error(
                        "Haven't received heartbeat response from worker")
                    self._connected = False
            await asyncio.sleep(1)

    async def close(self):
        logging.info('Closing the socket')
        self._running = False
        if self._read_task:
            self._read_task.cancel()
        if self._writer:
            self._writer.close()

    async def _read_from_socket(self):
        while self._running:
            if not self._connected:
                await asyncio.sleep(1)
                continue

            packet_type, packet_id, data = await read_packet(self._reader)
            if data is False:
                self._connected = False
                continue

            if len(data):
                data = qpack.unpackb(data, decode='utf-8')

            if packet_type == 0:
                logging.warning(
                    "Connection lost, trying to reconnect to worker")
                self._connected = False
                await asyncio.sleep(5)
            elif packet_type == HANDSHAKE_OK:
                logging.debug(f'Hands shaked with worker')
                self._last_heartbeat_received = time.time()
                self._handsshaked = True
            elif packet_type == HANDSHAKE_FAIL:
                logging.error(f'Worker does not want to shake hands')
            elif packet_type == HEARTBEAT:
                logging.debug(f'Heartbeat back from Worker')
                self._last_heartbeat_received = time.time()
            elif packet_type == RESPONSE_OK:
                logging.debug(f'Worker received update correctly')
            elif packet_type == UNKNOWN_CLIENT:
                logging.error(f'Worker does not recognize us')
                await self._handshake()
            elif packet_type == WORKER_QUERY_RESULT:
                QueryHandler.set_query_result(
                    data.get('request_id'),
                    data.get('data'))
            elif packet_type == WORKER_REQUEST:
                try:
                    series_name = data.get('series_name')
                    config = data.get('job_config')
                    job = EnodoJob(None, series_name,
                                   SeriesJobConfigModel(**config))
                    await EnodoJobManager.activate_job(job)
                except Exception as e:
                    logging.error("Cannot activate job")
                    logging.debug(f"Corresponding error: {str(e)}")
            elif packet_type == WORKER_QUERY:
                series_name = data.get('series_name')
                job_type = data.get('job_type')
                config_name = data.get('job_config_name')
                await ClientManager.query_series_state_from_worker(
                    series_name, job_type, self)
            else:
                logging.error(
                    f'Message type not implemented: {packet_type}')

    async def _send_message(self, length, message_type, data, id=1):
        header = create_header(length, message_type, id)

        logging.debug(f"Sending type: {message_type}")
        self._writer.write(header + data)
        try:
            await self._writer.drain()
        except Exception as e:
            self._connected = False

    async def send_message(self, body, message_type, id=1, use_qpack=True):
        if not self._connected:
            return False
        if use_qpack:
            body = qpack.packb(body)
        await self._send_message(len(body), message_type, body, id)

    async def _handshake(self):
        if self._read_task is not None:
            self._read_task.cancel()
        self._read_task = asyncio.Task(self._read_from_socket())
        data = {
            'worker_config': self._config,
            'hub_id': Config.hub_id
        }
        data = qpack.packb(data)
        await self._send_message(len(data), HANDSHAKE, data)
        self._last_heartbeat_send = time.time()

    async def _send_heartbeat(self):
        logging.debug('Sending heartbeat to worker')
        data = qpack.packb(1)
        await self._send_message(len(data), HEARTBEAT, data)
        self._last_heartbeat_send = time.time()
