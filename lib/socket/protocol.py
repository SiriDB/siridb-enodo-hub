from __future__ import annotations
import asyncio
import logging
from typing import Callable, Optional
from enodo.net import *
from enodo.protocol.packagedata import EnodoQuery
from enodo.model.enodoevent import EnodoEvent


class EnodoProtocol(BaseProtocol):

    def __init__(self, worker,
                 connection_lost: Optional[Callable] = None):
        super().__init__()
        self._worker = worker
        self.set_connection_lost(connection_lost)  # may be None at this time

    def connection_lost(self, exc: Optional[Exception]):
        super().connection_lost(exc)
        if self._connection_lost:
            self._connection_lost()

    def set_connection_lost(self, connection_lost: Callable):
        self._connection_lost = connection_lost

    async def _on_handshake_ok(self, pkg: Package):
        logging.debug("Hands shaked with worker")

    async def _on_handshake_fail(self, pkg: Package):
        logging.error("Worker rejected handshake")

    async def _on_heartbeat(self, pkg: Package):
        logging.debug(f'Heartbeat back from Worker')

    async def _on_worker_request(self, pkg: Package):
        logging.debug("Worker requested job")
        await self._worker.handle_worker_request(pkg.data)

    async def _on_worker_request_response(self,
                                          pkg: Package):
        logging.debug("Response for requested job")
        from lib.jobmanager import EnodoJobManager  # NOPEP circular import
        await EnodoJobManager.handle_job_result(pkg.data)

    async def _on_worker_request_response_redirect(self,
                                                   pkg: Package):
        logging.debug("Response for redirect requested job")
        await self._worker.redirect_response(pkg.data)

    async def _on_worker_query_response(self, pkg: Package):
        logging.debug("Received query response")
        try:
            query = EnodoQuery(**pkg.data)
        except Exception:
            logging.error("Invalid query data")
        else:
            self._worker.handle_query_resp(query)

    async def _on_event(self, pkg: Package):
        logging.debug(f'Received an event')
        try:
            event = EnodoEvent(**pkg.data)
        except Exception:
            logging.error("Invalid event data")
        else:
            await self._worker.handle_event(event)

    async def _on_worker_shutdown(self, pkg: Package):
        logging.warning("Worker is going down")

    def _get_future(self, pkg: Package) -> asyncio.Future:
        future, task = self._requests.pop(pkg.pid, (None, None))
        if future is None:
            logging.error(
                f'got a response on pkg id {pkg.pid} but the original '
                'request has probably timed-out'
            )
            return
        task.cancel()
        return future

    def on_package_received(self, pkg: Package, _map={
        PROTO_RES_HANDSHAKE_OK: _on_handshake_ok,
        PROTO_RES_HANDSHAKE_FAIL: _on_handshake_fail,
        PROTO_RES_HEARTBEAT: _on_heartbeat,
        PROTO_REQ_WORKER_REQUEST: _on_worker_request,
        PROTO_RES_WORKER_REQUEST: _on_worker_request_response,
        PROTO_RES_WORKER_REQUEST_REDIRECT:
            _on_worker_request_response_redirect,
        PROTO_RES_WORKER_QUERY: _on_worker_query_response,
        PROTO_REQ_EVENT: _on_event,
        PROTO_REQ_SHUTDOWN: _on_worker_shutdown
    }):
        handle = _map.get(pkg.tp)

        # populate pkg.data. this raises an error when unpack fails
        pkg.read_data()

        if handle is None:
            logging.error(f'unhandled package type: {pkg.tp}')
        else:
            asyncio.ensure_future(handle(self, pkg))
