from asyncio import Future
import functools
import logging
from uuid import uuid4

from enodo.protocol.package import WORKER_QUERY, WORKER_QUERY_RESULT
from enodo.protocol.packagedata import EnodoJobRequestDataModel


class QueryHandler:

    _futures = {}

    @classmethod
    async def _worker_callback(cls, future, query_id, worker):
        await worker.send_message(future.result(), WORKER_QUERY_RESULT)
        cls.clear_query(query_id)

    @classmethod
    async def do_query(cls, worker, series_name, origin=None):
        _id = str(uuid4()).replace("-", "")
        body = EnodoJobRequestDataModel(
            request_id=_id,
            request_type="query",
            series_name=series_name
        ).serialize()
        await worker.client.send_message(body, WORKER_QUERY)
        cls._futures[_id] = Future()

        if origin is None:
            return cls._futures[_id], _id

        cls._futures[_id].add_done_callback(functools.partial(
            cls._worker_callback, query_id=_id, worker=origin))

    @classmethod
    def set_query_result(cls, query_id, result):
        logging.debug('Received query result')
        if query_id in cls._futures:
            cls._futures[query_id].set_result(result)
            del cls._futures[query_id]

    @classmethod
    def clear_query(cls, query_id):
        try:
            if query_id in cls._futures:
                cls._futures[query_id].cancel()
                del cls._futures[query_id]
        except Exception:
            pass
