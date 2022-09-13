from asyncio import Future
import logging
from uuid import uuid4

from enodo.protocol.package import WORKER_QUERY
from enodo.protocol.packagedata import EnodoJobRequestDataModel


class QueryHandler:

    _futures = {}

    @classmethod
    async def do_query(cls, worker, series_name):
        _id = str(uuid4()).replace("-", "")
        body = EnodoJobRequestDataModel(
            request_id=_id,
            request_type="query",
            series_name=series_name
        ).serialize()
        await worker.client.send_message(body, WORKER_QUERY)
        cls._futures[_id] = Future()
        return cls._futures[_id]

    @classmethod
    def set_query_result(cls, query_id, result):
        logging.debug('Received query result')
        if query_id in cls._futures:
            cls._futures[query_id].set_result(result)
            del cls._futures[query_id]
