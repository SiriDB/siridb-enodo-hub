from asyncio import Future, wait_for
import asyncio
import logging
from uuid import uuid4

from enodo.net import PROTO_REQ_WORKER_QUERY
from enodo.protocol.packagedata import (
    EnodoQuery, QUERY_SUBJECT_STATS, QUERY_SUBJECT_STATE)


class QueryHandler:

    _futures = {}

    @classmethod
    async def do_query(cls, worker, series_name):
        _id = str(uuid4()).replace("-", "")
        body = EnodoQuery(
            _id,
            QUERY_SUBJECT_STATE,
            series_name=series_name
        )
        worker.send_message(body, PROTO_REQ_WORKER_QUERY)
        cls._futures[_id] = Future()

        return cls._futures[_id], _id

    @classmethod
    async def do_query_stats(cls, workers):
        _futs = {}
        for worker in workers:
            _id = str(uuid4()).replace("-", "")
            body = EnodoQuery(_id, QUERY_SUBJECT_STATS)
            worker.send_message(body, PROTO_REQ_WORKER_QUERY)
            _futs[_id] = Future()
            cls._futures[_id] = _futs[_id]

        return cls._wait_for_stats(_futs)

    @classmethod
    async def _wait_for_stats(cls, futs: dict):
        stats = []
        for _id, fut in futs.items():
            try:
                result = await wait_for(fut, timeout=2)
            except asyncio.TimeoutError:
                cls.clear_query(_id)
            else:
                stats.append(result)
        return stats

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
