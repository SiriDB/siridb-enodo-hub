import asyncio
import logging
from asyncio import StreamWriter
import time
from typing import Any, Optional, Union
from uuid import uuid4

import qpack
from enodo.jobs import *
from enodo.model.config.series import SeriesJobConfigModel
from enodo.protocol.package import WORKER_REQUEST
from enodo.protocol.packagedata import (
    EnodoJobDataModel, EnodoRequest, EnodoRequestResponse,
    REQUEST_TYPE_EXTERNAL)
from lib.outputmanager import EnodoOutputManager
from lib.socket.clientmanager import WorkerClient
from lib.state.resource import StoredResource
from lib.util import cls_lock

from .series.seriesmanager import SeriesManager
from .socket import ClientManager


class EnodoJob(StoredResource):
    __slots__ = ('rid', 'series_name', 'job_config', 'pool_idx',
                 'job_data', 'send_at', 'error', 'worker_id')

    def __init__(self,
                 rid: Union[int, str],
                 series_name: str,
                 job_config: SeriesJobConfigModel,
                 pool_idx: int,
                 job_data: Optional[dict] = None,
                 send_at: Optional[int] = None,
                 error: Optional[str] = None,
                 worker_id: Optional[str] = None):
        if not isinstance(
                job_data, EnodoJobDataModel) and job_data is not None:
            raise Exception('Unknown job data value')
        if rid is None:
            rid = str(uuid4()).replace("-", "")
        self.rid = rid
        self.series_name = series_name
        self.job_config = job_config
        self.pool_idx = pool_idx
        self.job_data = job_data
        self.send_at = send_at
        self.error = error
        self.worker_id = worker_id

    @property
    def should_be_stored(self):
        return self.error is None

    @property
    def to_store_data(self):
        return EnodoJob.to_dict(self)

    @classmethod
    @property
    def resource_type(self):
        return "failed_jobs"

    @classmethod
    def to_dict(cls, job: 'EnodoJob') -> dict:
        resp = {}
        for slot in cls.__slots__:
            resp[slot] = getattr(job, slot)
        return resp

    @classmethod
    def from_dict(cls, data: dict) -> 'EnodoJob':
        return EnodoJob(**data)


class EnodoJobManager:
    _lock = None
    _next_job_id = 0

    @classmethod
    async def send_job(cls, job: EnodoJob, request: EnodoRequest):
        worker = ClientManager.get_worker(job.pool_idx, job.series_name)
        if worker is None:
            logging.error(
                "Could not send job_type "
                f"{job.job_config.job_type_id} for series "
                f"{job.series_name}")
            return

        logging.info(
            f"Sending {job.series_name} to "
            f"Worker for job type {job.job_config.job_type_id}")
        await cls._send_worker_job_request(worker, request)

    @ classmethod
    async def handle_job_result(cls, data, pool_id, worker_id):
        data = qpack.unpackb(data)
        print(data)
        if not isinstance(data, dict):
            logging.error("Invalid job result, cannot handle")
            return
        response = EnodoRequestResponse(**data)

        if response.request.request_type == REQUEST_TYPE_EXTERNAL:
            await EnodoOutputManager.handle_result(response)

    @classmethod
    async def _send_worker_job_request(cls, worker: WorkerClient,
                                       request: EnodoRequest):
        try:
            await worker.client.send_message(request, WORKER_REQUEST)
        except Exception as e:
            logging.error(
                f"Something went wrong when sending job request to worker")
            import traceback
            traceback.print_exc()
            logging.debug(f"Corresponding error: {e}, "
                          f'exception class: {e.__class__.__name__}')

    @classmethod
    async def _send_worker_cancel_job(cls, worker_id: str, job_id: int):
        worker = await ClientManager.get_worker_by_id(worker_id)
        if worker is None:
            return
        try:
            logging.error(
                f"Asking worker {worker_id} to cancel job {job_id}")
            data = qpack.packb(
                {'job_id': job_id})
            # header = create_header(len(data), WORKER_JOB_CANCEL, 0)
            # worker.writer.write(header + data)
        except Exception as e:
            logging.error(
                f"Something went wrong when sending worker to cancel job")
            logging.debug(f"Corresponding error: {e}, "
                          f'exception class: {e.__class__.__name__}')
