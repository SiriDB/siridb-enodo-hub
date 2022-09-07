import asyncio
import datetime
import json
import logging
from asyncio import StreamWriter
import time
from typing import Any, Callable, Optional, Union
from uuid import uuid4

import qpack
from enodo.jobs import *
from enodo.model.config.series import SeriesJobConfigModel
from enodo.protocol.package import WORKER_REQUEST, create_header
from enodo.protocol.packagedata import (EnodoJobDataModel,
                                        EnodoJobRequestDataModel)
from lib.socket.clientmanager import WorkerClient
from lib.state.resource import StoredResource
from lib.util import cls_lock

from .config import Config
from .eventmanager import (ENODO_EVENT_JOB_QUEUE_TOO_LONG,
                           ENODO_EVENT_STATIC_RULE_FAIL, EnodoEvent,
                           EnodoEventManager)
from .series.seriesmanager import SeriesManager
from .serverstate import ServerState
from .socket import ClientManager
from .socketio import (SUBSCRIPTION_CHANGE_TYPE_ADD,
                       SUBSCRIPTION_CHANGE_TYPE_DELETE,
                       SUBSCRIPTION_CHANGE_TYPE_UPDATE)


class EnodoJob(StoredResource):
    __slots__ = ('rid', 'series_name', 'job_config',
                 'job_data', 'send_at', 'error', 'worker_id')

    def __init__(self,
                 rid: Union[int, str],
                 series_name: str,
                 job_config: SeriesJobConfigModel,
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
    _open_jobs = []
    _active_jobs = []
    _active_jobs_index = {}
    _failed_jobs = []
    _max_job_timeout = 60 * 5
    _lock = None
    _next_job_id = 0
    _lock = None

    _max_in_queue_before_warning = None
    _update_queue_cb = None

    @classmethod
    def setup(cls, update_queue_cb: Callable):
        cls._update_queue_cb = update_queue_cb
        cls._lock = asyncio.Lock()

    # @classmethod
    # async def check_for_jobs(cls):
    #     while ServerState.work_queue:
    #         ServerState.tasks_last_runs['check_jobs'] = datetime.datetime.now(
    #         )
    #         if len(cls._open_jobs) != 0:
    #             for next_job in cls._open_jobs:
    #                 await cls._try_activate_job(next_job)

    #         await asyncio.sleep(Config.watcher_interval)

    @classmethod
    @cls_lock()
    async def activate_job(cls, next_job: EnodoJob):
        try:
            series = await SeriesManager.get_config_read_only(
                next_job.series_name)
            if series is None:
                return

            worker = await ClientManager.get_worker(
                next_job.series_name, next_job.job_config.job_type)
            if worker is None:
                return

            logging.info(
                f"Adding series: sending {next_job.series_name} to "
                f"Worker for job type {next_job.job_config.job_type}")
            await cls._send_worker_job_request(worker, next_job)
            await cls._activate_job(next_job, worker.client_id)
        except Exception as e:
            logging.error(
                "Something went wrong when trying to activate job")
            logging.debug(
                f"Corresponding error: {e}, "
                f'exception class: {e.__class__.__name__}')
            import traceback
            print(traceback.format_exc())

    @classmethod
    async def _activate_job(cls, job: EnodoJob, worker_id: str):
        if job is None or worker_id is None:
            return

        job.send_at = time.time()
        job.worker_id = worker_id

    @classmethod
    async def receive_request_result(cls, writer: StreamWriter, packet_type,
                                     packet_id: int, response: Any,
                                     client_id: str):
        request_id = response.get('request_id')
        if response.get('error') is not None:
            logging.error(
                "Error returned by worker for series "
                f"{response.get('name')}")
            return

        job_type = response.get('job_type')
        job = cls.get_activated_job(request_id)
        if job is None:
            logging.error("Received a result for a non-existing job")
            return
        await cls.deactivate_job(request_id)
        config = await SeriesManager.get_config_read_only(
            response.get('name'))
        async with SeriesManager.get_state(response.get('name')) as state:
            await cls.handle_job_result(response, job_type,
                                        job, config, state)

    @classmethod
    async def handle_job_result(cls, job_response, job_type,
                                job, series, state):
        if job_type == JOB_TYPE_FORECAST_SERIES:
            try:
                await SeriesManager.add_forecast_to_series(
                    job_response.get('name'),
                    job.job_config.config_name,
                    job_response.get('data'))
                state.set_job_status(
                    job.job_config.config_name, JOB_STATUS_DONE)
                state.set_job_meta(
                    job.job_config.config_name,
                    {'analyse_region': job_response.get(
                        'analyse_region')})
                await SeriesManager.series_changed(
                    SUBSCRIPTION_CHANGE_TYPE_UPDATE, job_response.get('name'))
            except Exception as e:
                logging.error(
                    f"Something went wrong when receiving forecast job")
                logging.debug(
                    f"Corresponding error: {e}, "
                    f'exception class: {e.__class__.__name__}')
        elif job_type == JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES:
            if isinstance(
                    job_response.get('data'),
                    list):
                try:
                    await SeriesManager.add_anomalies_to_series(
                        job_response.get('name'),
                        job.job_config.config_name,
                        job_response.get('data'))
                    state.set_job_status(
                        job.job_config.config_name, JOB_STATUS_DONE)
                    state.set_job_meta(
                        job.job_config.config_name,
                        {'analyse_region': job_response.get(
                            'analyse_region')})
                    await SeriesManager.series_changed(
                        SUBSCRIPTION_CHANGE_TYPE_UPDATE,
                        job_response.get('name'))
                except Exception as e:
                    logging.error(
                        f"Something went wrong when receiving "
                        f"anomaly detection job")
                    logging.debug(
                        f"Corresponding error: {e}, "
                        f'exception class: {e.__class__.__name__}')
        elif job_type == JOB_TYPE_BASE_SERIES_ANALYSIS:
            try:
                state.characteristics = \
                    json.dumps(job_response.get('characteristics'))
                state.health = job_response.get('health')
                state.interval = job_response.get('interval')
                state.set_job_status(
                    job.job_config.config_name, JOB_STATUS_DONE)
                await SeriesManager.series_changed(
                    SUBSCRIPTION_CHANGE_TYPE_UPDATE, job_response.get('name'))
            except Exception as e:
                logging.error(
                    f"Something went wrong when receiving base analysis job")
                logging.debug(
                    f"Corresponding error: {e}, "
                    f'exception class: {e.__class__.__name__}')
        elif job_type == JOB_TYPE_STATIC_RULES:
            try:
                state.set_job_status(
                    job.job_config.config_name, JOB_STATUS_DONE)
                state.set_job_meta(
                    job.job_config.config_name,
                    {'analyse_region': job_response.get(
                        'analyse_region')})
                await SeriesManager.add_static_rule_hits_to_series(
                    job_response.get('name'),
                    job.job_config.config_name,
                    job_response.get('data'))
                if len(job_response.get('data')):
                    for failed_check in job_response.get('data'):
                        event = EnodoEvent(
                            'Static rule failed!',
                            (f'Series {job_response.get("name")} failed a'
                             f'static rule at ({failed_check[0]}):'
                             f'{failed_check[1]}'),
                            ENODO_EVENT_STATIC_RULE_FAIL,
                            series=series)
                        await EnodoEventManager.handle_event(
                            event, series=series)
                await SeriesManager.series_changed(
                    SUBSCRIPTION_CHANGE_TYPE_UPDATE, job_response.get('name'))
            except Exception as e:
                logging.error(
                    f"Something went wrong when receiving static rules job")
                logging.debug(
                    f"Corresponding error: {e}, "
                    f'exception class: {e.__class__.__name__}')
        else:
            logging.error(f"Received unknown job type: {job_type}")
        series.schedule_job(job.job_config.config_name, state)
        ServerState.index_series_schedules(series, state)

    @classmethod
    async def _send_worker_job_request(cls, worker: WorkerClient,
                                       job: EnodoJob):
        try:
            series = await SeriesManager.get_config_read_only(job.series_name)
            job_data = EnodoJobRequestDataModel(
                request_id=str(uuid4()),
                request_type="run",
                job_id=job.rid, job_config=job.job_config,
                series_name=job.series_name,
                series_config=series.config,
                siridb_ts_units=ServerState.siridb_ts_unit)
            data = qpack.packb(job_data.serialize())
            header = create_header(len(data), WORKER_REQUEST, 0)
            worker.writer.write(header + data)
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

    @classmethod
    async def receive_worker_cancelled_job(cls, writer: StreamWriter,
                                           packet_type: int,
                                           packet_id: int, data: Any,
                                           client_id: str):
        job_id = data.get('job_id')
        worker = await ClientManager.get_worker_by_id(client_id)
        if job_id in cls._active_jobs_index:
            await cls.set_job_failed(
                job_id,
                "Worker cancelled job. Check worker logging for details")
        logging.error(f"Worker {client_id} cancelled job {job_id}")
        if worker is None:
            return
        try:
            await ClientManager.check_for_pending_series(worker)
        except Exception as e:
            logging.error(
                f"Something went wrong when"
                f"receiving from worker to cancel job")
            logging.debug(f"Corresponding error: {e}, "
                          f'exception class: {e.__class__.__name__}')

    @classmethod
    def get_open_queue(cls) -> list:
        return [EnodoJob.to_dict(job) for job in cls._open_jobs]
