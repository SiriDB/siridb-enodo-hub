import asyncio
import datetime
import logging
import time
from asyncio import StreamWriter
from typing import Any, Callable, Optional, Union
import uuid

import qpack
from enodo.jobs import *
from enodo.model.config.series import SeriesJobConfigModel
from enodo.protocol.package import WORKER_JOB, WORKER_JOB_CANCEL, create_header
from enodo.protocol.packagedata import (EnodoJobDataModel,
                                        EnodoJobRequestDataModel)
from lib.series.series import Series
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
    _lock = asyncio.Lock()

    _max_in_queue_before_warning = None
    _update_queue_cb = None

    @classmethod
    def setup(cls, update_queue_cb: Callable):
        cls._update_queue_cb = update_queue_cb
        cls._max_in_queue_before_warning = Config.max_in_queue_before_warning

    @classmethod
    def _build_index(cls):
        cls._active_jobs_index = {}
        for job in cls._active_jobs:
            cls._active_jobs_index[job.rid] = job

    @classmethod
    def _get_next_job_id(cls) -> int:
        return str(uuid.uuid4()).replace("-", "")

    @classmethod
    def get_active_jobs(cls) -> list:
        return cls._active_jobs

    @classmethod
    def get_failed_jobs(cls) -> list:
        return cls._failed_jobs

    @classmethod
    def get_open_jobs_count(cls) -> list:
        return len(cls._open_jobs)

    @classmethod
    def get_active_jobs_count(cls) -> int:
        return len(cls._active_jobs)

    @classmethod
    def get_failed_jobs_count(cls) -> int:
        return len(cls._failed_jobs)

    @classmethod
    def get_active_jobs_by_worker(cls, worker_id: str) -> list:
        return [job for job in cls._active_jobs if job.worker_id == worker_id]

    @classmethod
    async def clear_jobs(cls):
        jobs = []
        for job in cls._active_jobs:
            jobs.append(job)
        for job in jobs:
            async with cls._lock:
                await cls._deactivate_job(job)
            await cls._send_worker_cancel_job(job.worker_id, job.rid)
            async with SeriesManager.get_series(job.series_name) as series:
                series.set_job_status(job.job_config.config_name,
                                      JOB_STATUS_NONE)
            jobs = []
        for job in cls._open_jobs:
            jobs.append(job)
        for job in jobs:
            cls._open_jobs.remove(job)
            async with SeriesManager.get_series(job.series_name) as series:
                series.set_job_status(job.job_config.config_name,
                                      JOB_STATUS_NONE)

    @classmethod
    async def create_job(cls, job_config_name: str, series_name: str):
        async with SeriesManager.get_series(series_name) as series:
            series.set_job_status(job_config_name, JOB_STATUS_OPEN)
            series.state.set_job_check_status(
                job_config_name,
                "Job created")
            job_config = series.get_job(job_config_name)
            job_id = cls._get_next_job_id()
            job = EnodoJob(job_id, series.name, job_config,
                           job_data=None)  # TODO: Catch exception
            await cls._add_job(job)

    @classmethod
    async def _add_job(cls, job: EnodoJob):
        if not isinstance(job, EnodoJob):
            raise Exception('Incorrect job instance')

        cls._open_jobs.append(job)
        if cls._update_queue_cb is not None:
            await cls._update_queue_cb(
                SUBSCRIPTION_CHANGE_TYPE_ADD, EnodoJob.to_dict(job))

    @classmethod
    def has_series_failed_jobs(cls, series_name: str) -> bool:
        for job in cls._failed_jobs:
            if job.series_name == series_name:
                return True
        return False

    @classmethod
    def get_failed_jobs_for_series(cls, series_name: str) -> list:
        jobs = []
        for job in cls._failed_jobs:
            if job.series_name == series_name:
                jobs.append(job)
        return jobs

    @classmethod
    async def remove_failed_jobs_for_series(cls, series_name: str):
        for job in cls.get_failed_jobs_for_series(series_name):
            cls._failed_jobs.remove(job)
            await job.delete()

    @classmethod
    @cls_lock()
    async def activate_job(cls, job_id: int, worker_id: str):
        j = None
        for job in cls._open_jobs:
            if job.rid == job_id:
                j = job
                break
        if j is not None:
            await cls._activate_job(j, worker_id)

    @classmethod
    async def _activate_job(cls, job: EnodoJob, worker_id: str):
        if job is None or worker_id is None:
            return

        if job in cls._open_jobs:
            cls._open_jobs.remove(job)
            if cls._update_queue_cb is not None:
                await cls._update_queue_cb(
                    SUBSCRIPTION_CHANGE_TYPE_DELETE, job.rid)
        job.send_at = time.time()
        job.worker_id = worker_id
        cls._active_jobs.append(job)
        cls._active_jobs_index[job.rid] = job

    @classmethod
    def get_activated_job(cls, job_id: int) -> EnodoJob:
        for job in cls._active_jobs:
            if job.rid == job_id:
                return job

        return None

    @classmethod
    @cls_lock()
    async def deactivate_job(cls, job_id: int):
        j = None
        for job in cls._active_jobs:
            if job.rid == job_id:
                j = job
                break

        await cls._deactivate_job(j)

    @classmethod
    async def _deactivate_job(cls, job: EnodoJob):
        if job in cls._active_jobs:
            cls._active_jobs.remove(job)
            del cls._active_jobs_index[job.rid]

    @classmethod
    @cls_lock()
    async def cancel_job(cls, job: EnodoJob):
        if job in cls._active_jobs:
            cls._active_jobs.remove(job)
            del cls._active_jobs_index[job.rid]
            cls._open_jobs.append(job)

    @classmethod
    @cls_lock()
    async def cancel_jobs_for_series(cls, series_name: str):
        await cls._cancel_jobs_for_series(series_name)

    @classmethod
    async def _cancel_jobs_for_series(cls, series_name: str):
        jobs = []
        for job in cls._open_jobs:
            if job.series_name == series_name:
                jobs.append(job)

        for job in jobs:
            cls._open_jobs.remove(job)
            if cls._update_queue_cb is not None:
                await cls._update_queue_cb(
                    SUBSCRIPTION_CHANGE_TYPE_DELETE, job.rid)

        jobs = []

        for job in cls._active_jobs:
            if job.series_name == series_name:
                jobs.append(job)

        for job in jobs:
            cls._active_jobs.remove(job)
            if cls._update_queue_cb is not None:
                await cls._update_queue_cb(
                    SUBSCRIPTION_CHANGE_TYPE_DELETE, job.rid)

    @classmethod
    @cls_lock()
    async def cancel_jobs_by_config_name(cls, series_name: str,
                                         job_config_name: str):
        jobs = []
        for job in cls._open_jobs:
            if job.series_name == series_name and \
                    job.job_config.config_name == job_config_name:
                jobs.append(job)
        for job in jobs:
            cls._open_jobs.remove(job)
        jobs = []

        for job in cls._active_jobs:
            if job.series_name == series_name and \
                    job.job_config.config_name == job_config_name:
                jobs.append(job)
        for job in jobs:
            cls._active_jobs.remove(job)

        for job in cls._failed_jobs:
            if job.series_name == series_name and \
                    job.job_config.config_name == job_config_name:
                jobs.append(job)
        for job in jobs:
            cls._active_jobs.remove(job)

    @classmethod
    @cls_lock()
    async def set_job_failed(cls, job_id: int, error: str):
        j = None
        for job in cls._active_jobs:
            if job.rid == job_id:
                j = job
                break
        await cls._set_job_failed(j, error)

    @classmethod
    async def _set_job_failed(cls, job: EnodoJob, error: str):
        if job is not None:
            job.error = error
            async with SeriesManager.get_series(job.series_name) as series:
                if series is not None:
                    series.set_job_status(
                        job.job_config.config_name, JOB_STATUS_FAILED)
            await cls._cancel_jobs_for_series(job.series_name)
            if job in cls._active_jobs:
                cls._active_jobs.remove(job)
            del cls._active_jobs_index[job.rid]
            cls._failed_jobs.append(job)

    @classmethod
    @cls_lock()
    async def clean_jobs(cls):
        for job in cls._active_jobs:
            now = int(time.time())
            if (now - job.send_at) > cls._max_job_timeout:
                await cls._set_job_failed(job, "Job timed-out")
                await cls._send_worker_cancel_job(job.worker_id, job.rid)

        if len(cls._open_jobs) > cls._max_in_queue_before_warning:
            event = EnodoEvent(
                'Job queue too long',
                f'{len(cls._open_jobs)} jobs waiting \
                in queue exceeds threshold of \
                    {cls._max_in_queue_before_warning}',
                ENODO_EVENT_JOB_QUEUE_TOO_LONG)
            await EnodoEventManager.handle_event(event)

    @classmethod
    @cls_lock()
    async def _try_activate_job(cls, next_job: EnodoJob):
        try:
            series = await SeriesManager.get_series_read_only(
                next_job.series_name)
            if series is None:
                return

            worker = await ClientManager.get_free_worker(
                next_job.series_name, next_job.job_config.job_type,
                series.get_module(
                    next_job.job_config.config_name))
            if worker is None:
                return

            if not worker.conform_params(
                    next_job.job_config.module, next_job.job_config.
                    job_type, next_job.job_config.module_params):
                async with SeriesManager.get_series(
                        next_job.series_name) as series:
                    series.state.set_job_check_status(
                        next_job.job_config.config_name,
                        "Module params not conform")
                return

            logging.info(
                f"Adding series: sending {next_job.series_name} to "
                f"Worker for job type {next_job.job_config.job_type}")
            await cls._send_worker_job_request(worker, next_job)
            worker.is_going_busy = True
            await cls._activate_job(next_job, worker.client_id)
        except Exception as e:
            logging.error(
                "Something went wrong when trying to activate job")
            logging.debug(
                f"Corresponding error: {e}, "
                f'exception class: {e.__class__.__name__}')

    @classmethod
    async def check_for_jobs(cls):
        while ServerState.work_queue:
            ServerState.tasks_last_runs['check_jobs'] = datetime.datetime.now(
            )
            if len(cls._open_jobs) == 0:
                await cls.clean_jobs()
                await asyncio.sleep(Config.watcher_interval)
                continue

            for next_job in cls._open_jobs:
                await cls._try_activate_job(next_job)

            await cls.clean_jobs()
            await asyncio.sleep(Config.watcher_interval)

    @classmethod
    async def receive_job_result(cls, writer: StreamWriter, packet_type,
                                 packet_id: int, job_response: Any,
                                 client_id: str):
        job_id = job_response.get('job_id')
        if job_response.get('error') is not None:
            logging.error(
                "Error returned by worker for series "
                f"{job_response.get('name')}")
            await cls.set_job_failed(job_id, job_response.get('error'))
            return

        job_type = job_response.get('job_type')
        job = cls.get_activated_job(job_id)
        if job is None:
            logging.error("Received a result for a non-existing job")
            return
        await cls.deactivate_job(job_id)
        async with SeriesManager.get_series(
                job_response.get('name')) as series:
            await cls.handle_job_result(job_response, job_type, job, series)

    @classmethod
    async def handle_job_result(cls, job_response, job_type, job, series):
        if job_type == JOB_TYPE_FORECAST_SERIES:
            try:
                await SeriesManager.add_forecast_to_series(
                    job_response.get('name'),
                    job.job_config.config_name,
                    job_response.get('data'))
                series.schedule_job(job.job_config.config_name)
                series.set_job_status(
                    job.job_config.config_name, JOB_STATUS_DONE)
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
                    series.schedule_job(job.job_config.config_name)
                    series.set_job_status(
                        job.job_config.config_name, JOB_STATUS_DONE)
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
                series.series_characteristics = \
                    job_response.get('characteristics')
                series.state.health = job_response.get('health')
                series.state.interval = job_response.get('interval')
                series.schedule_job(job.job_config.config_name)
                series.set_job_status(
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
                series.schedule_job(job.job_config.config_name)
                series.set_job_status(
                    job.job_config.config_name, JOB_STATUS_DONE)
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

    @classmethod
    async def _send_worker_job_request(cls, worker: WorkerClient,
                                       job: EnodoJob):
        try:
            series = await SeriesManager.get_series_read_only(job.series_name)
            job_data = EnodoJobRequestDataModel(
                job_id=job.rid, job_config=job.job_config,
                series_name=job.series_name,
                series_config=series.config,
                series_state=series.state,
                siridb_ts_units=ServerState.siridb_ts_unit)
            data = qpack.packb(job_data.serialize())
            header = create_header(len(data), WORKER_JOB, 0)
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
            header = create_header(len(data), WORKER_JOB_CANCEL, 0)
            worker.writer.write(header + data)
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

    @classmethod
    @cls_lock()
    async def load_from_disk(cls):
        failed_jobs = await ServerState.storage.load_by_type("failed_jobs")
        cls._failed_jobs = [
            EnodoJob.from_dict(job_data)
            for job_data in failed_jobs]

        cls._build_index()

        logging.info(
            f'Loaded {len(failed_jobs)} failed jobs from disk')
