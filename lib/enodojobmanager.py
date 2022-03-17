import asyncio
import datetime
import logging
import os
import datetime

import qpack
from enodo.jobs import *
from enodo.protocol.packagedata import EnodoJobDataModel, \
    EnodoJobRequestDataModel

from .events.enodoeventmanager import EnodoEvent, EnodoEventManager, \
    ENODO_EVENT_JOB_QUEUE_TOO_LONG, ENODO_EVENT_STATIC_RULE_FAIL
from .config import Config
from .series.seriesmanager import SeriesManager
from .serverstate import ServerState
from .socket import ClientManager
from .socket.package import create_header, WORKER_JOB, WORKER_JOB_CANCEL
from .socketio import SUBSCRIPTION_CHANGE_TYPE_UPDATE
from lib.util import load_disk_data, save_disk_data
from .socketio import SUBSCRIPTION_CHANGE_TYPE_DELETE, \
    SUBSCRIPTION_CHANGE_TYPE_ADD


class EnodoJob:
    __slots__ = ('rid', 'series_name', 'job_config',
                 'job_data', 'send_at', 'error', 'worker_id')

    def __init__(self, rid, series_name, job_config, job_data=None,
                 send_at=None, error=None, worker_id=None):
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

    @classmethod
    def to_dict(cls, job):
        resp = {}
        for slot in cls.__slots__:
            if isinstance(getattr(job, slot), datetime.datetime):
                resp[slot] = int(getattr(job, slot).timestamp())
            else:
                resp[slot] = getattr(job, slot)
        return resp

    @classmethod
    def from_dict(cls, data):
        return EnodoJob(**data)


class EnodoJobManager:
    _open_jobs = []
    _active_jobs = []
    _active_jobs_index = {}
    _failed_jobs = []
    _max_job_id = 1000
    _max_job_timeout = 60 * 5
    _next_job_id = None
    _locked = False
    _max_in_queue_before_warning = None

    _update_queue_cb = None

    @classmethod
    async def async_setup(cls, update_queue_cb):
        cls._next_job_id = 0

        cls._update_queue_cb = update_queue_cb
        cls._max_in_queue_before_warning = Config.max_in_queue_before_warning

    @classmethod
    async def _build_index(cls):
        cls._active_jobs_index = {}
        for job in cls._active_jobs:
            cls._active_jobs_index[job.rid] = job

    @classmethod
    async def _get_next_job_id(cls):
        await cls._lock()
        if cls._next_job_id + 1 >= cls._max_job_id:
            cls._next_job_id = 0
        cls._next_job_id += 1
        cls._unlock()
        return cls._next_job_id

    @classmethod
    async def _lock(cls):
        while cls._locked is True:
            await asyncio.sleep(0.1)
        cls._locked = True

    @classmethod
    def _unlock(cls):
        cls._locked = False

    @classmethod
    def get_active_jobs(cls):
        return cls._active_jobs

    @classmethod
    def get_failed_jobs(cls):
        return cls._failed_jobs

    @classmethod
    def get_open_jobs_count(cls):
        return len(cls._active_jobs)

    @classmethod
    def get_active_jobs_count(cls):
        return len(cls._active_jobs)

    @classmethod
    def get_failed_jobs_count(cls):
        return len(cls._failed_jobs)

    @classmethod
    def get_active_jobs_by_worker(cls, worker_id):
        return [job for job in cls._active_jobs if job.worker_id == worker_id]

    @classmethod
    async def create_job(cls, job_link_name, series_name):
        series = await SeriesManager.get_series(series_name)
        await series.set_job_status(job_link_name, JOB_STATUS_OPEN)
        job_config = series.get_job(job_link_name)
        job_id = await cls._get_next_job_id()
        job = EnodoJob(job_id, series_name, job_config,
                       job_data=None)  # TODO: Catch exception
        await cls._add_job(job)

    @classmethod
    async def _add_job(cls, job):
        if not isinstance(job, EnodoJob):
            raise Exception('Incorrect job instance')

        cls._open_jobs.append(job)
        if cls._update_queue_cb is not None:
            await cls._update_queue_cb(
                SUBSCRIPTION_CHANGE_TYPE_ADD, EnodoJob.to_dict(job))

    @classmethod
    def has_series_failed_jobs(cls, series_name):
        for job in cls._failed_jobs:
            if job.series_name == series_name:
                return True
        return False

    @classmethod
    def get_failed_jobs_for_series(cls, series_name):
        jobs = []
        for job in cls._failed_jobs:
            if job.series_name == series_name:
                jobs.append(job)
        return jobs

    @classmethod
    def remove_failed_jobs_for_series(cls, series_name):
        jobs = cls.get_failed_jobs_for_series(series_name)

        for job in jobs:
            cls._failed_jobs.remove(job)

    @classmethod
    async def activate_job(cls, job_id, worker_id):
        await cls._lock()

        j = None
        for job in cls._open_jobs:
            if job.rid == job_id:
                j = job
                break
        if j is not None:
            await cls._activate_job(j, worker_id)

        cls._unlock()

    @classmethod
    async def _activate_job(cls, job, worker_id):
        if job is None or worker_id is None:
            return

        if job in cls._open_jobs:
            cls._open_jobs.remove(job)
            if cls._update_queue_cb is not None:
                await cls._update_queue_cb(
                    SUBSCRIPTION_CHANGE_TYPE_DELETE, job.rid)
        job.send_at = datetime.datetime.now()
        job.worker_id = worker_id
        cls._active_jobs.append(job)
        cls._active_jobs_index[job.rid] = job

    @classmethod
    async def get_activated_job(cls, job_id):
        for job in cls._active_jobs:
            if job.rid == job_id:
                return job

        return None

    @classmethod
    async def deactivate_job(cls, job_id):
        await cls._lock()

        j = None
        for job in cls._active_jobs:
            if job.rid == job_id:
                j = job
                break

        cls._deactivate_job(j)
        cls._unlock()

    @classmethod
    def _deactivate_job(cls, job):
        if job in cls._active_jobs:
            cls._active_jobs.remove(job)
            del cls._active_jobs_index[job.rid]

    @classmethod
    async def cancel_job(cls, job):
        await cls._lock()

        if job in cls._active_jobs:
            cls._active_jobs.remove(job)
            del cls._active_jobs_index[job.rid]
            cls._open_jobs.append(job)
        cls._unlock()

    @classmethod
    async def cancel_jobs_for_series(cls, series_name):
        await cls._lock()
        await cls._cancel_jobs_for_series(series_name)
        cls._unlock()

    @classmethod
    async def _cancel_jobs_for_series(cls, series_name):
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
    async def set_job_failed(cls, job_id, error):
        await cls._lock()

        j = None
        for job in cls._active_jobs:
            if job.rid == job_id:
                j = job
                break
        await cls._set_job_failed(j, error)
        cls._unlock()

    @classmethod
    async def _set_job_failed(cls, job, error):
        if job is not None:
            job.error = error
            await cls._cancel_jobs_for_series(job.series_name)
            if job in cls._active_jobs:
                cls._active_jobs.remove(job)
            del cls._active_jobs_index[job.rid]
            cls._failed_jobs.append(job)

    @classmethod
    async def clean_jobs(cls):
        await cls._lock()

        for job in cls._active_jobs:
            now = datetime.datetime.now()
            if (now - job.send_at).total_seconds() > cls._max_job_timeout:
                await cls._set_job_failed(job, "Job timed-out")
                await cls._send_worker_cancel_job(job.worker_id, job.rid)
        cls._unlock()

        if len(cls._open_jobs) > cls._max_in_queue_before_warning:
            event = EnodoEvent(
                'Job queue too long',
                f'{len(cls._open_jobs)} jobs waiting \
                in queue exceeds threshold of \
                    {cls._max_in_queue_before_warning}',
                ENODO_EVENT_JOB_QUEUE_TOO_LONG)
            await EnodoEventManager.handle_event(event)

    @classmethod
    async def check_for_jobs(cls):
        while ServerState.running:
            ServerState.tasks_last_runs['check_jobs'] = datetime.datetime.now(
            )
            if len(cls._open_jobs) == 0:
                await cls.clean_jobs()
                await asyncio.sleep(Config.watcher_interval)
                continue

            for next_job in cls._open_jobs:
                try:
                    await cls._lock()
                    series = await SeriesManager.get_series(
                        next_job.series_name)
                    if series is None:
                        continue

                    worker = await ClientManager.get_free_worker(
                        next_job.series_name, next_job.job_config.job_type,
                        await series.get_model(next_job.job_config.link_name))
                    if worker is None:
                        continue

                    logging.info(
                        f"Adding series: sending {next_job.series_name} to "
                        f"Worker for job type {next_job.job_config.job_type}")
                    await cls._send_worker_job_request(worker, next_job)
                    worker.is_going_busy = True
                    await cls._activate_job(next_job, worker.client_id)
                except Exception as e:
                    logging.error(
                        "Something went wrong when trying to activate job")
                    logging.debug(f"Corresponding error: {e}")
                finally:
                    cls._unlock()
            await cls.clean_jobs()
            await asyncio.sleep(Config.watcher_interval)

    @classmethod
    async def receive_job_result(cls, writer, packet_type,
                                 packet_id, data, client_id):
        job_id = data.get('job_id')

        if data.get('error') is not None:
            logging.error(
                f"Error returned by worker for series {data.get('name')}")
            await cls.set_job_failed(job_id, data.get('error'))
            return

        job_type = data.get('job_type')
        job = await cls.get_activated_job(job_id)
        await cls.deactivate_job(job_id)
        series = await SeriesManager.get_series(data.get('name'))
        if job_type == JOB_TYPE_FORECAST_SERIES:
            try:
                await SeriesManager.add_forecast_to_series(
                    data.get('name'),
                    job.job_config.link_name,
                    data.get('points'))
                await series.set_job_status(
                    job.job_config.link_name, JOB_STATUS_DONE)
                await series.schedule_job(job.job_config.link_name)
                await SeriesManager.series_changed(
                    SUBSCRIPTION_CHANGE_TYPE_UPDATE, data.get('name'))
            except Exception as e:
                logging.error(
                    f"Something went wrong when receiving forecast job")
                logging.debug(f"Corresponding error: {e}")
        elif job_type == JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES:
            if isinstance(
                    data.get('anomalies'),
                    list) and len(
                    data.get('anomalies')) > 0:
                try:
                    await SeriesManager.add_anomalies_to_series(
                        data.get('name'),
                        job.job_config.link_name,
                        data.get('anomalies'))
                    await series.set_job_status(
                        job.job_config.link_name, JOB_STATUS_DONE)
                    await series.schedule_job(job.job_config.link_name)
                    await SeriesManager.series_changed(
                        SUBSCRIPTION_CHANGE_TYPE_UPDATE, data.get('name'))
                except Exception as e:
                    logging.error(
                        f"Something went wrong when receiving"
                        f"anomaly detection job")
                    logging.debug(f"Corresponding error: {e}")
        elif job_type == JOB_TYPE_BASE_SERIES_ANALYSIS:
            try:
                series.series_characteristics = data.get(
                    'data').get('characteristics')
                series.state.health = data.get('data').get('health')
                await series.set_job_status(
                    job.job_config.link_name, JOB_STATUS_DONE)
                await series.schedule_job(job.job_config.link_name)
                await SeriesManager.series_changed(
                    SUBSCRIPTION_CHANGE_TYPE_UPDATE, data.get('name'))
            except Exception as e:
                logging.error(
                    f"Something went wrong when receiving base analysis job")
                logging.debug(f"Corresponding error: {e}")
        elif job_type == JOB_TYPE_STATIC_RULES:
            try:
                await series.set_job_status(
                    job.job_config.link_name, JOB_STATUS_DONE)
                await series.schedule_job(job.job_config.link_name)
                if len(data.get('failed_checks')):
                    for key in data.get('failed_checks'):
                        event = EnodoEvent(
                            'Static rule failed!',
                            f'Series {data.get("name")} failed a \
                                static rule ({key}): \
                                    {data.get("failed_checks")[key]}',
                            ENODO_EVENT_STATIC_RULE_FAIL,
                            series=series)
                        await EnodoEventManager.handle_event(
                            event, series=series)
                await SeriesManager.series_changed(
                    SUBSCRIPTION_CHANGE_TYPE_UPDATE, data.get('name'))
            except Exception as e:
                logging.error(
                    f"Something went wrong when receiving static rules job")
                logging.debug(f"Corresponding error: {e}")
        else:
            logging.error(f"Received unknown job type: {job_type}")

    @classmethod
    async def _send_worker_job_request(cls, worker, job):
        try:
            series = await SeriesManager.get_series(job.series_name)
            job_data = EnodoJobRequestDataModel(
                job_id=job.rid, job_config=job.job_config,
                series_name=job.series_name,
                series_config=series.series_config.to_dict())
            data = qpack.packb(job_data.serialize())
            header = create_header(len(data), WORKER_JOB, 0)
            worker.writer.write(header + data)
        except Exception as e:
            logging.error(
                f"Something went wrong when sending job request to worker")
            import traceback
            traceback.print_exc()
            logging.debug(f"Corresponding error: {e}")

    @classmethod
    async def _send_worker_cancel_job(cls, worker_id, job_id):
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
            logging.debug(f"Corresponding error: {e}")

    @classmethod
    async def receive_worker_cancelled_job(cls, writer, packet_type,
                                           packet_id, data, client_id):
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
            logging.debug(f"Corresponding error: {e}")

    @classmethod
    async def get_open_queue(cls):
        return [EnodoJob.to_dict(job) for job in cls._open_jobs]

    @classmethod
    async def save_to_disk(cls):
        await cls._lock()
        try:
            job_data = {
                'next_job_id': cls._next_job_id,
                'failed_jobs': [
                    EnodoJob.to_dict(job) for job in cls._failed_jobs],
            }
            save_disk_data(Config.jobs_save_path, job_data)
        except Exception as e:
            logging.error(
                f"Something went wrong when saving jobmanager data to disk")
            logging.debug(f"Corresponding error: {e}")
        cls._unlock()

    @classmethod
    async def load_from_disk(cls):
        loaded_failed_jobs = 0
        await cls._lock()
        try:
            if not os.path.exists(Config.jobs_save_path):
                raise Exception()
            data = load_disk_data(Config.jobs_save_path)
        except Exception as e:
            data = {}

        if isinstance(data, dict):
            if 'next_job_id' in data:
                cls._next_job_id = int(data.get('next_job_id'))
            if 'failed_jobs' in data:
                loaded_failed_jobs += len(data.get('failed_jobs'))
                cls._failed_jobs = [
                    EnodoJob.from_dict(job_data)
                    for job_data in data.get('failed_jobs')]

        await cls._build_index()

        logging.info(
            f'Loaded {loaded_failed_jobs} failed jobs from disk')

        cls._unlock()
