import asyncio
import datetime
import json
import logging
import os

import qpack

from lib.analyser.analyserwrapper import AnalyserWrapper
from lib.config.config import Config
from lib.jobmanager import *
from lib.serie.seriemanager import SerieManager
from lib.serverstate import ServerState
from lib.socket.clientmanager import ClientManager
from lib.socket.package import create_header, WORKER_JOB, WORKER_JOB_CANCEL
from lib.socketio import SUBSCRIPTION_CHANGE_TYPE_UPDATE
from lib.util.util import safe_json_dumps


class EnodoJob:
    __slots__ = ('job_id', 'job_type', 'serie_name', 'job_data', 'send_at', 'error', 'worker_id')

    def __init__(self, job_id, job_type, serie_name, job_data=None, send_at=None, error=None, worker_id=None):
        if job_type not in JOB_TYPES:
            raise Exception('unknow job type')
        self.job_id = job_id
        self.job_type = job_type
        self.serie_name = serie_name
        self.job_data = job_data
        self.send_at = send_at
        self.error = error
        self.worker_id = worker_id

    @classmethod
    async def to_dict(cls, job):
        resp = {}
        for slot in cls.__slots__:
            resp[slot] = getattr(job, slot)
        return resp

    @classmethod
    async def from_dict(cls, data):
        return EnodoJob(**data)


class EnodoJobManager:
    _new_jobs = None
    _active_jobs = None
    _active_jobs_index = None
    _failed_jobs = None
    _failed_jobs_index = None
    _finished_jobs = None
    _finished_jobs_index = None
    _max_job_id = 1000
    _max_job_timeout = 60 * 5
    _next_job_id = None
    _lock = None

    @classmethod
    async def async_setup(cls):
        cls._next_job_id = 0
        cls._lock = False
        cls._new_jobs = []
        cls._active_jobs = []
        cls._active_jobs_index = {}
        cls._failed_jobs = []
        cls._failed_jobs_index = {}
        cls._finished_jobs = []
        cls._finished_jobs_index = {}

    @classmethod
    async def _build_index(cls):
        cls._active_jobs_index = {}
        for job in cls._active_jobs:
            cls._active_jobs_index[job.job_id] = job

        cls._failed_jobs_index = {}
        for job in cls._failed_jobs:
            cls._failed_jobs_index[job.job_id] = job

        cls._finished_jobs_index = {}
        for job in cls._finished_jobs:
            cls._finished_jobs_index[job.job_id] = job

    @classmethod
    async def _get_next_job_id(cls):
        while cls._lock is True:
            await asyncio.sleep(0.1)
        cls._lock = True
        if cls._next_job_id + 1 >= cls._max_job_id:
            cls._next_job_id = 0
        cls._next_job_id += 1
        cls._lock = False
        return cls._next_job_id

    @classmethod
    async def create_job(cls, job_type, serie_name, job_data=None):
        job_id = await cls._get_next_job_id()
        job = EnodoJob(job_id, job_type, serie_name, job_data=job_data)  # TODO: Catch exception
        await cls._add_job(job)

    @classmethod
    async def _add_job(cls, job):
        if not isinstance(job, EnodoJob):
            raise Exception('Incorrect job instance')

        cls._new_jobs.append(job)

    @classmethod
    async def has_series_failed_jobs(cls, serie_name):
        for job in cls._failed_jobs:
            if job.serie_name == serie_name:
                return True
        return False

    @classmethod
    async def get_failed_jobs_for_series(cls, serie_name):
        jobs = []
        for job in cls._failed_jobs:
            if job.serie_name == serie_name:
                jobs.append(job)
        return jobs

    @classmethod
    async def remove_failed_jobs_for_series(cls, serie_name):
        jobs = await cls.get_failed_jobs_for_series(serie_name)

        for job in jobs:
            cls._failed_jobs.remove(job)

    @classmethod
    async def activate_job(cls, job_id, worker_id):
        while cls._lock is True:
            await asyncio.sleep(0.1)
        cls._lock = True

        j = None
        for job in cls._new_jobs:
            if job.job_id == job_id:
                j = job
                break
        if j is not None:
            await cls._active_jobs(j, worker_id)

        cls._lock = False

    @classmethod
    async def _activate_job(cls, job, worker_id):
        if job is None or worker_id is None:
            return

        if job in cls._new_jobs:
            cls._new_jobs.remove(job)
        job.send_at = datetime.datetime.now()
        job.worker_id = worker_id
        cls._active_jobs.append(job)
        cls._active_jobs_index[job.job_id] = job

    @classmethod
    async def deactivate_job(cls, job_id):
        while cls._lock is True:
            await asyncio.sleep(0.1)
        cls._lock = True

        j = None
        for job in cls._active_jobs:
            if job.job_id == job_id:
                j = job
                break

        await cls._deactivate_job(j)
        cls._lock = False

    @classmethod
    async def _deactivate_job(cls, job):
        if job in cls._active_jobs:
            cls._active_jobs.remove(job)
            del cls._active_jobs_index[job.job_id]
            cls._finished_jobs.append(job)
            cls._active_jobs_index[job.job_id] = job

    @classmethod
    async def cancel_jobs_for_serie(cls, serie_name):
        while cls._lock is True:
            await asyncio.sleep(0.1)
        cls._lock = True

        await cls.cancel_jobs_for_serie(serie_name)

        cls._lock = False

    @classmethod
    async def _cancel_jobs_for_serie(cls, serie_name):
        jobs = []
        for job in cls._new_jobs:
            if job.serie_name == serie_name:
                jobs.append(job)

        for job in jobs:
            cls._new_jobs.remove(job)

    @classmethod
    async def set_job_failed(cls, job_id):
        while cls._lock is True:
            await asyncio.sleep(0.1)
        cls._lock = True

        j = None
        for job in cls._active_jobs:
            if job.job_id == job_id:
                j = job
                break
        await cls._set_job_failed(j)
        cls._lock = False

    @classmethod
    async def _set_job_failed(cls, job):
        if job is not None:
            await cls._cancel_jobs_for_serie(job.serie_name)
            cls._active_jobs.remove(job)
            del cls._active_jobs_index[job.job_id]
            cls._failed_jobs.append(job)
            cls._failed_jobs_index[job.job_id] = job

    @classmethod
    async def clean_jobs(cls):
        while cls._lock is True:
            await asyncio.sleep(0.1)
        cls._lock = True

        for job in cls._active_jobs:
            if (datetime.datetime.now() - job.send_at).total_seconds() > cls._max_job_timeout:
                job.error = "Job timed-out"
                await cls._set_job_failed(job)
                await cls._send_worker_cancel_job(job.worker_id, job.job_id)
        cls._lock = False

    @classmethod
    async def check_for_jobs(cls):
        while ServerState.running:
            if len(cls._new_jobs) > 0:
                try:
                    worker = await ClientManager.get_free_worker()
                    if worker is not None:
                        while cls._lock is True:
                            print("here1")
                            await asyncio.sleep(0.1)
                        cls._lock = True
                        next_job = cls._new_jobs[0]
                        serie = await SerieManager.get_serie(next_job.serie_name)
                        if serie is None:
                            pass
                        elif next_job.job_type is JOB_TYPE_FORECAST_SERIE:
                            logging.info(f"Adding serie: sending {next_job.serie_name} to Worker for forecasting")
                            await cls._send_worker_job_request(worker, serie, next_job)
                            worker.is_going_busy = True
                            await cls._activate_job(next_job, worker.client_id)
                        elif next_job.job_type is JOB_TYPE_DETECT_ANOMALIES_FOR_SERIE:
                            logging.info(f"Adding serie: sending {next_job.serie_name} to Worker for anomaly detection")
                            await cls._send_worker_job_request(worker, serie, next_job)
                            worker.is_going_busy = True
                            await cls._activate_job(next_job, worker.client_id)
                        else:
                            pass
                        cls._lock = False
                except Exception as e:
                    print(e)
            await cls.clean_jobs()
            await asyncio.sleep(Config.watcher_interval)

    @classmethod
    async def receive_job_result(cls, writer, packet_type, packet_id, data, client_id):
        job_id = data.get('job_id')

        if data.get('error') is not None:
            logging.error(f"Error returned by worker for series {data.get('name')}")
            series = await SerieManager.get_serie(data.get('name'))
            if series is not None:
                await series.set_error(data.get('error'))
            await cls.set_job_failed(job_id)
        else:
            job_type = data.get('job_type')
            await cls.deactivate_job(job_id)
            if job_type is JOB_TYPE_FORECAST_SERIE:
                try:
                    await SerieManager.add_forecast_to_serie(data.get('name'), data.get('points'))
                    await SerieManager.series_changed(SUBSCRIPTION_CHANGE_TYPE_UPDATE, data.get('name'))
                except Exception as e:
                    print(e)
            elif job_type is JOB_TYPE_DETECT_ANOMALIES_FOR_SERIE:
                if isinstance(data.get('anomalies'), list) and len(data.get('anomalies')) > 0:
                    try:
                        await SerieManager.add_anomalies_to_serie(data.get('name'), data.get('anomalies'))
                        await SerieManager.series_changed(SUBSCRIPTION_CHANGE_TYPE_UPDATE, data.get('name'))
                    except Exception as e:
                        print(e)
            else:
                print("UNKNOWN")

    @classmethod
    async def _send_worker_job_request(cls, worker, serie, job):
        try:
            model = await serie.get_model_pkl()
            wrapper = (AnalyserWrapper(model, await serie.get_model(), await serie.get_model_parameters())).__dict__()
            data = qpack.packb(
                {'serie_name': serie.name, 'wrapper': wrapper, 'job_type': job.job_type,
                 'job_id': job.job_id, 'job_data': job.job_data})
            header = create_header(len(data), WORKER_JOB, 0)
            if serie not in worker.pending_series:
                worker.pending_series.append(serie.name)
            worker.writer.write(header + data)
        except Exception as e:
            print("something when wrong", e)

    @classmethod
    async def _send_worker_cancel_job(cls, worker_id, job_id):
        worker = await ClientManager.get_worker_by_id(worker_id)
        if worker is None:
            return
        try:
            logging.error(f"Asking worker {worker_id} to cancel job {job_id}")
            data = qpack.packb(
                {'job_id': job_id})
            header = create_header(len(data), WORKER_JOB_CANCEL, 0)
            worker.writer.write(header + data)
        except Exception as e:
            print("something when wrong", e)

    @classmethod
    async def receive_worker_cancelled_job(cls, writer, packet_type, packet_id, data, client_id):
        job_id = data.get('job_id')
        worker = await ClientManager.get_worker_by_id(client_id)
        if job_id in cls._active_jobs_index:
            await cls.set_job_failed(job_id)
        logging.error(f"Worker {client_id} cancelled job {job_id}")
        if worker is None:
            return
        try:
            await ClientManager.check_for_pending_series(worker)
        except Exception as e:
            print("something when wrong", e)

    @classmethod
    async def save_to_disk(cls):
        while cls._lock is True:
            await asyncio.sleep(0.1)
        cls._lock = True
        try:
            job_data = {
                'next_job_id': cls._next_job_id,
                'active_jobs': [await EnodoJob.to_dict(job) for job in cls._active_jobs],
                'failed_jobs': [await EnodoJob.to_dict(job) for job in cls._failed_jobs],
                'finished_jobs': [await EnodoJob.to_dict(job) for job in cls._finished_jobs],
            }
            f = open(Config.jobs_save_path, "w")
            f.write(json.dumps(job_data, default=safe_json_dumps))
            f.close()
        except Exception as e:
            print(e)
        cls._lock = False

    @classmethod
    async def load_from_disk(cls):
        loaded_active_jobs = 0
        loaded_failed_jobs = 0
        loaded_finished_jobs = 0
        while cls._lock is True:
            await asyncio.sleep(0.1)
        cls._lock = True
        try:
            if not os.path.exists(Config.jobs_save_path):
                raise Exception()
            f = open(Config.series_save_path, "r")
            data = f.read()
            f.close()
        except Exception as e:
            data = "{}"

        data = json.loads(data)

        if isinstance(data, dict):
            if 'next_job_id' in data:
                cls._next_job_id = int(data.get('next_job_id'))
            if 'active_jobs' in data:
                loaded_active_jobs += len(data.get('active_jobs'))
                cls._active_jobs = [await EnodoJob.from_dict(job_data) for job_data in data.get('active_jobs')]
            if 'failed_jobs' in data:
                loaded_failed_jobs += len(data.get('failed_jobs'))
                cls._failed_jobs = [await EnodoJob.from_dict(job_data) for job_data in data.get('failed_jobs')]
            if 'finished_jobs' in data:
                loaded_finished_jobs += len(data.get('finished_jobs'))
                cls._finished_jobs = [await EnodoJob.from_dict(job_data) for job_data in data.get('finished_jobs')]

        await cls._build_index()

        logging.info(f'Loaded {loaded_active_jobs} active jobs, {loaded_failed_jobs} failed jobs and {loaded_finished_jobs} finished jobs from disk')

        cls._lock = False
