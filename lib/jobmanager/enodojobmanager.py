import asyncio
import datetime
import qpack

from lib.analyser.analyserwrapper import AnalyserWrapper
from lib.config.config import Config
from lib.jobmanager import *
from lib.logging.eventlogger import EventLogger
from lib.serie.seriemanager import SerieManager
from lib.serverstate import ServerState
from lib.socket.clientmanager import ClientManager
from lib.socket.package import create_header, WORKER_JOB, WORKER_JOB_CANCEL


class EnodoJob:
    serie_name = None
    job_type = None
    send_at = None
    job_id = None
    error = None
    send_to_worker = None

    def __init__(self, job_id, job_type, serie_name):
        if job_type not in JOB_TYPES:
            raise Exception('unknow job type')

        self.job_id = job_id
        self.job_type = job_type
        self.serie_name = serie_name


class EnodoJobManager:
    _new_jobs = None
    _active_jobs = None
    _failed_jobs = None
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
        cls._failed_jobs = []

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
    async def create_job(cls, job_type, serie_name):
        job_id = await cls._get_next_job_id()
        job = EnodoJob(job_id, job_type, serie_name)
        await cls._add_job(job)

    @classmethod
    async def _add_job(cls, job):
        if not isinstance(job, EnodoJob):
            raise Exception('Incorrect job instance')

        cls._new_jobs.append(job)

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
        job.send_to_worker = worker_id
        cls._active_jobs.append(job)

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
            cls._active_jobs.remove(job)
            cls._failed_jobs.append(job)

    @classmethod
    async def clean_jobs(cls):
        while cls._lock is True:
            await asyncio.sleep(0.1)
        cls._lock = True

        for job in cls._active_jobs:
            if (datetime.datetime.now() - job.send_at).total_seconds() > cls._max_job_timeout:
                job.error = "Job timed-out"
                await cls._set_job_failed(job)
                await cls._send_worker_cancel_job(job.send_to_worker, job.job_id)
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
                            EventLogger.log(f"Adding serie: sending {next_job.serie_name} to Worker for forecasting",
                                            "info",
                                            "serie_add_queue")
                            await cls._send_worker_job_request(worker, serie, next_job.job_type, next_job.job_id)
                            worker.is_going_busy = True
                            await cls._activate_job(next_job, worker.client_id)
                        elif next_job.job_type is JOB_TYPE_DETECT_ANOMALIES_FOR_SERIE:
                            EventLogger.log(
                                f"Adding serie: sending {next_job.serie_name} to Worker for anomaly detection",
                                "info",
                                "serie_add_queue")
                            await cls._send_worker_job_request(worker, serie, next_job.job_type, next_job.job_id)
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
            EventLogger.log(f"Error returned by worker for series {data.get('name')}", "error")
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
                    await SerieManager.series_changed()
                except Exception as e:
                    print(e)
            elif job_type is JOB_TYPE_DETECT_ANOMALIES_FOR_SERIE:
                try:
                    await SerieManager.add_anomalies_to_serie(data.get('name'), data.get('anomalies'))
                    await SerieManager.series_changed()
                except Exception as e:
                    print(e)
            else:
                print("UNKNOWN")

    @classmethod
    async def _send_worker_job_request(cls, worker, serie, job_type, job_id):
        try:
            model = await serie.get_model_pkl()
            wrapper = (AnalyserWrapper(model, await serie.get_model(), await serie.get_model_parameters())).__dict__()
            data = qpack.packb(
                {'serie_name': await serie.get_name(), 'wrapper': wrapper, 'job_type': job_type, 'job_id': job_id})
            header = create_header(len(data), WORKER_JOB, 0)
            if serie not in worker.pending_series:
                worker.pending_series.append(await serie.get_name())
            worker.writer.write(header + data)
        except Exception as e:
            print("something when wrong", e)

    @classmethod
    async def _send_worker_cancel_job(cls, worker_id, job_id):
        worker = await ClientManager.get_worker_by_id(worker_id)
        if worker is None:
            return
        try:
            EventLogger.log(f"Asking worker {worker_id} to cancel job {job_id}", "error")
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
        if worker is None:
            return
        try:
            EventLogger.log(f"Worker {client_id} cancelled job {job_id}", "error")
            await ClientManager.check_for_pending_series(worker)
        except Exception as e:
            print("something when wrong", e)
