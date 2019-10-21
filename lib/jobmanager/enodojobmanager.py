import asyncio

from lib.config.config import Config
from lib.logging.eventlogger import EventLogger
from lib.serie.seriemanager import SerieManager
from lib.serie.series import DETECT_ANOMALIES_STATUS_PENDING
from lib.serverstate import ServerState
from lib.socket.clientmanager import ClientManager
from lib.socket.handler import send_worker_job_request, WORKER_JOB_FORECAST, WORKER_JOB_DETECT_ANOMALIES

JOB_TYPE_FORECAST_SERIE = 1
JOB_TYPE_DETECT_ANOMALIES_FOR_SERIE = 2
JOB_TYPES = [JOB_TYPE_FORECAST_SERIE, JOB_TYPE_DETECT_ANOMALIES_FOR_SERIE]


class EnodoJob:
    serie_name = None
    job_type = None

    def __init__(self, job_type, serie_name):
        if job_type not in JOB_TYPES:
            raise Exception('unknow job type')

        self.job_type = job_type
        self.serie_name = serie_name


class EnodoJobManager:
    _jobs = None

    @classmethod
    async def async_setup(cls):
        cls._jobs = []

    @classmethod
    async def add_job(cls, job):
        if not isinstance(job, EnodoJob):
            raise Exception('Incorrect job instance')

        cls._jobs.append(job)

    @classmethod
    async def _remove_job(cls, job):
        if job in cls._jobs:
            cls._jobs.remove(job)

    @classmethod
    async def check_for_jobs(cls):
        while ServerState.running:
            if len(cls._jobs) > 0:
                worker = await ClientManager.get_free_worker()
                if worker is not None:
                    next_job = cls._jobs[0]
                    serie = await SerieManager.get_serie(next_job.serie_name)
                    if serie is None:
                        continue
                    if next_job.job_type is JOB_TYPE_FORECAST_SERIE:
                        EventLogger.log(f"Adding serie: sending {next_job.serie_name} to Worker for forecasting",
                                        "info",
                                        "serie_add_queue")
                        await serie.set_pending_forecast(True)
                        await send_worker_job_request(worker, serie, WORKER_JOB_FORECAST)
                        worker.is_going_busy = True
                        await cls._remove_job(next_job)
                    elif next_job.job_type is JOB_TYPE_DETECT_ANOMALIES_FOR_SERIE:
                        EventLogger.log(f"Adding serie: sending {next_job.serie_name} to Worker for anomaly detection",
                                        "info",
                                        "serie_add_queue")
                        await serie.set_detect_anomalies_status(DETECT_ANOMALIES_STATUS_PENDING)
                        await send_worker_job_request(worker, serie, WORKER_JOB_DETECT_ANOMALIES)
                        await cls._remove_job(next_job)
                    else:
                        return False
            await asyncio.sleep(Config.watcher_interval)
