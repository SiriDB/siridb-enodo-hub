import asyncio
import time

from enodo.jobs import JOB_TYPE_BASE_SERIES_ANALYSIS, JOB_STATUS_NONE, \
    JOB_STATUS_DONE
from enodo.model.config.series import SeriesConfigModel, SeriesState, \
    SeriesJobConfigModel

from lib.socket.clientmanager import ClientManager


class Series:
    __slots__ = ('rid', 'name', 'series_config', 'state',
                 '_datapoint_count_lock', 'series_characteristics')

    def __init__(self, name: str, config: dict, state=None,
                 series_characteristics=None, **kwargs):
        self.rid = name
        self.name = name
        self.series_config = SeriesConfigModel(**config)
        if state is None:
            state = {}
        self.state = SeriesState(**state)
        self.series_characteristics = series_characteristics
        self._datapoint_count_lock = asyncio.Lock()

    def get_errors(self) -> list:
        # To stop circular import
        from ..jobmanager import EnodoJobManager
        errors = [
            job.error
            for job in EnodoJobManager.get_failed_jobs_for_series(
                self.name)]
        return errors

    def is_ignored(self) -> bool:
        # To stop circular import
        from ..jobmanager import EnodoJobManager
        return EnodoJobManager.has_series_failed_jobs(self.name)

    async def get_module(self, job_name: str) -> SeriesJobConfigModel:
        return self.series_config.get_config_for_job(job_name).module

    async def get_job_status(self, job_config_name: str) -> int:
        return self.state.get_job_status(job_config_name)

    async def set_job_status(self, config_name: str, status: int):
        self.state.set_job_status(config_name, status)

    @property
    def base_analysis_job(self) -> SeriesJobConfigModel:
        job_config = self.series_config.get_config_for_job_type(
            JOB_TYPE_BASE_SERIES_ANALYSIS)
        if job_config is None or job_config is False:
            return False
        return job_config

    def get_job(self, job_config_name: str) -> SeriesJobConfigModel:
        return self.series_config.get_config_for_job(job_config_name)

    def base_analysis_status(self) -> int:
        job_config = self.series_config.get_config_for_job_type(
            JOB_TYPE_BASE_SERIES_ANALYSIS)
        if job_config is None or job_config is False:
            return False
        return self.state.get_job_status(job_config.config_name)

    def get_datapoints_count(self) -> int:
        return self.state.datapoint_count

    async def add_to_datapoints_count(self, add_to_count: int):
        """
        Add value to existing value of data points counter
        :param add_to_count:
        :return:
        """
        async with self._datapoint_count_lock:
            self.state.datapoint_count += add_to_count

    async def schedule_job(self, job_config_name: str, initial=False):
        job_config = self.series_config.get_config_for_job(
            job_config_name)
        if job_config is None:
            return False

        job_schedule = self.state.get_job_schedule(job_config_name)

        if job_schedule is None:
            job_schedule = {"value": 0,
                            "type": job_config.job_schedule_type}

        next_value = None
        if job_schedule["type"] == "TS":
            current_ts = int(time.time())
            if initial:
                next_value = current_ts
            elif job_schedule["value"] <= current_ts:
                next_value = current_ts + job_config.job_schedule
        elif job_schedule["type"] == "N":
            if initial:
                next_value = self.state.datapoint_count
            elif job_schedule["value"] <= self.state.datapoint_count:
                next_value = \
                    self.state.datapoint_count + job_config.job_schedule

        if next_value is not None:
            job_schedule['value'] = next_value
            self.state.set_job_schedule(job_config_name, job_schedule)

    async def is_job_due(self, job_config_name: str) -> bool:
        job_status = self.state.get_job_status(job_config_name)
        job_schedule = self.state.get_job_schedule(job_config_name)

        if job_status in [JOB_STATUS_NONE, JOB_STATUS_DONE]:
            job_config = self.series_config.get_config_for_job(
                job_config_name)
            module = ClientManager.get_module(job_config.module)
            if module is None:
                self.state.set_job_check_status(
                    job_config_name,
                    "Unknown module")
                return False
            if job_config.requires_job is not None:
                required_job_status = self.state.get_job_status(
                    job_config.requires_job)
                if required_job_status is not JOB_STATUS_DONE:
                    self.state.set_job_check_status(
                        job_config_name,
                        f"Waiting for required job {job_config.requires_job}")
                    return False
            if job_schedule["value"] is None:
                return True
            if job_schedule["type"] == "TS" and \
                    job_schedule["value"] <= int(time.time()):
                return True
            if job_schedule["type"] == "N" and \
                    job_schedule["value"] <= self.state.datapoint_count:
                return True
            self.state.set_job_check_status(
                job_config_name,
                "Not yet scheduled")
            return False
        self.state.set_job_check_status(
            job_config_name,
            "Already active")
        return False

    def update(self, data: dict) -> bool:
        config = data.get('config')
        if config is not None:
            self.series_config = SeriesConfigModel(**config)

        return True

    def to_dict(self, static_only=False) -> dict:
        if static_only:
            return {
                'name': self.name,
                'state': self.state,
                'config': self.series_config,
                'series_characteristics': self.series_characteristics
            }
        return {
            'rid': self.rid,
            'name': self.name,
            'state': self.state,
            'config': self.series_config,
            'ignore': self.is_ignored(),
            'error': self.get_errors(),
            'series_characteristics': self.series_characteristics
        }

    @classmethod
    def from_dict(cls, data_dict: dict) -> 'Series':
        return Series(**data_dict)
