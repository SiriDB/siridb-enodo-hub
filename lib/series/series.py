import asyncio
import time
from typing import Optional, Union

from enodo.jobs import JOB_TYPE_BASE_SERIES_ANALYSIS, JOB_STATUS_NONE, \
    JOB_STATUS_DONE, JOB_STATUS_FAILED
from enodo.model.config.series import SeriesConfigModel, SeriesState, \
    SeriesJobConfigModel
from lib.serverstate import ServerState

from lib.socket.clientmanager import ClientManager
from lib.state.resource import StoredResource


class Series(StoredResource):
    __slots__ = ('rid', 'name', 'config', 'state', 'meta',
                 '_datapoint_count_lock', '_config_from_template')

    def __init__(self,
                 name: str,
                 config: Union[dict, str],
                 state: Optional[dict] = None,
                 meta: Optional[dict] = None,
                 **kwargs):
        self.rid = name
        self.name = name
        self.meta = meta
        self._config_from_template = False
        self._setup_config(config)
        self.state = SeriesState() if state is None else SeriesState(**state)
        self._datapoint_count_lock = asyncio.Lock()
        self.lock = asyncio.Lock()

    def _setup_config(self, config):
        if isinstance(config, dict):
            self.config = SeriesConfigModel(**config)
            return
        self._config_from_template = True
        config = ServerState.series_config_template_rm.get_cached_resource(
            config)
        if config is None:
            raise Exception("Invalid series config template rid")
        config = SeriesConfigModel(**config.series_config)
        self.config = config

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

    def get_module(self, job_name: str) -> SeriesJobConfigModel:
        return self.config.get_config_for_job(job_name).module

    def get_job_status(self, job_config_name: str) -> int:
        return self.state.get_job_status(job_config_name)

    def set_job_status(self, config_name: str, status: int):
        self.state.set_job_status(config_name, status)

    @property
    def base_analysis_job(self) -> SeriesJobConfigModel:
        job_config = self.config.get_config_for_job_type(
            JOB_TYPE_BASE_SERIES_ANALYSIS)
        if job_config is None or job_config is False:
            return False
        return job_config

    def get_job(self, job_config_name: str) -> SeriesJobConfigModel:
        return self.config.get_config_for_job(job_config_name)

    def base_analysis_status(self) -> int:
        job_config = self.config.get_config_for_job_type(
            JOB_TYPE_BASE_SERIES_ANALYSIS)
        if job_config is None or job_config is False:
            return False
        return self.state.get_job_status(job_config.config_name)

    def add_job_config(self, job_config):
        self.config.add_config_for_job(job_config)

    def remove_job_config(self, job_config_name):
        removed = self.config.remove_config_for_job(
            job_config_name)
        self.state.remove_job_state(job_config_name)
        return removed

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

    def schedule_job(self, job_config_name: str, initial=False):
        job_config = self.config.get_config_for_job(job_config_name)
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
        ServerState.index_series_schedules(self)

    def is_job_due(self, job_config_name: str) -> bool:
        job_status = self.state.get_job_status(job_config_name)
        job_schedule = self.state.get_job_schedule(job_config_name)

        if job_status not in [JOB_STATUS_NONE, JOB_STATUS_DONE]:
            jcs = "Job failed" if job_status == JOB_STATUS_FAILED else \
                "Already active"
            self.state.set_job_check_status(job_config_name, jcs)
            return False

        job_config = self.config.get_config_for_job(job_config_name)
        module = ClientManager.get_module(job_config.module)
        if module is None:
            self.state.set_job_check_status(
                job_config_name, "Unknown module")
            return False
        r_job = job_config.requires_job
        if r_job is not None and self.config.get_config_for_job(
                r_job) is not None:
            required_job_status = self.state.get_job_status(r_job)
            if required_job_status is not JOB_STATUS_DONE:
                self.state.set_job_check_status(
                    job_config_name,
                    f"Waiting for required job {r_job}")
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
            job_config_name, "Not yet scheduled")
        return False

    def update(self, data: dict) -> bool:
        config = data.get('config')
        if config is not None:
            self.config = SeriesConfigModel(**config)
        return True

    @classmethod
    @property
    def resource_type(self):
        return "series"

    @property
    def to_store_data(self):
        return self.to_dict(static_only=True)

    def to_dict(self, static_only=False) -> dict:
        if static_only:
            return {
                'rid': self.rid,
                'name': self.name,
                'meta': self.meta,
                'state': self.state,
                'config': self.config if self._config_from_template is False
                else self.config.rid
            }
        return {
            'rid': self.rid,
            'name': self.name,
            'meta': self.meta,
            'state': self.state,
            'config': self.config,
            'ignore': self.is_ignored(),
            'error': self.get_errors()
        }

    @classmethod
    def from_dict(cls, data_dict: dict) -> 'Series':
        return Series(**data_dict)
