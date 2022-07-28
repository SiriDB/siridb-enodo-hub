import asyncio
import time
from typing import Optional, Union

from enodo.jobs import JOB_TYPE_BASE_SERIES_ANALYSIS
from enodo.model.config.series import SeriesConfigModel, \
    SeriesJobConfigModel
from lib.series.seriesstate import SeriesState
from lib.serverstate import ServerState

from lib.state.resource import StoredResource


class Series(StoredResource):
    __slots__ = ('rid', 'name', 'config',
                 'meta', '_config_from_template')

    def __init__(self,
                 name: str,
                 config: Union[dict, str],
                 rid: Optional[str] = None,
                 meta: Optional[dict] = None,
                 **kwargs):
        self.rid = rid
        self.name = name
        self.meta = meta
        self._config_from_template = False
        self._setup_config(config)
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

    def is_ignored(self) -> bool:
        # To stop circular import
        from ..jobmanager import EnodoJobManager
        return EnodoJobManager.has_series_failed_jobs(self.name)

    def get_module(self, job_name: str) -> SeriesJobConfigModel:
        return self.config.get_config_for_job(job_name).module

    @property
    def base_analysis_job(self) -> SeriesJobConfigModel:
        job_config = self.config.get_config_for_job_type(
            JOB_TYPE_BASE_SERIES_ANALYSIS)
        if job_config is None or job_config is False:
            return False
        return job_config

    def get_job(self, job_config_name: str) -> SeriesJobConfigModel:
        return self.config.get_config_for_job(job_config_name)

    def add_job_config(self, job_config):
        self.config.add_config_for_job(job_config)

    def remove_job_config(self, job_config_name):
        removed = self.config.remove_config_for_job(
            job_config_name)
        return removed

    def schedule_job(
            self, job_config_name: str, state: SeriesState,
            initial=False):
        job_config = self.config.get_config_for_job(job_config_name)
        if job_config is None:
            return False

        job_schedule = state.get_job_schedule(job_config_name)

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
                next_value = state.datapoint_count
            elif job_schedule["value"] <= state.datapoint_count:
                next_value = \
                    state.datapoint_count + job_config.job_schedule
        if next_value is not None:
            job_schedule['value'] = next_value
            state.set_job_schedule(job_config_name, job_schedule)
        ServerState.index_series_schedules(self, state)

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
                'config': self.config if self._config_from_template is False
                else self.config.rid
            }
        return {
            'rid': self.rid,
            'name': self.name,
            'meta': self.meta,
            'config': self.config
        }

    @classmethod
    def from_dict(cls, data_dict: dict) -> 'Series':
        return Series(**data_dict)
