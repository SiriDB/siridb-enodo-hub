import asyncio
from typing import Optional, Union

from enodo.model.config.series import SeriesConfigModel, \
    SeriesJobConfigModel
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
        self.meta = meta  # TODO: save to thingsdb
        self._config_from_template = False
        self._setup_config(config)
        self.lock = asyncio.Lock()

    def _setup_config(self, config):
        if isinstance(config, dict):
            raise Exception("Invalid config")
        self._config_from_template = True
        config = ServerState.series_config_rm.get_cached_resource(
            int(config))
        if config is None:
            raise Exception("Invalid series config rid")
        config = SeriesConfigModel(**config.series_config)
        self.config = config

    def is_ignored(self) -> bool:
        # To stop circular import
        from ..jobmanager import EnodoJobManager
        return EnodoJobManager.has_series_failed_jobs(self.name)

    def get_module(self, job_name: str) -> SeriesJobConfigModel:
        return self.config.get_config_for_job(job_name).module

    def get_job(self, job_config_name: str) -> SeriesJobConfigModel:
        return self.config.get_config_for_job(job_config_name)

    def add_job_config(self, job_config):
        self.config.add_config_for_job(job_config)

    def remove_job_config(self, job_config_name):
        removed = self.config.remove_config_for_job(
            job_config_name)
        return removed

    def schedule_jobs(self, state, delay=0):
        job_schedules = state.get_all_job_schedules()
        for job_config_name in self.config.job_config:
            self.schedule_job(job_config_name, state, initial=not (
                job_config_name in job_schedules), delay=delay)
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
