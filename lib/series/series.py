import time

from enodo.jobs import JOB_TYPE_BASE_SERIES_ANALYSIS, JOB_STATUS_NONE, JOB_STATUS_OPEN, JOB_STATUS_PENDING, JOB_STATUS_DONE, JOB_STATUS_FAILED
from enodo.model.config.series import SeriesConfigModel, SeriesState


class Series:
    # detecting_anomalies_status forecast_status series_analysed_status
    __slots__ = (
        'rid', 'name', 'series_config', 'state', '_datapoint_count_lock', 'series_characteristics')

    def __init__(self, name, config, state=None, series_characteristics=None, **kwargs):
        self.rid = name
        self.name = name
        self.series_config = SeriesConfigModel.from_dict(config)
        if state is None:
            state = {}
        self.state = SeriesState.from_dict(state)
        self.series_characteristics = series_characteristics

        self._datapoint_count_lock = False
        
    async def set_datapoints_counter_lock(self, is_locked):
        """
        Set lock so it can or can not be changed
        :param is_locked:
        :return:
        """
        self._datapoint_count_lock = is_locked

    async def get_datapoints_counter_lock(self):
        return self._datapoint_count_lock

    def get_errors(self):
        errors = [job.error for job in EnodoJobManager.get_failed_jobs_for_series(self.name)]
        return errors

    def is_ignored(self):
        return EnodoJobManager.has_series_failed_jobs(self.name)

    async def get_model(self, job_name):
        return self.series_config.get_config_for_job(job_name).model

    async def get_job_status(self, job_link_name):
        return self.state.get_job_status(job_link_name)

    async def set_job_status(self, link_name, status):
        self.state.set_job_status(link_name, status)

    @property
    def base_analysis_job(self):
        job_config = self.series_config.get_config_for_job_type(JOB_TYPE_BASE_SERIES_ANALYSIS)
        if job_config is None or job_config is False:
            return False
        return job_config

    def get_job(self, job_link_name):
        return self.series_config.get_config_for_job(job_link_name)

    def base_analysis_status(self):
        job_config = self.series_config.get_config_for_job_type(JOB_TYPE_BASE_SERIES_ANALYSIS)
        if job_config is None or job_config is False:
            return False
        return self.state.get_job_status(job_config.link_name)

    def get_datapoints_count(self):
        return self.state.datapoint_count

    async def add_to_datapoints_count(self, add_to_count):
        """
        Add value to existing value of data points counter
        :param add_to_count:
        :return:
        """
        if self._datapoint_count_lock is False:
            self.state.datapoint_count += add_to_count

    async def schedule_job(self, job_link_name, initial=False):
        job_config = self.series_config.get_config_for_job(job_link_name)  
        if job_config is None:                  
            return False

        job_schedule = self.state.get_job_schedule(job_link_name)
            
        if job_schedule is None:
            job_schedule = {"value": 0, "type": job_config.job_schedule_type}

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
                next_value = self.state.datapoint_count + job_config.job_schedule

        if next_value is not None:
            job_schedule['value'] = next_value
            self.state.set_job_schedule(job_link_name, job_schedule)
            
    async def is_job_due(self, job_link_name):
        job_status = self.state.get_job_status(job_link_name)
        job_schedule = self.state.get_job_schedule(job_link_name)
        
        if job_status == JOB_STATUS_NONE or job_status == JOB_STATUS_DONE:
            job_config = self.series_config.get_config_for_job(job_link_name)
            if job_config.requires_job is not None:
                required_job_status = self.state.get_job_status(job_config.requires_job)
                if required_job_status is not JOB_STATUS_DONE:
                    return False
            if job_schedule["value"] is None:
                return True
            elif job_schedule["type"] == "TS" and job_schedule["value"] <= int(time.time()):
                return True
            elif job_schedule["type"] == "N" and job_schedule["value"] <= self.state.datapoint_count:
                return True
        return False

    def update(self, data):
        config = data.get('config')
        if config is not None:
            self.series_config = SeriesConfigModel.from_dict(config)

        return True

    def to_dict(self, static_only=False):
        if static_only:
            return {
                'name': self.name,
                'datapoint_count': self.state.datapoint_count,
                'job_statuses': self.state.job_schedule.to_dict(),
                'job_schedule': self.state.job_schedule.to_dict(),
                'config': self.series_config.to_dict(),
                'series_characteristics': self.series_characteristics,
                'health': self.state.health
            }
        return {
            'rid': self.rid,
            'name': self.name,
            'datapoint_count': self.state.datapoint_count,
            'job_statuses': self.state.job_statuses.to_dict(),
            'job_schedule': self.state.job_schedule.to_dict(),
            'config': self.series_config.to_dict(),
            'ignore': self.is_ignored(),
            'error': self.get_errors(),
            'series_characteristics': self.series_characteristics,
            'health': self.state.health
        }

    @classmethod
    def from_dict(cls, data_dict):
        return Series(**data_dict)


from ..enodojobmanager import EnodoJobManager
