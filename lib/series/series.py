import datetime

from enodo.jobs import JOB_TYPE_BASE_SERIES_ANALYSIS, JOB_STATUS_NONE, JOB_STATUS_OPEN, JOB_STATUS_PENDING, JOB_STATUS_DONE, JOB_STATUS_FAILED
from enodo.model.config.series import SeriesConfigModel


class Series:
    # detecting_anomalies_status forecast_status series_analysed_status
    __slots__ = (
        'name', 'series_config', 'series_job_statuses', '_job_schedule', '_datapoint_count', '_datapoint_count_lock', 'series_characteristics')

    def __init__(self, name, config, datapoint_count, job_schedule=None, job_statuses=None, series_characteristics=None):
        self.name = name
        self.series_config = SeriesConfigModel.from_dict(config)
        self.series_job_statuses = job_statuses
        if self.series_job_statuses is None:
            self.series_job_statuses = dict()
        self.series_characteristics = series_characteristics

        self._job_schedule = job_schedule
        if self._job_schedule is None:
            self._job_schedule = {}

        self._datapoint_count = datapoint_count
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

    async def clear_errors(self):
        await EnodoJobManager.remove_failed_jobs_for_series(self.name)

    async def get_errors(self):
        errors = [job.error for job in (await EnodoJobManager.get_failed_jobs_for_series(self.name))]
        return errors

    async def is_ignored(self):
        return await EnodoJobManager.has_series_failed_jobs(self.name)

    async def get_model(self, job_type):
        return self.series_config.get_model_for_job(job_type)

    async def get_job_status(self, job_type):
        status = self.series_job_statuses.get(job_type)
        if status is None:
            status = JOB_STATUS_NONE
        return status

    async def set_job_status(self, job_type, status):
        self.series_job_statuses[job_type] = status

    async def get_datapoints_count(self):
        return self._datapoint_count

    async def add_to_datapoints_count(self, add_to_count):
        """
        Add value to existing value of data points counter
        :param add_to_count:
        :return:
        """
        if self._datapoint_count_lock is False:
            self._datapoint_count += add_to_count

    async def schedule_job(self, job_type):
        if job_type in self.series_config.job_models:
            if job_type in self._job_schedule and job_type in self.series_config.job_schedule:
                if self._job_schedule[job_type] <= self._datapoint_count:
                    self._job_schedule[job_type] = self._datapoint_count + self.series_config.job_schedule[job_type]

    async def is_job_due(self, job_type):
        if job_type not in self.series_config.job_models:
            return False
        
        if self.series_job_statuses.get(job_type) != JOB_STATUS_DONE:
            return True
        elif self._job_schedule.get(job_type) is not None and self._job_schedule.get(job_type) <= self._datapoint_count:
            return True


    async def to_dict(self, static_only=False):
        if static_only:
            return {
                'name': self.name,
                'datapoint_count': self._datapoint_count,
                'job_statuses': self.series_job_statuses,
                'job_schedule': self._job_schedule,
                'config': self.series_config.to_dict(),
                'series_characteristics': self.series_characteristics
            }
        return {
            'name': self.name,
            'datapoint_count': self._datapoint_count,
            'job_statuses': self.series_job_statuses,
            'job_schedule': self._job_schedule,
            'config': self.series_config.to_dict(),
            'ignore': await self.is_ignored(),
            'error': await self.get_errors(),
            'series_characteristics': self.series_characteristics
        }

    @classmethod
    async def from_dict(cls, data_dict):
        return Series(**data_dict)


from ..enodojobmanager import EnodoJobManager
