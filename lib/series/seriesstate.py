import asyncio
import json
import time
from typing import Any

from recordclass import dataobject
from enodo.jobs import (
    JOB_STATUS_NONE, JOB_STATUS_DONE, JOB_STATUS_FAILED,
    JOB_TYPE_BASE_SERIES_ANALYSIS, JOB_STATUS_OPEN, JOB_STATUS_PENDING)
from lib.serverstate import ServerState
from lib.siridb.siridb import query_series_datapoint_count

from lib.socket.clientmanager import ClientManager


class JobData(dataobject):
    config_name: str
    schedule_value: int = None
    schedule_type: str = None
    status: int = JOB_STATUS_NONE
    check_status: str = None
    analysis_meta: str = None

    def serialize(self):
        return {
            "config_name": self.config_name,
            "schedule_value": self.schedule_value,
            "schedule_type": self.schedule_type,
            "status": self.status,
            "check_status": self.check_status,
            "analysis_meta": self.analysis_meta
        }


class SeriesState(dataobject):
    name: str
    datapoint_count: int = None
    health: float = None
    interval: int = None
    characteristics: str = None
    job_data: list = None
    lock: asyncio.Lock = None
    last_checked_dp: int = None
    _update_dp_at: int = None

    def defaults(self):
        if self.job_data is None:
            self.job_data = []
        if self.lock is None:
            self.lock = asyncio.Lock()

    @classmethod
    def unserialize(cls, data):
        try:
            state = cls(**data)
            job_data = [JobData(**jd) for jd in data.get("job_data")]
            state.job_data = job_data
        except Exception:
            return None
        state.lock = asyncio.Lock()
        for job in state.job_data:
            if job.status in [JOB_STATUS_PENDING, JOB_STATUS_OPEN]:
                job.status = JOB_STATUS_NONE
        return state

    def serialize(self):
        return {
            "name": self.name,
            "datapoint_count": self.datapoint_count,
            "health": self.health,
            "interval": self.interval,
            "characteristics": self.characteristics,
            "job_data": [jd.serialize() for jd in self.job_data]
        }

    def _get_job(self, job_config_name, read_create=True) -> JobData:
        for job in self.job_data:
            if job.config_name == job_config_name:
                return job
        if not read_create:
            return None
        job = JobData(job_config_name)
        self.job_data.append(job)
        return job

    def get_job_status(self, job_config_name: str) -> int:
        return self._get_job(job_config_name).status

    def set_job_status(self, config_name: str, status: int):
        self._get_job(config_name).status = status

    def get_job_schedule(self, job_config_name: str) -> int:
        job = self._get_job(job_config_name, read_create=False)
        if job is None:
            return None
        if job.schedule_type is None:
            return None
        return {
            "value": job.schedule_value,
            "type": job.schedule_type
        }

    def get_all_job_schedules(self):
        return {job.config_name: {
            "value": job.schedule_value,
            "type": job.schedule_type
        } for job in self.job_data}

    def set_job_schedule(self, config_name: str, schedule: dict):
        job = self._get_job(config_name)
        job.schedule_value = schedule.get("value")
        job.schedule_type = schedule.get("type")

    def get_job_check_status(self, job_config_name: str) -> int:
        return self._get_job(job_config_name).check_status

    def set_job_check_status(self, config_name: str, status: str):
        self._get_job(config_name).check_status = status

    def get_job_meta(self, job_config_name: str) -> str:
        job = self._get_job(job_config_name)
        return json.loads(
            job.analysis_meta) if job.analysis_meta else ""

    def set_job_meta(self, config_name: str, analysis_meta: Any):
        self._get_job(config_name).analysis_meta = json.dumps(
            analysis_meta)

    def get_datapoints_count(self) -> int:
        return self.datapoint_count

    async def add_to_datapoints_count(self, add_to_count: int):
        """
        Add value to existing value of data points counter
        :param add_to_count:
        :return:
        """
        async with self.lock:
            self.last_checked_dp = int(time.time())
            self.datapoint_count += add_to_count

    async def set_datapoints_count(self, new_count: int):
        async with self.lock:
            self.last_checked_dp = int(time.time())
            self.datapoint_count = new_count

    async def update_datapoints_count(self, at: int):
        if self.datapoint_count >= at:
            return
        if self.last_checked_dp is None:
            self.last_checked_dp = int(time.time())
        if self._update_dp_at is None:
            diff = at - self.datapoint_count
            self._update_dp_at = diff * self.interval + self.last_checked_dp
        if self._update_dp_at <= int(time.time()):
            collected_datapoints = await query_series_datapoint_count(
                ServerState.get_siridb_data_conn(), self.name)
            await self.set_datapoints_count(collected_datapoints)
            if collected_datapoints < at:
                self._update_dp_at = None

    def is_job_due(self, job_config_name: str,
                   series) -> bool:
        job_status = self.get_job_status(job_config_name)
        job_schedule = self.get_job_schedule(job_config_name)
        job_config = series.config.get_config_for_job(job_config_name)

        if job_config.job_type != JOB_TYPE_BASE_SERIES_ANALYSIS:
            if self.get_job_status(
                    series.base_analysis_job.config_name) != JOB_STATUS_DONE:
                return False

        if job_status not in [JOB_STATUS_NONE, JOB_STATUS_DONE]:
            jcs = "Job failed" if job_status == JOB_STATUS_FAILED else \
                "Already active"
            self.set_job_check_status(job_config_name, jcs)
            return False
        module = ClientManager.get_module(job_config.module)
        if module is None:
            self.set_job_check_status(
                job_config_name, "Unknown module")
            return False
        r_job = job_config.requires_job
        if r_job is not None and series.config.get_config_for_job(
                r_job) is not None:
            required_job_status = self.get_job_status(r_job)
            if required_job_status is not JOB_STATUS_DONE:
                self.set_job_check_status(
                    job_config_name,
                    f"Waiting for required job {r_job}")
                return False
        if job_schedule is None or job_schedule["value"] is None:
            return True
        if job_schedule["type"] == "TS" and \
                job_schedule["value"] <= int(time.time()):
            return True
        if job_schedule["type"] == "N" and \
                job_schedule["value"] <= self.datapoint_count:
            return True
        self.set_job_check_status(
            job_config_name, "Not yet scheduled")
        return False

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

    def to_dict(self):
        state = {
            "datapoint_count": self.datapoint_count,
            "health": self.health,
            "characteristics": json.loads(
                self.characteristics) if self.characteristics else "",
            "interval": self.interval,
            "job_schedule": self.get_all_job_schedules(),
            "job_statuses": {},
            "job_check_statuses": {},
            "job_analysis_meta": {},
            'ignore': self.is_ignored(),
            'error': self.get_errors()
        }

        for job in self.job_data:
            state["job_schedule"][job.config_name] = {
                "value": job.schedule_value,
                "type": job.schedule_type
            }
            state["job_statuses"][job.config_name] = job.status
            state["job_check_statuses"][
                job.config_name] = job.check_status
            state["job_analysis_meta"][job.config_name] = json.loads(
                job.analysis_meta) if job.analysis_meta else ""

        return state
