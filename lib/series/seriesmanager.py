import asyncio
import contextlib
import json
import logging
import os
import re

import qpack
from enodo.protocol.package import create_header, UPDATE_SERIES
from enodo.jobs import (JOB_TYPE_FORECAST_SERIES,
                        JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES,
                        JOB_TYPE_STATIC_RULES)
from lib.config import Config

from lib.serverstate import ServerState
from lib.siridb.siridb import (
    drop_series, query_group_expression_by_name,
    query_series_anomalies, query_series_data, query_series_forecasts,
    query_series_static_rules_hits)
from lib.socket import ClientManager
from lib.util.util import generate_worker_lookup


class SeriesManager:
    _labels = {}
    _labels_last_update = None
    _update_cb = None
    _srm = None  # Series Resource Manager
    _schedule = {}
    _states = {}
    _worker_lookup = None

    @classmethod
    def prepare(cls, update_cb=None):
        cls._update_cb = update_cb
        cls._labels_last_update = None
        cls._srm = ServerState.series_rm
        cls.update_worker_lookup(0)
        logging.info("Loading saved series...")

    @classmethod
    def update_worker_lookup(cls, count):
        cls._worker_lookup = generate_worker_lookup(count)

    @classmethod
    def get_active_series(cls) -> list:
        return cls._srm.get_resource_rid_values()

    @classmethod
    async def add_series(cls, series: dict):
        resp = await cls._add_series(series)
        if resp:
            logging.info(f"Added new series: {series.get('name')}")
            return True
        return resp

    @classmethod
    async def _add_series(cls, series: dict):
        if await cls._srm.get_resource_by_key("name",
                                              series.get('name')) is not None:
            return False
        async with cls._srm.create_resource(series) as resp:
            pass
        asyncio.ensure_future(
            cls.update_listeners(await cls.get_listener_series_info()))
        return True

    @classmethod
    @contextlib.asynccontextmanager
    async def get_config(cls, series_name):
        series = await cls._srm.get_resource_by_key("name", series_name)
        if series is None:
            yield None
            return
        async with series.lock:
            yield series
            await series.store()

    @classmethod
    async def get_listener_series_info(cls):
        resp = []
        async for series in cls._srm.itter():
            if series is not None:
                resp.append({"name": series.rid,
                             "realtime": series.config.realtime})
        return resp

    @classmethod
    async def _update_listeners(cls):
        ClientManager.update_listeners(await cls.get_listener_series_info())

    @classmethod
    def get_labels_data(cls):
        return {
            "last_update": cls._labels_last_update,
            "labels": list(cls._labels.values())
        }

    @classmethod
    async def add_label(cls, description, name, series_config):
        if name not in cls._labels:
            # TODO: Change auto type == "group" to a input value
            # when tags are added
            group_expression = await query_group_expression_by_name(
                ServerState.get_siridb_data_conn(), name)
            cls._labels[name] = {
                "description": description, "name": name,
                "series_config": series_config, "type": "group",
                "selector": group_expression}
            await cls._update_listeners()

    @classmethod
    async def remove_label(cls, name):
        if name in cls._labels:
            del cls._labels[name]
            await cls._update_listeners()
            return True
        return False

    @classmethod
    def get_series_count(cls):
        return cls._srm.get_resource_count()

    @classmethod
    def get_all_series_names(cls, regex_filter=None):
        series_names = cls._srm.get_resource_rid_values()
        if regex_filter is not None:
            pattern = re.compile(regex_filter)
            return [
                series_name for series_name in series_names if
                pattern.match(series_name)]
        return series_names

    @classmethod
    async def remove_series(cls, series_name):
        # TODO: check if fetch is necesarry
        series = await cls._srm.get_resource_by_key("name", series_name)
        if series is not None:
            await cls._srm.delete_resource(series)
            asyncio.ensure_future(
                cls.update_listeners(await cls.get_listener_series_info()))
            await cls.cleanup_series(series_name)
            return True
        return False

    @classmethod
    async def cleanup_series(cls, series_name):
        name_escaped = re.escape(series_name).replace('/', r'\/')
        await drop_series(
            ServerState.get_siridb_output_conn(),
            f"/enodo_{name_escaped}.*?.*?$/")

    @classmethod
    async def get_series_output_by_job_type(cls, series_name, job_type,
                                            forecast_future_only=False):
        if job_type == JOB_TYPE_FORECAST_SERIES:
            return await query_series_forecasts(
                ServerState.get_siridb_output_conn(),
                series_name, only_future=forecast_future_only)
        elif job_type == JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES:
            return await query_series_anomalies(
                ServerState.get_siridb_output_conn(), series_name)
        elif job_type == JOB_TYPE_STATIC_RULES:
            return await query_series_static_rules_hits(
                ServerState.get_siridb_output_conn(), series_name)

        return {}

    @classmethod
    async def update_listeners(cls, series):
        for listener in ClientManager.listeners.values():
            update = qpack.packb(series)
            series_update = create_header(len(update), UPDATE_SERIES, 0)
            listener.writer.write(series_update + update)

    @classmethod
    async def close(cls):
        _states = {state.name: state.serialize()
                   for state in cls._states.values()}
        try:
            if not os.path.exists(Config.base_dir):
                raise Exception("Path does not exist")
            state_file_path = os.path.join(
                Config.base_dir, "state.json")
            with open(state_file_path, 'w') as f:
                f.write(json.dumps(_states))
        except Exception as e:
            logging.error("Cannot save state to disk...")
            logging.debug(f"Corresponding error: {e}")
