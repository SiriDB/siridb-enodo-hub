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

from lib.eventmanager import ENODO_EVENT_ANOMALY_DETECTED,\
    EnodoEventManager, EnodoEvent
from enodo.protocol.package import UPDATE_SERIES, create_header
from lib.eventmanager import (ENODO_EVENT_ANOMALY_DETECTED, EnodoEvent,
                              EnodoEventManager)
from lib.series.seriesstate import SeriesState
from lib.serverstate import ServerState
from lib.siridb.siridb import (
    drop_series, insert_points, query_group_expression_by_name,
    query_series_anomalies, query_series_data,
    query_series_datapoint_count, query_series_forecasts,
    query_series_static_rules_hits)
from lib.socket import ClientManager
from lib.socketio import (SUBSCRIPTION_CHANGE_TYPE_ADD,
                          SUBSCRIPTION_CHANGE_TYPE_DELETE)


class SeriesManager:
    _labels = {}
    _labels_last_update = None
    _update_cb = None
    _srm = None  # Series Resource Manager
    _schedule = {}
    _states = {}

    @classmethod
    async def prepare(cls, update_cb=None):
        cls._update_cb = update_cb
        cls._labels_last_update = None
        cls._srm = ServerState.series_rm
        logging.info("Loading saved series states...")
        await cls._load_states()
        async for series in cls._srm.itter():
            async with cls.get_state(series.name) as state:
                ServerState.index_series_schedules(series, state)

    @classmethod
    async def _load_states(cls):
        state_file_path = os.path.join(Config.base_dir, "state.json")
        if not os.path.exists(state_file_path):
            cls._states = {}
            return
        with open(state_file_path, 'r') as f:
            _states = json.loads(f.read())
            cls._states = {
                state["name"]: SeriesState.unserialize(state)
                for state in _states.values()}

    @classmethod
    async def series_changed(cls, change_type: str, series_name: str):
        pass
        # if cls._update_cb is not None:
        #     if change_type == "delete":
        #         await cls._update_cb(change_type, series_name, series_name)
        #     else:
        #         async with SeriesManager.get_series(series_name) as series:
        #             await cls._update_cb(
        #                 change_type,
        #                 series.to_dict(),
        #                 series_name)

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
        collected_datapoints = await query_series_datapoint_count(
            ServerState.get_siridb_data_conn(), series.get('name'))
        # If collected_datapoints is None, the series does not exist.
        if collected_datapoints is not None:
            async with cls._srm.create_resource(series) as resp:
                if series.get('name') not in cls._states:
                    cls._states[series.get('name')] = SeriesState(
                        series.get('name'))
                cls._states[series.get('name')].datapoint_count = \
                    collected_datapoints
            asyncio.ensure_future(cls.series_changed(
                SUBSCRIPTION_CHANGE_TYPE_ADD, series.get('name')))
            asyncio.ensure_future(
                cls.update_listeners(await cls.get_listener_series_info()))
            return True
        return None

    @classmethod
    @contextlib.asynccontextmanager
    async def get_series(cls, series_name):
        config = await cls._srm.get_resource_by_key("name", series_name)
        state = cls._states.get(series_name)
        if config is None or state is None:
            yield None, None
            return
        async with config.lock:
            async with state.lock:
                yield config, state
                await config.store()

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
    @contextlib.asynccontextmanager
    async def get_state(cls, series_name) -> SeriesState:
        state = cls._states.get(series_name)
        if state is None:
            cls._states[series_name] = SeriesState(series_name)
            yield cls._states[series_name]
            return
        async with state.lock:
            yield state

    @classmethod
    def get_state_read_only(cls, series_name):
        return cls._states.get(series_name, SeriesState(series_name))

    @classmethod
    async def get_series_read_only(cls, series_name):
        state = cls._states.get(series_name)
        if state is None:
            state = SeriesState(series_name)
            cls._states[series_name] = state
        return await cls._srm.get_resource_by_key("name", series_name), state

    @classmethod
    async def get_config_read_only(cls, series_name):
        return await cls._srm.get_resource_by_key("name", series_name)

    # @classmethod
    # def get_all_series(cls):
    #     return cls._srm.get_resource_rid_values()

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
    async def get_ignored_series_count(cls):
        count = 0
        async for series in cls._srm.itter():
            if series.is_ignored is True:
                count += 1

        return count

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
            del cls._states[series_name]
            await cls.cleanup_series(series_name)
            asyncio.ensure_future(
                cls.series_changed(
                    SUBSCRIPTION_CHANGE_TYPE_DELETE, series_name)
            )
            return True
        return False

    @classmethod
    async def cleanup_series(cls, series_name):
        if series_name in ServerState.job_schedule_index:
            del ServerState.job_schedule_index[series_name]
        await drop_series(
            ServerState.get_siridb_output_conn(),
            f"/enodo_{re.escape(series_name)}.*?.*?$/")

    @classmethod
    async def add_to_datapoint_counter(cls, series_name, value):
        series = await cls._srm.get_resource_by_key("name", series_name)
        if series is not None:
            await series.add_to_datapoints_count(value)
        elif series_name not in cls._series:
            await cls.add_series(series_name)

    @classmethod
    async def add_forecast_to_series(cls, series_name,
                                     job_config_name, points):
        if await cls._srm.get_resource_by_key(
                "name", series_name) is not None:
            await drop_series(
                ServerState.get_siridb_output_conn(),
                f'"enodo_{series_name}_forecast_{job_config_name}"')
            await insert_points(
                ServerState.get_siridb_output_conn(),
                f'enodo_{series_name}_forecast_{job_config_name}', points)

    @classmethod
    async def add_anomalies_to_series(cls, series_name,
                                      job_config_name, points):
        series = await cls._srm.get_resource_by_key("name", series_name)
        if series is not None:
            event = EnodoEvent(
                'Anomaly detected!',
                f'{len(points)} anomalies detected for series {series_name} \
                    via job {job_config_name}',
                ENODO_EVENT_ANOMALY_DETECTED, series=series)
            await EnodoEventManager.handle_event(event)
            await drop_series(
                ServerState.get_siridb_output_conn(),
                f'"enodo_{series_name}_anomalies_{job_config_name}"')
            if len(points) > 0:
                await insert_points(
                    ServerState.get_siridb_output_conn(),
                    f'enodo_{series_name}_anomalies_{job_config_name}',
                    points)

    @classmethod
    async def add_static_rule_hits_to_series(cls, series_name,
                                             job_config_name, points):
        if await cls._srm.get_resource_by_key(
                "name", series_name) is not None:
            await drop_series(
                ServerState.get_siridb_output_conn(),
                f'"enodo_{series_name}_static_rules_{job_config_name}"')
            await insert_points(
                ServerState.get_siridb_output_conn(),
                f'enodo_{series_name}_static_rules_{job_config_name}', points)

    @classmethod
    async def get_series_forecast(cls, series_name):
        values = await query_series_data(
            ServerState.get_siridb_output_conn(), f'forecast_{series_name}')
        if values is not None:
            return values.get(f'forecast_{series_name}', None)
        return None

    @classmethod
    async def get_series_anomalies(cls, series_name):
        values = await query_series_data(
            ServerState.get_siridb_output_conn(), f'anomalies_{series_name}')
        if values is not None:
            return values.get(f'anomalies_{series_name}', None)
        return None

    @classmethod
    async def get_series_static_rules(cls, series_name):
        values = await query_series_data(
            ServerState.get_siridb_output_conn(),
            f'static_rules_{series_name}')
        if values is not None:
            return values.get(f'static_rules_{series_name}', None)
        return None

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
