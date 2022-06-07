import asyncio
import logging
import re

import qpack
from enodo.protocol.package import create_header, UPDATE_SERIES

from lib.eventmanager import ENODO_EVENT_ANOMALY_DETECTED,\
    EnodoEventManager, EnodoEvent
from enodo.protocol.package import UPDATE_SERIES, create_header
from lib.eventmanager import (ENODO_EVENT_ANOMALY_DETECTED, EnodoEvent,
                              EnodoEventManager)
from lib.series.series import Series
from lib.serverstate import ServerState
from lib.siridb.siridb import (
    drop_series, insert_points, query_group_expression_by_name,
    query_series_data, query_series_datapoint_count)
from lib.socket import ClientManager
from lib.socketio import (SUBSCRIPTION_CHANGE_TYPE_ADD,
                          SUBSCRIPTION_CHANGE_TYPE_DELETE)


class SeriesManager:
    _series = {}
    _labels = {}
    _labels_last_update = None
    _update_cb = None

    @classmethod
    def prepare(cls, update_cb=None):
        cls._update_cb = update_cb
        cls._labels_last_update = None

    @classmethod
    async def series_changed(cls, change_type: str, series_name: str):
        if cls._update_cb is not None:
            if change_type == "delete":
                await cls._update_cb(change_type, series_name, series_name)
            else:
                await cls._update_cb(
                    change_type,
                    cls.get_series(series_name).to_dict(),
                    series_name)

    @classmethod
    async def add_series(cls, series: dict):
        resp = await cls._add_series(series)
        if resp:
            logging.info(f"Added new series: {series.get('name')}")
            return True
        return resp

    @classmethod
    async def _add_series(cls, series: dict):
        if series.get('name') in cls._series:
            return False
        collected_datapoints = await query_series_datapoint_count(
            ServerState.get_siridb_data_conn(), series.get('name'))
        # If collected_datapoints is None, the series does not exist.
        if collected_datapoints is not None:
            cls._series[series.get(
                'name')] = Series.from_dict(series)
            cls._series[series.get(
                'name')].state.datapoint_count = collected_datapoints
            asyncio.ensure_future(cls.series_changed(
                SUBSCRIPTION_CHANGE_TYPE_ADD, series.get('name')))
            asyncio.ensure_future(
                cls.update_listeners(cls.get_listener_series_info()))
            return True
        return None

    @classmethod
    def get_series(cls, series_name):
        series = None
        if series_name in cls._series:
            series = cls._series.get(series_name)
        return series

    @classmethod
    def get_all_series(cls):
        return list(cls._series.keys())

    @classmethod
    def get_listener_series_info(cls):
        series = [{"name": series_name,
                   "realtime": series.config.realtime}
                  for series_name, series in cls._series.items()]
        labels = [
            {"name": label.get('selector'),
             "realtime": label.get('series_config').get('realtime'),
             "isGroup": label.get('type') == "group"}
            for label in cls._labels.values()]
        return series + labels

    @classmethod
    def _update_listeners(cls):
        ClientManager.update_listeners(cls.get_listener_series_info())

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
            cls._update_listeners()

    @classmethod
    def remove_label(cls, name):
        if name in cls._labels:
            del cls._labels[name]
            cls._update_listeners()
            return True
        return False

    @classmethod
    def get_series_count(cls):
        return len(list(cls._series.keys()))

    @classmethod
    def get_ignored_series_count(cls):
        return len([cls._series[rid] for rid in cls._series
                    if cls._series[rid].is_ignored is True])

    @classmethod
    def get_series_to_dict(cls, regex_filter=None):
        if regex_filter is not None:
            pattern = re.compile(regex_filter)
            return [
                series.to_dict() for series in cls._series.values() if
                pattern.match(series.name)]
        return [series.to_dict() for series in cls._series.values()]

    @classmethod
    async def remove_series(cls, series_name):
        if series_name in cls._series:
            await cls.cleanup_series(series_name)
            cls._series[series_name].delete()
            asyncio.ensure_future(
                cls.series_changed(
                    SUBSCRIPTION_CHANGE_TYPE_DELETE, series_name)
            )
            del cls._series[series_name]
            return True
        return False

    @classmethod
    async def cleanup_series(cls, series_name):
        await drop_series(
            ServerState.get_siridb_output_conn(),
            f"/enodo_{re.escape(series_name)}.*?.*?$/")

    @classmethod
    async def add_to_datapoint_counter(cls, series_name, value):
        series = cls._series.get(series_name, None)
        if series is not None:
            await series.add_to_datapoints_count(value)
        elif series_name not in cls._series:
            await cls.add_series(series_name)

    @classmethod
    async def add_forecast_to_series(cls, series_name,
                                     job_config_name, points):
        series = cls._series.get(series_name, None)
        if series is not None:
            await drop_series(
                ServerState.get_siridb_output_conn(),
                f'"enodo_{series_name}_forecast_{job_config_name}"')
            await insert_points(
                ServerState.get_siridb_output_conn(),
                f'enodo_{series_name}_forecast_{job_config_name}', points)

    @classmethod
    async def add_anomalies_to_series(cls, series_name,
                                      job_config_name, points):
        series = cls._series.get(series_name, None)
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
        series = cls._series.get(series_name, None)
        if series is not None:
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
    async def update_listeners(cls, series):
        for listener in ClientManager.listeners.values():
            update = qpack.packb(series)
            series_update = create_header(len(update), UPDATE_SERIES, 0)
            listener.writer.write(series_update + update)

    @classmethod
    async def load_from_disk(cls):
        series_data = await ServerState.storage.load_by_type("series")
        for s in series_data:
            try:
                await cls._add_series(s)
            except Exception as e:
                logging.warning(
                    "Tried loading invalid data when "
                    "loading series")
                logging.debug(
                    f"Corresponding error: {e}, "
                    f'exception class: {e.__class__.__name__}')
        # TODO: Labels
        # label_data = data.get('labels')
        # if label_data is not None:
        #     for la in label_data:
        #         cls._labels[la.get('grouptag')] = la
