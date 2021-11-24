import datetime
import json
import logging
import os
import re

import qpack

from lib.config import Config
from lib.events import EnodoEvent
from lib.events.enodoeventmanager import ENODO_EVENT_ANOMALY_DETECTED,\
    EnodoEventManager
from lib.serverstate import ServerState
from lib.siridb.siridb import query_series_datapoint_count,\
    drop_series, insert_points, query_series_data, does_series_exist,\
    query_group_expression_by_name
from lib.socket import ClientManager
from lib.socket.package import create_header, UPDATE_SERIES
from lib.socketio import SUBSCRIPTION_CHANGE_TYPE_ADD,\
    SUBSCRIPTION_CHANGE_TYPE_DELETE
from lib.util import load_disk_data, save_disk_data

from enodo.jobs import JOB_STATUS_DONE, JOB_TYPE_BASE_SERIES_ANALYSIS,\
    JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES, JOB_TYPE_FORECAST_SERIES


class SeriesManager:
    _series = None
    _labels = None
    _labels_last_update = None
    _update_cb = None

    @classmethod
    async def prepare(cls, update_cb=None):
        cls._series = {}
        cls._update_cb = update_cb
        cls._labels = {}
        cls._labels_last_update = None

    @classmethod
    async def series_changed(cls, change_type, series_name):
        if cls._update_cb is not None:
            if change_type == "delete":
                await cls._update_cb(change_type, series_name, series_name)
            else:
                await cls._update_cb(change_type, (await cls.get_series(series_name)).to_dict(), series_name)

    @classmethod
    async def add_series(cls, series):
        if series.get('name') not in cls._series:
            if await does_series_exist(ServerState.get_siridb_data_conn(), series.get('name')):
                collected_datapoints = await query_series_datapoint_count(ServerState.get_siridb_data_conn(), series.get('name'))
                if collected_datapoints:
                    series['datapoint_count'] = collected_datapoints
                    cls._series[series.get('name')] = Series.from_dict(series)
                    logging.info(f"Added new series: {series.get('name')}")
                    await cls.series_changed(SUBSCRIPTION_CHANGE_TYPE_ADD, series.get('name'))
                    await cls.update_listeners(cls.get_listener_series_info())
                    return True
        return False

    @classmethod
    async def get_series(cls, series_name):
        series = None
        if series_name in cls._series:
            series = cls._series.get(series_name)
        return series

    @classmethod
    def get_all_series(cls):
        return list(cls._series.keys())

    @classmethod
    def get_listener_series_info(cls):
        series = [{"name": series_name, "realtime": series.series_config.realtime} for series_name, series in cls._series.items()]
        labels = [{"name": label.get('selector'), "realtime": label.get('series_config').get('realtime'), "isGroup": label.get('type') == "group"} for label in cls._labels.values()]
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
            # TODO: Change auto type == "group" to a input value when tags are added
            group_expression = await query_group_expression_by_name(ServerState.get_siridb_data_conn(), name)
            cls._labels[name] = {"description": description, "name": name, "series_config": series_config, "type": "group", "selector": group_expression}
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
        return len([cls._series[rid] for rid in cls._series if cls._series[rid].is_ignored is True])

    @classmethod
    async def get_series_to_dict(cls, regex_filter=None):
        if regex_filter is not None:
            pattern = re.compile(regex_filter)
            return [series.to_dict() for series in cls._series.values() if pattern.match(series.name)]
        return [series.to_dict() for series in cls._series.values()]

    @classmethod
    async def remove_series(cls, series_name):
        if series_name in cls._series:            
            await cls.series_changed(SUBSCRIPTION_CHANGE_TYPE_DELETE, series_name)
            del cls._series[series_name]
            return True
        return False

    @classmethod
    async def add_to_datapoint_counter(cls, series_name, value):
        series = cls._series.get(series_name, None)
        if series is not None:
            await series.add_to_datapoints_count(value)
        elif series_name not in cls._series and series_name in Config.names_enabled_series_for_analysis:
            await cls.add_series(series_name)

    @classmethod
    async def add_forecast_to_series(cls, series_name, points):
        series = cls._series.get(series_name, None)
        if series is not None:
            await drop_series(ServerState.get_siridb_forecast_conn(), f'forecast_{series_name}')
            await insert_points(ServerState.get_siridb_forecast_conn(), f'forecast_{series_name}', points)

            # date_1 = datetime.datetime.now()
            # # end_date = date_1 + datetime.timedelta(days=1)
            # end_date = date_1 + datetime.timedelta(seconds=Config.interval_schedules_series)
            # await series.schedule_forecast(end_date)

    @classmethod
    async def add_anomalies_to_series(cls, series_name, points):
        series = cls._series.get(series_name, None)
        if series is not None:
            event = EnodoEvent('Anomaly detected!', f'{len(points)} anomalies detected for series {series_name}',
                               ENODO_EVENT_ANOMALY_DETECTED, series=series)
            await EnodoEventManager.handle_event(event)
            await drop_series(ServerState.get_siridb_forecast_conn(), f'anomalies_{series_name}')
            await insert_points(ServerState.get_siridb_forecast_conn(), f'anomalies_{series_name}', points)

    @classmethod
    async def get_series_forecast(cls, series_name):
        values = await query_series_data(ServerState.get_siridb_forecast_conn(), f'forecast_{series_name}')
        if values is not None:
            return values.get(f'forecast_{series_name}', None)
        return None

    @classmethod
    async def get_series_anomalies(cls, series_name):
        values = await query_series_data(ServerState.get_siridb_forecast_conn(), f'anomalies_{series_name}')
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
    async def read_from_disk(cls):
        if not os.path.exists(Config.series_save_path):
            pass
        else:
            data = load_disk_data(Config.series_save_path)
            series_data = data.get('series')
            if series_data is not None:
                for s in series_data:
                    cls._series[s.get('name')] = Series.from_dict(s)
            label_data = data.get('labels')
            if label_data is not None:
                for l in label_data:
                    cls._labels[l.get('grouptag')] = l

    @classmethod
    async def save_to_disk(cls):
        try:
            serialized_series = []
            for series in cls._series.values():
                serialized_series.append(series.to_dict(static_only=True))
            serialized_labels = list(cls._labels.values())

            serialized_data = {
                "series": serialized_series,
                "labels": serialized_labels
            }
            save_disk_data(Config.series_save_path, serialized_data)
        except Exception as e:
            logging.error(f"Something went wrong when writing seriesmanager data to disk")
            logging.debug(f"Corresponding error: {e}")

from lib.series.series import Series