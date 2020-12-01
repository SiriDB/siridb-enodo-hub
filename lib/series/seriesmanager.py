import datetime
import json
import logging
import os
import re

import qpack

from lib.config.config import Config
from lib.events import EnodoEvent
from lib.events.enodoeventmanager import ENODO_EVENT_ANOMALY_DETECTED, EnodoEventManager
from lib.serverstate import ServerState
from lib.siridb.siridb import query_series_datapoint_count, drop_series, \
    insert_points, query_series_data, does_series_exist
from lib.socket import ClientManager
from lib.socket.package import create_header, UPDATE_SERIES
from lib.socketio import SUBSCRIPTION_CHANGE_TYPE_ADD, SUBSCRIPTION_CHANGE_TYPE_DELETE
from lib.util import safe_json_dumps

from enodo.jobs import JOB_STATUS_DONE, JOB_TYPE_BASE_SERIES_ANALYSIS, JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES, JOB_TYPE_FORECAST_SERIES


class SeriesManager:
    _series = None
    _update_cb = None

    @classmethod
    async def prepare(cls, update_cb=None):
        cls._series = {}
        cls._update_cb = update_cb

    @classmethod
    async def series_changed(cls, change_type, series_name):
        if cls._update_cb is not None:
            if change_type == "remove":
                await cls._update_cb(change_type, series_name, series_name)
            else:
                await cls._update_cb(change_type, await cls.get_series(series_name), series_name)

    @classmethod
    async def add_series(cls, series):
        if series.get('name') not in cls._series:
            if await does_series_exist(ServerState.siridb_data_client, series.get('name')):
                collected_datapoints = await query_series_datapoint_count(ServerState.siridb_data_client, series.get('name'))
                if collected_datapoints:
                    series['datapoint_count'] = collected_datapoints
                    cls._series[series.get('name')] = await Series.from_dict(series)
                    logging.info(f"Added new series: {series.get('name')}")
                    await cls.series_changed(SUBSCRIPTION_CHANGE_TYPE_ADD, series.get('name'))
                    await cls.update_listeners(await cls.get_all_series())
                    return True
        return False

    @classmethod
    async def get_series(cls, series_name):
        series = None
        if series_name in cls._series:
            series = cls._series.get(series_name)
        return series

    @classmethod
    async def get_all_series(cls):
        return list(cls._series.keys())

    @classmethod
    async def get_series_to_dict(cls, regex_filter=None):
        if regex_filter is not None:
            pattern = re.compile(regex_filter)
            return [await series.to_dict() for series in cls._series.values() if pattern.match(series.name)]
        return [await series.to_dict() for series in cls._series.values()]

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
            await drop_series(ServerState.siridb_forecast_client, f'forecast_{series_name}')
            await insert_points(ServerState.siridb_forecast_client, f'forecast_{series_name}', points)
            await series.set_job_status(JOB_TYPE_FORECAST_SERIES, JOB_STATUS_DONE)

            # date_1 = datetime.datetime.now()
            # # end_date = date_1 + datetime.timedelta(days=1)
            # end_date = date_1 + datetime.timedelta(seconds=Config.interval_schedules_series)
            # await series.schedule_forecast(end_date)

    @classmethod
    async def add_anomalies_to_series(cls, series_name, points):
        series = cls._series.get(series_name, None)
        if series is not None:
            event = EnodoEvent('Anomaly detected!', f'{len(points)} anomalies detected for series {series_name}',
                               ENODO_EVENT_ANOMALY_DETECTED)
            await EnodoEventManager.handle_event(event)
            await drop_series(ServerState.siridb_forecast_client, f'anomalies_{series_name}')
            await insert_points(ServerState.siridb_forecast_client, f'anomalies_{series_name}', points)
            await series.set_job_status(JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES, JOB_STATUS_DONE)

    @classmethod
    async def get_series_forecast(cls, series_name):
        values = await query_series_data(ServerState.siridb_forecast_client, f'forecast_{series_name}')
        if values is not None:
            return values.get(f'forecast_{series_name}', None)
        return None

    @classmethod
    async def get_series_anomalies(cls, series_name):
        values = await query_series_data(ServerState.siridb_forecast_client, f'anomalies_{series_name}')
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
            f = open(Config.series_save_path, "r")
            data = f.read()
            f.close()
            series_data = json.loads(data)
            for s in series_data:
                cls._series[s.get('name')] = await Series.from_dict(s)

    @classmethod
    async def save_to_disk(cls):
        try:
            serialized_series = []
            for series in cls._series.values():
                serialized_series.append(await series.to_dict(static_only=True))
            f = open(Config.series_save_path, "w")
            f.write(json.dumps(serialized_series, default=safe_json_dumps))
            f.close()
        except Exception as e:
            logging.error(f"Something went wrong when writing seriesmanager data to disk")
            logging.debug(f"Corresponding error: {e}")

from lib.series.series import Series