import datetime
import json
import os
import qpack
import re

from lib.config.config import Config
from lib.events import EnodoEvent
from lib.events.enodoeventmanager import ENODO_EVENT_ANOMALY_DETECTED, EnodoEventManager
from lib.serie.series import Series, DETECT_ANOMALIES_STATUS_DONE
from lib.siridb.siridb import SiriDB
from lib.socket.clientmanager import ClientManager
from lib.socket.package import create_header, UPDATE_SERIES
from lib.socketio import SUBSCRIPTION_CHANGE_TYPE_ADD, SUBSCRIPTION_CHANGE_TYPE_DELETE
from lib.util.util import safe_json_dumps


class SerieManager:
    _series = None
    _siridb_data_client = None
    _siridb_forecast_client = None
    _update_cb = None

    @classmethod
    async def prepare(cls, update_cb=None):
        cls._series = {}
        cls._update_cb = update_cb
        cls._siridb_data_client = SiriDB(username=Config.siridb_user,
                                         password=Config.siridb_password,
                                         dbname=Config.siridb_database,
                                         hostlist=[(Config.siridb_host, Config.siridb_port)])
        cls._siridb_forecast_client = SiriDB(username=Config.siridb_forecast_user,
                                             password=Config.siridb_forecast_password,
                                             dbname=Config.siridb_forecast_database,
                                             hostlist=[(Config.siridb_forecast_host, Config.siridb_forecast_port)])

    @classmethod
    async def series_changed(cls, change_type, series_name):
        if cls._update_cb is not None:
            series = await cls.get_serie(series_name)
            if series is not None:
                await cls._update_cb(change_type, series_name, await series.to_dict())

    @classmethod
    async def add_serie(cls, serie):
        if serie.get('name') not in cls._series:
            collected_datapoints = await cls._siridb_data_client.query_serie_datapoint_count(serie.get('name'))
            if collected_datapoints:
                serie['datapoint_count'] = collected_datapoints
                cls._series[serie.get('name')] = await Series.from_dict(serie)
                print(f"Added new serie: {serie.get('name')}")
                await cls.series_changed(SUBSCRIPTION_CHANGE_TYPE_ADD, serie.get('name'))
                await cls.update_listeners(await cls.get_series())

    @classmethod
    async def get_serie(cls, serie_name):
        serie = None
        if serie_name in cls._series:
            serie = cls._series.get(serie_name, None)

        return serie

    @classmethod
    async def get_series(cls):
        return list(cls._series.keys())

    @classmethod
    async def get_series_to_dict(cls, regex_filter=None):
        if regex_filter is not None:
            pattern = re.compile(regex_filter)
            return [await serie.to_dict() for serie in cls._series.values() if pattern.match(await serie.get_name())]
        return [await serie.to_dict() for serie in cls._series.values()]

    @classmethod
    async def remove_serie(cls, serie_name):
        if serie_name in cls._series:
            del cls._series[serie_name]
            await cls.series_changed(SUBSCRIPTION_CHANGE_TYPE_DELETE, serie_name)
            return True
        return False

    @classmethod
    async def add_to_datapoint_counter(cls, serie_name, value):
        serie = cls._series.get(serie_name, None)
        if serie is not None:
            await serie.add_to_datapoints_count(value)
        elif serie_name not in cls._series and serie_name in Config.names_enabled_series_for_analysis:
            await cls.add_serie(serie_name)

    @classmethod
    async def add_forecast_to_serie(cls, serie_name, points):
        serie = cls._series.get(serie_name, None)
        if serie is not None:
            await cls._siridb_forecast_client.drop_serie(f'forecast_{serie_name}')
            await cls._siridb_forecast_client.insert_points(f'forecast_{serie_name}', points)
            await serie.set_pending_forecast(False)

            date_1 = datetime.datetime.now()
            # end_date = date_1 + datetime.timedelta(days=1)
            end_date = date_1 + datetime.timedelta(seconds=Config.interval_schedules_series)
            await serie.schedule_forecast(end_date)

    @classmethod
    async def add_anomalies_to_serie(cls, serie_name, points):
        serie = cls._series.get(serie_name, None)
        if serie is not None:
            print(points)
            event = EnodoEvent('Anomaly detected!', f'{len(points)} anomalies detected for series {serie_name}',
                               ENODO_EVENT_ANOMALY_DETECTED)
            await EnodoEventManager.handle_event(event)
            await cls._siridb_forecast_client.drop_serie(f'anomalies_{serie_name}')
            await cls._siridb_forecast_client.insert_points(f'anomalies_{serie_name}', points)
            await serie.set_detect_anomalies_status(DETECT_ANOMALIES_STATUS_DONE)

    @classmethod
    async def get_serie_forecast(cls, serie_name):
        values = await cls._siridb_forecast_client.query_serie_data(f'forecast_{serie_name}')
        if values is not None:
            return values.get(f'forecast_{serie_name}', None)
        return None

    @classmethod
    async def get_serie_anomalies(cls, serie_name):
        values = await cls._siridb_forecast_client.query_serie_data(f'anomalies_{serie_name}')
        if values is not None:
            return values.get(f'anomalies_{serie_name}', None)
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
            for serie in cls._series.values():
                serialized_series.append(await serie.to_dict())

            if not os.path.exists(Config.series_save_path):
                raise Exception()
            f = open(Config.series_save_path, "w")
            f.write(json.dumps(serialized_series, default=safe_json_dumps))
            f.close()
        except Exception as e:
            print(e)
