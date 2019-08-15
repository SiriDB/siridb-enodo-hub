import datetime
import json
import os

from lib.config.config import Config
from lib.serie.series import Series
from lib.siridb.siridb import SiriDB
from lib.socket.clientmanager import ClientManager
from lib.socket.package import create_header, UPDATE_SERIES


class SerieManager:
    _series = None
    _siridb_client = None

    @classmethod
    async def prepare(cls):
        cls._series = {}
        cls._siridb_client = SiriDB()
        # if SiriDB.siridb_connected:
        #     await cls.check_for_config_changes()

    # @classmethod
    # async def check_for_config_changes(cls):
    #     for serie_name in cls._series:
    #         if serie_name not in Config.names_enabled_series_for_analysis:
    #             del cls._series[serie_name]
    #
    #     for serie_name in Config.names_enabled_series_for_analysis:
    #         if serie_name not in cls._series:
    #             await cls.add_serie(serie_name)
    #
    #     await cls.save_to_disk()

    @classmethod
    async def add_serie(cls, serie):
        if serie.get('name') not in cls._series:
            collected_datapoints = await cls._siridb_client.query_serie_datapoint_count(serie.get('name'))
            if collected_datapoints:
                serie['datapoint_count'] = collected_datapoints
                cls._series[serie.get('name')] = await Series.from_dict(serie)
                print(f"Added new serie: {serie.get('name')}")

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
    async def get_series_to_dict(cls):
        return [await serie.to_dict() for serie in cls._series.values()]

    @classmethod
    async def remove_serie(cls, serie_name):
        if serie_name in cls._series:
            del cls._series[serie_name]
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
        print("H2", serie_name, points)
        serie = cls._series.get(serie_name, None)
        if serie is not None:
            print("H3")
            await cls._siridb_client.drop_serie(f'forecast_{serie_name}')
            await cls._siridb_client.insert_points(f'forecast_{serie_name}', points)
            await serie.set_pending_forecast(False)

            date_1 = datetime.datetime.now()
            end_date = date_1 + datetime.timedelta(days=1)
            await serie.schedule_forecast(end_date)

    @classmethod
    async def get_serie_forecast(cls, serie_name):
        values = await cls._siridb_client.query_serie_data(f'forecast_{serie_name}')
        if values is not None:
            return values.get(f'forecast_{serie_name}', None)
        return None

    @classmethod
    async def update_listeners(cls, series):
        for listener in ClientManager.listeners.values():
            update = json.dumps(series)
            series_update = create_header(len(update), UPDATE_SERIES, 0)
            listener.writer.write(series_update + update.encode("utf-8"))

    @classmethod
    async def read_from_disk(cls):
        if not os.path.exists(os.path.join(Config.series_save_path, "series.json")):
            print("No saved series")
        else:
            f = open(os.path.join(Config.series_save_path, "series.json"), "r")
            data = f.read()
            f.close()
            if len(data):
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
            f = open(os.path.join(Config.series_save_path, "series.json"), "w")
            f.write(json.dumps(serialized_series))
            f.close()
        except Exception as e:
            print(e)
