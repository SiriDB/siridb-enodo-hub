from lib.config.config import Config
from lib.serie.serie import Serie
from lib.siridb.siridb import SiriDB


class SerieManager:
    _monitored_series = None
    _series = None

    @classmethod
    async def prepare(cls):
        cls._monitored_series = set()
        cls._series = {}

        await cls.check_for_config_changes()

    @classmethod
    async def check_for_config_changes(cls):
        for serie_name in cls._monitored_series:
            if serie_name not in Config.enabled_series_for_analysis:
                del cls._series[serie_name]
                del cls._monitored_series[serie_name]

        for serie_name in Config.enabled_series_for_analysis:
            if serie_name not in cls._monitored_series:
                await cls.add_serie(serie_name)

    @classmethod
    async def add_serie(cls, serie_name):
        if serie_name in Config.enabled_series_for_analysis and serie_name not in cls._monitored_series:
            collected_datapoints = await SiriDB.query_serie_datapoint_count(serie_name)
            if collected_datapoints:
                cls._series[serie_name] = Serie(serie_name, collected_datapoints)
                cls._monitored_series.add(serie_name)
                print(f"Added new serie: {serie_name}")

    @classmethod
    async def read_state(cls):
        # ToDo
        pass

    @classmethod
    async def get_serie(cls, serie_name):
        serie = None
        if serie_name in cls._monitored_series:
            serie = cls._series.get(serie_name, None)

        return serie

    @classmethod
    async def remove_serie(cls, serie_name):
        if serie_name in cls._monitored_series:
            if serie_name in cls._series:
                del cls._series[serie_name]
            cls._monitored_series.remove(serie_name)

    @classmethod
    async def add_to_datapoint_counter(cls, serie_name, value):
        if serie_name in cls._monitored_series:
            serie = cls._series.get(serie_name, None)
            if serie is not None:
                await serie.add_to_datapoints_count(value)
            else:
                pass
                # TODO exception
        else:
            await cls.add_serie(serie_name)
