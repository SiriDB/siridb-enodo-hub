import configparser
from tinydb import TinyDB
import os


class Config:
    _config = None
    pipe_path = None
    analysis_save_path = None
    min_data_points = None
    watcher_interval = None
    period_to_forecast = None
    enabled_series_for_analysis = None
    names_enabled_series_for_analysis = None
    db = None

    @classmethod
    async def read_config(cls):
        """
        Read config from conf file and json database
        :return:
        """

        cls._config = configparser.ConfigParser()
        cls._config.read(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'analyser.conf'))

        cls.pipe_path = cls._config['analyser']['pipe_path']
        cls.analysis_save_path = cls._config['analyser']['analysis_save_path']
        cls.min_data_points = int(cls._config['analyser']['min_data_points'])
        cls.watcher_interval = int(cls._config['analyser']['watcher_interval'])
        cls.period_to_forecast = int(cls._config['analyser']['period_to_forecast'])

        if not os.path.exists(Config.analysis_save_path):
            os.makedirs(Config.analysis_save_path)
        if not os.path.exists(os.path.join(Config.analysis_save_path, 'db.json')):
            open(os.path.join(Config.analysis_save_path, 'db.json'), 'a').close()

        cls.db = TinyDB(os.path.join(Config.analysis_save_path, 'db.json'))

        cls.enabled_series_for_analysis = {}

        for serie in cls.db.all():
            if 'serie_name' in serie:
                cls.enabled_series_for_analysis[serie.get('serie_name')] = serie
        cls.names_enabled_series_for_analysis = [serie.get('serie_name') for serie in cls.db.all()]
