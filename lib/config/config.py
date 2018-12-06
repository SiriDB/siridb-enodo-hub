import configparser
import json
import os


class Config:
    _config = None
    pipe_path = None
    enabled_series_for_analysis = None
    min_data_points = None
    watcher_interval = None

    @classmethod
    async def read_config(cls):
        cls._config = configparser.ConfigParser()
        cls._config.read(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'analyser.conf'))

        cls.pipe_path = cls._config['analyser']['pipe_path']
        cls.min_data_points = int(cls._config['analyser']['min_data_points'])
        cls.watcher_interval = int(cls._config['analyser']['watcher_interval'])
        cls.enabled_series_for_analysis = json.loads(cls._config['series']['enabled_series'])
