import configparser
import os


class Config:
    _config = None
    pipe_path = None

    @classmethod
    async def read_config(cls):
        cls._config = configparser.ConfigParser()
        cls._config.read(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'analyser.conf'))

        cls.pipe_path = cls._config['analyser']['pipe_path']