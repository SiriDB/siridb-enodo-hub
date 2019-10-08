from configparser import NoOptionError, NoSectionError, RawConfigParser, ConfigParser
import os
from secrets import token_urlsafe

from lib.exceptions.enodoexception import EnodoInvalidConfigException, EnodoException

EMPTY_CONFIG_FILE = config = {
    'enodo': {
        'log_path': '',
        'client_max_timeout': '35',
        'internal_socket_server_hostname': 'localhost',
        'internal_socket_server_port': '9103',
        'model_pkl_save_path': '',
        'series_save_path': '',
        'save_to_disk_interval': '120',
        'enable_rest_api': 'true',
        'enable_socket_io_api': 'false',
    },
    'siridb': {
        'host': '',
        'port': '',
        'user': '',
        'password': '',
        'database': '',
    },
    'siridb_forecast': {
        'host': '',
        'port': '',
        'user': '',
        'password': '',
        'database': '',
    },
    'analyser': {
        'analysis_save_path': '',
        'min_data_points': '100',
        'watcher_interval': '2',
        'siridb_connection_check_interval': '30',
        'period_to_forecast': '',
        'interval_schedules_series': '3600',
    }
}


class EnodoConfigParser(RawConfigParser):
    def get_r(self, section, option, required=True, default=None):
        try:
            return RawConfigParser.get(self, section, option)
        except (NoOptionError, NoSectionError) as e:
            if required:
                raise EnodoInvalidConfigException(f'Invalid config, missing option "{option}" in section "{section}"')
            else:
                return default


class Config:
    _config = None
    _path = None
    analysis_save_path = None
    min_data_points = None
    watcher_interval = None
    siridb_connection_check_interval = None
    period_to_forecast = None
    db = None
    interval_schedules_series = None

    # Siridb
    siridb_host = None
    siridb_port = None
    siridb_user = None
    siridb_password = None
    siridb_database = None

    # Siridb forecast
    siridb_forecast_host = None
    siridb_forecast_port = None
    siridb_forecast_user = None
    siridb_forecast_password = None
    siridb_forecast_database = None

    # Enodo
    base_dir = None
    basic_auth_username = None
    basic_auth_password = None
    log_path = None
    client_max_timeout = None
    socket_server_host = None
    socket_server_port = None
    model_pkl_save_path = None
    series_save_path = None
    save_to_disk_interval = None
    enable_rest_api = None
    enable_socket_io_api = None
    internal_security_token = None

    @classmethod
    def create_standard_config_file(cls, path):
        _config = ConfigParser()

        for section in config:
            _config.add_section(section)
            for option in config[section]:
                _config.set(section, option, config[section][option])

        with open(path, "w") as fh:
            _config.write(fh)

    @classmethod
    def setup_internal_security_token(cls):
        """
        Method checks if a token is already setup, if not, it will generate one.
        (used for handshakes in internal communication)

        This method can only be called after the configfile is parsed
        :return:
        """

        if cls._config is None:
            raise EnodoException('Can only setup token when config is parsed')

        if os.path.exists(os.path.join(cls.base_dir, 'internal_token.cred')):
            f = open(os.path.join(cls.base_dir, 'internal_token.cred'), "r")
            cls.internal_security_token = f.read()
        else:
            f = open(os.path.join(cls.base_dir, 'internal_token.cred'), "w+")
            cls.internal_security_token = token_urlsafe(16)
            f.write(cls.internal_security_token)
        f.close()

    @classmethod
    def read_config(cls, path):
        """
        Read config from conf file and json database
        :return:
        """

        cls._path = path
        cls._config = EnodoConfigParser()
        cls._config.read(path)

        cls.analysis_save_path = cls._config.get_r('analyser', 'analysis_save_path')
        cls.min_data_points = cls.to_int(cls._config.get_r('analyser', 'min_data_points'))
        cls.watcher_interval = cls.to_int(cls._config.get_r('analyser', 'watcher_interval'))
        cls.siridb_connection_check_interval = cls.to_int(
            cls._config.get_r('analyser', 'siridb_connection_check_interval'))
        cls.period_to_forecast = cls.to_int(cls._config.get_r('analyser', 'period_to_forecast'))
        cls.interval_schedules_series = cls.to_int(cls._config.get_r('analyser', 'interval_schedules_series'))

        # SiriDB
        cls.siridb_host = cls._config.get_r('siridb', 'host')
        cls.siridb_port = cls.to_int(cls._config.get_r('siridb', 'port'))
        cls.siridb_user = cls._config.get_r('siridb', 'user')
        cls.siridb_password = cls._config.get_r('siridb', 'password')
        cls.siridb_database = cls._config.get_r('siridb', 'database')

        # SiriDB Forecast
        cls.siridb_forecast_host = cls._config.get_r('siridb_forecast', 'host')
        cls.siridb_forecast_port = cls.to_int(cls._config.get_r('siridb_forecast', 'port'))
        cls.siridb_forecast_user = cls._config.get_r('siridb_forecast', 'user')
        cls.siridb_forecast_password = cls._config.get_r('siridb_forecast', 'password')
        cls.siridb_forecast_database = cls._config.get_r('siridb_forecast', 'database')

        # Enodo
        cls.basic_auth_username = cls._config.get_r('enodo', 'basic_auth_username', required=False, default=None)
        cls.basic_auth_password = cls._config.get_r('enodo', 'basic_auth_password', required=False, default=None)
        cls.log_path = cls._config.get_r('enodo', 'log_path')
        cls.client_max_timeout = cls.to_int(cls._config.get_r('enodo', 'client_max_timeout'))
        if cls.client_max_timeout < 35:  # min value enforcement
            cls.client_max_timeout = 35
        cls.socket_server_host = cls._config.get_r('enodo', 'internal_socket_server_hostname')
        cls.socket_server_port = cls.to_int(cls._config.get_r('enodo', 'internal_socket_server_port'))
        cls.model_pkl_save_path = cls._config.get_r('enodo', 'model_pkl_save_path')
        cls.series_save_path = cls._config.get_r('enodo', 'series_save_path')
        cls.save_to_disk_interval = cls.to_int(cls._config.get_r('enodo', 'save_to_disk_interval'))
        cls.enable_rest_api = cls.to_bool(
            cls._config.get_r('enodo', 'enable_rest_api', required=False, default='true'), True)
        cls.enable_socket_io_api = cls.to_bool(
            cls._config.get_r('enodo', 'enable_socket_io_api', required=False, default='false'), False)
        cls.base_dir = cls._config.get_r('enodo', 'enodo_base_save_path')

        if not os.path.exists(Config.series_save_path):
            os.makedirs(Config.series_save_path)
        if not os.path.exists(Config.model_pkl_save_path):
            os.makedirs(Config.model_pkl_save_path)
        if not os.path.exists(Config.analysis_save_path):
            os.makedirs(Config.analysis_save_path)
        if not os.path.exists(os.path.join(Config.analysis_save_path, 'db.json')):
            open(os.path.join(Config.analysis_save_path, 'db.json'), 'a').close()

    @classmethod
    async def save_config(cls):
        cls._config.set('analyser', 'pipe_path', cls.pipe_path)
        cls._config.set('analyser', 'analysis_save_path', cls.analysis_save_path)
        cls._config.set('analyser', 'min_data_points', str(cls.min_data_points))
        cls._config.set('analyser', 'watcher_interval', str(cls.watcher_interval))
        cls._config.set('analyser', 'siridb_connection_check_interval', str(cls.siridb_connection_check_interval))
        cls._config.set('analyser', 'period_to_forecast', str(cls.period_to_forecast))
        cls._config.set('analyser', 'interval_schedules_series', str(cls.interval_schedules_series))

        # SiriDB
        cls._config.set('siridb', 'host', cls.siridb_host)
        cls._config.set('siridb', 'port', str(cls.siridb_port))
        cls._config.set('siridb', 'user', cls.siridb_user)
        cls._config.set('siridb', 'password', cls.siridb_password)
        cls._config.set('siridb', 'database', cls.siridb_database)

        with open(cls._path, "w") as fh:
            cls._config.write(fh)

    @staticmethod
    def to_int(val):
        return_val = None
        try:
            return_val = int(val)
        except Exception:
            pass
        return return_val

    @staticmethod
    def to_bool(v, default):
        if isinstance(v, bool):
            return v
        if v.lower() in ('yes', 'true', 't', 'y', '1'):
            return True
        elif v.lower() in ('no', 'false', 'f', 'n', '0'):
            return False
        else:
            return default
