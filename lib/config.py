from configparser import NoOptionError, NoSectionError, \
    RawConfigParser, ConfigParser
import os
import logging
from secrets import token_urlsafe

from lib.exceptions.enodoexception import EnodoInvalidConfigException, \
    EnodoException

EMPTY_CONFIG_FILE = {
    'hub': {
        'basic_auth_username': 'enodo',
        'basic_auth_password': 'enodo',
        'log_path': '',
        'client_max_timeout': '35',
        'internal_socket_server_hostname': 'localhost',
        'internal_socket_server_port': '9103',
        'series_save_path': '',
        'enodo_base_save_path': '',
        'save_to_disk_interval': '20',
        'enable_rest_api': 'true',
        'enable_socket_io_api': 'false',
        'disable_safe_mode': 'false'
    },
    'events': {
        'max_in_queue_before_warning': '25'
    },
    'analyser': {
        'min_data_points': '100',
        'watcher_interval': '2',
        'siridb_connection_check_interval': '30',
        'interval_schedules_series': '3600',
    }
}

EMPTY_SETTINGS_FILE = {
    'siridb': {
        'host': '',
        'port': '',
        'user': '',
        'password': '',
        'database': '',
    },
    'siridb_output': {
        'host': '',
        'port': '',
        'user': '',
        'password': '',
        'database': '',
    }
}


class EnodoConfigParser(RawConfigParser):
    def __init__(self, env_support=True, **kwargs):
        self.env_support = env_support
        super(EnodoConfigParser, self).__init__(**kwargs)

    def get_r(self, section, option, required=True, default=None):
        value = None
        try:
            value = RawConfigParser.get(self, section, option)
        except (NoOptionError, NoSectionError) as _:
            pass

        if self.env_support:
            env_value = os.getenv(f"ENODO_{section.upper()}_{option.upper()}")
            if env_value is not None:
                value = env_value

        if value is None:
            if required:
                raise EnodoInvalidConfigException(
                    f'Invalid config, missing option "{option}" in section \
                        "{section}" or environment variable "{option.upper()}"')
            return default
        return value


class Config:
    _config = None
    _path = None
    _settings = None
    min_data_points = None
    watcher_interval = None
    siridb_connection_check_interval = None
    db = None
    interval_schedules_series = None

    # Siridb
    siridb_host = None
    siridb_port = None
    siridb_user = None
    siridb_password = None
    siridb_database = None

    # Siridb forecast
    siridb_output_host = None
    siridb_output_port = None
    siridb_output_user = None
    siridb_output_password = None
    siridb_output_database = None

    # Enodo
    base_dir = None
    basic_auth_username = None
    basic_auth_password = None
    log_path = None
    client_max_timeout = None
    socket_server_host = None
    socket_server_port = None
    series_save_path = None
    event_outputs_save_path = None
    save_to_disk_interval = None
    enable_rest_api = None
    enable_socket_io_api = None
    internal_security_token = None
    jobs_save_path = None
    module_save_path = None
    disable_safe_mode = None

    # Enodo Events
    max_in_queue_before_warning = None

    @classmethod
    def create_standard_config_file(cls, path):
        _config = ConfigParser()

        for section in EMPTY_CONFIG_FILE:
            _config.add_section(section)
            for option in EMPTY_CONFIG_FILE[section]:
                _config.set(
                    section, option, EMPTY_CONFIG_FILE[section]
                    [option])

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

        if cls.disable_safe_mode is True:
            logging.info(
                'Safe mode disabled for internal communication')
            return

        if cls._config is None:
            raise EnodoException(
                'Can only setup token when config is parsed')

        if os.path.exists(
                os.path.join(cls.base_dir, 'internal_token.cred')):
            f = open(os.path.join(cls.base_dir,
                                  'internal_token.cred'), "r")
            cls.internal_security_token = f.read()
        else:
            f = open(os.path.join(cls.base_dir,
                                  'internal_token.cred'), "w+")
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
        if path is not None and os.path.exists(path):
            cls._config.read(path)

        cls.setup_config_variables()

        settings_path = os.path.join(cls.base_dir, 'enodo.settings')
        if not os.path.exists(settings_path):
            tmp_settings_parser = ConfigParser()

            for section in EMPTY_SETTINGS_FILE:
                tmp_settings_parser.add_section(section)
                for option in EMPTY_SETTINGS_FILE[section]:
                    tmp_settings_parser.set(
                        section, option,
                        EMPTY_SETTINGS_FILE[section][option])

            with open(settings_path, "w") as fh:
                tmp_settings_parser.write(fh)

        cls._settings = EnodoConfigParser(env_support=False)
        cls._settings.read(settings_path)
        cls.setup_settings_variables()

    @classmethod
    def get_siridb_settings(cls):
        return {
            "username": cls.siridb_user,
            "password": cls.siridb_password,
            "dbname": cls.siridb_database,
            "hostlist": [(cls.siridb_host, cls.siridb_port)]
        }, {
            "username":  cls.siridb_output_user,
            "password": cls.siridb_output_password,
            "dbname": cls.siridb_output_database,
            "hostlist": [(cls.siridb_output_host, cls.siridb_output_port)]
        }

    @classmethod
    def update_settings(cls, section, key, value):
        cls._settings[section][key] = value

    @classmethod
    def write_settings(cls):
        with open(os.path.join(
                cls.base_dir, 'enodo.settings'), 'w') as settingsfile:
            cls._settings.write(settingsfile)

    @classmethod
    def setup_config_variables(cls):
        cls.min_data_points = cls.to_int(
            cls._config.get_r('analyser', 'min_data_points'))
        cls.watcher_interval = cls.to_int(
            cls._config.get_r('analyser', 'watcher_interval'))
        cls.siridb_connection_check_interval = cls.to_int(
            cls._config.get_r('analyser', 'siridb_connection_check_interval'))
        cls.interval_schedules_series = cls.to_int(
            cls._config.get_r('analyser', 'interval_schedules_series'))

        # Enodo
        cls.basic_auth_username = cls._config.get_r(
           'hub', 'basic_auth_username', required=False, default=None)
        cls.basic_auth_password = cls._config.get_r(
           'hub', 'basic_auth_password', required=False, default=None)

        cls.client_max_timeout = cls.to_int(
            cls._config.get_r('hub', 'client_max_timeout'))
        if cls.client_max_timeout < 35:  # min value enforcement
            cls.client_max_timeout = 35
        cls.socket_server_host = cls._config.get_r(
           'hub', 'internal_socket_server_hostname')
        cls.socket_server_port = cls.to_int(
            cls._config.get_r('hub', 'internal_socket_server_port'))
        cls.save_to_disk_interval = cls.to_int(
            cls._config.get_r('hub', 'save_to_disk_interval'))
        cls.enable_rest_api = cls.to_bool(
            cls._config.get_r(
               'hub', 'enable_rest_api', required=False,
                default='true'),
            True)
        cls.enable_socket_io_api = cls.to_bool(
            cls._config.get_r(
               'hub', 'enable_socket_io_api', required=False,
                default='false'),
            False)
        cls.base_dir = cls._config.get_r(
           'hub', 'enodo_base_save_path')
        cls.disable_safe_mode = cls.to_bool(
            cls._config.get_r('hub', 'disable_safe_mode'), False)
        cls.log_path = os.path.join(cls.base_dir, 'log.log')
        cls.series_save_path = os.path.join(
            cls.base_dir, 'data/series.json')
        cls.jobs_save_path = os.path.join(
            cls.base_dir, 'data/jobs.json')
        cls.event_outputs_save_path = os.path.join(
            cls.base_dir, 'data/outputs.json')
        cls.module_save_path = os.path.join(
            cls.base_dir, 'data/modules.json')
        cls.max_in_queue_before_warning = cls.to_int(
            cls._config.get_r('events', 'max_in_queue_before_warning'))

        if not os.path.exists(os.path.join(cls.base_dir, 'data')):
            os.makedirs(os.path.join(cls.base_dir, 'data'))

    @classmethod
    def setup_settings_variables(cls):
        # SiriDB
        cls.siridb_host = cls._settings.get_r('siridb', 'host')
        cls.siridb_port = cls.to_int(
            cls._settings.get_r('siridb', 'port'))
        cls.siridb_user = cls._settings.get_r('siridb', 'user')
        cls.siridb_password = cls._settings.get_r('siridb', 'password')
        cls.siridb_database = cls._settings.get_r('siridb', 'database')

        # SiriDB Forecast
        cls.siridb_output_host = cls._settings.get_r(
            'siridb_output',
            'host')
        cls.siridb_output_port = cls.to_int(
            cls._settings.get_r('siridb_output', 'port'))
        cls.siridb_output_user = cls._settings.get_r(
            'siridb_output',
            'user')
        cls.siridb_output_password = cls._settings.get_r(
            'siridb_output',
            'password')
        cls.siridb_output_database = cls._settings.get_r(
            'siridb_output',
            'database')

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

    @classmethod
    def get_settings(cls):
        return cls._settings._sections

    @staticmethod
    def is_runtime_configurable(section, key):
        _is_runtime_configurable = {
            "siridb": [
                "host",
                "port",
                "user",
                "password",
                "database"
            ],
            "siridb_output": [
                "host",
                "port",
                "user",
                "password",
                "database"
            ]
        }

        return section in _is_runtime_configurable and \
            key in _is_runtime_configurable[section]
