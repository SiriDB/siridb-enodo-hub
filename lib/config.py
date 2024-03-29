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
        'client_max_timeout': '35',
        'internal_socket_server_hostname': 'localhost',
        'internal_socket_server_port': '9103',
        'base_path': '',
        'save_to_disk_interval': '20',
        'enable_rest_api': 'true',
        'enable_socket_io_api': 'false',
        'disable_safe_mode': 'false'
    },
    'analyser': {
        'watcher_interval': '2',
    },
    'siridb_data': {
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

EMPTY_SETTINGS_FILE = {
    'max_in_queue_before_warning': '25',
    'min_data_points': '100'
}


class EnodoConfigParser(RawConfigParser):
    def __init__(self, env_support=True, **kwargs):
        self.env_support = env_support
        super(EnodoConfigParser, self).__init__(**kwargs)

    def get_r(self, section, option, required=True, default=None) -> str:
        value = None
        try:
            value = RawConfigParser.get(self, section, option)
        except (NoOptionError, NoSectionError) as _:
            pass

        if self.env_support:
            env_variable = f"ENODO_{section.upper()}_{option.upper()}"
            env_value = os.getenv(env_variable)
            if env_value is not None:
                value = env_value

        if value is None:
            if required:
                raise EnodoInvalidConfigException(
                    f'Invalid config, missing option "{option}" in section '
                    f'"{section}" or environment variable "{env_variable}"')
            return default
        return value


class Config:
    _config = None
    _path = None
    min_data_points = None
    watcher_interval = None

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
    client_max_timeout = None
    socket_server_host = None
    socket_server_port = None
    series_save_path = None
    clients_save_path = None
    event_outputs_save_path = None
    save_to_disk_interval = None
    enable_rest_api = None
    enable_socket_io_api = None
    internal_security_token = None
    jobs_save_path = None
    disable_safe_mode = None

    # ThingsDB
    thingsdb_host = None
    thingsdb_port = None
    thingsdb_auth_token = None
    thingsdb_scope = None

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
        Method checks if a token is already setup,
        if not, it will generate one.
        (used for handshakes in internal communication)

        This method can only be called after the configfile is parsed
        :return:
        """
        cls.internal_security_token = None
        if cls.disable_safe_mode is False:
            cls.internal_security_token = token_urlsafe(16)

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

    @classmethod
    async def read_settings(cls, client):
        convert_to = {
            'max_in_queue_before_warning': int,
            'min_data_points': int
        }
        for option in EMPTY_SETTINGS_FILE:
            resp = await client.query("""
                if (.settings.has(option) == false) {
                    .settings.set(option, default)
                }
                .settings.get(option)
            """, option=option, default=EMPTY_SETTINGS_FILE[option])
            if option in convert_to:
                resp = convert_to[option](resp)
            setattr(cls, option, resp)

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
    async def update_settings(cls, client, option, value):
        setattr(cls, option, value)
        await client.query("""
        .settings.set(option, value)
        """, option=option, value=value)

    @classmethod
    def setup_config_variables(cls):
        cls.watcher_interval = cls.to_int(
            cls._config.get_r('analyser', 'watcher_interval'))

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
                'hub', 'enable_rest_api',
                required=False,
                default='true'),
            True)
        cls.enable_socket_io_api = cls.to_bool(
            cls._config.get_r(
                'hub', 'enable_socket_io_api',
                required=False,
                default='false'),
            False)
        cls.base_dir = cls._config.get_r(
            'hub', 'base_path')
        cls.disable_safe_mode = cls.to_bool(
            cls._config.get_r('hub', 'disable_safe_mode'), False)

        # ThingsDB
        cls.thingsdb_host = cls._config.get_r(
            'thingsdb', 'host', True)
        cls.thingsdb_port = cls.to_int(
            cls._config.get_r('thingsdb', 'port', True))
        cls.thingsdb_auth_token = cls._config.get_r(
            'thingsdb', 'auth_token', True)
        cls.thingsdb_scope = cls._config.get_r(
            'thingsdb', 'scope', True)

        # SiriDB
        cls.siridb_host = cls._config.get_r('siridb_data', 'host')
        cls.siridb_port = cls.to_int(
            cls._config.get_r('siridb_data', 'port'))
        cls.siridb_user = cls._config.get_r('siridb_data', 'user')
        cls.siridb_password = cls._config.get_r(
            'siridb_data', 'password')
        cls.siridb_database = cls._config.get_r(
            'siridb_data', 'database')

        # SiriDB Forecast
        cls.siridb_output_host = cls._config.get_r(
            'siridb_output',
            'host')
        cls.siridb_output_port = cls.to_int(
            cls._config.get_r('siridb_output', 'port'))
        cls.siridb_output_user = cls._config.get_r(
            'siridb_output',
            'user')
        cls.siridb_output_password = cls._config.get_r(
            'siridb_output',
            'password')
        cls.siridb_output_database = cls._config.get_r(
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

    @staticmethod
    def _remove_dict_key_recursive(data, keys):
        Config._remove_dict_key_recursive(
            data[keys[0]],
            keys[1:]) if len(keys) > 1 else data.pop(
            keys[0],
            None)

    @classmethod
    def get_settings(cls, include_secrets=True):
        return {
            'max_in_queue_before_warning': cls.max_in_queue_before_warning,
            'min_data_points': cls.min_data_points
        }

    @staticmethod
    def is_runtime_configurable(key):
        _is_runtime_configurable = [
            "max_in_queue_before_warning", "min_data_points"]

        return key in _is_runtime_configurable
