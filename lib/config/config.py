import configparser
import os


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
    log_path = None
    client_max_timeout = None
    socket_server_host = None
    socket_server_port = None
    model_pkl_save_path = None
    series_save_path = None
    save_to_disk_interval = None

    @classmethod
    async def read_config(cls, path):
        """
        Read config from conf file and json database
        :return:
        """

        cls._path = path
        cls._config = configparser.ConfigParser()
        cls._config.read(path)

        cls.analysis_save_path = cls._config['analyser']['analysis_save_path']
        cls.min_data_points = await cls.to_int(cls._config['analyser']['min_data_points'])
        cls.watcher_interval = await cls.to_int(cls._config['analyser']['watcher_interval'])
        cls.siridb_connection_check_interval = await cls.to_int(
            cls._config['analyser']['siridb_connection_check_interval'])
        cls.period_to_forecast = await cls.to_int(cls._config['analyser']['period_to_forecast'])
        cls.interval_schedules_series = await cls.to_int(cls._config['analyser']['interval_schedules_series'])

        # SiriDB
        cls.siridb_host = cls._config['siridb']['host']
        cls.siridb_port = await cls.to_int(cls._config['siridb']['port'])
        cls.siridb_user = cls._config['siridb']['user']
        cls.siridb_password = cls._config['siridb']['password']
        cls.siridb_database = cls._config['siridb']['database']

        # SiriDB Forecast
        cls.siridb_forecast_host = cls._config['siridb_forecast']['host']
        cls.siridb_forecast_port = await cls.to_int(cls._config['siridb_forecast']['port'])
        cls.siridb_forecast_user = cls._config['siridb_forecast']['user']
        cls.siridb_forecast_password = cls._config['siridb_forecast']['password']
        cls.siridb_forecast_database = cls._config['siridb_forecast']['database']

        # Enodo
        cls.log_path = cls._config['enodo']['log_path']
        cls.client_max_timeout = await cls.to_int(cls._config['enodo']['client_max_timeout'])
        cls.socket_server_host = cls._config['enodo']['socket_server_hostname']
        cls.socket_server_port = await cls.to_int(cls._config['enodo']['socker_server_port'])
        cls.model_pkl_save_path = cls._config['enodo']['model_pkl_save_path']
        cls.series_save_path = cls._config['enodo']['series_save_path']
        cls.save_to_disk_interval = await cls.to_int(cls._config['enodo']['save_to_disk_interval'])

        if not os.path.exists(Config.series_save_path):
            os.makedirs(Config.series_save_path)
        if not os.path.exists(Config.model_pkl_save_path):
            os.makedirs(Config.model_pkl_save_path)
        if not os.path.exists(Config.analysis_save_path):
            os.makedirs(Config.analysis_save_path)
        if not os.path.exists(os.path.join(Config.analysis_save_path, 'db.json')):
            open(os.path.join(Config.analysis_save_path, 'db.json'), 'a').close()

        # cls.db = TinyDB(os.path.join(Config.analysis_save_path, 'db.json'))

        # cls.enabled_series_for_analysis = {}
        #
        # for serie in cls.db.all():
        #     if 'name' in serie:
        #         cls.enabled_series_for_analysis[serie.get('name')] = serie
        # cls.names_enabled_series_for_analysis = [serie.get('name') for serie in cls.db.all()]

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
    async def to_int(val):
        return_val = None
        try:
            return_val = int(val)
        except Exception:
            pass
        return return_val
