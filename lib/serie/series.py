import datetime
import os

from lib.analyser.analyserwrapper import MODEL_NAMES, MODEL_PARAMETERS
from lib.config.config import Config

DETECT_ANOMALIES_STATUS_REQUESTED = 1
DETECT_ANOMALIES_STATUS_PENDING = 2
DETECT_ANOMALIES_STATUS_DONE = 4
DETECT_ANOMALIES_STATUS_NONE = None
DETECT_ANOMALIES_STATUSES = [DETECT_ANOMALIES_STATUS_REQUESTED, DETECT_ANOMALIES_STATUS_PENDING,
                             DETECT_ANOMALIES_STATUS_DONE, DETECT_ANOMALIES_STATUS_NONE]


class Series:
    _name = None
    _datapoint_count = None
    _datapoint_count_lock = False
    new_forecast_at = None
    _detecting_anomalies_status = DETECT_ANOMALIES_STATUS_REQUESTED
    _pending_forecast = None
    _model = None
    _model_parameters = None
    _ignore = None
    _error = None
    _anomalies = None

    def __init__(self, name, datapoint_count, model, scheduled_forecast=None, model_parameters=None, ignore=False,
                 error=None, detecting_anomalies_status=DETECT_ANOMALIES_STATUS_NONE):
        self._name = name
        self._datapoint_count = datapoint_count

        if model not in MODEL_NAMES.keys():
            raise Exception('Invalid model')

        if model_parameters is None and len(MODEL_PARAMETERS.get(model, [])) > 0:
            raise Exception('Invalid model parameters')
        for key in MODEL_PARAMETERS.get(model, {}):
            if key not in model_parameters.keys():
                raise Exception('Invalid model parameters')

        self._model = model
        self.new_forecast_at = scheduled_forecast
        self._model_parameters = model_parameters
        self._awaiting_forecast = False
        self._ignore = ignore
        self._error = error
        self._detecting_anomalies_status = detecting_anomalies_status

    async def set_datapoints_counter_lock(self, is_locked):
        """
        Set lock so it can or can not be changed
        :param is_locked:
        :return:
        """
        self._datapoint_count_lock = is_locked

    async def get_datapoints_counter_lock(self):
        return self._datapoint_count_lock

    async def set_error(self, error_message):
        self._ignore = True
        self._error = error_message

    async def clear_error(self):
        self._ignore = False
        self._error = None

    async def get_error(self):
        return self._error

    async def ignored(self):
        return self._ignore

    async def get_name(self):
        return self._name

    async def get_model(self):
        return self._model

    async def set_model(self, model):
        if model not in MODEL_NAMES.keys():
            raise Exception()
        self._model = model

    async def get_model_parameters(self):
        return self._model_parameters

    async def set_model_parameters(self, params):
        self._model_parameters = params

    async def get_model_pkl(self):
        if not os.path.exists(os.path.join(Config.model_pkl_save_path, self._name + ".pkl")):
            return None
        f = open(os.path.join(Config.model_pkl_save_path, self._name + ".pkl"), "r")
        data = f.read()
        f.close()
        return data

    async def set_model_pkl(self, pkl):
        if not os.path.exists(Config.model_pkl_save_path):
            raise Exception()
        f = open(os.path.join(Config.model_pkl_save_path, self._name + ".pkl"), "wb")
        f.write(pkl)
        f.close()

    async def get_detect_anomalies_status(self):
        return self._detecting_anomalies_status

    async def set_detect_anomalies_status(self, status):
        if status not in DETECT_ANOMALIES_STATUSES:
            raise Exception("unknow status")
        self._detecting_anomalies_status = status

    async def get_anomalies(self):
        return self._anomalies

    async def set_anomalies(self, anomalies):
        self._anomalies = anomalies

    async def get_datapoints_count(self):
        return self._datapoint_count

    async def add_to_datapoints_count(self, add_to_count):
        """
        Add value to existing value of data points counter
        :param add_to_count:
        :return:
        """
        if self._datapoint_count_lock is False:
            self._datapoint_count += add_to_count

    async def get_forecast(self):
        # if self._analysed:
        #     analysis = ARIMAModel.load(self._name)
        #     if analysis is not None:
        #         return analysis.forecast_values

        return None

    async def pending_forecast(self):
        return self._pending_forecast

    async def set_pending_forecast(self, pending):
        self._pending_forecast = pending

    async def schedule_forecast(self, datetime):
        self.new_forecast_at = datetime

    async def is_forecasted(self):
        return self.new_forecast_at is not None

    async def save_forecast(self, forecast):
        pass

    async def to_dict(self):
        return {
            'name': self._name,
            'datapoint_count': self._datapoint_count,
            'analysed': await self.is_forecasted(),
            'new_forecast_at': self.new_forecast_at,
            'model': self._model,
            'model_parameters': self._model_parameters,
            'ignore': self._ignore,
            'error': self._error,
            'detecting_anomalies_status': self._detecting_anomalies_status
        }

    @classmethod
    async def from_dict(cls, data_dict):
        timestamp = data_dict.get('new_forecast_at', None)
        new_forecast_at = None
        if timestamp is not None:
            timestamp = float(timestamp)
            new_forecast_at = datetime.datetime.fromtimestamp(timestamp)

        return Series(data_dict.get('name'), data_dict.get('datapoint_count', None), data_dict.get('model'),
                      new_forecast_at, data_dict.get('model_parameters', None), ignore=data_dict.get('ignore', False),
                      error=data_dict.get('error', None),
                      detecting_anomalies_status=data_dict.get('detecting_anomalies_status', None))
