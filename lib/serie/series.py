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

FORECAST_STATUS_NONE = 1
FORECAST_STATUS_PENDING = 2
FORECAST_STATUS_DONE = 3


class Series:
    __slots__ = (
        'name', 'forecast_status', 'model_parameters', 'new_forecast_at', '_datapoint_count', '_datapoint_count_lock',
        '_detecting_anomalies_status', '_model')

    def __init__(self, name, datapoint_count, model, scheduled_forecast=None, model_parameters=None,
                 detecting_anomalies_status=DETECT_ANOMALIES_STATUS_NONE, forecast_status=FORECAST_STATUS_NONE):
        self.name = name
        self._datapoint_count = datapoint_count

        if model not in MODEL_NAMES.keys():
            raise Exception('Invalid model')

        self._model = model
        self._datapoint_count_lock = False
        self.new_forecast_at = scheduled_forecast
        self.model_parameters = model_parameters
        self._detecting_anomalies_status = detecting_anomalies_status
        self.forecast_status = forecast_status

    async def set_datapoints_counter_lock(self, is_locked):
        """
        Set lock so it can or can not be changed
        :param is_locked:
        :return:
        """
        self._datapoint_count_lock = is_locked

    async def get_datapoints_counter_lock(self):
        return self._datapoint_count_lock

    async def clear_errors(self):
        pass

    async def get_errors(self):
        return []

    async def is_ignored(self):
        return False

    async def get_model(self):
        return self._model

    async def set_model(self, model):
        if model not in MODEL_NAMES.keys():
            raise Exception()
        self._model = model

    async def get_detect_anomalies_status(self):
        return self._detecting_anomalies_status

    async def set_detect_anomalies_status(self, status):
        if status not in DETECT_ANOMALIES_STATUSES:
            raise Exception("unknow status")
        self._detecting_anomalies_status = status

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

    async def schedule_forecast(self, datetime):
        self.new_forecast_at = datetime

    async def is_forecasted(self):
        return self.forecast_status is FORECAST_STATUS_DONE

    async def to_dict(self):
        return {
            'name': self.name,
            'datapoint_count': self._datapoint_count,
            'analysed': await self.is_forecasted(),
            'new_forecast_at': self.new_forecast_at,
            'model': self._model,
            'model_parameters': self.model_parameters,
            'ignore': await self.is_ignored(),
            'error': await self.get_errors(),
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
                      new_forecast_at, data_dict.get('model_parameters', None),
                      detecting_anomalies_status=data_dict.get('detecting_anomalies_status', None),
                      forecast_status=data_dict.get('forecast_status', None))
