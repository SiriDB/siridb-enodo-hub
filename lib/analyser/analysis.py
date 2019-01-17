import os
import pickle

from pmdarima import auto_arima
import pandas as pd
from pmdarima.arima.utils import ndiffs, nsdiffs

from lib.config.config import Config
from lib.exceptions.analyserexception import AnalyserException


class Analysis:

    def __init__(self, serie_name, dataset, m, d=None, d_large=None):
        self.forecast_values = None
        self._serie_name = serie_name
        begin = list(dataset.iloc[[0]].to_dict().get(1).keys())[0]
        end = list(dataset.iloc[[int(dataset.size / 4)]].to_dict().get(1).keys())[0]
        self._interval_indexes = (end - begin) / int(dataset.size / 4)
        self._last_value = list(dataset.iloc[[-1]].to_dict().get(1).keys())[0]
        dataset.set_index(0, inplace=True)
        dataset.head()
        dataset.index = pd.to_datetime(dataset.index, unit='ms')
        if d is None:
            # Estimate the number of differences using an ADF test:
            d = ndiffs(dataset, test='adf')

        if d_large is None:
            # estimate number of seasonal differences
            d_large = nsdiffs(dataset,
                              m=m,  # commonly requires knowledge of dataset
                              max_D=12,
                              test='ch')
        try:
            self._stepwise_model = auto_arima(dataset, start_p=1, start_q=1,
                                              max_p=3, max_q=3, m=m,
                                              seasonal=True,
                                              d=d, D=d_large,
                                              error_action='ignore',
                                              trace=True,
                                              suppress_warnings=True,
                                              stepwise=True)
        except Exception as e:
            print(e)
            raise AnalyserException()
        else:
            print(self.do_forecast())

    def do_forecast(self, update=False):
        if update or self.forecast_values is None:
            if self._stepwise_model is not None:

                periods = 24
                indexes = []

                current_value = self._last_value + self._interval_indexes
                for i in range(periods):
                    indexes.append(current_value)
                    current_value = self._last_value + self._interval_indexes

                future_forecast = self._stepwise_model.predict(n_periods=len(indexes))
                self.forecast_values = future_forecast
                return future_forecast
            else:
                return self.forecast_values

    async def save(self):
        if not os.path.exists(Config.analysis_save_path):
            raise Exception()
        pickle.dump(self,
                    open(os.path.join(Config.analysis_save_path, self._serie_name + ".pkl"), "wb"))

    @classmethod
    async def load(cls, serie_name):
        if not os.path.exists(Config.analysis_save_path):
            raise Exception()
        return pickle.load(open(os.path.join(Config.analysis_save_path, serie_name + ".pkl"), "rb"))
