import os
import pickle

from pmdarima import auto_arima
import pandas as pd
from pmdarima.arima.utils import ndiffs, nsdiffs
from statsmodels.tsa.stattools import adfuller

from lib.analyser.model.base import Model
from lib.config.config import Config
from lib.exceptions.analyserexception import AnalyserException, AnalysisInvalidArgumentException, \
    AnalysisInvalidDatasetSize


class ARIMAModel(Model):

    def __init__(self, serie_name, dataset, m, d=None, d_large=None):
        """
        Start modelling a time serie
        :param serie_name: name of the serie
        :param dataset: dataframe (Panda) with datapoints
        :param m: the seasonality factor
        :param d: the de-rending differencing factor
        :param d_large: the de-seasonality differencing factor
        """
        super().__init__(serie_name, dataset)
        self._m = m
        self._d = d
        self._d_large = d_large

        if self._m is None:
            raise AnalysisInvalidArgumentException

        self.forecast_values = None
        self.is_stationary = False

    def create_model(self):
        try:
            self._adf_stationarity_test(self._dataset)
        except:
            pass

        # Determine average interval between measurements
        begin = self._dataset.iloc[[0]][0].to_dict().get(0)
        end = next(iter(self._dataset.iloc[[int(self._dataset.size / 4)]][0].to_dict().values()))

        if int(self._dataset.size / 4) <= 0:
            raise AnalysisInvalidDatasetSize

        self._interval_indexes = int((end - begin) / int(self._dataset.size / 4))
        self._interval_indexes = 118338570000
        self._interval_indexes = 2678400000

        # Determine index of last measurement
        self._last_value = next(iter(self._dataset.iloc[[-1]][0].to_dict().values()))

        # Set index to the first column (datetime)
        self._dataset.set_index(0, inplace=True)

        # Parse indexes to datetimes
        self._dataset.index = pd.to_datetime(self._dataset.index, unit='ms')

        if self._d is None:
            # Estimate the number of differences using an ADF test:
            self._d = ndiffs(self._dataset, test='adf')

        if self._d_large is None:
            # estimate number of seasonal differences
            self._d_large = nsdiffs(self._dataset,
                                    m=self._m,  # commonly requires knowledge of dataset
                                    max_D=12,
                                    test='ch')
        try:
            self._stepwise_model = auto_arima(self._dataset, start_p=1, start_q=1,
                                              max_p=3, max_q=3, m=self._m,
                                              seasonal=True,
                                              d=int(self._d), D=int(self._d_large),
                                              error_action='ignore',
                                              trace=True,
                                              suppress_warnings=True,
                                              stepwise=True)

            self._stepwise_model.fit(self._dataset.iloc[int(len(self._dataset.index) / 2):])
        except Exception as e:
            print(e)
            raise AnalyserException()
        else:
            print(self.do_forecast())

    def do_forecast(self, update=False):
        """
        When is a model is present, a set of forecasted future values can be generated.
        :param update:
        :return:
        """
        if update or self.forecast_values is None:
            if self._stepwise_model is not None:

                periods = 12  # TODO: make default and settable per serie
                indexes = []

                current_value = self._last_value + self._interval_indexes
                for i in range(periods):
                    indexes.append(current_value)
                    current_value = current_value + self._interval_indexes

                future_forecast = self._stepwise_model.predict(n_periods=len(indexes))
                indexed_forecast_values = []
                for i in range(len(future_forecast)):
                    indexed_forecast_values.append([indexes[i], future_forecast[i]])
                self.forecast_values = indexed_forecast_values
                return indexed_forecast_values
            else:
                return self.forecast_values

    def save(self):
        if not os.path.exists(Config.analysis_save_path):
            raise Exception()
        pickle.dump(self,
                    open(os.path.join(Config.analysis_save_path, self._serie_name + ".pkl"), "wb"))

    @classmethod
    def load(cls, serie_name):
        """
        Load Analysis class from existing model.
        :param serie_name:
        :return:
        """
        if not os.path.exists(Config.analysis_save_path):
            raise Exception()
        return pickle.load(open(os.path.join(Config.analysis_save_path, serie_name + ".pkl"), "rb"))
