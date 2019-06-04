import os
import pickle

from fbprophet import Prophet

from lib.analyser.model.base import Model
from lib.config.config import Config
from lib.exceptions.analyserexception import AnalyserException, AnalysisInvalidArgumentException, \
    AnalysisInvalidDatasetSize


class ProphetModel(Model):

    def __init__(self, serie_name, dataset):
        """
        Start modelling a time serie
        :param serie_name: name of the serie
        :param dataset: dataframe (Panda) with datapoints
        :param m: the seasonality factor
        :param d: the de-rending differencing factor
        :param d_large: the de-seasonality differencing factor
        """
        super().__init__(serie_name, dataset)
        self._model = None
        self._dataset = dataset

        self.forecast_values = None
        self.is_stationary = False
        self._dataset.columns = ['ds', 'y']

    def create_model(self):
        self._model = Prophet()
        self._model.fit(self._dataset)

    def do_forecast(self, update=False):
        """
        When is a model is present, a set of forecasted future values can be generated.
        :param update:
        :return:
        """
        if update or self.forecast_values is None:
            future = self._model.make_future_dataframe(periods=5)
            future.tail()

            forecast = self._model.predict(future)
            # forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail()
            forecast.set_index('ds')

            indexed_forecast_values = []
            for i in range(len(forecast)):
                indexed_forecast_values.append([forecast[i]['ds'], forecast[i]['yhat']])

            self.forecast_values = indexed_forecast_values
            return self.forecast_values
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
