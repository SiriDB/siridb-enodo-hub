from statsmodels.tsa.stattools import adfuller


class Model:
    def __init__(self, serie_name, dataset, initialize=True):
        """
        Start modelling a time serie
        :param serie_name: name of the serie
        :param dataset: dataframe (Panda) with datapoints\
        """
        self._serie_name = serie_name
        self._dataset = dataset

        if initialize:
            self._has_trend = self._adf_stationarity_test(self._dataset)

    def _adf_stationarity_test(self, timeseries):

        # Dickey-Fuller test:
        adf_test = adfuller(timeseries[1], autolag='AIC')

        p_value = adf_test[1]

        return p_value >= .05

    def create_model(self):
        pass

    def do_forecast(self):
        pass

    def save(self):
        pass

    @classmethod
    def load(cls, serie_name):
        pass
