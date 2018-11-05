from lib.exceptions.analyserexception import AnalyserException


class MeasurementPackage:

    serie_name = None

    def __init__(self, data):

        keys = data.keys()
        if len(keys) != 1:
            raise AnalyserException('Raw data is not a data point')

        self.serie_name = list(keys)[0]




