from lib.exceptions.analyserexception import AnalyserException
from lib.serie.seriemanager import SerieManager
from lib.siridb.measurementpackage import MeasurementPackage


class Filter:

    @classmethod
    async def should_handle_data(cls, data):
        try:
            dp = MeasurementPackage(data)
        except AnalyserException as e:
            return False
        else:
            if not await SerieManager.is_serie_allowed(dp.serie_name):
                return False

        return True
