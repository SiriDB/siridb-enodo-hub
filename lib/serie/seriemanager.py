
class SerieManager:
        _seriesRequestedForAnalysis = ['some_measurement']

        @classmethod
        async def read_state(cls):
            # ToDo
            pass

        @classmethod
        async def is_serie_allowed(cls, serie_name):
            return serie_name in cls._seriesRequestedForAnalysis

