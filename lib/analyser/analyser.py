class Analyser:
    _series_analysed = None

    @classmethod
    async def prepare(cls):
        cls._series_analysed = set()

    @classmethod
    async def is_serie_analysed(cls, serie_name):
        return serie_name in cls._series_analysed

    @classmethod
    async def analyse_serie(cls, serie_name):
        cls._series_analysed.add(serie_name)
        pass
