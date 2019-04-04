from lib.analyser.analysis import Analysis


class Serie:
    _name = None
    _datapoints_count = None
    _serie_type = None
    _datapoints_count_lock = None
    _analysed = None
    _serie_parameters = None

    def __init__(self, name, datapoints_count=None, serie_type="ms", serie_parameters=None):
        self._name = name
        self._datapoints_count = datapoints_count
        self._serie_type = serie_type
        self._datapoints_count_lock = False
        self._analysed = False
        self._serie_parameters = serie_parameters if serie_parameters is not None else dict()

    async def set_datapoints_counter_lock(self, is_locked):
        self._datapoints_count_lock = is_locked

    async def get_datapoints_counter_lock(self):
        return self._datapoints_count_lock

    async def get_name(self):
        return self._name

    async def get_type(self):
        return self._serie_type

    async def get_datapoints_count(self):
        return self._datapoints_count

    async def add_to_datapoints_count(self, add_to_count):
        if self._datapoints_count_lock is False:
            self._datapoints_count += add_to_count

    async def get_forecast(self):
        if self._analysed:
            analysis = await Analysis.load(self._name)
            if analysis is not None:
                return analysis.forecast_values

        return None

    async def to_dict(self):
        return {
            'name': self._name,
            'type': self._serie_type,
            'data_points': self._datapoints_count,
            'analysed': self._analysed,
            'parameters': self._serie_parameters
        }

    async def get_analysed(self):
        return self._analysed

    async def set_analysed(self, analysed):
        self._analysed = analysed

    async def get_serie_parameters(self):
        return self._serie_parameters
