ARIMA_MODEL = 1
PROPHET_MODEL = 2


class AnalyserWrapper:
    _analyser_model = None
    _model_type = None
    _model_arguments = None

    def __init__(self, model, model_type, arguments):
        self._analyser_model = model
        self._model_type = model_type
        self._model_arguments = arguments
