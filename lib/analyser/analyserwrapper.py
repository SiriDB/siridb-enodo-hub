ARIMA_MODEL = 1
PROPHET_MODEL = 2
AR_MODEL = 3
MA_MODEL = 4

MODEL_NAMES = {ARIMA_MODEL: 'ARIMA', PROPHET_MODEL: 'Prophet', AR_MODEL: 'Autoregression', MA_MODEL: 'Moving average'}
MODEL_PARAMETERS = {ARIMA_MODEL: ['m', 'd', 'D'], PROPHET_MODEL: [], AR_MODEL: [], MA_MODEL: []}


class AnalyserWrapper:
    _analyser_model = None
    _model_type = None
    _model_arguments = None

    def __init__(self, model, model_type, arguments):
        self._analyser_model = model
        self._model_type = model_type
        self._model_arguments = arguments

    def __dict__(self):
        return {
            '_analyser_model': self._analyser_model,
            '_model_type': self._model_type,
            '_model_arguments': self._model_arguments
        }
