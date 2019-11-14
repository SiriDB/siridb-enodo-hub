ARIMA_MODEL = 1
PROPHET_MODEL = 2
AR_MODEL = 3
MA_MODEL = 4

MODEL_NAMES = {ARIMA_MODEL: 'ARIMA', PROPHET_MODEL: 'Prophet', AR_MODEL: 'Autoregression', MA_MODEL: 'Moving average'}
MODEL_PARAMETERS = {ARIMA_MODEL: ['m', 'd', 'D'], PROPHET_MODEL: [], AR_MODEL: [], MA_MODEL: []}

GENERAL_PARAMETERS = {
    'forecast_points_in_future': 10,
    'min_points_for_forecast': 100,
    # 'anomaly_detection_level': '',
    'use_data_since_timestamp': None,
}


async def setup_default_model_arguments(model_arguments):
    for key in GENERAL_PARAMETERS.keys():
        if key not in model_arguments:
            model_arguments[key] = GENERAL_PARAMETERS[key]

    return model_arguments


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
