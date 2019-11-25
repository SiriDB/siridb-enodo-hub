class EnodoModel:
    __slots__ = ('model_name', 'model_arguments', 'supports_forecasting', 'supports_anomaly_detection')

    def __init__(self, name, model_arguments, supports_forecasting, supports_anomaly_detection):
        """
        :param name:
        :param model_arguments:  in form of  {'key': True} Where key is argument name and
                                    value is wether or not it is mandatory
        :param supports_forecasting:
        :param supports_anomaly_detection:
        """
        self.model_name = name
        self.model_arguments = model_arguments
        self.supports_forecasting = supports_forecasting
        self.supports_anomaly_detection = supports_anomaly_detection

    @classmethod
    async def to_dict(cls, model):
        return {
            'model_name': model.model_name,
            'model_arguments': model.model_arguments,
            'supports_forecasting': model.supports_forecasting,
            'supports_anomaly_detection': model.supports_anomaly_detection
        }

    @classmethod
    async def from_dict(cls, model):
        return EnodoModel(model.get('model_name'),
                          model.get('model_arguments'),
                          model.get('supports_forecasting'),
                          model.get('supports_anomaly_detection'))


