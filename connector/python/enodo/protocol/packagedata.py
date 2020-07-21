import json
import logging
from abc import ABC, abstractmethod

class EnodoJobDataModel():

    def __init__(self, **kwargs):
        self._dict_values = kwargs
        if not self.validate():
            raise Exception("invalid data for packaga data")
        
        # self.__dict__ = json.loads(self._raw_data)

    def validate(self):
        if self.required_fields is not None:
            for key in self.required_fields:
                if key not in self._dict_values.keys():
                    logging.info(f"Missing '{key}' in enodo job data model data")
                    return False
        return "job_type" in self._dict_values.keys()

    @property
    @abstractmethod
    def required_fields(self):
        """ return the sound the animal makes """

    def get(self, key):
        return self._dict_values.get(key)

    def serialize(self):
        return json.dumps(self._dict_values)

    @classmethod
    def unserialize(cls, json_data):
        data = json.loads(json_data)
        print("DICT", data)
        model_type = data.get("job_type")

        if model_type == "forecast_request":
            return EnodoForecastJobRequestDataModel(**data)
        elif model_type == "forecast_response":
            return EnodoForecastJobResponseDataModel(**data)
        elif model_type == "anomaly_request":
            return EnodoDetectAnomaliesJobRequestDataModel(**data)
        elif model_type == "anomaly_response":
            return EnodoDetectAnomaliesJobResponseDataModel(**data)
        elif model_type == "base_request":
            return EnodoBaseAnalysisJobRequestDataModel(**data)
        elif model_type == "base_response":
            return EnodoBaseAnalysisJobResponseDataModel(**data)
        

        return None

class EnodoForecastJobRequestDataModel(EnodoJobDataModel):

    def __init__(self, **kwargs):
        kwargs['job_type'] = "forecast_request"
        super().__init__(**kwargs)

    @property
    def required_fields(self):
        return [
            "serie_name",
            "model_name",
            "model_parameters"
        ]

class EnodoForecastJobResponseDataModel(EnodoJobDataModel):

    def __init__(self, **kwargs):
        kwargs['job_type'] = "forecast_response"
        super().__init__(**kwargs)

    @property
    def required_fields(self):
        return [
            "successful",
            "forecast_points"
        ]

class EnodoDetectAnomaliesJobRequestDataModel(EnodoJobDataModel):

    def __init__(self, **kwargs):
        kwargs['job_type'] = "anomaly_request"
        super().__init__(**kwargs)

    @property
    def required_fields(self):
        return [
            "serie_name",
            "model_name",
            "model_parameters"
        ]

class EnodoDetectAnomaliesJobResponseDataModel(EnodoJobDataModel):

    def __init__(self, **kwargs):
        kwargs['job_type'] = "anomaly_response"
        super().__init__(**kwargs)

    @property
    def required_fields(self):
        return [
            "successful",
            "flagged_anomaly_points"
        ]


class EnodoBaseAnalysisJobRequestDataModel(EnodoJobDataModel):

    def __init__(self, **kwargs):
        kwargs['job_type'] = "base_request"
        super().__init__(**kwargs)

    @property
    def required_fields(self):
        return [
            "serie_name"
        ]

class EnodoBaseAnalysisJobResponseDataModel(EnodoJobDataModel):

    def __init__(self, **kwargs):
        kwargs['job_type'] = "base_reponse"
        super().__init__(**kwargs)

    @property
    def required_fields(self):
        return [
            "successful",
            "trend_slope_value",
            "noise_value",
            "has_seasonality",
            "health_of_serie"
        ]