import json
import logging
from enodo.model.config.series import SeriesConfigModel

from lib.state.resource import StoredResource


class SeriesConfig(StoredResource, SeriesConfigModel):

    def __init__(self, config, rid=None):
        if 'job_config' not in config:
            logging.error(
                f"Invalid config: {json.dumps(config)}")
        try:
            super(SeriesConfigModel, self).__init__(**config)
        except Exception as e:
            raise Exception(
                f"Invalid job config {rid}")

        super(StoredResource, self).__init__({
            "rid": rid,
            "config": config
        })

    @classmethod
    @property
    def resource_type(cls):
        return "series_configs"

    @property
    def to_store_data(self):
        return dict(self)

    @property
    def rid(self):
        return self.get("rid")

    @rid.setter
    def rid(self, value):
        self["rid"] = value

    @property
    def config(self):
        return self["config"]

    @property
    def series_config(self):
        config = self["config"]
        config["rid"] = self["rid"]
        return config

    @property
    def name(self):
        return self["name"]

    @property
    def description(self):
        return self["description"]
