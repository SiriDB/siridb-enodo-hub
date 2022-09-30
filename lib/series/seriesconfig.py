import json
import logging
from enodo.model.config.series import SeriesConfigModel

from lib.state.resource import StoredResource


class SeriesConfig(dict, StoredResource):

    def __init__(self, name, description, config, rid=None):
        if 'job_config' not in config:
            logging.error(
                f"Invalid config: {json.dumps(config)}")
        try:
            config = SeriesConfigModel(**config)
        except Exception as e:
            print(e)
            logging.error(f"Invalid job config {rid}")
            raise e

        super(SeriesConfig, self).__init__({
            "rid": rid,
            "name": name,
            "description": description,
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
