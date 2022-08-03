import json
import logging
from enodo.model.config.series import SeriesJobConfigModel

from lib.state.resource import StoredResource


class SeriesConfigTemplate(StoredResource, dict):

    def __init__(self, name, description, config, rid=None):
        if 'job_config' not in config:
            logging.error(
                f"Invalid template config: {json.dumps(config)}")
        for job_config in config.get('job_config'):
            try:
                SeriesJobConfigModel(**job_config)
            except Exception as e:
                raise Exception(
                    f"Invalid job config for template {rid}")

        super(SeriesConfigTemplate, self).__init__({
            "rid": rid,
            "name": name,
            "description": description,
            "config": config
        })

    @classmethod
    @property
    def resource_type(cls):
        return "series_config_templates"

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
