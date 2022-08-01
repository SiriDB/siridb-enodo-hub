from enodo.model.config.series import SeriesJobConfigModel

from lib.state.resource import StoredResource


# {
#    "name":"Static rules on Forecast template",
#    "description":"Apply static rules on the result of a forecast job",
#    "category":"thresholds",
#    "job_config_template":{
#       "activated":true,
#       "job_type":"job_static_rules",
#       "module":"static_rules@0.1.0-beta3.2.2",
#       "job_schedule_type":"N",
#       "job_schedule":200,
#       "max_n_points":1000,
#       "module_params":{
#          "min_threshold":800,
#          "max_threshold":3200,
#          "delta_threshold":1,
#          "last_n_points":200,
#       }
#    }
# }


class SeriesJobConfigTemplate(StoredResource, dict):

    def __init__(self, rid, name, description, category,
                 job_config_template):

        try:
            job_config_template = SeriesJobConfigModel(
                **job_config_template)
        except Exception:
            raise Exception(f"Invalid job config for template {rid}")

        super(SeriesJobConfigTemplate, self).__init__({
            "rid": rid,
            "name": name,
            "description": description,
            "category": category,
            "job_config_template": job_config_template
        })

    @classmethod
    @property
    def resource_type(cls):
        return "job_config_templates"

    @property
    def to_store_data(self):
        return dict(self)

    @property
    def rid(self):
        return self.get("rid")

    @property
    def job_config_template(self):
        return self["job_config_template"]

    @property
    def name(self):
        return self["name"]

    @property
    def category(self):
        return self["category"]

    @property
    def description(self):
        return self["description"]
