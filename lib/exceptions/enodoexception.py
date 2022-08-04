class EnodoException(Exception):
    pass


class EnodoInvalidConfigException(EnodoException):
    pass


class EnodoInvalidArgumentException(Exception):
    pass


class EnodoInvalidDatasetSize(Exception):
    pass


class EnodoScheduleException(Exception):

    def __init__(self, message, job_config_name):
        # Call the base class constructor with the parameters it needs
        super().__init__(message)
        self.job_config_name = job_config_name
