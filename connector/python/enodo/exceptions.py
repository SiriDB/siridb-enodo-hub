class EnodoError(Exception):
    def __init__(self, *args, errdata=None):
        if isinstance(errdata, dict):
            args = (errdata['error_msg'],)
            self.error_code = errdata['error_code']
        super().__init__(*args)


class EnodoConnectionError(EnodoError):
    pass
