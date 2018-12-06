class Serie:
    name = None
    datapoints_count = None
    serie_type = None
    datapoints_count_lock = None

    def __init__(self, name, datapoints_count, serie_type="miliseconds"):
        self.name = name
        self.datapoints_count = datapoints_count
        self.type = serie_type
        self.datapoints_count_lock = False

    async def set_datapoints_counter_lock(self, is_locked):
        self.datapoints_count_lock = is_locked

    async def get_datapoints_counter_lock(self):
        return self.datapoints_count_lock

    async def get_name(self):
        return self.name

    async def get_type(self):
        return self.serie_type

    async def get_datapoints_count(self):
        return self.datapoints_count

    async def add_to_datapoints_count(self, add_to_count):
        if self.datapoints_count_lock is False:
            self.datapoints_count += add_to_count
