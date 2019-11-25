class EnodoJobDataModel:
    def __init__(self):
        pass

    def to_dict(self):
        return None


class EnodoForecastJobDataModel(EnodoJobDataModel):
    def __init__(self):
        super().__init__()


class EnodoAnomalyDetectionJobDataModel(EnodoJobDataModel):
    def __init__(self, points_since):
        super().__init__()
        self.points_since = points_since

    def to_dict(self):
        return {
            'points_since': self.points_since
        }
