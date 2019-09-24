import datetime
import json


def _json_datetime_serializer(o):
    if isinstance(o, datetime.datetime):
        return datetime.datetime.timestamp(o)


def safe_json_dumps(data):
    return json.dumps(data, default=_json_datetime_serializer)
