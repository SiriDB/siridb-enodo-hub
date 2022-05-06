import datetime
import functools
import json
import logging
import re
from packaging import version

MIN_DATA_FILE_VERSION = "1.0.0"
CURRENT_DATA_FILE_VERSION = "1.0.0"


def _json_datetime_serializer(o):
    if isinstance(o, datetime.datetime):
        return datetime.datetime.timestamp(o)


def safe_json_dumps(data):
    return json.dumps(data, default=_json_datetime_serializer)


def print_custom_aiohttp_startup_message(line):
    if line.startswith("======== Running on"):
        logging.info('REST API is up')


def regex_valid(regex):
    try:
        re.compile(regex)
        return True
    except re.error:
        return False


def load_disk_data(path):
    f = open(path, "r")
    data = f.read()
    f.close()
    data = json.loads(data)

    if data.get("version") is None or \
        version.parse(data.get("version")) < \
            version.parse(MIN_DATA_FILE_VERSION):
        logging.error(
            "Error: "
            f"invalid data file version found at {path}, "
            "please update the file structure or remove it "
            "before proceeding")
        raise Exception("Invalid data file version")

    return data.get('data')


def save_disk_data(path, data):
    try:
        f = open(path, "r")
        current_data = f.read()
        f.close()
        save_data = json.loads(current_data)
    except Exception as e:
        save_data = {}

    if save_data is None or save_data.get("version") is None:
        save_data['version'] = CURRENT_DATA_FILE_VERSION
    save_data["data"] = data

    f = open(path, "w+")
    f.write(json.dumps(save_data, default=safe_json_dumps))
    f.close()


def cls_lock():
    def wrapper(func):
        @functools.wraps(func)
        async def wrapped(*args, **kwargs):
            if len(args) > 0 and hasattr(args[0], "_lock"):
                async with args[0]._lock:
                    return await func(*args, **kwargs)
            else:
                raise Exception("Incorrect usage of cls_lock func")
        return wrapped
    return wrapper
