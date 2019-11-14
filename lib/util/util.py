import datetime
import json
import logging
import re


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
