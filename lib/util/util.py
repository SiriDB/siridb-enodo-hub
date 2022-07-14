import datetime
import functools
import json
import logging
import re
from packaging import version
import urllib.parse

from enodo.jobs import (
    JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES, JOB_TYPE_FORECAST_SERIES,
    JOB_TYPE_STATIC_RULES)

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


def parse_output_series_name(series_name, output_series_name):
    # enodo_{series_name}_forecast_{job_config_name}
    type_and_config_name = output_series_name.replace(
        f"enodo_{series_name}_", "")
    job_type = type_and_config_name.split("_")[0]
    config_name = type_and_config_name.replace(f"{job_type}_", "")
    return job_type, config_name


def get_job_config_output_series_name(
        series_name, job_type, config_name):
    job_prefix_mapping = {
        JOB_TYPE_FORECAST_SERIES: "forecast",
        JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES: "anomalies",
        JOB_TYPE_STATIC_RULES: "static_rules"
    }
    return f"enodo_{series_name}_{job_prefix_mapping[job_type]}_{config_name}"


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


def apply_fields_filter(resources: list, fields: list) -> list:
    resources = [dict(t) for t in resources]
    if fields is not None:
        resources = [
            filter_single_dict(resource, fields)
            for resource in resources]
    return resources


def filter_single_dict(
        resource: dict, fields: list) -> dict:
    return {k: v for k, v in resource.items() if k in fields}


def async_simple_fields_filter(
        is_list=True, is_wrapped=True, return_index=None):
    def wrapper(func):
        @functools.wraps(func)
        async def wrapped(*args, **kwargs):
            fields = kwargs.pop('fields', None)
            resp = await func(*args, **kwargs)
            if return_index is not None:
                org_resp = list(resp)
                resp = resp[return_index]
            if is_wrapped:
                resp = resp.get('data', [])
            if fields is not None:
                if is_list is True:
                    resp = apply_fields_filter(resp, fields)
                else:
                    resp = filter_single_dict(resp, fields)
            if is_wrapped:
                resp = {"data": resp}
            if return_index is not None:
                org_resp[return_index] = resp
                resp = tuple(org_resp)
            return resp
        return wrapped
    return wrapper


def sync_simple_fields_filter(
        is_list=True, is_wrapped=True, return_index=None):
    def wrapper(func):
        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            fields = kwargs.pop('fields', None)
            resp = func(*args, **kwargs)
            if return_index is not None:
                org_resp = list(resp)
                resp = resp[return_index]
            if is_wrapped:
                resp = resp.get('data', [])
            if fields is not None:
                if is_list is True:
                    resp = apply_fields_filter(resp, fields)
                else:
                    resp = filter_single_dict(resp, fields)
            if is_wrapped:
                resp = {"data": resp}
            if return_index is not None:
                org_resp[return_index] = resp
                resp = tuple(org_resp)
            return resp
        return wrapped
    return wrapper


def implement_fields_query(func):
    @functools.wraps(func)
    async def wrapped(cls, request):
        fields = None
        if "fields" in request.rel_url.query:
            fields = urllib.parse.unquote(
                request.rel_url.query['fields'])
            if fields == "":
                fields = None
            if fields is not None:
                fields = fields.split(",")
        return await func(cls, request, fields=fields)
    return wrapped
