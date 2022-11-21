from asyncore import loop
import datetime
import functools
import json
import logging
import re
import struct
from packaging import version
import urllib.parse

from enodo.jobs import (
    JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES, JOB_TYPE_FORECAST_SERIES)

MIN_DATA_FILE_VERSION = "1.0.0"
CURRENT_DATA_FILE_VERSION = "1.0.0"
LOOKUP_SZ = 8192


def _json_datetime_serializer(o):
    if isinstance(o, datetime.datetime):
        return datetime.datetime.timestamp(o)


def safe_json_dumps(data):
    return json.dumps(data, default=_json_datetime_serializer)


def print_custom_aiohttp_startup_message(line):
    if line.startswith("======== Running on"):
        logging.info('REST API is up')


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
        JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES: "anomalies"
    }
    return f"enodo_{series_name}_{job_prefix_mapping[job_type]}_{config_name}"


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
                if 'data' not in resp:
                    return resp  # might be an error
                resp = resp.get('data', [] if is_list else {})
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
                if 'data' not in resp:
                    return resp  # might be an error
                resp = resp.get('data', [] if is_list else {})
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


def generate_worker_lookup(worker_count: int) -> dict:
    lookup = {}
    for i in range(LOOKUP_SZ):
        lookup[i] = 0
    counters = {}
    for n in range(1, worker_count):
        m = n+1
        for i in range(n):
            counters[i] = i
        for i in range(LOOKUP_SZ):
            lookup[i]
            counters[lookup[i]] += 1
            if counters[lookup[i]] % m == 0:
                lookup[i] = n
    return lookup


def get_worker_for_series(lookup: dict, series_name: str) -> int:
    n = sum(bytearray(series_name, encoding='utf-8'))
    return lookup[n % LOOKUP_SZ]


def gen_worker_idx(pool_id: int, job_type_id: int,  idx: int):
    binary_data = struct.pack('>III', pool_id, job_type_id, idx)
    return int.from_bytes(binary_data, 'big')


def decode_worker_id(wid: int):
    try:
        return struct.unpack('>III', wid.to_bytes(12, byteorder="big"))
    except Exception:
        return False


def gen_pool_idx(pool_id: int, job_type_id: int) -> int:
    ba = pool_id.to_bytes(4, byteorder='big') + \
        job_type_id.to_bytes(4, byteorder='big')
    return int.from_bytes(ba, 'big')
