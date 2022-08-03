import logging
import re

from siridb.connector.lib.exceptions import (AuthenticationError, InsertError,
                                             PoolError, QueryError,
                                             ServerError, UserAuthError)


# @classmethod
async def query_series_datapoint_count(siridb_client, series_name):
    count = None
    try:
        result = await siridb_client.query(
            f'select count() from "{series_name}"')
    except (QueryError, InsertError, ServerError, PoolError,
            AuthenticationError, UserAuthError) as e:
        logging.error("Connection problem with SiriDB server")
        msg = str(e) or type(e).__name__
        logging.debug(f"Problem with SiriDB: {msg}")
    else:
        counts = result.get(series_name, [])
        if counts:
            count = counts[0][1]
    return count


async def query_time_unit(siridb_client):
    result = None
    try:
        result = await siridb_client.query(
            f'show time_precision')
    except (QueryError, InsertError, ServerError, PoolError,
            AuthenticationError, UserAuthError) as e:
        logging.error("Connection problem with SiriDB server")
        msg = str(e) or type(e).__name__
        logging.debug(f"Problem with SiriDB: {msg}")
    if result is None:
        return None
    return result["data"][0]["value"]


async def query_series_data(siridb_client, series_name, selector="*"):
    result = None
    try:
        result = await siridb_client.query(
            f'select {selector} from "{series_name}"')
    except (QueryError, InsertError, ServerError, PoolError,
            AuthenticationError, UserAuthError) as e:
        logging.error("Connection problem with SiriDB server")
        msg = str(e) or type(e).__name__
        logging.debug(f"Problem with SiriDB: {msg}")
        pass
    return result


async def drop_series(siridb_client, series_name):
    result = None
    try:
        result = await siridb_client.query(f'drop series {series_name}')
    except (QueryError, InsertError, ServerError, PoolError,
            AuthenticationError, UserAuthError) as e:
        logging.error("Connection problem with SiriDB server")
        msg = str(e) or type(e).__name__
        logging.debug(f"Problem with SiriDB: {msg}")
    return result


async def insert_points(siridb_client, series_name, points):
    result = None
    try:
        await siridb_client.insert({series_name: points})
    except (QueryError, InsertError, ServerError, PoolError,
            AuthenticationError, UserAuthError) as e:
        logging.error("Connection problem with SiriDB server")
        msg = str(e) or type(e).__name__
        logging.debug(f"Problem with SiriDB: {msg}")
    return result


async def query_group_expression_by_name(siridb_client, group_name):
    result = None
    try:
        result = await siridb_client.query(
            f'list groups where name == "{group_name}"')
    except (QueryError, InsertError, ServerError, PoolError,
            AuthenticationError, UserAuthError) as e:
        logging.error("Connection problem with SiriDB server")
        msg = str(e) or type(e).__name__
        logging.debug(f"Problem with SiriDB: {msg}")
    groups = result.get('groups')
    if groups is None or len(groups) < 1:
        return None

    return groups[0][0]


async def query_all_series_results(siridb_client, series_name, selector="*"):
    result = None
    try:
        result = await siridb_client.query(
            f'select {selector} from '
            f'/enodo_{re.escape(series_name)}_.*?$/')
    except (QueryError, InsertError, ServerError, PoolError,
            AuthenticationError, UserAuthError) as e:
        logging.error("Connection problem with SiriDB server")
        msg = str(e) or type(e).__name__
        logging.debug(f"Problem with SiriDB: {msg}")
    return result


async def query_series_forecasts(siridb_client, series_name, selector="*",
                                 only_future=False):
    result = None
    after = ""
    if only_future:
        after = " after now"
    try:
        result = await siridb_client.query(
            f'select {selector} from '
            f'/enodo_{re.escape(series_name)}_forecast_.*?$/{after}')
    except (QueryError, InsertError, ServerError, PoolError,
            AuthenticationError, UserAuthError) as e:
        logging.error("Connection problem with SiriDB server")
        msg = str(e) or type(e).__name__
        logging.debug(f"Problem with SiriDB: {msg}")
    return result


async def query_series_anomalies(siridb_client, series_name, selector="*"):
    result = None
    try:
        result = await siridb_client.query(
            f'select {selector} from '
            f'/enodo_{re.escape(series_name)}_anomalies_.*?$/')
    except (QueryError, InsertError, ServerError, PoolError,
            AuthenticationError, UserAuthError) as e:
        logging.error("Connection problem with SiriDB server")
        msg = str(e) or type(e).__name__
        logging.debug(f"Problem with SiriDB: {msg}")
    return result


async def query_series_static_rules_hits(
        siridb_client, series_name, selector="*"):
    result = None
    try:
        result = await siridb_client.query(
            f'select {selector} from '
            f'/enodo_{re.escape(series_name)}_static_rules_.*?$/')
    except (QueryError, InsertError, ServerError, PoolError,
            AuthenticationError, UserAuthError) as e:
        logging.error("Connection problem with SiriDB server")
        msg = str(e) or type(e).__name__
        logging.debug(f"Problem with SiriDB: {msg}")
    return result
