from siridb.connector.lib.exceptions import QueryError, InsertError, ServerError, PoolError, AuthenticationError, \
    UserAuthError


# @classmethod
async def query_series_datapoint_count(siridb_client, series_name):
    count = None
    try:
        result = await siridb_client.query(f'select count() from "{series_name}"')
    except (QueryError, InsertError, ServerError, PoolError, AuthenticationError, UserAuthError) as e:
        print(e)
        print("Connection problem with SiriDB server")
        pass
    else:
        count = result.get(series_name, [])[0][1]
    return count


async def does_series_exist(siridb_client, series_name):
    exists = False
    try:
        result = await siridb_client.query(f'select count() from "{series_name}"')
        if result.get(series_name) is not None:
            exists = True
    except (QueryError, InsertError, ServerError, PoolError, AuthenticationError, UserAuthError) as e:
        print("Connection problem with SiriDB server")
        pass
    return exists


# @classmethod
async def query_series_data(siridb_client, series_name, selector="*"):
    result = None
    try:
        result = await siridb_client.query(f'select {selector} from "{series_name}"')
    except (QueryError, InsertError, ServerError, PoolError, AuthenticationError, UserAuthError) as e:
        print("Connection problem with SiriDB server")
        pass
    return result


async def drop_series(siridb_client, series_name):
    result = None
    try:
        result = await siridb_client.query(f'drop series "{series_name}"')
    except (QueryError, InsertError, ServerError, PoolError, AuthenticationError, UserAuthError) as e:
        print("Connection problem with SiriDB server")
        pass
    return result


async def insert_points(siridb_client, series_name, points):
    result = None
    try:
        await siridb_client.insert({series_name: points})
    except (QueryError, InsertError, ServerError, PoolError, AuthenticationError, UserAuthError) as e:
        print("Connection problem with SiriDB server")
        print(e)
        pass
    return result
