from siridb.connector.lib.exceptions import QueryError, InsertError, ServerError, PoolError, AuthenticationError, \
    UserAuthError


# @classmethod
async def query_serie_datapoint_count(siridb_client, serie_name):
    count = None
    try:
        result = await siridb_client.query(f'select count() from "{serie_name}"')
    except (QueryError, InsertError, ServerError, PoolError, AuthenticationError, UserAuthError) as e:
        print(e)
        print("Connection problem with SiriDB server")
        pass
    else:
        count = result.get(serie_name, [])[0][1]
    return count


async def does_series_exist(siridb_client, series_name):
    exists = False
    try:
        result = await siridb_client.query(f'select count() from "{serie_name}"')
        if result.get(series_name) is not None:
            exists = True
    except (QueryError, InsertError, ServerError, PoolError, AuthenticationError, UserAuthError) as e:
        print("Connection problem with SiriDB server")
        pass
    return exists


# @classmethod
async def query_serie_data(siridb_client, serie_name, selector="*"):
    result = None
    try:
        result = await siridb_client.query(f'select {selector} from "{serie_name}"')
    except (QueryError, InsertError, ServerError, PoolError, AuthenticationError, UserAuthError) as e:
        print("Connection problem with SiriDB server")
        pass
    return result


async def drop_serie(siridb_client, serie_name):
    result = None
    try:
        result = await siridb_client.query(f'drop series "{serie_name}"')
    except (QueryError, InsertError, ServerError, PoolError, AuthenticationError, UserAuthError) as e:
        print("Connection problem with SiriDB server")
        pass
    return result


async def insert_points(siridb_client, serie_name, points):
    result = None
    try:
        await siridb_client.insert({serie_name: points})
    except (QueryError, InsertError, ServerError, PoolError, AuthenticationError, UserAuthError) as e:
        print("Connection problem with SiriDB server")
        pass
    return result
