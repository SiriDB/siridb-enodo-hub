from siridb.connector import SiriDBClient
from siridb.connector.lib.exceptions import QueryError, InsertError, ServerError, PoolError, AuthenticationError, UserAuthError


class SiriDB:
    siri = None

    def __init__(self):
        self.siri = SiriDBClient(
            username='iris',
            password='siri',
            dbname='testdata_1',
            hostlist=[('localhost', 9000)],  # Multiple connections are supported
            keepalive=True)

    # @classmethod
    async def query_serie_datapoint_count(self, serie_name):
        await self.siri.connect()
        count = None
        try:
            result = await self.siri.query(f'select count() from "{serie_name}"')
        except (QueryError, InsertError, ServerError, PoolError, AuthenticationError, UserAuthError) as e:
            print("Connection problem with SiriDB server")
            pass
        else:
            count = result.get(serie_name, [])[0][1]
        self.siri.close()
        return count

    # @classmethod
    async def query_serie_data(self, serie_name, selector="*"):
        await self.siri.connect()
        result = None
        try:
            result = await self.siri.query(f'select {selector} from "{serie_name}"')
        except (QueryError, InsertError, ServerError, PoolError, AuthenticationError, UserAuthError) as e:
            print("Connection problem with SiriDB server")
            pass
        self.siri.close()
        return result
