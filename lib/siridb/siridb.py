from siridb.connector import SiriDBClient
from siridb.connector.lib.exceptions import QueryError, InsertError, ServerError, PoolError, AuthenticationError, \
    UserAuthError

from lib.config.config import Config


class SiriDB:
    siri = None
    siridb_connected = False
    siridb_status = ""

    def __init__(self):
        self.siri = SiriDBClient(
            username=Config.siridb_user,
            password=Config.siridb_password,
            dbname=Config.siridb_database,
            hostlist=[(Config.siridb_host, Config.siridb_port)],  # Multiple connections are supported
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
            print(serie_name, result)
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

    async def test_connection(self):
        try:
            await self.siri.connect()
        except Exception:
            return "Cannot connect", False

        try:
            await self.siri.query(f'show dbname')
        except (QueryError, InsertError, ServerError, PoolError, AuthenticationError, UserAuthError) as e:
            return repr(e), False
        else:
            return "", True
