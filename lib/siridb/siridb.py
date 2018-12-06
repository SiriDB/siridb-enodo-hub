from siridb.connector import SiriDBClient
from siridb.connector.lib.exceptions import QueryError


class SiriDB:
    siri = None

    @classmethod
    async def prepare(cls):
        cls.siri = SiriDBClient(
            username='iris',
            password='siri',
            dbname='testdata_1',
            hostlist=[('localhost', 9000)],  # Multiple connections are supported
            keepalive=True)

    @classmethod
    async def query_serie_datapoint_count(cls, serie_name):
        await cls.siri.connect()
        count = None
        try:
            result = await cls.siri.query(f'select count() from "{serie_name}"')
        except QueryError as e:
            pass
        else:
            count = result.get(serie_name, [])[0][1]
        cls.siri.close()
        return count
