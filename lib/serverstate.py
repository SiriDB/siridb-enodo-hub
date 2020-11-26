from siridb.connector import SiriDBClient


class ServerState:
    running = None
    sio = None
    siridb_data_client = None
    siridb_forecast_client = None

    @classmethod
    async def async_setup(cls, sio, siridb_data_username,
                          siridb_data_password,
                          siridb_data_dbname,
                          siridb_data_hostlist,
                          siridb_forecast_username,
                          siridb_forecast_password,
                          siridb_forecast_dbname,
                          siridb_forecast_hostlist):
        cls.running = True
        cls.sio = sio
        
        cls.siridb_data_client = SiriDBClient(
            username=siridb_data_username,
            password=siridb_data_password,
            dbname=siridb_data_dbname,
            hostlist=siridb_data_hostlist,  # Multiple connections are supported
            keepalive=True)
        await cls.siridb_data_client.connect()
        cls.siridb_forecast_client = SiriDBClient(
            username=siridb_forecast_username,
            password=siridb_forecast_password,
            dbname=siridb_forecast_dbname,
            hostlist=siridb_forecast_hostlist,  # Multiple connections are supported
            keepalive=True)
        await cls.siridb_forecast_client.connect()

    @classmethod
    def stop(cls):
        cls.siridb_data_client.close()
        cls.siridb_forecast_client.close()
