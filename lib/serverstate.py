class ServerState:
    running = None

    @classmethod
    async def async_setup(cls):
        cls.running = True
