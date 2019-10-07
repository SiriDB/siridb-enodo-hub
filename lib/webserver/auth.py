from aiohttp_basicauth import BasicAuthMiddleware


class EnodoAuth:
    auth = BasicAuthMiddleware(username=None, password=None, force=False)
