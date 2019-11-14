from lib.socketio.socketiohandlers import SocketIoHandler


class SocketIoRouter:

    def __init__(self, sio):
        self._sio = sio

        self._sio.on(
            event='connect',
            handler=SocketIoHandler.connect)
        self._sio.on(
            event='authorize',
            handler=SocketIoHandler.authenticate)
        self._sio.on(
            event='disconnect',
            handler=SocketIoHandler.disconnect)

        self._sio_on(
            event='/api/series',
            handler=SocketIoHandler.get_all_series)
        self._sio_on(
            event='/api/series/create',
            handler=SocketIoHandler.create_series)
        self._sio_on(
            event='/api/series/details',
            handler=SocketIoHandler.get_serie_details)
        self._sio_on(
            event='/api/series/delete',
            handler=SocketIoHandler.remove_series)
        self._sio_on(
            event='/api/enodo/models',
            handler=SocketIoHandler.get_enodo_models)

        self._setup_listen_events()

    def _setup_listen_events(self):
        self._sio_on(
            event='/subscribe/series',
            handler=SocketIoHandler.subscribe_series)
        self._sio_on(
            event='/subscribe/filtered/series',
            handler=SocketIoHandler.subscribe_filtered_series)

    def _sio_on(self, event, handler):
        async def fun(sid, data):
            return await handler(sid, data, event)

        self._sio.on(
            event=event,
            handler=fun)
