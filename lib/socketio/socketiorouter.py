from lib.socketio.socketiohandlers import SocketIoHandler


class SocketIoRouter:

    def __init__(self, sio):
        self._sio = sio

        self._sio_on(
            event='/api/series',
            handler=SocketIoHandler.get_all_series)
        self._sio_on(
            event='/api/series/create',
            handler=SocketIoHandler.get_all_series)
        self._sio_on(
            event='/api/series/details',
            handler=SocketIoHandler.get_all_series)
        self._sio_on(
            event='/api/series/delete',
            handler=SocketIoHandler.get_all_series)
        self._sio_on(
            event='/api/enodo/models',
            handler=SocketIoHandler.get_all_series)

    def _sio_on(self, event, handler):
        async def fun(sid, data):
            return await handler(sid, data, event)

        self._sio.on(
            event=event,
            handler=fun)
