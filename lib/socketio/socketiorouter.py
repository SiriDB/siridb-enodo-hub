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
            handler=SocketIoHandler.get_series_details)
        self._sio_on(
            event='/api/series/delete',
            handler=SocketIoHandler.remove_series)
        self._sio_on(
            event='/api/series/update',
            handler=SocketIoHandler.update_series)
        self._sio_on(
            event='/api/event/output',
            handler=SocketIoHandler.get_event_outputs)
        self._sio_on(
            event='/api/event/output/create',
            handler=SocketIoHandler.add_event_output)
        self._sio_on(
            event='/api/event/output/update',
            handler=SocketIoHandler.update_event_output)
        self._sio_on(
            event='/api/event/output/delete',
            handler=SocketIoHandler.remove_event_output)
        self._sio_on(
            event='/api/enodo/models',
            handler=SocketIoHandler.get_enodo_models)
        self._sio_on(
            event='/api/enodo/status',
            handler=SocketIoHandler.get_enodo_hub_status)
        self._sio_on(
            event='/api/job/open',
            handler=SocketIoHandler.get_open_jobs)
        self._sio_on(
            event='/api/job/active',
            handler=SocketIoHandler.get_active_jobs)
        self._sio_on(
            event='/api/job/failed',
            handler=SocketIoHandler.get_failed_jobs)

        self._setup_listen_events()

    def _setup_listen_events(self):
        self._sio_on(
            event='/subscribe/series',
            handler=SocketIoHandler.subscribe_series)
        self._sio_on(
            event='/subscribe/queue',
            handler=SocketIoHandler.subscribe_queue)
        self._sio_on(
            event='/subscribe/filtered/series',
            handler=SocketIoHandler.subscribe_filtered_series)
        self._sio_on(
            event='/subscribe/enodo/model',
            handler=SocketIoHandler.subscribe_enodo_models)
        self._sio_on(
            event='/subscribe/event/output',
            handler=SocketIoHandler.subscribe_event_output)

    def _sio_on(self, event, handler):
        async def fun(sid, data):
            return await handler(sid, data, event)

        self._sio.on(
            event=event,
            handler=fun)
