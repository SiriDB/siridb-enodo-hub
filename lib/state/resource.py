from abc import abstractproperty
import functools
from lib.serverstate import ServerState


class StoredResource:

    @staticmethod
    def async_changed(func):
        @functools.wraps(func)
        async def wrapped(self, *args, **kwargs):
            resp = await func(self, *args, **kwargs)
            if self.should_be_stored:
                ServerState.storage.store(self)
            return resp
        return wrapped

    @staticmethod
    def changed(func):
        @functools.wraps(func)
        def wrapped(self, *args, **kwargs):
            resp = func(self, *args, **kwargs)
            if self.should_be_stored:
                ServerState.storage.store(self)
            return resp
        return wrapped

    def created(self):
        if self.should_be_stored:
            ServerState.storage.store(self)

    @property
    def should_be_stored(self):
        return True

    @abstractproperty
    def resource_type(self):
        pass

    @property
    def to_store_data(self):
        return self.to_dict()
