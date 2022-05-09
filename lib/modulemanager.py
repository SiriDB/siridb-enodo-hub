from enodo import EnodoModule

from lib.socketio import SUBSCRIPTION_CHANGE_TYPE_ADD, \
    SUBSCRIPTION_CHANGE_TYPE_DELETE


class EnodoModuleManager:
    modules = None

    @classmethod
    async def async_setup(cls, update_cb):
        cls.modules = []
        cls._update_cb = update_cb

    @classmethod
    async def get_module(cls, name):
        for m in cls.modules:
            if m.name == name:
                return m
        return None

    @classmethod
    async def add_module(cls, name, module_arguments):
        if await cls.get_module(name) is None:
            module = EnodoModule(name, module_arguments)
            cls.modules.append(module)
            await cls._update_cb(
                SUBSCRIPTION_CHANGE_TYPE_ADD, module)

    @classmethod
    async def add_enodo_module(cls, module):
        if await cls.get_module(module.name) is None:
            cls.modules.append(module)
            await cls._update_cb(
                SUBSCRIPTION_CHANGE_TYPE_ADD, module)
            return True
        return False

    @classmethod
    async def remove_module(cls, name):
        module = await cls.get_module(name)
        cls.modules.remove(module)
        await cls._update_cb(SUBSCRIPTION_CHANGE_TYPE_DELETE, name)
