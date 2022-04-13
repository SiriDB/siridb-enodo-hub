import os
import logging

from enodo import EnodoModule

from lib.config import Config
from lib.util import load_disk_data, save_disk_data
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

    @classmethod
    async def load_from_disk(cls):
        try:
            if not os.path.exists(Config.module_save_path):
                raise Exception()
            data = load_disk_data(Config.module_save_path)
        except Exception as e:
            data = {}

        if isinstance(data, list):
            for module_data in data:
                module = EnodoModule(**module_data)
                cls.modules.append(module)

    @classmethod
    async def save_to_disk(cls):
        module_list = []
        if cls.modules is None:
            return
        for module in cls.modules:
            module_list.append(module)

        try:
            save_disk_data(Config.module_save_path, module_list)
        except Exception as e:
            logging.error("Something went wrong when writing"
                          "enodo modules to disk")
            logging.debug(f"Corresponding error: {e}, "
                          f'exception class: {e.__class__.__name__}')
