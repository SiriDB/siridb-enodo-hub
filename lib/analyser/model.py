import os

from enodo import EnodoModel

from lib.config import Config
from lib.util import load_disk_data, save_disk_data
from lib.socketio import SUBSCRIPTION_CHANGE_TYPE_ADD, \
    SUBSCRIPTION_CHANGE_TYPE_DELETE


class EnodoModelManager:
    models = None

    @classmethod
    async def async_setup(cls, update_cb):
        cls.models = []
        cls._update_cb = update_cb

    @classmethod
    async def get_model(cls, name):
        for m in cls.models:
            if m.name == name:
                return m
        return None

    @classmethod
    async def add_model(cls, name, model_arguments):
        if await cls.get_model(name) is None:
            model = EnodoModel(name, model_arguments)
            cls.models.append(model)
            await cls._update_cb(
                SUBSCRIPTION_CHANGE_TYPE_ADD, EnodoModel.to_dict(model))

    @classmethod
    async def add_enodo_model(cls, model):
        if await cls.get_model(model.name) is None:
            cls.models.append(model)
            await cls._update_cb(
                SUBSCRIPTION_CHANGE_TYPE_ADD, EnodoModel.to_dict(model))
            return True
        return False

    @classmethod
    async def remove_model(cls, name):
        model = await cls.get_model(name)
        cls.models.remove(model)
        await cls._update_cb(SUBSCRIPTION_CHANGE_TYPE_DELETE, name)

    @classmethod
    async def load_from_disk(cls):
        try:
            if not os.path.exists(Config.model_save_path):
                raise Exception()
            data = load_disk_data(Config.model_save_path)
        except Exception as e:
            data = {}

        if isinstance(data, list):
            for model_data in data:
                model = EnodoModel.from_dict(model_data)
                cls.models.append(model)

    @classmethod
    async def save_to_disk(cls):
        model_list = []
        if cls.models is None:
            return
        for model in cls.models:
            model_list.append(EnodoModel.to_dict(model))

        try:
            save_disk_data(Config.model_save_path, model_list)
        except Exception as e:
            logging.error(f"Something went wrong when writing \
                enodo models to disk")
            logging.debug(f"Corresponding error: {e}")
