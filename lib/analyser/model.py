import json
import os

from enodo import EnodoModel
from lib.config.config import Config
from lib.util import safe_json_dumps
from lib.socketio import SUBSCRIPTION_CHANGE_TYPE_ADD, SUBSCRIPTION_CHANGE_TYPE_DELETE


class EnodoModelManager:
    models = None

    @classmethod
    async def async_setup(cls, update_cb):
        cls.models = []
        cls._update_cb = update_cb

    @classmethod
    async def get_model(cls, model_name):
        for m in cls.models:
            if m.model_name == model_name:
                return m
        return None

    @classmethod
    async def add_model(cls, model_name, model_arguments):
        if await cls.get_model(model_name) is None:
            model = EnodoModel(model_name, model_arguments)
            cls.models.append(model)
            await cls._update_cb(SUBSCRIPTION_CHANGE_TYPE_ADD, await EnodoModel.to_dict(model))

    @classmethod
    async def add_model_from_dict(cls, dict_data):
        try:
            model = await EnodoModel.from_dict(dict_data)
        except Exception as e:
            return False
        else:
            if await cls.get_model(model.model_name) is None:
                cls.models.append(model)
                await cls._update_cb(SUBSCRIPTION_CHANGE_TYPE_ADD, await EnodoModel.to_dict(model))
                return True
            return False

    @classmethod
    async def remove_model(cls, model_name):
        model = await cls.get_model(model_name)
        cls.models.remove(model)
        await cls._update_cb(SUBSCRIPTION_CHANGE_TYPE_DELETE, model_name)

    @classmethod
    async def load_from_disk(cls):
        try:
            if not os.path.exists(Config.model_save_path):
                raise Exception()
            f = open(Config.model_save_path, "r")
            data = f.read()
            f.close()
        except Exception as e:
            data = "{}"

        data = json.loads(data)

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
            f = open(Config.model_save_path, "w")
            f.write(json.dumps(model_list, default=safe_json_dumps))
            f.close()
        except Exception as e:
            logging.error(f"Something went wrong when writing enodo models to disk")
            logging.debug(f"Corresponding error: {e}")
