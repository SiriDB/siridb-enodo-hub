import json
import logging
import os
from typing import Any
from lib.state import StoredResource, StorageBase


class DiskStorage(StorageBase):

    def __init__(self, base_path: str):
        self._base_path = os.path.join(base_path, "data")
        self.load_as_needed = False

    async def delete(self, resource: StoredResource):
        rid = resource.rid
        resource_type = resource.resource_type
        logging.debug(f"Removing data of {rid} of type {resource_type}")
        file_path = os.path.join(
            self._base_path, resource_type, f"{rid}.json")
        if os.path.isfile(file_path):
            try:
                os.remove(file_path)
            except Exception:
                logging.error("Could not delete file {file_path}")

    async def store(self, resource: StoredResource):
        data = resource.to_store_data
        rid = resource.rid
        resource_type = resource.resource_type
        logging.debug(
            f"Saving data of {rid} of type {resource_type}")
        file_path = os.path.join(
            self._base_path, resource_type, f"{rid}.json")
        if not os.path.exists(os.path.join(
                self._base_path,
                resource_type)):
            os.makedirs(
                os.path.join(
                    self._base_path, resource_type))
        with open(file_path, 'w') as file:
            file.write(json.dumps(data))

    async def load_by_type(self, resource_type: str) -> list:
        resp = []
        type_path = os.path.join(self._base_path, resource_type)
        if not os.path.isdir(type_path):
            return resp
        for filename in os.listdir(type_path):
            file_path = os.path.join(type_path, filename)
            if os.path.isfile(file_path):
                with open(file_path, 'r') as file:
                    file_content = file.read()
                    try:
                        data = json.loads(file_content)
                    except Exception:
                        logging.error("Could not load resource "
                                      f"file {filename}")
                    else:
                        resp.append(data)
        return resp

    async def load_by_type_and_rid(
            self, resource_type: str, rid: Any) -> dict:
        file_path = os.path.join(self._base_path,
                                 resource_type, f"{rid}.json")
        if os.path.isfile(file_path):
            with open(file_path, 'r') as file:
                file_content = file.read()
                try:
                    data = json.loads(file_content)
                except Exception:
                    logging.error(
                        f"Could not load resource file {rid}.json")
                else:
                    return data
        return False

    async def get_all_rids_for_type(self, resource_type: str) -> list:
        resp = []
        type_path = os.path.join(self._base_path, resource_type)
        if not os.path.isdir(type_path):
            return resp
        for filename in os.listdir(type_path):
            file_path = os.path.join(type_path, filename)
            if os.path.isfile(file_path):
                resp.append(filename.removesuffix(".json"))
        return resp

    async def close(self):
        pass
