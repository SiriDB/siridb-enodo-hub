from abc import abstractmethod
from typing import Any

from lib.state.resource import StoredResource


class StorageBase:

    load_as_needed = False

    def __init__():
        pass

    @abstractmethod
    async def startup(self):
        pass

    @abstractmethod
    async def store(self, resource: StoredResource):
        pass

    @abstractmethod
    async def delete(self, resource: StoredResource):
        pass

    @abstractmethod
    async def load_by_type(self, resource_type: str) -> list:
        pass

    @abstractmethod
    async def load_by_type_and_rid(self, resource_type: str, rid: Any) -> dict:
        pass

    @abstractmethod
    async def get_all_rids_for_type(self, resource_type: str,
                                    with_storage_id: bool = False) -> list:
        pass

    @abstractmethod
    async def close(self):
        pass
