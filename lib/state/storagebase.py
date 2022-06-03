from abc import abstractmethod
from typing import Any

from lib.state.resource import StoredResource


class StorageBase:

    def __init__():
        pass

    @abstractmethod
    def store(self, resource: StoredResource):
        pass

    @abstractmethod
    def delete(self, resource: StoredResource):
        pass

    @abstractmethod
    def load_by_type(self, resource_type: str) -> list:
        pass

    @abstractmethod
    def load_by_type_and_rid(self, resource_type: str, rid: Any) -> dict:
        pass
