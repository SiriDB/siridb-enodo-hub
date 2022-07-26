from typing import Any


class SimpleQueue:

    # initialising capacity
    def __init__(self):
        self.cache = dict()

    def all(self) -> list:
        return list(self.cache.values())

    def remove(self, key: str):
        self.cache.pop(key)

    def get(self, key: str) -> Any:
        return self.cache.get(key)

    def put(self, key: str, value: Any) -> None:
        self.cache[key] = value
