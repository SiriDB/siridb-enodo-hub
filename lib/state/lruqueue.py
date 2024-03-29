from collections import OrderedDict
from typing import Any, Optional


class LRUQueue:

    # initialising capacity
    def __init__(self, capacity: int):
        self.cache = OrderedDict()
        self.capacity = capacity

    def all(self) -> list:
        return list(self.cache.values())

    def remove(self, key: str):
        if key in self.cache:
            self.cache.pop(key)

    def get(self, key: str) -> Any:
        if key not in self.cache:
            return None
        else:
            self.cache.move_to_end(key)
            return self.cache[key]

    def put(self, key: str, value: Any) -> None:
        self.cache[key] = value
        self.cache.move_to_end(key)
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)
