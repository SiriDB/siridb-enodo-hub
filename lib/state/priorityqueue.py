import asyncio
from bisect import bisect
import contextlib
from typing import Optional


class SeriesPriorityQueue:

    def __init__(self, items: Optional[dict] = None):
        self.items = []
        self.mapping = {}
        if items is not None:
            self.mapping = items
            keys = list(items.keys())
            self.items = sorted(keys, key=lambda key: items[key])
        self._lock = asyncio.Lock()

    def get_series_ts(self, series_name: str) -> int:
        return self.mapping.get(series_name)

    async def insert_schedule(self, series_name, ts):
        async with self._lock:
            if series_name in self.mapping:
                if self.mapping[series_name] == ts:
                    return False
                self.items.remove(series_name)
            self.mapping[series_name] = ts
            pos_to_insert = bisect(
                self.items, ts, key=lambda key: self.mapping[key])
            self.items.insert(pos_to_insert, series_name)
        return True

    async def remove(self, series_name):
        async with self._lock:
            if series_name in self.mapping:
                del self.mapping[series_name]
            if series_name in self.items:
                self.items.remove(series_name)

    def seek_next(self) -> tuple:
        return (self.items[0], self.mapping.get(self.items[0]))

    async def pop(self) -> str:
        async with self._lock:
            sn = self.items.pop(0)
            ts = self.mapping.get(sn)
            del self.mapping[sn]
        return (sn, ts)

    @contextlib.asynccontextmanager
    async def seek_and_hold(self):
        async with self._lock:
            yield (self.items[0], self.mapping.get(self.items[0]))
