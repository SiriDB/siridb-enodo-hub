import asyncio
import contextlib
from typing import Optional


# bisect (right) from https://github.com/python/cpython/blob/3.10/Lib/bisect.py
def bisect(a, x, lo=0, hi=None, *, key=None):
    """Return the index where to insert item x in list a, assuming a is sorted.
    The return value i is such that all e in a[:i] have e <= x, and all e in
    a[i:] have e > x.  So if x already appears in the list, a.insert(i, x) will
    insert just after the rightmost x already there.
    Optional args lo (default 0) and hi (default len(a)) bound the
    slice of a to be searched.
    """

    if lo < 0:
        raise ValueError('lo must be non-negative')
    if hi is None:
        hi = len(a)
    # Note, the comparison uses "<" to match the
    # __lt__() logic in list.sort() and in heapq.
    if key is None:
        while lo < hi:
            mid = (lo + hi) // 2
            if x < a[mid]:
                hi = mid
            else:
                lo = mid + 1
    else:
        x = key(x)
        while lo < hi:
            mid = (lo + hi) // 2
            if x < key(a[mid]):
                hi = mid
            else:
                lo = mid + 1
    return lo


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
                self.items, series_name, key=lambda key: self.mapping[key])
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
