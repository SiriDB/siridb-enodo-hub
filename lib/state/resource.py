from asyncio import ensure_future
import contextlib
from typing import Any
from abc import abstractproperty
import functools

from lib.serverstate import ServerState
from lib.state.lruqueue import LRUQueue
from lib.state.queue import SimpleQueue


resource_manager_index = {}


def from_thing(thing: dict) -> dict:
    if '#' in thing:
        thing['rid'] = thing['#']
        remove_things_id(thing)


def remove_things_id(val: dict) -> dict:
    if "#" in val:
        del val['#']
    for key in val:
        if isinstance(val[key], dict):
            remove_things_id(val[key])
        if isinstance(val[key], list):
            for item in val[key]:
                if isinstance(item, dict):
                    remove_things_id(item)


class ResourceIterator:
    def __init__(self, resource_manager):
        self._resource_manager = resource_manager
        self._rids = list(resource_manager.get_resource_rids())

    def __iter__(self):
        self.i = 0
        return self

    def __next__(self):
        if self.i >= len(self._rids):
            raise StopIteration
        resp = self._resource_manager.get_resource(self.rids[self.i])
        self.i += 1
        return resp


