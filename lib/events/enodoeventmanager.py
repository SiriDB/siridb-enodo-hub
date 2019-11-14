import logging
import time

import aiohttp
import json
import os

from lib.config.config import Config

ENODO_EVENT_ANOMALY_DETECTED = 1
ENODO_EVENT_TYPES = [ENODO_EVENT_ANOMALY_DETECTED]

ENODO_EVENT_OUTPUT_WEBHOOK = 1
ENODO_EVENT_OUTPUT_TYPES = [ENODO_EVENT_OUTPUT_WEBHOOK]


class EnodoEvent:
    def __init__(self, title, message, event_type):
        if event_type not in ENODO_EVENT_TYPES:
            raise Exception()  # TODO Nice exception
        self.title = title
        self.message = message
        self.event_type = event_type
        self.ts = time.time()

    async def to_dict(self):
        return {
            'title': self.title,
            'event_type': self.event_type,
            'message': self.message,
            'ts': self.ts
        }


class EnodoEventOutput:

    def __init__(self, output_id):
        self.output_id = output_id

    async def send_event(self, event):
        pass

    @classmethod
    async def create(cls, output_id, output_type, data):
        if output_type not in ENODO_EVENT_OUTPUT_TYPES:
            raise Exception  # TODO nice exception

        if output_type == ENODO_EVENT_OUTPUT_WEBHOOK:
            return EnodoEventOutputWebhook(output_id, **data)
        else:
            return EnodoEventOutput(output_id)

    async def to_dict(self):
        return {}


class EnodoEventOutputWebhook(EnodoEventOutput):

    def __init__(self, output_id, url, headers=None):
        """
        Call webhook url with JSON data of EnodoEvent
        :param output_id: id of output
        :param url: url to call
        """
        super().__init__(output_id)
        self.url = url
        self.headers = headers
        if self.headers is None:
            self.headers = {}

    async def send_event(self, event):
        try:
            logging.debug(f'Calling EnodoEventOutput webhook {self.url}')
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=2)) as session:
                await session.post(self.url, json=await event.to_dict(), headers=self.headers)
        except Exception:
            logging.warning('Calling EnodoEventOutput webhook failed')

    async def to_dict(self):
        return {
            'output_id': self.output_id,
            'output_type': ENODO_EVENT_OUTPUT_WEBHOOK,
            'data': {
                'url': self.url,
                'headers': self.headers
            }
        }


class EnodoEventManager:
    outputs = None
    _next_output_id = None
    _lock = None
    _max_output_id = None

    @classmethod
    async def async_setup(cls):
        cls.outputs = []
        cls._next_output_id = 0
        cls._max_output_id = 1000
        cls._lock = False

    @classmethod
    async def _get_next_output_id(cls):
        while cls._lock is True:
            await aiohttp.asyncio.sleep(0.1)
        cls._lock = True
        if cls._next_output_id + 1 >= cls._max_output_id:
            cls._next_output_id = 0
        cls._next_output_id += 1
        cls._lock = False
        return cls._next_output_id

    @classmethod
    async def create_event_output(cls, output_type, data):
        output_id = await cls._get_next_output_id()
        output = await EnodoEventOutput.create(output_id, output_type, data)  # TODO: Catch exception
        cls.outputs.append(output)

    @classmethod
    async def remove_event_output(cls, output_id):
        for output in cls.outputs:
            if output.output_id == output_id:
                await cls._remove_event_output(output)
                return True
        return False

    @classmethod
    async def _remove_event_output(cls, output):
        while cls._lock is True:
            await aiohttp.asyncio.sleep(0.1)
        cls._lock = True
        cls.outputs.remove(output)
        cls._lock = False

    @classmethod
    async def handle_event(cls, event):
        if isinstance(event, EnodoEvent):
            for output in cls.outputs:
                await output.send_event(event)

    @classmethod
    async def load_from_disk(cls):
        if not os.path.exists(Config.event_outputs_save_path):
            pass
        else:
            f = open(Config.event_outputs_save_path, "r")
            data = f.read()
            f.close()
            output_data = json.loads(data)
            if 'next_output_id' in output_data:
                cls._next_output_id = output_data.get('next_output_id')
            if 'outputs' in output_data:
                for s in output_data.get('outputs'):
                    cls.outputs.append(
                        await EnodoEventOutput.create(s.get('output_id'), s.get('output_type'), s.get('data')))

    @classmethod
    async def save_to_disk(cls):
        try:
            serialized_outputs = []
            for output in cls.outputs:
                serialized_outputs.append(await output.to_dict())

            output_data = {
                'next_output_id': cls._next_output_id,
                'outputs': serialized_outputs
            }
            f = open(Config.event_outputs_save_path, "w")
            f.write(json.dumps(output_data))
            f.close()
        except Exception as e:
            print(e)
