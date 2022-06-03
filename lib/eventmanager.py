import asyncio
import json
import logging
import os
import time
import uuid

import aiohttp
from jinja2 import Environment

from lib.config import Config
from lib.serverstate import ServerState
from lib.socketio import (SUBSCRIPTION_CHANGE_TYPE_ADD,
                          SUBSCRIPTION_CHANGE_TYPE_DELETE,
                          SUBSCRIPTION_CHANGE_TYPE_UPDATE)
from lib.util import cls_lock, load_disk_data, save_disk_data

ENODO_EVENT_ANOMALY_DETECTED = "event_anomaly_detected"
ENODO_EVENT_JOB_QUEUE_TOO_LONG = "job_queue_too_long"
ENODO_EVENT_LOST_CLIENT_WITHOUT_GOODBYE = "lost_client_without_goodbye"
ENODO_EVENT_STATIC_RULE_FAIL = "event_static_rule_fail"
ENODO_EVENT_TYPES = [ENODO_EVENT_ANOMALY_DETECTED,
                     ENODO_EVENT_JOB_QUEUE_TOO_LONG,
                     ENODO_EVENT_LOST_CLIENT_WITHOUT_GOODBYE,
                     ENODO_EVENT_STATIC_RULE_FAIL]
ENODO_SERIES_RELATED_EVENT_TYPES = [ENODO_EVENT_ANOMALY_DETECTED,
                                    ENODO_EVENT_STATIC_RULE_FAIL]

ENODO_EVENT_OUTPUT_WEBHOOK = 1
ENODO_EVENT_OUTPUT_TYPES = [ENODO_EVENT_OUTPUT_WEBHOOK]

ENODO_EVENT_SEVERITY_INFO = "info"
ENODO_EVENT_SEVERITY_WARNING = "warning"
ENODO_EVENT_SEVERITY_ERROR = "error"
ENODO_EVENT_SEVERITY_LEVELS = [
    ENODO_EVENT_SEVERITY_INFO,
    ENODO_EVENT_SEVERITY_WARNING,
    ENODO_EVENT_SEVERITY_ERROR]


class EnodoEvent:
    """
    EnodoEvent class. Holds data for an event (error/warning/etc)
    that occured. No state data is saved.
    """
    __slots__ = ('title', 'message', 'event_type',
                 'series_name', 'ts', 'severity', 'uuid')

    def __init__(self, title, message, event_type, series=None):
        if event_type not in ENODO_EVENT_TYPES:
            raise Exception()  # TODO Nice exception
        self.title = title
        self.message = message
        self.event_type = event_type
        self.series_name = series  # only needs to be set if it regards a
        # series related event
        self.ts = int(time.time())
        self.uuid = str(uuid.uuid4()).replace("-", "")

    def to_dict(self):
        return {
            'title': self.title,
            'event_type': self.event_type,
            'message': self.message,
            'series_name': self.series_name,
            'ts': self.ts,
            'uuid': self.uuid
        }


class EnodoEventOutput:
    """
    EnodoEventOutput Class. Class to describe base output method of events
    """

    def __init__(self, rid,
                 severity=ENODO_EVENT_SEVERITY_ERROR,
                 for_event_types=ENODO_EVENT_TYPES,
                 vendor_name=None,
                 custom_name=None):
        """
        Call webhook url with data of EnodoEvent
        :param id: id of output
        :param severity: severity for each event to pass through
        :param for_event_types: only accepts certain event types
        :param vendor_name: vendor name, for gui or client purposes,
            to setup default ouputs for third part systems
        :param custom_name: custom name
        """
        self.rid = rid
        self.severity = severity
        self.for_event_types = for_event_types
        self.vendor_name = vendor_name
        self.custom_name = custom_name

    async def send_event(self, event):
        pass

    @classmethod
    def create(cls, output_type, data):
        if output_type not in ENODO_EVENT_OUTPUT_TYPES:
            raise Exception  # TODO nice exception

        if output_type == ENODO_EVENT_OUTPUT_WEBHOOK:
            return EnodoEventOutputWebhook(**data)
        return EnodoEventOutput(**data)

    def update(self, data):
        self.custom_name = data.get('custom_name') if data.get(
            'custom_name') is not None else self.custom_name
        self.vendor_name = data.get('vendor_name') if data.get(
            'vendor_name') is not None else self.vendor_name
        self.severity = data.get('severity') if data.get(
            'severity') is not None else self.severity
        self.for_event_types = data.get('for_event_types') if data.get(
            'for_event_types') is not None else self.for_event_types

    def to_dict(self):
        return {
            "rid": self.rid,
            "severity": self.severity,
            "for_event_types": self.for_event_types,
            "vendor_name": self.vendor_name,
            "custom_name": self.custom_name
        }


class EnodoEventOutputWebhook(EnodoEventOutput):
    """
    EnodoEventOutputWebhook Class. Class to describe webhook method as
    output method of events. This method uses a template which can be
    used {{ event.var }} will be replaced with a instance variable named var
    on the event instance
    """

    def __init__(self, rid, url, headers=None, payload=None, **kwargs):
        """
        Call webhook url with data of EnodoEvent
        :param id: id of output
        :param url: url to call
        """
        super().__init__(rid, **kwargs)
        self.url = url
        self.payload = payload
        self.headers = headers
        if not isinstance(
                self.headers, dict) and self.headers is not None:
            self.headers = None
        if self.payload is None:
            self.payload = ""

    def _get_payload(self, event):
        env = Environment()
        env.filters['jsonify'] = json.dumps
        template = env.from_string(self.payload)
        return template.render(event=event, severity=self.severity)

    async def send_event(self, event):
        if event.event_type in self.for_event_types:
            try:
                logging.debug(
                    f'Calling EnodoEventOutput webhook {self.url}')
                async with aiohttp.ClientSession(
                        timeout=aiohttp.ClientTimeout(total=2)) as session:
                    await session.post(
                        self.url,
                        data=self._get_payload(event),
                        headers=self.headers)
            except Exception as e:
                logging.warning(
                    'Calling EnodoEventOutput webhook failed')
                logging.debug(
                    f'Corresponding error: {e}, '
                    f'exception class: {e.__class__.__name__}')

    def update(self, data):
        self.url = data.get('url') if data.get(
            'url') is not None else self.url
        self.payload = data.get('payload') if data.get(
            'payload') is not None else self.payload
        self.headers = data.get('headers') if data.get(
            'headers') is not None else self.headers

        if not isinstance(
                self.headers, dict) and self.headers is not None:
            self.headers = None
        if self.payload is None:
            self.payload = ""

        super().update(data)

    def to_dict(self):
        return {
            'output_type': ENODO_EVENT_OUTPUT_WEBHOOK,
            'data': {
                **(super().to_dict()),
                **{
                    'url': self.url,
                    'headers': self.headers,
                    'payload': self.payload
                }
            }
        }


class EnodoEventManager:
    outputs = None
    # Next id is always current. will be incremented when setting new id
    _next_output_id = None
    _lock = None
    _max_output_id = None

    @classmethod
    async def async_setup(cls):
        cls.outputs = []
        cls._next_output_id = 0
        cls._max_output_id = 1000
        cls._lock = asyncio.Lock()

    @classmethod
    @cls_lock()
    async def _get_next_output_id(cls):
        if cls._next_output_id + 1 >= cls._max_output_id:
            cls._next_output_id = 0
        cls._next_output_id += 1
        return cls._next_output_id

    @classmethod
    async def create_event_output(cls, output_type, data):
        data["rid"] = await cls._get_next_output_id()
        # TODO: Catch exception
        output = EnodoEventOutput.create(output_type, data)
        cls.outputs.append(output)
        asyncio.ensure_future(internal_updates_event_output_subscribers(
            SUBSCRIPTION_CHANGE_TYPE_ADD, output.to_dict()))
        return output

    @classmethod
    async def update_event_output(cls, output_id, data):
        for output in cls.outputs:
            if output.rid == output_id:
                await cls._update_event_output(output, data)
                return output
        return False

    @classmethod
    async def remove_event_output(cls, output_id):
        for output in cls.outputs:
            if output.rid == output_id:
                await cls._remove_event_output(output)
                return True
        return False

    @classmethod
    @cls_lock()
    async def _remove_event_output(cls, output):
        cls.outputs.remove(output)
        asyncio.ensure_future(internal_updates_event_output_subscribers(
            SUBSCRIPTION_CHANGE_TYPE_DELETE, output.rid))

    @classmethod
    @cls_lock()
    async def _update_event_output(cls, output, data):
        output.update(data)
        asyncio.ensure_future(internal_updates_event_output_subscribers(
            SUBSCRIPTION_CHANGE_TYPE_UPDATE, output.to_dict()))

    @classmethod
    async def handle_event(cls, event, series=None):
        if isinstance(event, EnodoEvent):
            if event.event_type in ENODO_SERIES_RELATED_EVENT_TYPES:
                if series is not None and series.is_ignored() is True:
                    return False
            for output in cls.outputs:
                await output.send_event(event)

    @classmethod
    async def load_from_disk(cls):
        try:
            if not os.path.exists(Config.event_outputs_save_path):
                raise Exception()
            data = load_disk_data(Config.event_outputs_save_path)
        except Exception as _:
            data = {}

        if data == "" or data is None:
            data = {}

        output_data = data
        if 'next_output_id' in output_data:
            cls._next_output_id = output_data.get('next_output_id')
        if 'outputs' in output_data:
            for s in output_data.get('outputs'):
                cls.outputs.append(
                    EnodoEventOutput.create(
                        s.get('output_type'), s.get('data')))

    @classmethod
    async def save_to_disk(cls):
        try:
            serialized_outputs = []
            for output in cls.outputs:
                serialized_outputs.append(output.to_dict())

            output_data = {
                'next_output_id': cls._next_output_id,
                'outputs': serialized_outputs
            }
            save_disk_data(Config.event_outputs_save_path, output_data)
        except Exception as e:
            logging.error(
                f"Something went wrong when writing eventmanager data to disk")
            logging.debug(f"Corresponding error: {e}, "
                          f'exception class: {e.__class__.__name__}')


async def internal_updates_event_output_subscribers(change_type, data):
    sio = ServerState.sio
    if sio is not None:
        await sio.emit('update', {
            'resource': 'event_output',
            'updateType': change_type,
            'resourceData': data
        }, room='event_output_updates')
