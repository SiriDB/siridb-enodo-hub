import json
import logging
import time
import uuid

import aiohttp
from jinja2 import Environment

from lib.state.resource import StoredResource
from lib.state.resource import ResourceManager

from enodo.protocol.packagedata import EnodoRequestResponse

ENODO_EVENT_ANOMALY_DETECTED = "event_anomaly_detected"
ENODO_EVENT_JOB_QUEUE_TOO_LONG = "job_queue_too_long"
ENODO_EVENT_LOST_CLIENT_WITHOUT_GOODBYE = "lost_client_without_goodbye"
ENODO_EVENT_TYPES = [ENODO_EVENT_ANOMALY_DETECTED,
                     ENODO_EVENT_JOB_QUEUE_TOO_LONG,
                     ENODO_EVENT_LOST_CLIENT_WITHOUT_GOODBYE]
ENODO_SERIES_RELATED_EVENT_TYPES = [ENODO_EVENT_ANOMALY_DETECTED]

ENODO_OUTPUT_WEBHOOK = 1
ENODO_OUTPUT_TYPES = [ENODO_OUTPUT_WEBHOOK]

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
                 'series', 'ts', 'severity', 'uuid')

    def __init__(self, title, message, event_type, series=None):
        if event_type not in ENODO_EVENT_TYPES:
            raise Exception()  # TODO Nice exception
        self.title = title
        self.message = message
        self.event_type = event_type
        self.series = series  # only needs to be set if it regards a
        # series related event
        self.ts = int(time.time())
        self.uuid = str(uuid.uuid4()).replace("-", "")

    def to_dict(self):
        return {
            'title': self.title,
            'event_type': self.event_type,
            'message': self.message,
            'series': self.series,
            'ts': self.ts,
            'uuid': self.uuid
        }


class EnodoEventOutput(StoredResource):
    """
    EnodoEventOutput Class. Class to describe base output method of events
    """

    def __init__(self, rid=None,
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
    @property
    def resource_type(self):
        return "event_outputs"

    @classmethod
    def create(cls, output_type, data):
        if output_type not in ENODO_OUTPUT_TYPES:
            raise Exception  # TODO nice exception

        if output_type == ENODO_OUTPUT_WEBHOOK:
            return EnodoEventOutputWebhook(**data)
        return EnodoEventOutput(**data)

    @StoredResource.changed
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

    def __init__(
            self, url, headers=None, payload=None, **kwargs):
        """
        Call webhook url with data of EnodoEvent
        :param id: id of output
        :param url: url to call
        """
        super().__init__(**kwargs)
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
                        timeout=aiohttp.ClientTimeout(total=3),
                        connector=aiohttp.TCPConnector(
                            verify_ssl=False)) as session:
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
            **(super().to_dict()),
            **{
                'url': self.url,
                'headers': self.headers,
                'payload': self.payload
            }
        }


class EnodoResultOutputWebhook(StoredResource):
    """
    EnodoResultOutputWebhook Class. Class to describe webhook method as
    output method of results. This method uses a template which can be
    used {{ result.var }} will be replaced with a instance variable named var
    on the result instance
    """

    def __init__(
            self, url, rid=None, params=None, headers=None, payload=None):
        """
        Call webhook url with data of result
        :param id: id of output
        :param url: url to call
        """
        self.rid = rid
        self.url = url
        self.params = params
        self.payload = payload
        self.headers = headers
        if not isinstance(
                self.headers, dict) and self.headers is not None:
            self.headers = None
        if self.payload is None:
            self.payload = ""

    def _get_query_params(self, request, result):
        if not isinstance(self.params, dict):
            return self.params if isinstance(self.params, str) else ""

        _params = {}
        for key, value in self.params.items():
            env = Environment()
            env.filters['jsonify'] = json.dumps
            template = env.from_string(value)
            _params[key] = template.render(request=request, result=result)
        _params = '&'.join('{} : {}'.format(key, value)
                           for key, value in _params.items())
        return f"?{_params}" if _params is not "" else _params

    def _get_headers(self, request, result):
        if not isinstance(self.headers, dict):
            return {}

        _headers = {}
        for key, value in self.headers.items():
            env = Environment()
            env.filters['jsonify'] = json.dumps
            template = env.from_string(value)
            _headers[key] = template.render(request=request, result=result)
        return _headers

    def _get_payload(self, request, result):
        if not isinstance(self.payload, str):
            return self.payload
        env = Environment()
        env.filters['jsonify'] = json.dumps
        template = env.from_string(self.payload)
        return json.loads(template.render(request=request, result=result))

    def _get_url(self, request, result):
        return f"{self.url}{self._get_query_params(request, result)}"

    async def trigger(self, r: EnodoRequestResponse):
        try:
            logging.debug(
                'Calling result webhook '
                f'{self._get_url(r.request, r.response)}')
            async with aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=3),
                    connector=aiohttp.TCPConnector(
                        verify_ssl=False)) as session:
                await session.post(
                    self.url,
                    data=self._get_payload(r.request, r.response),
                    headers=self._get_headers(r.request, r.response))
        except Exception as e:
            logging.error(
                'Calling result webhook failed')
            logging.warning(
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

    @classmethod
    @property
    def resource_type(self):
        return "result_outputs"

    def to_dict(self):
        return {
            'rid': self.rid,
            'url': self.url,
            'params': self.params,
            'headers': self.headers,
            'payload': self.payload
        }


class EnodoOutputManager:
    _erm = None
    _rrm = None

    @classmethod
    async def async_setup(cls):
        cls._erm = ResourceManager(
            "event_outputs", EnodoEventOutputWebhook, 50)
        await cls._erm.load()
        cls._rrm = ResourceManager(
            "result_outputs", EnodoResultOutputWebhook, 50)
        await cls._rrm.load()

    @classmethod
    async def create_output(cls, output_type, data):
        if output_type == "event":
            async with cls._erm.create_resource(data) as resp:
                output = resp
        elif output_type == "result":
            async with cls._rrm.create_resource(data) as resp:
                output = resp
        return output

    @classmethod
    async def get_outputs(cls, output_type: str) -> list:
        if output_type == "event":
            return [output.to_dict() async for output in cls._erm.itter()]
        elif output_type == "result":
            return [output.to_dict() async for output in cls._rrm.itter()]
        return []

    @classmethod
    async def remove_output(cls, output_type, output_id):
        _rm = cls._erm if output_type == "event" else cls._rrm
        async for output in _rm.itter():
            if output.rid == output_id:
                await _rm.delete_resource(output)
                return True
        return False

    @classmethod
    async def handle_event(cls, event, series=None):
        if isinstance(event, EnodoEvent):
            if event.event_type in ENODO_SERIES_RELATED_EVENT_TYPES:
                if series is not None and series.is_ignored() is True:
                    return False
            async for output in cls._erm.itter():
                await output.send_event(event)

    @classmethod
    async def handle_result(cls, result: EnodoRequestResponse):
        if isinstance(result, EnodoRequestResponse):
            output = await cls._rrm.get_resource(
                result.request.response_output_id)
            if output:
                await output.trigger(result)
