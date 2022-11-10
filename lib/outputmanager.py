import logging

import aiohttp
from lib.serverstate import ServerState
from lib.util import Template

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


class EnodoEventOutput:
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
        _sub_data = {
            "event": event,
            "severity": self.severity
        }
        template = Template(self.payload)
        return template.safe_substitute(**_sub_data)

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


class EnodoResultOutputWebhook:
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

    def _get_query_params(self, response: EnodoRequestResponse):
        if not isinstance(self.params, dict):
            return self.params if isinstance(self.params, str) else ""

        _sub_data = {
            "request": response.request,
            "response": response
        }
        _params = {}
        for key, value in self.params.items():
            template = Template(value)
            _params[key] = template.safe_substitute(**_sub_data)
        _params = '&'.join('{}={}'.format(key, value)
                           for key, value in _params.items())
        return f"?{_params}" if _params != "" else _params

    def _get_headers(self, response: EnodoRequestResponse):
        if not isinstance(self.headers, dict):
            return {}

        _sub_data = {
            "request": response.request,
            "response": response
        }
        _headers = {}
        for key, value in self.headers.items():
            template = Template(value)
            _headers[key] = template.safe_substitute(**_sub_data)
        return _headers

    def _get_payload(self, response: EnodoRequestResponse):
        if not isinstance(self.payload, str):
            return self.payload
        _sub_data = {
            "request": response.request,
            "response": response
        }
        template = Template(self.payload)
        return template.safe_substitute(**_sub_data)

    def _get_url(self, response: EnodoRequestResponse):
        return f"{self.url}{self._get_query_params(response)}"

    async def trigger(self, r: EnodoRequestResponse):
        try:
            logging.debug(
                f'Calling result webhook {self._get_url(r)}')
            async with aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=3),
                    connector=aiohttp.TCPConnector(
                        verify_ssl=False)) as session:
                await session.post(
                    self._get_url(r),
                    data=self._get_payload(r),
                    headers=self._get_headers(r))
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
    _eos = None
    _ros = None

    @classmethod
    async def async_setup(cls):
        # circular import
        from lib.state.stores import EventOutputStore, ResultOutputStore
        cls._eos = await EventOutputStore.setup(ServerState.thingsdb_client)
        cls._ros = await ResultOutputStore.setup(ServerState.thingsdb_client)

    @classmethod
    async def create_output(cls, output_type, data):
        if output_type == "event":
            return await cls._eos.create(data)
        elif output_type == "result":
            return await cls._ros.create(data)

        logging.error("Cannot create output: Invalid output type")
        return False

    @classmethod
    async def get_outputs(cls, output_type: str) -> list:
        if output_type == "event":
            return [o.to_dict() for o in cls._eos.outputs.values()]
        elif output_type == "result":
            return [o.to_dict() for o in cls._ros.outputs.values()]
        logging.error("Cannot return outputs: Invalid output type")
        return []

    @classmethod
    async def remove_output(cls, output_type, output_id):
        if output_type == "event":
            await cls._eos.delete_output(output_id)
        elif output_type == "result":
            await cls._ros.delete_output(output_id)
        else:
            logging.error("Cannot delete output: Invalid output type")

    @classmethod
    async def handle_event(cls, event, series=None):
        if isinstance(event, EnodoEvent):
            for output in cls._eos.outputs.values():
                await output.send_event(event)

    @classmethod
    async def handle_result(cls, result: EnodoRequestResponse):
        if isinstance(result, EnodoRequestResponse):
            output = cls._ros.outputs.get(result.request.response_output_id)
            if output:
                await output.trigger(result)
