import urllib.parse
from json import JSONDecodeError
from urllib.parse import unquote

from aiohttp import web
from aiohttp_basicauth import BasicAuthMiddleware
from attr import field
from lib.config import Config
from lib.serverstate import ServerState
from lib.socket import ClientManager
from lib.util import safe_json_dumps
from lib.util.util import implement_fields_query
from lib.webserver.auth import EnodoAuth
from lib.webserver.basehandler import BaseHandler

auth = BasicAuthMiddleware(username=None, password=None, force=False)


class ApiHandlers:

    @classmethod
    def prepare(cls):
        EnodoAuth.auth.username = Config.basic_auth_username
        EnodoAuth.auth.password = Config.basic_auth_password

    @classmethod
    @EnodoAuth.auth.required
    async def get_monitored_series(cls, request):
        """Returns a list of monitored series

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_

        Query args:
            filter (String): regex filter
        """
        regex_filter = urllib.parse.unquote(
            request.rel_url.query['filter']) \
            if 'filter' in request.rel_url.query else None
        # TODO implement filter

        return web.json_response(
            data=BaseHandler.resp_get_monitored_series(regex_filter),
            dumps=safe_json_dumps)

    @classmethod
    @EnodoAuth.auth.required
    @implement_fields_query
    async def get_single_monitored_series(cls, request, fields=None):
        """Returns all details

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        series_name = unquote(request.match_info['series_name'])
        data, status = await BaseHandler.resp_get_single_monitored_series(
            series_name, fields=fields)
        return web.json_response(
            data, dumps=safe_json_dumps, status=status)

    @classmethod
    @EnodoAuth.auth.required
    @implement_fields_query
    async def get_all_series_output(cls, request, fields=None):
        """Returns forecast data of a specific series.

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        series_name = unquote(request.match_info['series_name'])
        only_future = False
        if "forecastFutureOnly" in request.rel_url.query:
            val = request.rel_url.query['forecastFutureOnly']
            if val == "1" or val.lower() == "true":
                only_future = True
        types = None
        if "types" in request.rel_url.query:
            types_str = request.rel_url.query['types']
            if types_str != "":
                types = types_str.split(",")
                if len(types) < 1:
                    types = None
        return web.json_response(
            data=await BaseHandler.resp_get_all_series_output(
                series_name, fields=fields, forecast_future_only=only_future,
                types=types),
            dumps=safe_json_dumps)

    @classmethod
    @EnodoAuth.auth.required
    async def run_siridb_query(cls, request):
        """Run siridb query

        Args:
            query (string): siridb query, free format

        Returns:
            dict with siridb response
        """
        try:
            data = await request.json()
        except JSONDecodeError as e:
            resp, status = {'error': 'Invalid JSON'}, 400
        else:
            resp, status = await BaseHandler.resp_run_siridb_query(
                data['query'])
        return web.json_response(
            resp, dumps=safe_json_dumps, status=status)

    @classmethod
    @EnodoAuth.auth.required
    async def add_series(cls, request):
        """Add new series to monitor.

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        try:
            data = await request.json()
        except JSONDecodeError as e:
            resp, status = {'error': 'Invalid JSON'}, 400
        else:
            resp, status = await BaseHandler.resp_add_series(data)
        return web.json_response(data=resp, status=status)

    @classmethod
    @EnodoAuth.auth.required
    async def remove_series(cls, request):
        """Remove series with specific name

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        series_name = urllib.parse.unquote(
            request.match_info['series_name'])
        return web.json_response(
            data={}, status=await BaseHandler.resp_remove_series(series_name))

    @classmethod
    @EnodoAuth.auth.required
    async def add_series_job_config(cls, request):
        """Add a job config to a series

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        series_name = urllib.parse.unquote(
            request.match_info['series_name'])
        try:
            data = await request.json()
        except JSONDecodeError as e:
            resp, status = {'error': 'Invalid JSON'}, 400
        else:
            resp, status = await BaseHandler.resp_add_job_config(
                series_name, data)
        return web.json_response(
            data=resp, status=status)

    @classmethod
    @EnodoAuth.auth.required
    async def remove_series_job_config(cls, request):
        """Remove a job config from a series

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        series_name = urllib.parse.unquote(
            request.match_info['series_name'])
        job_config_name = urllib.parse.unquote(
            request.match_info['job_config_name'])
        data, status = await BaseHandler.resp_remove_job_config(
            series_name, job_config_name)
        return web.json_response(
            data=data, status=status)

    @classmethod
    @EnodoAuth.auth.required
    @implement_fields_query
    async def get_series_config_templates(cls, request, fields=None):
        """Get all series config templates

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        data, status = BaseHandler.resp_get_series_config_templates(
            fields=fields)
        return web.json_response(
            data=data, status=status)

    @classmethod
    @EnodoAuth.auth.required
    async def add_series_config_templates(cls, request):
        """Add a series config template

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        try:
            data = await request.json()
        except JSONDecodeError as e:
            resp, status = {'error': 'Invalid JSON'}, 400
        else:
            resp, status = await BaseHandler.resp_add_series_config_templates(
                data)
        return web.json_response(data=resp, status=status)

    @classmethod
    @EnodoAuth.auth.required
    async def remove_series_config_templates(cls, request):
        """Remove a series config template

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        rid = urllib.parse.unquote(
            request.match_info['rid'])
        data, status = await BaseHandler.resp_remove_series_config_templates(
            rid)
        return web.json_response(
            data=data, status=status)

    @classmethod
    @EnodoAuth.auth.required
    async def update_series_config_templates_static(cls, request):
        """Update a series config template

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        try:
            data = await request.json()
        except JSONDecodeError as e:
            resp, status = {'error': 'Invalid JSON'}, 400
        rid = urllib.parse.unquote(
            request.match_info['rid'])
        data, status = \
            await BaseHandler.resp_update_series_config_templates_static(
                rid, data.get("name"), data.get("description"))
        return web.json_response(
            data=data, status=status)

    @classmethod
    @EnodoAuth.auth.required
    async def update_series_config_templates(cls, request):
        """Update a series config template

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        try:
            data = await request.json()
        except JSONDecodeError as e:
            resp, status = {'error': 'Invalid JSON'}, 400
        rid = urllib.parse.unquote(
            request.match_info['rid'])
        data, status = \
            await BaseHandler.resp_update_series_config_templates(
                rid, data)
        return web.json_response(
            data=data, status=status)

    @classmethod
    @EnodoAuth.auth.required
    @implement_fields_query
    async def get_enodo_event_outputs(cls, request, fields=None):
        """Get all event outputs

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        resp, status = await BaseHandler.resp_get_event_outputs(fields=fields)
        return web.json_response(data=resp, status=status)

    @classmethod
    @EnodoAuth.auth.required
    async def add_enodo_event_output(cls, request):
        """Add a new event output

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_

        JSON POST data:
            output_type (int): type of output stream
            data        (Object): data for output
        """
        try:
            data = await request.json()
        except JSONDecodeError as e:
            resp, status = {'error': 'Invalid JSON'}, 400
        else:
            output_type = data.get('output_type')
            output_data = data.get('data')

            resp, status = await BaseHandler.resp_add_event_output(
                output_type, output_data)
        return web.json_response(data=resp, status=status)

    @classmethod
    @EnodoAuth.auth.required
    async def remove_enodo_event_output(cls, request):
        """Add a new event output

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        output_id = request.match_info['output_id']

        resp, status = await BaseHandler.resp_remove_event_output(output_id)
        return web.json_response(data=resp, status=status)

    @classmethod
    @EnodoAuth.auth.required
    @implement_fields_query
    async def get_possible_analyser_modules(cls, request, fields=None):
        """Returns list of possible modules with corresponding parameters

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """

        return web.json_response(
            data=BaseHandler.resp_get_possible_analyser_modules(
                fields=fields),
            status=200)

    @classmethod
    @EnodoAuth.auth.required
    @implement_fields_query
    async def get_siridb_enodo_status(cls, request, fields=None):
        """Get status of enodo hub

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        resp = BaseHandler.resp_get_enodo_hub_status(fields=fields)
        return web.json_response(data=resp, status=200)

    @classmethod
    async def get_enodo_readiness(cls, request):
        """Get ready status of this hub instance

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        ready = ServerState.get_readiness()
        return web.Response(
            body="OK\r\n" if ready else "SERVICE UNAVAILABLE\r\n",
            status=200 if ready else 503)

    @classmethod
    async def get_enodo_liveness(cls, request):
        """Get liveness status of this hub instance

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        return web.Response(
            body="OK\r\n", status=200)

    @classmethod
    @EnodoAuth.auth.required
    async def get_event_log(cls, request):
        """Returns enodo event log

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        log = ""
        return web.json_response(data={'data': log}, status=200)

    @classmethod
    @EnodoAuth.auth.required
    @implement_fields_query
    async def get_settings(cls, request, fields=None):
        """Returns current settings dict

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        resp = BaseHandler.resp_get_enodo_config(fields=fields)
        return web.json_response(data=resp, status=200)

    @classmethod
    @EnodoAuth.auth.required
    async def update_settings(cls, request):
        """Update settings

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_

        JSON POST data:
            key/value pairs settings
        """
        data = await request.json()
        resp = BaseHandler.resp_set_config(data)
        return web.json_response(data=resp, status=200)

    @classmethod
    @EnodoAuth.auth.required
    async def get_connected_clients(cls, request):
        """Return connected listeners and workers

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        return web.json_response(data={
            'data': {
                'listeners': [
                    li.to_dict() for li in ClientManager.listeners.values()],
                'workers': [
                    wo.to_dict() for wo in ClientManager.workers.values()]
            }
        },
            status=200,
            dumps=safe_json_dumps)

    @classmethod
    @EnodoAuth.auth.required
    @implement_fields_query
    async def get_enodo_stats(cls, request, fields=None):
        """Return stats and numbers from enodo domain

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        resp = await BaseHandler.resp_get_enodo_stats(fields=fields)
        return web.json_response(data=resp, status=200)

    @classmethod
    @EnodoAuth.auth.required
    async def get_enodo_labels(cls, request):
        """Return enodo labels and last update timestamp

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        resp = BaseHandler.resp_get_enodo_labels()
        return web.json_response(data={
            'data': resp}, status=200)

    @classmethod
    @EnodoAuth.auth.required
    async def add_enodo_label(cls, request):
        """Add enodo label

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_

        JSON POST data:
            description (String): description of the label
            name (String): name of the label
            series_config (Object): series config to assign to child series
                                    of label
        """
        try:
            data = await request.json()
        except JSONDecodeError as e:
            resp, status = {'error': 'Invalid JSON'}, 400
        else:
            resp, status = await BaseHandler.resp_add_enodo_label(data)
        return web.json_response(data={
            'data': resp}, status=status)

    @classmethod
    @EnodoAuth.auth.required
    async def remove_enodo_label(cls, request):
        """Remove enodo label

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        try:
            data = await request.json()
        except JSONDecodeError as e:
            resp, status = {'error': 'Invalid JSON'}, 400
        else:
            resp, status = await BaseHandler.resp_remove_enodo_label(data)
        return web.json_response(data={
            'data': resp}, status=status)
