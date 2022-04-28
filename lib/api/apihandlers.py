import urllib.parse
from urllib.parse import unquote
from aiohttp import web

from aiohttp_basicauth import BasicAuthMiddleware

from lib.config import Config
from lib.serverstate import ServerState
from lib.socket import ClientManager
from lib.util import safe_json_dumps
from lib.webserver.auth import EnodoAuth
from lib.webserver.basehandler import BaseHandler

auth = BasicAuthMiddleware(username=None, password=None, force=False)


class ApiHandlers:

    @classmethod
    async def prepare(cls):
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
            data=await BaseHandler.resp_get_monitored_series(regex_filter),
            dumps=safe_json_dumps)

    @classmethod
    @EnodoAuth.auth.required
    async def get_single_monitored_series(cls, request):
        """Returns all details

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        series_name = unquote(request.match_info['series_name'])

        return web.json_response(
            data=await BaseHandler.resp_get_single_monitored_series(
                series_name), dumps=safe_json_dumps)

    @classmethod
    @EnodoAuth.auth.required
    async def get_series_forecast(cls, request):
        """Returns forecast data of a specific series.

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        series_name = unquote(request.match_info['series_name'])

        return web.json_response(
            data=await BaseHandler.resp_get_series_forecasts(
                series_name), dumps=safe_json_dumps)

    @classmethod
    @EnodoAuth.auth.required
    async def get_series_anomalies(cls, request):
        """Returns anomalies data of a specific series.

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        series_name = unquote(request.match_info['series_name'])

        return web.json_response(
            data=await BaseHandler.resp_get_series_anomalies(
                series_name), dumps=safe_json_dumps)

    @classmethod
    @EnodoAuth.auth.required
    async def get_series_static_rules_hits(cls, request):
        """Returns static rules hits of a specific series.

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        series_name = unquote(request.match_info['series_name'])

        return web.json_response(
            data=await BaseHandler.resp_get_series_static_rules_hits(
                series_name), dumps=safe_json_dumps)

    @classmethod
    @EnodoAuth.auth.required
    async def run_siridb_query(cls, request):
        """Run siridb query

        Args:
            query (string): siridb query, free format

        Returns:
            dict with siridb response
        """
        data = await request.json()
        data, status = await BaseHandler.resp_run_siridb_query(
            data['query'])
        return web.json_response(
            data, dumps=safe_json_dumps, status=status)

    @classmethod
    @EnodoAuth.auth.required
    async def add_series(cls, request):
        """Add new series to monitor.

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        data = await request.json()
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
    async def get_enodo_event_outputs(cls, request):
        """Get all event outputs

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        resp, status = await BaseHandler.resp_get_event_outputs()
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
        data = await request.json()
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
        output_id = int(request.match_info['output_id'])

        resp, status = await BaseHandler.resp_remove_event_output(output_id)
        return web.json_response(data=resp, status=status)

    @classmethod
    @EnodoAuth.auth.required
    async def get_possible_analyser_modules(cls, request):
        """Returns list of possible modules with corresponding parameters

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """

        return web.json_response(
            data=await BaseHandler.resp_get_possible_analyser_modules(),
            status=200)

    @classmethod
    @EnodoAuth.auth.required
    async def get_siridb_enodo_status(cls, request):
        """Get status of enodo hub

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        resp = await BaseHandler.resp_get_enodo_hub_status()
        return web.json_response(data=resp, status=200)

    @classmethod
    async def get_enodo_readiness(cls, request):
        """Get status of this analyser instance

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
        """Get liveness status of this analyser instance

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
    async def get_settings(cls, request):
        """Returns current settings dict

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        resp = await BaseHandler.resp_get_enodo_config()
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
        resp = await BaseHandler.resp_set_config(data)
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
    async def get_enodo_stats(cls, request):
        """Return stats and numbers from enodo domain

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        resp = await BaseHandler.resp_get_enodo_stats()
        return web.json_response(data={
            'data': resp}, status=200)

    @classmethod
    @EnodoAuth.auth.required
    async def get_enodo_labels(cls, request):
        """Return enodo labels and last update timestamp

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        resp = await BaseHandler.resp_get_enodo_labels()
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
        data = await request.json()
        resp = await BaseHandler.resp_add_enodo_label(data)
        return web.json_response(data={
            'data': resp}, status=201)

    @classmethod
    @EnodoAuth.auth.required
    async def remove_enodo_label(cls, request):
        """Remove enodo label

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        data = await request.json()
        resp = await BaseHandler.resp_remove_enodo_label(data)
        return web.json_response(data={
            'data': resp}, status=200 if resp else 400)
