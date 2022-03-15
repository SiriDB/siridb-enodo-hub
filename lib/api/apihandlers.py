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
        """
        Returns a list of monitored series
        :param request:
        :return:
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
    async def get_monitored_series_details(cls, request):
        """
        Returns all details and data points of a specific series.
        :param request:
        :return:
        """
        include_points = True \
            if 'include_points' in request.rel_url.query and \
            request.rel_url.query['include_points'] == 'true' else False
        series_name = unquote(request.match_info['series_name'])

        return web.json_response(
            data=await BaseHandler.resp_get_monitored_series_details(
                series_name, include_points),
            dumps=safe_json_dumps)

    @classmethod
    @EnodoAuth.auth.required
    async def add_series(cls, request):
        """
        Add new series to the application.
        :param request:
        :return:
        """
        data = await request.json()
        resp, status = await BaseHandler.resp_add_series(data)
        return web.json_response(data=resp, status=status)

    @classmethod
    @EnodoAuth.auth.required
    async def remove_series(cls, request):
        """
        Remove series with certain name
        :param request:
        :return:
        """
        series_name = urllib.parse.unquote(
            request.match_info['series_name'])
        return web.json_response(
            data={}, status=await BaseHandler.resp_remove_series(series_name))

    @classmethod
    @EnodoAuth.auth.required
    async def get_enodo_event_outputs(cls, request):
        """
        Get all event outputs.
        :param request:
        :return:
        """
        resp, status = await BaseHandler.resp_get_event_outputs()
        return web.json_response(data=resp, status=status)

    @classmethod
    @EnodoAuth.auth.required
    async def add_enodo_event_output(cls, request):
        """
        Add a new event output.
        :param request:
        :return:
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
        """
        Add a new event output.
        :param request:
        :return:
        """
        output_id = int(request.match_info['output_id'])

        resp, status = await BaseHandler.resp_remove_event_output(output_id)
        return web.json_response(data=resp, status=status)

    @classmethod
    @EnodoAuth.auth.required
    async def get_possible_analyser_models(cls, request):
        """
        Returns list of possible models with corresponding parameters
        :param request:
        :return:
        """

        return web.json_response(
            data=await BaseHandler.resp_get_possible_analyser_models(),
            status=200)

    @classmethod
    @EnodoAuth.auth.required
    async def add_analyser_models(cls, request):
        """
        Returns list of possible models with corresponding parameters
        :param request:
        :return:
        """
        data = await request.json()
        resp, status = await BaseHandler.resp_add_model(data)
        return web.json_response(data=resp, status=status)

    @classmethod
    @EnodoAuth.auth.required
    async def get_siridb_enodo_status(cls, request):
        """
        Get status of this analyser instance
        :param request:
        :return:
        """
        resp = await BaseHandler.resp_get_enodo_hub_status()
        return web.json_response(data=resp, status=200)

    @classmethod
    async def get_enodo_readiness(cls, request):
        """
        Get status of this analyser instance
        :param request:
        :return:
        """
        ready = ServerState.get_readiness()
        return web.Response(
            body="OK\r\n" if ready else "SERVICE UNAVAILABLE\r\n",
            status=200 if ready else 503)

    @classmethod
    @EnodoAuth.auth.required
    async def get_event_log(cls, request):
        """
        Returns event log
        :param request:
        :return:
        """
        log = ""
        return web.json_response(data={'data': log}, status=200)

    @classmethod
    @EnodoAuth.auth.required
    async def get_settings(cls, request):
        resp = await BaseHandler.resp_get_enodo_config()
        return web.json_response(data=resp, status=200)

    @classmethod
    @EnodoAuth.auth.required
    async def update_settings(cls, request):
        data = await request.json()
        resp = await BaseHandler.resp_set_config(data)
        return web.json_response(data=resp, status=200)

    @classmethod
    @EnodoAuth.auth.required
    async def get_connected_clients(cls, request):
        """
        Return connected listeners and workers
        :param request:
        :return:
        """
        return web.json_response(data={
            'data': {
                'listeners': [
                    l.to_dict() for l in ClientManager.listeners.values()],
                'workers': [
                    w.to_dict() for w in ClientManager.workers.values()]
            }
        },
            status=200,
            dumps=safe_json_dumps)

    @classmethod
    @EnodoAuth.auth.required
    async def get_enodo_stats(cls, request):
        """
        Return stats and numbers from enodo domain
        :param request:
        :return:
        """
        resp = await BaseHandler.resp_get_enodo_stats()
        return web.json_response(data={
            'data': resp}, status=200)

    @classmethod
    @EnodoAuth.auth.required
    async def get_enodo_labels(cls, request):
        """
        Return enodo labels and last update timestamp
        :param request:
        :return:
        """
        resp = await BaseHandler.resp_get_enodo_labels()
        return web.json_response(data={
            'data': resp}, status=200)

    @classmethod
    @EnodoAuth.auth.required
    async def add_enodo_label(cls, request):
        """
        Add enodo label
        :param request:
        :return:
        """
        data = await request.json()
        resp = await BaseHandler.resp_add_enodo_label(data)
        return web.json_response(data={
            'data': resp}, status=201)

    @classmethod
    @EnodoAuth.auth.required
    async def remove_enodo_label(cls, request):
        """
        Remove enodo label
        :param request:
        :return:
        """
        data = await request.json()
        resp = await BaseHandler.resp_remove_enodo_label(data)
        return web.json_response(data={
            'data': resp}, status=200 if resp else 400)
