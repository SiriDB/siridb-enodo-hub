from json import JSONDecodeError
from urllib.parse import unquote

from aiohttp import web
from aiohttp_basicauth import BasicAuthMiddleware
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
    async def run_job_for_series(cls, request):
        """Run job for series

        Args:

        Returns:
            dict with siridb response
        """
        series_name = unquote(request.match_info['series_name'])
        pool_id = int(request.rel_url.query.get('poolID', 0))
        response_output_id = request.rel_url.query.get('responseOutputID')
        if response_output_id is None:
            resp, status = {'error': 'Invalid responseOutputID'}, 400
        try:
            data = await request.json()
        except JSONDecodeError as e:
            resp, status = {'error': 'Invalid JSON'}, 400
        else:
            resp, status = await BaseHandler.resp_run_job_for_series(
                series_name, data, pool_id, int(response_output_id))
        return web.json_response(
            resp, dumps=safe_json_dumps, status=status)

    @classmethod
    @EnodoAuth.auth.required
    async def query_series_state(cls, request):
        """Query series state for a specific job type

        Args:

        Returns:
            dict with state response
        """
        series_name = unquote(request.match_info['series_name'])

        try:
            pool_id = int(request.match_info['pool_id'])
        except Exception:
            return web.json_response(
                {"error": "Invalid pool_id"},
                dumps=safe_json_dumps, status=400)

        try:
            job_type_id = int(request.match_info['job_type_id'])
        except Exception:
            return web.json_response(
                {"error": "Invalid job_type_id"},
                dumps=safe_json_dumps, status=400)

        data, status = await BaseHandler.resp_query_series_state(
            pool_id,
            series_name,
            job_type_id)
        return web.json_response(
            data, dumps=safe_json_dumps, status=status)

    @classmethod
    @EnodoAuth.auth.required
    async def query_worker_stats(cls, request):
        """Query stats of workers in a pool

        Args:

        Returns:
            dict with stats response
        """
        pool_id = unquote(request.match_info['pool_id'])

        if pool_id is None:
            return web.json_response(
                {}, dumps=safe_json_dumps, status=400)
        pool_id = int(pool_id)
        data, status = await BaseHandler.resp_query_worker_stats(pool_id)
        return web.json_response(data, dumps=safe_json_dumps, status=status)

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
    @implement_fields_query
    async def get_enodo_outputs(cls, request, fields=None):
        """Get all event outputs

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_

        Query args:
            fields (String, comma seperated): list of fields to return
        """
        output_type = request.match_info['output_type']
        resp, status = await BaseHandler.resp_get_outputs(
            output_type, fields=fields)
        return web.json_response(data=resp, status=status)

    @classmethod
    @EnodoAuth.auth.required
    async def add_enodo_output(cls, request):
        """Add a new event output

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_

        JSON POST data:
            output_type (int): type of output (event/result)
            data        (Object): data for output
        """
        try:
            data = await request.json()
        except JSONDecodeError as e:
            resp, status = {'error': 'Invalid JSON'}, 400
        else:
            output_type = request.match_info['output_type']

            resp, status = await BaseHandler.resp_add_output(
                output_type, data)
        return web.json_response(data=resp, status=status)

    @classmethod
    @EnodoAuth.auth.required
    async def remove_enodo_output(cls, request):
        """Add a new event output

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        output_id = request.match_info['output_id']
        output_type = request.match_info['output_type']

        resp, status = await BaseHandler.resp_remove_output(
            output_type, output_id)
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

        Query args:
            fields (String, comma seperated): list of fields to return
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

        Query args:
            fields (String, comma seperated): list of fields to return
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
    @implement_fields_query
    async def get_settings(cls, request, fields=None):
        """Returns current settings dict

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_

        Query args:
            fields (String, comma seperated): list of fields to return
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
                    wo.to_dict() for wo in ClientManager.get_workers()]
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

        Query args:
            fields (String, comma seperated): list of fields to return
        """
        resp = await BaseHandler.resp_get_enodo_stats(fields=fields)
        return web.json_response(data=resp, status=200)

    @classmethod
    @EnodoAuth.auth.required
    async def get_workers(cls, request):
        """Get workers in a pool

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        try:
            pool_id = int(request.match_info['pool_id'])
        except Exception:
            return web.json_response(
                data={"error": "Invalid pool_id"},
                status=400)

        status, resp = BaseHandler.resp_get_workers(pool_id)
        return web.json_response(data=resp, status=status)

    @classmethod
    @EnodoAuth.auth.required
    async def add_worker(cls, request):
        """Add new worker

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        try:
            pool_id = int(request.match_info['pool_id'])
        except Exception:
            return web.json_response(
                data={"error": "Invalid pool_id"},
                status=400)

        try:
            data = await request.json()
        except Exception:
            return web.json_response(
                data={"error": "Invalid config in payload, invalid JSON"},
                status=400)
        if not isinstance(data, dict):
            return web.json_response(
                data={"error": "Invalid config in payload"},
                status=400)

        data = await request.json()
        status, resp = await BaseHandler.resp_add_worker(pool_id, data)
        return web.json_response(data=resp, status=status)

    @classmethod
    @EnodoAuth.auth.required
    async def delete_worker(cls, request):
        """Delete worker

        Args:
            request (Request): aiohttp request

        Returns:
            _type_: _description_
        """
        try:
            pool_id = int(request.match_info['pool_id'])
        except Exception:
            return web.json_response(
                data={"error": "Invalid pool_id"},
                status=400)

        try:
            job_type_id = int(request.match_info['job_type_id'])
        except Exception:
            return web.json_response(
                data={"error": "Invalid job_type_id"},
                status=400)

        status, resp = await BaseHandler.resp_delete_worker(
            pool_id, job_type_id)
        return web.json_response(data=resp, status=status)
