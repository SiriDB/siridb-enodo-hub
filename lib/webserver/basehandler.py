from asyncio import wait_for
import asyncio
import logging
from aiohttp import web

from siridb.connector.lib.exceptions import QueryError, InsertError, \
    ServerError, PoolError, AuthenticationError, UserAuthError

from aiohttp import web
from enodo.model.config.series import SeriesJobConfigModel
from enodo.protocol.packagedata import REQUEST_TYPE_EXTERNAL, EnodoRequest

from lib.config import Config
from lib.outputmanager import EnodoOutputManager
from lib.jobmanager import EnodoJob, EnodoJobManager
from lib.series.seriesmanager import SeriesManager
from lib.serverstate import ServerState
from lib.siridb.siridb import query_all_series_results
from lib.socket.clientmanager import ClientManager
from lib.socket.queryhandler import QueryHandler
from lib.util import regex_valid
from siridb.connector.lib.exceptions import (
    AuthenticationError, InsertError, PoolError, QueryError,
    ServerError, UserAuthError)
from lib.util.util import (
    get_job_config_output_series_name,
    apply_fields_filter,
    async_simple_fields_filter,
    sync_simple_fields_filter)
from version import VERSION


class BaseHandler:

    @classmethod
    def resp_get_monitored_series(cls, regex_filter=None):
        """Get monitored series

        Args:
            regex_filter (string, optional): filter by regex. Defaults to None.

        Returns:
            dict: dict with data
        """
        if regex_filter is not None and not regex_valid(regex_filter):
            return {'data': []}
        return {'data': list(
            SeriesManager.get_all_series_names(regex_filter))}

    @classmethod
    @async_simple_fields_filter(is_list=False, return_index=0)
    async def resp_get_single_monitored_series(cls, series_name_rid,
                                               by_rid=True):
        """Get monitored series details

        Args:
            series_name (string): name of series

        Returns:
            dict: dict with data
        """
        if by_rid:
            series = await ServerState.series_rm.get_resource(
                int(series_name_rid))
        else:
            series = await SeriesManager.get_config_read_only(series_name_rid)
        if series is None:
            return {'data': {}}, 404
        series_data = series.to_dict()
        return {'data': series_data}, 200

    @classmethod
    async def resp_get_all_series_output(cls, rid,
                                         fields=None,
                                         forecast_future_only=False,
                                         types=None,
                                         by_name=False):
        """Get all series results

        Args:
            series_name (string): name of series

        Returns:
            dict: dict with data
        """
        if by_name:
            series_name = rid
        else:
            series_name = ServerState.series_rm.get_resource_rid_value(
                int(rid))
        series, state = await SeriesManager.get_series_read_only(series_name)
        if series is None:
            return web.json_response(data={'data': ''}, status=404)

        should_fetch_data = True if fields is None or \
            (fields is not None and "data" in fields) else False

        data = None
        if should_fetch_data and not forecast_future_only and types is None:
            data = await query_all_series_results(
                ServerState.get_siridb_output_conn(), series_name)

        resp = []
        for job_config in series.config.job_config.values():
            if types is not None and job_config.job_type not in types:
                continue  # Ignore due to filter
            points = None
            output_series_name = get_job_config_output_series_name(
                series_name, job_config.job_type, job_config.config_name)
            if data is not None:
                points = data.get(output_series_name)
            elif should_fetch_data:
                points = (await SeriesManager.get_series_output_by_job_type(
                    series_name, job_config.job_type,
                    forecast_future_only=forecast_future_only)).get(
                        output_series_name)
            if points is None:
                points = []
            resp.append({
                "output_series_name": output_series_name,
                "config_name": job_config.config_name,
                "job_type": job_config.job_type,
                "job_meta": state.get_job_meta(job_config.config_name),
                "data": points
            })
        if fields is not None:
            resp = apply_fields_filter(resp, fields)
        return {'data': resp}

    @classmethod
    async def resp_run_siridb_query(cls, query):
        """Run siridb query

        Args:
            query (string): siridb query, free format

        Returns:
            dict with siridb response
        """
        output = None
        try:
            result = await ServerState.get_siridb_data_conn().query(
                query)
        except (QueryError, InsertError, ServerError, PoolError,
                AuthenticationError, UserAuthError) as _:
            return {'data': output}, 400
        else:
            output = result
        return {'data': output}, 200

    @classmethod
    @async_simple_fields_filter(return_index=0, is_wrapped=False)
    async def resp_get_outputs(cls, output_type):
        """get all event output steams

        Returns:
            dict: dict with data
        """
        if output_type not in ['event', 'result']:
            return {'error': "Invalid output type"}, 400
        outputs = await EnodoOutputManager.get_outputs(output_type)
        return {'data': outputs}, 200

    @classmethod
    async def resp_add_output(cls, output_type, data):
        """create output stream

        Args:
            output_type (int): output type
            data (dict): data for output stream

        Returns:
            dict: dict with data
        """
        if output_type not in ['event', 'result']:
            return {'error': "Invalid output type"}, 400
        output = await EnodoOutputManager.create_output(
            output_type, data)
        return {'data': output.to_dict()}, 201

    @classmethod
    async def resp_remove_output(cls, output_type, rid):
        """remove output stream

        Args:
            output_id (int): id of an existing output stream

        Returns:
            dict: dict with data
        """
        if output_type not in ['event', 'result']:
            return {'error': "Invalid output type"}, 400
        await EnodoOutputManager.remove_output(output_type, int(rid))
        return {'data': None}, 200

    @classmethod
    async def resp_add_series(cls, data):
        """Add series for monitoring

        Args:
            data (dict): config of series

        Returns:
            dict: dict with data
        """
        config = ServerState.series_config_rm.get_cached_resource(
            int(data.get('config')))
        if config is None:
            return {'error': 'Series config template not found'}, 404
        if 'meta' in data and (data.get('meta') is not None and
                               not isinstance(data.get('meta'), dict)):
            return {'error': 'Something went wrong when adding '
                    'the series. Meta data must be a dict'}, 400
        is_added = await SeriesManager.add_series(data)
        if is_added is False:
            return {'error': 'Something went wrong when adding the series. '
                    'Series already added'}, 400
        if is_added is None:
            return {'error': 'Something went wrong when adding the series. '
                    'Series does not exists'}, 400

        return {'data': True}, 201

    @classmethod
    async def resp_remove_series(cls, rid, by_name=False):
        """Remove series

        Args:
            series_name (string): name of series

        Returns:
            dict: dict with data
        """
        if by_name:
            series_name = rid
        else:
            series_name = ServerState.series_rm.get_resource_rid_value(
                int(rid))
        if await SeriesManager.remove_series(series_name):
            return 200
        return 404

    @classmethod
    @sync_simple_fields_filter(return_index=0)
    def resp_get_series_configs(cls):
        scrm = ServerState.series_config_rm
        templates = scrm.get_cached_resources()
        return {'data': templates}, 200

    @classmethod
    async def resp_add_series_config(cls, config_template: dict):
        scrm = ServerState.series_config_rm

        if scrm.rid_exists(int(config_template.get("rid", -1))):
            return {'error': "template already exists"}, 400

        # if config_template.get("rid") is None:
        #     config_template["rid"] = str(uuid4()).replace("-", "")

        async with scrm.create_resource(config_template) as template:
            return {'data': template}, 201

    @classmethod
    async def resp_remove_series_config(cls, rid: str):
        scrm = ServerState.series_config_rm
        rid = int(rid)
        if not scrm.rid_exists(rid):
            return {'error': "config does not exists"}, 404

        async for series in ServerState.series_rm.itter():
            if series._config_from_template:
                if series.config.rid == rid:
                    return {'error': "template is used by series"}, 400

        template = await scrm.get_resource(rid)
        await scrm.delete_resource(template)

        return {'data': None}, 200

    @classmethod
    async def resp_update_series_config_static(cls, rid: str,
                                               name: str, desc: str):
        scrm = ServerState.series_config_rm
        rid = int(rid)
        if not scrm.rid_exists(rid):
            return {'error': "config does not exists"}, 404

        template = await scrm.get_resource(rid)
        if name is not None:
            template['name'] = name
        if desc is not None:
            template['description'] = desc
        await template.store()

        return {'data': template}, 201

    @classmethod
    async def resp_update_series_config(cls, rid: str,
                                        config: dict):
        scrm = ServerState.series_config_rm
        rid = int(rid)
        if not scrm.rid_exists(rid):
            return {'error': "config does not exists"}, 404

        template = await scrm.get_resource(rid)
        if config is None:
            return {'error', 'no config given', 400}
        template['config'] = config
        await template.store()
        async for series in ServerState.series_rm.itter(update=True):
            if series._config_from_template:
                async with SeriesManager.get_state(series.name) as state:
                    ServerState.index_series_schedules(series, state)

        return {'data': template}, 201

    @classmethod
    async def resp_run_job_for_series(cls,
                                      series_name: str,
                                      data: dict,
                                      pool_id: int,
                                      response_output_id: int):
        try:
            config = SeriesJobConfigModel(**data.get('config'))
            job = EnodoJob(
                None, series_name, config, (pool_id << 8) | config.job_type_id)
            request = EnodoRequest(
                request_type=REQUEST_TYPE_EXTERNAL,
                config=job.job_config,
                series_name=job.series_name,
                response_output_id=response_output_id,
                meta=data.get('meta'))
            await EnodoJobManager.send_job(job, request)
        except Exception as e:
            logging.error("Cannot activate job")
            logging.debug(f"Corresponding error: {str(e)}")
            return {'error': "Cannot activate job"}, 400

        return {}, 200

    @classmethod
    async def resp_query_series_state(
            cls, pool_id, series_name: str, job_type_id: int):
        fut, fut_id = await ClientManager.query_series_state(
            pool_id, job_type_id, series_name)
        try:
            result = await wait_for(fut, timeout=5)
        except asyncio.TimeoutError:
            QueryHandler.clear_query(fut_id)
            return {'error': "Cannot get result. No response from worker"}, 400
        except Exception:
            return {'error': "Cannot get result"}, 400
        else:
            return {'data': result}, 200

    @classmethod
    @sync_simple_fields_filter()
    def resp_get_possible_analyser_modules(cls):
        """Get all modules that are available

        Returns:
            dict: dict with data
        """
        return {'data': list(ClientManager.modules.values())}

    @classmethod
    @sync_simple_fields_filter(is_list=False)
    def resp_get_enodo_hub_status(cls):
        queue = {}
        queue_items = list(ServerState.job_schedule_index.items)
        for series_name in queue_items:
            queue[series_name] = ServerState.job_schedule_index.mapping.get(
                series_name)
        return {'data': {
            'version': VERSION,
            'job_index_queue': queue
        }}

    @classmethod
    @sync_simple_fields_filter(is_list=False)
    def resp_get_enodo_config(cls):
        return {'data': Config.get_settings(include_secrets=False)}

    @classmethod
    def resp_set_config(cls, data):
        """Update config

        Args:
            data (dict): config/settings

        Returns:
            dict: dict with boolean if succesful
        """
        keys_and_values = data.get('entries')
        for key in keys_and_values:
            if Config.is_runtime_configurable(key):
                Config.update_settings(
                    ServerState.storage.client,
                    key, keys_and_values[key])

        return {'data': True}

    @classmethod
    @async_simple_fields_filter(is_list=False)
    async def resp_get_enodo_stats(cls):
        return {'data': {
            "no_series": SeriesManager.get_series_count(),
            "no_ignored_series":
                await SeriesManager.get_ignored_series_count(),
                "no_open_jobs": EnodoJobManager.get_open_jobs_count(),
                "no_active_jobs": EnodoJobManager.get_active_jobs_count(),
                "no_failed_jobs": EnodoJobManager.get_failed_jobs_count(),
                "no_listeners": ClientManager.get_listener_count(),
                "no_workers": ClientManager.get_worker_count(),
                "no_busy_workers": ClientManager.get_busy_worker_count(),
                "no_output_streams": len(await EnodoOutputManager.get_outputs())
        }}

    @classmethod
    def resp_get_enodo_labels(cls):
        data = SeriesManager.get_labels_data()
        return {'data': data}

    @classmethod
    async def resp_add_enodo_label(cls, data):
        await SeriesManager.add_label(data.get('description'),
                                      data.get('name'),
                                      data.get('series_config'))
        return {'data': True}, 201

    @classmethod
    async def resp_remove_enodo_label(cls, data):
        data = await SeriesManager.remove_label(data.get('name'))
        if not data:
            return {'error': "Cannot remove label"}, 400
        return {'data': data}, 200
