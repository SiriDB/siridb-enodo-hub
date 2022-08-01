import logging
from uuid import uuid4
from aiohttp import web

from siridb.connector.lib.exceptions import QueryError, InsertError, \
    ServerError, PoolError, AuthenticationError, UserAuthError

from aiohttp import web
from enodo.jobs import JOB_TYPE_BASE_SERIES_ANALYSIS, JOB_STATUS_NONE
from enodo.model.config.series import SeriesConfigModel

from lib.config import Config
from lib.eventmanager import EnodoEventManager
from lib.jobmanager import EnodoJob, EnodoJobManager
from lib.series.seriesmanager import SeriesManager
from lib.serverstate import ServerState
from lib.siridb.siridb import query_all_series_results
from lib.socket.clientmanager import ClientManager
from lib.socketio import SUBSCRIPTION_CHANGE_TYPE_UPDATE
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
            series = await ServerState.series_rm.get_resource(series_name_rid)
        else:
            series = await SeriesManager.get_config_read_only(series_name_rid)
        if series is None:
            return {'data': {}}, 404
        series_data = series.to_dict()
        series_data["state"] = SeriesManager.get_state_read_only(
            series.name).to_dict()
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
                rid)
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
            if job_config.job_type == JOB_TYPE_BASE_SERIES_ANALYSIS:
                continue  # Ignore since no output
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
    @async_simple_fields_filter(return_index=0)
    async def resp_get_event_outputs(cls):
        """get all event output steams

        Returns:
            dict: dict with data
        """
        outputs = await EnodoEventManager.get_outputs()
        return {'data': outputs}, 200

    @classmethod
    async def resp_add_event_output(cls, output_type, data):
        """create event output stream

        Args:
            output_type (int): output type
            data (dict): data for output stream

        Returns:
            dict: dict with data
        """
        output = await EnodoEventManager.create_event_output(
            output_type, data)
        return {'data': output.to_dict()}, 201

    @classmethod
    async def resp_update_event_output(cls, output_id, data):
        """Update event output stream

        Args:
            output_id (int): id of an existing stream
            data (dict): data for output stream

        Returns:
            dict: dict with data
        """
        output = await EnodoEventManager.update_event_output(output_id, data)
        return {'data': output.to_dict()}, 201

    @classmethod
    async def resp_remove_event_output(cls, output_id):
        """remove output stream

        Args:
            output_id (int): id of an existing output stream

        Returns:
            dict: dict with data
        """
        await EnodoEventManager.remove_event_output(output_id)
        return {'data': None}, 200

    @classmethod
    async def resp_add_series(cls, data):
        """Add series for monitoring

        Args:
            data (dict): config of series

        Returns:
            dict: dict with data
        """
        if not isinstance(data.get('config'), str):
            try:
                series_config = SeriesConfigModel(**data.get('config'))
            except Exception as e:
                return {'error': 'Invalid series config',
                        'message': str(e)}, 400
            bc = series_config.get_config_for_job_type(
                JOB_TYPE_BASE_SERIES_ANALYSIS, first_only=True)
            if bc is None:
                return {'error': 'Something went wrong when adding '
                        'the series. Missing base analysis job'}, 400
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
                rid)
        if await SeriesManager.remove_series(series_name):
            await EnodoJobManager.cancel_jobs_for_series(series_name)
            await EnodoJobManager.remove_failed_jobs_for_series(series_name)
            return 200
        return 404

    @classmethod
    async def resp_add_job_config(cls, rid, job_config):
        """add job config to series config

        Args:
            series_name (string): name of sereis
            job_config_name (string): name of job config

        Returns:
            dict: dict with data if succeeded and error when necessary
        """
        series_name = ServerState.series_rm.get_resource_rid_value(rid)
        async with SeriesManager.get_config(series_name) as config:
            if config is None:
                return {"error": "Series does not exist"}, 400

            try:
                config.add_job_config(job_config)
            except Exception as e:
                return {"error": str(e)}, 400
            else:
                logging.info(
                    f"Added new job config to series {series_name}")
                return {"data": {"successful": True}}, 200

    @classmethod
    async def resp_remove_job_config(cls, rid, job_config_name, by_name=False):
        """Remove job config from series config

        Args:
            series_name (string): name of sereis
            job_config_name (string): name of job config

        Returns:
            dict: dict with data if succeeded and error when necessary
        """
        if by_name:
            series_name = rid
        else:
            series_name = ServerState.series_rm.get_resource_rid_value(
                rid)
        async with SeriesManager.get_series(series_name) as config:
            if config is None:
                return {"error": "Series does not exist"}, 400

            try:
                removed = config.remove_job_config(job_config_name)
            except Exception as e:
                return {"error": str(e)}, 400
            else:
                if removed:
                    await EnodoJobManager.cancel_jobs_by_config_name(
                        series_name,
                        job_config_name)
                logging.info(
                    f"Removed job config \"{job_config_name}\" "
                    f"from series {series_name}")
                return {"data": {"successful": removed}}, 200 \
                    if removed else 404

    @classmethod
    @sync_simple_fields_filter(return_index=0)
    def resp_get_series_config_templates(cls):
        scrm = ServerState.series_config_template_rm
        templates = scrm.get_cached_resources()
        return {'data': templates}, 200

    @classmethod
    async def resp_add_series_config_templates(cls, config_template: dict):
        scrm = ServerState.series_config_template_rm

        if scrm.rid_exists(config_template.get("rid")):
            return {'error': "template already exists"}, 400

        if config_template.get("rid") is None:
            config_template["rid"] = str(uuid4()).replace("-", "")

        async with scrm.create_resource(config_template) as template:
            return {'data': template}, 201

    @classmethod
    async def resp_remove_series_config_templates(cls, rid: str):
        scrm = ServerState.series_config_template_rm

        if not scrm.rid_exists(rid):
            return {'error': "template does not exists"}, 404

        async for series in ServerState.series_rm.itter():
            if series._config_from_template:
                if series.config.rid == rid:
                    return {'error': "template is used by series"}, 400

        template = await scrm.get_resource(rid)
        await scrm.delete_resource(template)

        return {'data': None}, 200

    @classmethod
    async def resp_update_series_config_templates_static(cls, rid: str,
                                                         name: str, desc: str):
        scrm = ServerState.series_config_template_rm

        if not scrm.rid_exists(rid):
            return {'error': "template does not exists"}, 404

        template = await scrm.get_resource(rid)
        if name is not None:
            template['name'] = name
        if desc is not None:
            template['description'] = desc
        await template.store()

        return {'data': template}, 201

    @classmethod
    async def resp_update_series_config_templates(cls, rid: str,
                                                  config: dict):
        scrm = ServerState.series_config_template_rm

        if not scrm.rid_exists(rid):
            return {'error': "template does not exists"}, 404

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
    def resp_get_jobs_queue(cls):
        return {'data': EnodoJobManager.get_open_queue()}

    @classmethod
    def resp_get_open_jobs(cls):
        return {'data': EnodoJobManager.get_open_queue()}

    @classmethod
    def resp_get_active_jobs(cls):
        return {'data': EnodoJobManager.get_active_jobs()}

    @classmethod
    def resp_get_failed_jobs(cls):
        return {'data': [
            EnodoJob.to_dict(j) for j in EnodoJobManager.get_failed_jobs()]}

    @classmethod
    async def resp_resolve_failed_job(cls, series_name):
        return {
            'data': await EnodoJobManager.remove_failed_jobs_for_series(
                series_name)}

    @classmethod
    async def resp_resolve_series_job_status(cls, rid,
                                             job_config_name, by_name=False):
        if by_name:
            series_name = rid
        else:
            series_name = ServerState.series_rm.get_resource_rid_value(
                rid)
        await EnodoJobManager.remove_failed_jobs_for_series(series_name,
                                                            job_config_name)
        async with SeriesManager.get_series(series_name) as (config, state):
            state.set_job_status(job_config_name, JOB_STATUS_NONE)
            config.schedule_job(job_config_name, state, initial=True)
            ServerState.index_series_schedules(config, state)
        return {'data': "OK"}

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
        return {'data': {
            'version': VERSION,
            'job_index_queue': ServerState.job_schedule_index.mapping
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
                "no_output_streams": len(await EnodoEventManager.get_outputs())
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
