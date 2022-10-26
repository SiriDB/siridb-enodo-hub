from asyncio import wait_for
import asyncio
import logging

from siridb.connector.lib.exceptions import QueryError, InsertError, \
    ServerError, PoolError, AuthenticationError, UserAuthError

from enodo.model.config.series import SeriesJobConfigModel
from enodo.protocol.packagedata import REQUEST_TYPE_EXTERNAL, EnodoRequest

from lib.config import Config
from lib.outputmanager import EnodoOutputManager
from lib.jobmanager import EnodoJob, EnodoJobManager
from lib.serverstate import ServerState
from lib.socket.clientmanager import ClientManager
from lib.socket.queryhandler import QueryHandler
from siridb.connector.lib.exceptions import (
    AuthenticationError, InsertError, PoolError, QueryError,
    ServerError, UserAuthError)
from lib.util.util import (
    async_simple_fields_filter,
    sync_simple_fields_filter)
from version import VERSION


class BaseHandler:

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
        # async for series in ServerState.series_rm.itter(update=True):
        #     if series._config_from_template:
        #         async with SeriesManager.get_state(series.name) as state:
        #             ServerState.index_series_schedules(series, state)

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
        return {'data': {
            'version': VERSION,
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
                    ServerState.thingsdb_client,
                    key, keys_and_values[key])

        return {'data': True}

    @classmethod
    @async_simple_fields_filter(is_list=False)
    async def resp_get_enodo_stats(cls):
        return {
            'data': {
                "no_listeners": ClientManager.get_listener_count(),
                "no_workers": ClientManager.get_worker_count(),
                "no_output_streams": len(
                    await EnodoOutputManager.get_outputs())
            }}
