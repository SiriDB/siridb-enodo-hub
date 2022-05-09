from aiohttp import web

from siridb.connector.lib.exceptions import QueryError, InsertError, \
    ServerError, PoolError, AuthenticationError, UserAuthError

from enodo.model.config.series import SeriesConfigModel

from version import VERSION

from lib.modulemanager import EnodoModuleManager
from lib.eventmanager import EnodoEventManager
from lib.series.seriesmanager import SeriesManager
from lib.serverstate import ServerState
from lib.siridb.siridb import query_series_anomalies, query_series_forecasts, \
    query_series_static_rules_hits
from lib.util import regex_valid
from lib.jobmanager import EnodoJobManager, EnodoJob
from lib.socketio import SUBSCRIPTION_CHANGE_TYPE_UPDATE
from lib.config import Config
from lib.socket.clientmanager import ClientManager


class BaseHandler:

    @classmethod
    async def resp_get_monitored_series(cls, regex_filter=None):
        """Get monitored series

        Args:
            regex_filter (string, optional): filter by regex. Defaults to None.

        Returns:
            dict: dict with data
        """
        if regex_filter is not None and not regex_valid(regex_filter):
            return {'data': []}
        return {'data': list(
            await SeriesManager.get_series_to_dict(regex_filter))}

    @classmethod
    async def resp_get_single_monitored_series(cls, series_name):
        """Get monitored series details

        Args:
            series_name (string): name of series

        Returns:
            dict: dict with data
        """
        series = await SeriesManager.get_series(series_name)
        if series is None:
            return {'data': ''}, 404
        series_data = series.to_dict()
        return {'data': series_data}, 200

    @classmethod
    async def resp_get_series_forecasts(cls, series_name):
        """Get series forecast results

        Args:
            series_name (string): name of series

        Returns:
            dict: dict with data
        """
        series = await SeriesManager.get_series(series_name)
        if series is None:
            return web.json_response(data={'data': ''}, status=404)
        return {'data': await query_series_forecasts(
            ServerState.get_siridb_output_conn(), series_name)}

    @classmethod
    async def resp_get_series_anomalies(cls, series_name):
        """Get series anomalies results

        Args:
            series_name (string): name of series

        Returns:
            dict: dict with data
        """
        series = await SeriesManager.get_series(series_name)
        if series is None:
            return web.json_response(data={'data': ''}, status=404)
        return {'data': await query_series_anomalies(
            ServerState.get_siridb_output_conn(), series_name)}

    @classmethod
    async def resp_get_series_static_rules_hits(cls, series_name):
        """Get series static rules hits

        Args:
            series_name (string): name of series

        Returns:
            dict: dict with data
        """
        series = await SeriesManager.get_series(series_name)
        if series is None:
            return web.json_response(data={'data': ''}, status=404)
        return {'data': await query_series_static_rules_hits(
            ServerState.get_siridb_output_conn(), series_name)}

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
    async def resp_get_event_outputs(cls):
        """get all event output steams

        Returns:
            dict: dict with data
        """
        return {'data': [
            output.to_dict() for output in EnodoEventManager.outputs]}, 200

    @classmethod
    async def resp_add_event_output(cls, output_type, data):
        """create event output stream

        Args:
            output_type (int): output type
            data (dict): data for output stream

        Returns:
            dict: dict with data
        """
        output = await EnodoEventManager.create_event_output(output_type, data)
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
        try:
            series_config = SeriesConfigModel(**data.get('config'))
        except Exception as e:
            return {'error': 'Invalid series config', 'message': str(e)}, 400
        for job_config in series_config.job_config.values():
            module_parameters = job_config.module_params

            module = await EnodoModuleManager.get_module(job_config.module)
            if module is not None:
                if module_parameters is None and len(
                        module.module_arguments.keys()) > 0:
                    return {'error': 'Missing module parameters'}, 400
                for m_args in module.module_arguments:
                    if job_config.job_type in m_args.get("job_types", []) and \
                            m_args.get("required") and \
                            m_args.get('name') not in module_parameters.keys():
                        return {'error': "Missing required module parameter '"
                                f"{m_args.get('name')}' for job type "
                                f"{job_config.job_type}"}, 400

        if not await SeriesManager.add_series(data):
            return {'error': 'Something went wrong when adding the series. \
                Are you sure the series exists?'}, 400

        return {'data': list(await SeriesManager.get_series_to_dict())}, 201

    @classmethod
    async def resp_update_series(cls, series_name, data):
        """Update series

        Args:
            series_name (string): name of series
            data (dict): config of series

        Returns:
            dict: dict with data
        """
        try:
            series_config = SeriesConfigModel(**data.get('config'))
        except Exception as e:
            return {'error': 'Invalid series config', 'message': str(e)}, 400
        for job_config in list(series_config.job_config.values()):
            module_parameters = job_config.module_params

            module = await EnodoModuleManager.get_module(job_config.module)
            if module is not None:
                if module_parameters is None and len(
                        module.module_arguments.keys()) > 0:
                    return {'error': 'Missing required fields'}, 400
                for key in module.module_arguments:
                    if key not in module_parameters.keys():
                        return {'error': f'Missing required field {key}'}, 400

        series = await SeriesManager.get_series(series_name)
        if series is None:
            return {'error': 'Something went wrong when updating the series. \
                Are you sure the series exists?'}, 400
        series.update(data)

        await SeriesManager.series_changed(
            SUBSCRIPTION_CHANGE_TYPE_UPDATE, series_name)

        return {'data': list(await SeriesManager.get_series_to_dict())}, 201

    @classmethod
    async def resp_remove_series(cls, series_name):
        """Remove series

        Args:
            series_name (string): name of series

        Returns:
            dict: dict with data
        """
        if await SeriesManager.remove_series(series_name):
            await EnodoJobManager.cancel_jobs_for_series(series_name)
            EnodoJobManager.remove_failed_jobs_for_series(series_name)
            return 200
        return 404

    @classmethod
    async def resp_get_jobs_queue(cls):
        return {'data': await EnodoJobManager.get_open_queue()}

    @classmethod
    async def resp_get_open_jobs(cls):
        return {'data': await EnodoJobManager.get_open_queue()}

    @classmethod
    async def resp_get_active_jobs(cls):
        return {'data': EnodoJobManager.get_active_jobs()}

    @classmethod
    async def resp_get_failed_jobs(cls):
        return {'data': [
            EnodoJob.to_dict(j) for j in EnodoJobManager.get_failed_jobs()]}

    @classmethod
    async def resp_resolve_failed_job(cls, series_name):
        return {
            'data': EnodoJobManager.remove_failed_jobs_for_series(series_name)}

    @classmethod
    async def resp_get_possible_analyser_modules(cls):
        """Get all modules that are available

        Returns:
            dict: dict with data
        """
        data = {'modules': EnodoModuleManager.modules}
        return {'data': data}

    @classmethod
    async def resp_get_enodo_hub_status(cls):
        return {'data': {'version': VERSION}}

    @classmethod
    async def resp_get_enodo_config(cls):
        return {'data': Config.get_settings(include_secrets=False)}

    @classmethod
    async def resp_set_config(cls, data):
        """Update config

        Args:
            data (dict): config/settings

        Returns:
            dict: dict with boolean if succesful
        """
        section = data.get('section')
        keys_and_values = data.get('entries')
        changed = False
        for key in keys_and_values:
            if Config.is_runtime_configurable(section, key):
                changed = changed or Config.update_settings(
                    section, key, keys_and_values[key])
        Config.write_settings()
        Config.setup_settings_variables()

        if changed and section == "siridb":
            await ServerState.setup_siridb_data_connection()
        elif changed and section == "siridb_output":
            await ServerState.setup_siridb_output_connection()

        return {'data': True}

    @classmethod
    async def resp_get_enodo_stats(cls):
        return {'data': {
            "no_series": SeriesManager.get_series_count(),
            "no_ignored_series": SeriesManager.get_ignored_series_count(),
            "no_open_jobs": EnodoJobManager.get_open_jobs_count(),
            "no_active_jobs": EnodoJobManager.get_active_jobs_count(),
            "no_failed_jobs": EnodoJobManager.get_failed_jobs_count(),
            "no_listeners": ClientManager.get_listener_count(),
            "no_workers": ClientManager.get_worker_count(),
            "no_busy_workers": ClientManager.get_busy_worker_count(),
            "no_output_streams": len(EnodoEventManager.outputs)
        }}

    @classmethod
    async def resp_get_enodo_labels(cls):
        data = SeriesManager.get_labels_data()
        return {'data': data}

    @classmethod
    async def resp_add_enodo_label(cls, data):
        await SeriesManager.add_label(data.get('description'),
                                      data.get('name'),
                                      data.get('series_config'))
        return {'data': True}

    @classmethod
    async def resp_remove_enodo_label(cls, data):
        data = SeriesManager.remove_label(data.get('name'))
        if not data:
            return {'error': "Cannot remove label"}, 400
        return {'data': data}
