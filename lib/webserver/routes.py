from lib.webserver.apihandlers import ApiHandlers


def setup_routes(app, cors):
    """Add REST API routes

    Args:
        app (instance): AIOHTTP app instance
        cors (CorsConfig): cors
    """
    # Add rest api routes
    app.router.add_get(
        "/api/series", ApiHandlers.get_monitored_series,
        allow_head=False)
    app.router.add_post(
        "/api/series", ApiHandlers.add_series)
    app.router.add_get(
        "/api/series/{series_name}", ApiHandlers.get_single_monitored_series,
        allow_head=False)

    app.router.add_get(
        "/api/series/{rid}/output",
        ApiHandlers.get_all_series_output,
        allow_head=False)

    app.router.add_delete(
        "/api/series/{rid}", ApiHandlers.remove_series)
    app.router.add_post(
        "/api/series/{rid}/run", ApiHandlers.run_job_for_series)
    app.router.add_get(
        "/api/series/{rid}/state/{job_type}", ApiHandlers.query_series_state)
    app.router.add_get(
        "/api/config/series", ApiHandlers.get_series_configs)
    app.router.add_post(
        "/api/config/series", ApiHandlers.add_series_config)
    app.router.add_delete(
        "/api/config/series/{rid}", ApiHandlers.remove_series_config)
    app.router.add_put(
        "/api/config/series/{rid}/static",
        ApiHandlers.update_series_config_static)
    app.router.add_put(
        "/api/config/series/{rid}", ApiHandlers.update_series_config)
    app.router.add_get(
        "/api/enodo/module", ApiHandlers.get_possible_analyser_modules,
        allow_head=False)
    app.router.add_get(
        "/api/enodo/output/{output_type}", ApiHandlers.get_enodo_outputs)
    app.router.add_delete(
        "/api/enodo/output/{output_type}/{output_id}",
        ApiHandlers.remove_enodo_output)
    app.router.add_post(
        "/api/enodo/output/{output_type}", ApiHandlers.add_enodo_output)
    app.router.add_get(
        "/api/enodo/stats", ApiHandlers.get_enodo_stats)
    app.router.add_get(
        "/api/enodo/label", ApiHandlers.get_enodo_labels, allow_head=False)
    app.router.add_post(
        "/api/enodo/label", ApiHandlers.add_enodo_label)
    app.router.add_delete(
        "/api/enodo/label", ApiHandlers.remove_enodo_label)

    # Add internal api routes
    app.router.add_get(
        "/api/settings", ApiHandlers.get_settings, allow_head=False)
    app.router.add_post(
        "/api/settings", ApiHandlers.update_settings)
    app.router.add_get(
        "/api/enodo/status", ApiHandlers.get_siridb_enodo_status,
        allow_head=False)
    app.router.add_get(
        "/api/enodo/log", ApiHandlers.get_event_log, allow_head=False)
    app.router.add_get(
        "/api/enodo/clients", ApiHandlers.get_connected_clients,
        allow_head=False)

    # SiriDB proxy
    app.router.add_get(
        "/api/siridb/query", ApiHandlers.run_siridb_query, allow_head=False)

    # Add non api routes
    app.router.add_get(
        "/status/ready", ApiHandlers.get_enodo_readiness, allow_head=False)
    app.router.add_get(
        "/status/live", ApiHandlers.get_enodo_liveness, allow_head=False)

    # Configure CORS on all routes.
    for route in list(app.router.routes()):
        cors.add(route)
