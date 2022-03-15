from lib.api.apihandlers import ApiHandlers


def setup_routes(app, cors):
    # Add rest api routes
    app.router.add_get(
        "/api/series", ApiHandlers.get_monitored_series,
        allow_head=False)
    app.router.add_post(
        "/api/series", ApiHandlers.add_series)
    app.router.add_get(
        "/api/series/{series_name}", ApiHandlers.get_monitored_series_details,
        allow_head=False)
    app.router.add_delete(
        "/api/series/{series_name}", ApiHandlers.remove_series)
    app.router.add_get(
        "/api/enodo/model", ApiHandlers.get_possible_analyser_models,
        allow_head=False)
    app.router.add_post(
        "/api/enodo/model", ApiHandlers.add_analyser_models)
    app.router.add_get(
        "/api/enodo/event/output", ApiHandlers.get_enodo_event_outputs)
    app.router.add_delete(
        "/api/enodo/event/output/{output_id}",
        ApiHandlers.remove_enodo_event_output)
    app.router.add_post(
        "/api/enodo/event/output", ApiHandlers.add_enodo_event_output)
    app.router.add_get(
        "/api/enodo/stats", ApiHandlers.get_enodo_stats)
    app.router.add_get(
        "/api/enodo/label", ApiHandlers.get_enodo_labels,
        allow_head=False)
    app.router.add_post(
        "/api/enodo/label", ApiHandlers.add_enodo_label)
    app.router.add_delete(
        "/api/enodo/label", ApiHandlers.remove_enodo_label)

    # Add internal api routes
    app.router.add_get(
        "/api/settings", ApiHandlers.get_settings,
        allow_head=False)
    app.router.add_post(
        "/api/settings", ApiHandlers.update_settings)
    app.router.add_get(
        "/api/enodo/status", ApiHandlers.get_siridb_enodo_status,
        allow_head=False)
    app.router.add_get(
        "/api/enodo/log", ApiHandlers.get_event_log,
        allow_head=False)
    app.router.add_get(
        "/api/enodo/clients", ApiHandlers.get_connected_clients,
        allow_head=False)

    # Add non api routes
    app.router.add_get(
        "/status/ready", ApiHandlers.get_enodo_readiness,
        allow_head=False)

    # Configure CORS on all routes.
    for route in list(app.router.routes()):
        cors.add(route)
