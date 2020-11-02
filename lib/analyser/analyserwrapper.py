GENERAL_PARAMETERS = {
    'forecast_points_in_future': 10,
    'use_data_since_timestamp': None,
}


async def setup_default_model_arguments(model_arguments):
    for key in GENERAL_PARAMETERS.keys():
        if key not in model_arguments:
            model_arguments[key] = GENERAL_PARAMETERS[key]

    return model_arguments
