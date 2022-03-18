GENERAL_PARAMETERS = {
    'forecast_points_in_future': 10,
    'use_data_since_timestamp': None,
}


async def setup_default_model_arguments(model_arguments):
    """Setup default model params

    Args:
        model_arguments (dict): model params

    Returns:
        dict: params
    """
    for key in GENERAL_PARAMETERS:
        if key not in model_arguments:
            model_arguments[key] = GENERAL_PARAMETERS[key]

    return model_arguments
