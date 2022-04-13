GENERAL_PARAMETERS = {
    'forecast_points_in_future': 10,
    'use_data_since_timestamp': None,
}


async def setup_default_module_arguments(module_arguments):
    """Setup default module params

    Args:
        module_arguments (dict): module params

    Returns:
        dict: params
    """
    for key in GENERAL_PARAMETERS:
        if key not in module_arguments:
            module_arguments[key] = GENERAL_PARAMETERS[key]

    return module_arguments
