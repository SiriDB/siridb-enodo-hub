import logging

AVAILABLE_LOG_LEVELS = {'error': logging.ERROR, 'warning': logging.WARNING, 'info': logging.INFO,
                        'debug': logging.DEBUG}


def prepare_logger(log_level_label):
    log_level = AVAILABLE_LOG_LEVELS.get(log_level_label)
    logger = logging.getLogger()
    logger.setLevel(log_level)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        fmt='[%(levelname)1.1s %(asctime)s hub] ' +
            '%(message)s',
        datefmt='%y%m%d %H:%M:%S',
        style='%')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
