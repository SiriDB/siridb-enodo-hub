import argparse
import asyncio
import os

from lib.config.config import Config
from server import Server


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process config')
    parser.add_argument('--config', help='Config path', required=False)
    parser.add_argument('--log_level', help='Log level, error/warning/info/verbose, default: info', required=False,
                        default='info')
    parser.add_argument('--docs_only', help='Host docs only', action='store_true', default=False)
    parser.add_argument('--create_config', help='Create standard config file', action='store_true', default=False)

    if parser.parse_args().create_config:
        Config.create_standard_config_file(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'default.conf'))
        exit()
    elif not parser.parse_args().config:
        parser.error('No config given')

    docs_only = parser.parse_args().docs_only

    loop = asyncio.get_event_loop()

    server = Server(loop, parser.parse_args().config, parser.parse_args().log_level, docs_only)
    server.start_server()
