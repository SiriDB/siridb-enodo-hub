import argparse
import asyncio
import os

from lib.config import Config
from server import Server


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process config')
    parser.add_argument('--config', help='Config path', required=False)
    parser.add_argument('--log-level', help='Log level, error/warning/info/debug, default: info', required=False,
                        default='info')
    parser.add_argument('--port', help='Port to serve on, default: 80', required=False,
                        default=80)
    parser.add_argument('--create-config', help='Create standard config file', action='store_true', default=False)

    if parser.parse_args().create_config:
        Config.create_standard_config_file(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'default.conf'))
        exit()
    # elif not parser.parse_args().config:
    #     parser.error('No config given')

    loop = asyncio.get_event_loop()

    server = Server(loop, parser.parse_args().port, parser.parse_args().config, parser.parse_args().log_level)
    server.start_server()
