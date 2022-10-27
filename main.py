import argparse
import os

from lib.config import Config
from server import Server


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process config')
    parser.add_argument('--config', help='Config path', required=False)
    parser.add_argument(
        '--create-config',
        help='Create standard config file',
        action='store_true',
        default=False)

    if parser.parse_args().create_config:
        Config.create_standard_config_file(
            os.path.join(os.path.dirname(
                os.path.realpath(__file__)), 'default.conf'))
        exit()

    server = Server(parser.parse_args().config)
    server.start_server()
