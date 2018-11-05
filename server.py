import asyncio
import os

from lib.config.config import Config
from lib.filter.filter import Filter
from lib.siridb.pipeserver import PipeServer

loop = asyncio.get_event_loop()


async def start_up():
    await Config.read_config()

    if os.path.exists(Config.pipe_path):
        os.unlink(Config.pipe_path)


def on_data(data):
    asyncio.ensure_future(handle_data(data))


async def handle_data(data):
    should_be_handled = await Filter.should_handle_data(data)
    print(f"Measurement(s) {data} {'Will be handled' if should_be_handled else 'Will not be handled'}\n")


async def start_siridb_pipeserver():
    pipe_server = PipeServer(Config.pipe_path, on_data)
    await pipe_server.create()


loop.run_until_complete(start_up())
loop.run_until_complete(start_siridb_pipeserver())
loop.run_forever()
