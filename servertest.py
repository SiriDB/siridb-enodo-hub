import asyncio
import os

from lib.siridb.pipeserver import PipeServer

PIPE_NAME = '/tmp/dbtest_tee.sock'

if os.path.exists(PIPE_NAME):
    os.unlink(PIPE_NAME)


def on_data(data):
    print("HI\r\n")


async def start_siridb_pipeserver():
    pipe_server = PipeServer(PIPE_NAME, on_data)
    await pipe_server.create()


loop = asyncio.get_event_loop()
loop.run_until_complete(start_siridb_pipeserver())
loop.run_forever()
