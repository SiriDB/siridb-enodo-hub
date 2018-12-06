import asyncio
import aiojobs
import os

from lib.analyser.analyser import Analyser
from lib.config.config import Config
from lib.serie.seriemanager import SerieManager
from lib.siridb.pipeserver import PipeServer
from lib.siridb.siridb import SiriDB

loop = asyncio.get_event_loop()


async def start_up():
    await Config.read_config()

    if os.path.exists(Config.pipe_path):
        os.unlink(Config.pipe_path)

    await SiriDB.prepare()
    await SerieManager.prepare()
    await Analyser.prepare()

    scheduler = await aiojobs.create_scheduler()
    await scheduler.spawn(watch_series())


def on_data(data):
    asyncio.ensure_future(handle_data(data))


async def watch_series():
    while True:
        for serie_name in Config.enabled_series_for_analysis:
            if (await Analyser.is_serie_analysed(serie_name)) is False:
                serie = await SerieManager.get_serie(serie_name)
                if serie is not None and await serie.get_datapoints_count() >= Config.min_data_points:
                    print(f"Start analysing serie: {serie_name}")
                    await Analyser.analyse_serie(serie_name)
        await asyncio.sleep(Config.watcher_interval)


async def handle_data(data):
    for serie_name, values in data.items():
        should_be_handled = serie_name in Config.enabled_series_for_analysis

        if should_be_handled:
            await SerieManager.add_to_datapoint_counter(serie_name, len(values))
            print(f"Handling new data for serie: {serie_name}")


async def start_siridb_pipeserver():
    pipe_server = PipeServer(Config.pipe_path, on_data)
    await pipe_server.create()


loop.run_until_complete(start_up())
loop.run_until_complete(start_siridb_pipeserver())
loop.run_forever()
