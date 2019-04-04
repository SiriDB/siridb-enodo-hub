import asyncio
import pandas as pd

from lib.analyser.analysis import Analysis
from lib.exceptions.analyserexception import AnalyserException
from lib.serie.seriemanager import SerieManager
# from lib.siridb.siridb import SiriDB
from lib.siridb.siridb import SiriDB


class Analyser:
    _analyse_queue = None
    _busy = None
    _query_serie_data_cb = None
    _siridb_client = None

    @classmethod
    async def prepare(cls, queue):
        cls._siridb_client = SiriDB()
        cls._analyse_queue = queue
        cls._busy = False

    @classmethod
    async def _analyse_serie(cls, serie_name):
        serie_data = await cls._siridb_client.query_serie_data(serie_name, "mean (4d)")
        serie = await SerieManager.get_serie(serie_name)
        dataset = pd.DataFrame(serie_data[serie_name])
        serie_parameters = await serie.get_serie_parameters()
        try:
            analysis = Analysis(serie_name, dataset, m=serie_parameters.get('m', 12),
                                d=serie_parameters.get('d', None),
                                d_large=serie_parameters.get('D', None))
        except AnalyserException:
            print("error")
            pass
        except Exception as e:
            print(e)
        else:
            await analysis.save()
            await serie.set_analysed(True)

    @classmethod
    async def work_queue(cls):
        cls._busy = True
        if len(cls._analyse_queue) > 0:
            serie_name = cls._analyse_queue[0]
            print(f"Picking up {serie_name} from Analyser queue")
            await cls._analyse_serie(serie_name)
            del cls._analyse_queue[0]
        cls._busy = False

    @classmethod
    async def add_to_queue(cls, serie_name):
        if serie_name not in cls._analyse_queue:
            cls._analyse_queue.append(serie_name)

    @classmethod
    async def remove_from_queue(cls, serie_name):
        if serie_name in cls._analyse_queue:
            cls._analyse_queue.remove(serie_name)

    @classmethod
    def is_busy(cls):
        return cls._busy

    @classmethod
    def get_queue_size(cls):
        return len(cls._analyse_queue)

    @classmethod
    async def serie_in_queue(cls, serie_name):
        return serie_name in cls._analyse_queue


def start_analyser_worker(loop):
    """Switch to new event loop and run forever"""
    asyncio.set_event_loop(loop)
    loop.run_forever()


async def watch_queue():
    while True:
        print(f"Watching Analyser queue")
        if not Analyser.is_busy() and Analyser.get_queue_size():
            await Analyser.work_queue()
        await asyncio.sleep(5)
