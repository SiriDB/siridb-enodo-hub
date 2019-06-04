import asyncio
import pandas as pd

from lib.analyser.model.arimamodel import ARIMAModel
from lib.analyser.model.prophetmodel import ProphetModel
from lib.exceptions.analyserexception import AnalyserException
from lib.logging.eventlogger import EventLogger
from lib.serie.seriemanager import SerieManager
from lib.siridb.siridb import SiriDB


class Analyser:
    _analyser_queue = None
    _busy = None
    _siridb_client = None
    _shutdown = None
    _current_future = None

    @classmethod
    async def prepare(cls, queue):
        cls._siridb_client = SiriDB()
        cls._analyser_queue = queue
        cls._busy = False
        cls._shutdown = False

    @classmethod
    async def shutdown(cls):
        cls._shutdown = True
        return cls._current_future

    @classmethod
    async def _analyse_serie(cls, serie_name):
        """
        Collects data for starting an analysis of a specific time serie
        :param serie_name:
        :return:
        """
        # serie_data = await cls._siridb_client.query_serie_data(serie_name, "mean (4d)")
        serie_data = await cls._siridb_client.query_serie_data(serie_name)
        serie = await SerieManager.get_serie(serie_name)
        dataset = pd.DataFrame(serie_data[serie_name])
        serie_parameters = await serie.get_serie_parameters()
        try:
            analysis = ARIMAModel(serie_name, dataset, m=serie_parameters.get('m', 12),
                                  d=serie_parameters.get('d', None),
                                  d_large=serie_parameters.get('D', None))
            # analysis = ProphetModel(serie_name, dataset)
            analysis.create_model()
            print(analysis.do_forecast())
        except AnalyserException:
            print("error")
            pass
        except Exception as e:
            print(e)
            import traceback
            traceback.print_exc()
        else:
            EventLogger.log(f"Done analysing serie", "info", message_type='serie_done_analyse', serie=serie_name)
            analysis.save()
            await serie.set_analysed(True)

    @classmethod
    async def work_queue(cls):
        """
        Check queue if new serie needs to be analysed
        :return:
        """
        cls._busy = True
        if len(cls._analyser_queue) > 0:
            serie_name = cls._analyser_queue[0]
            print()
            EventLogger.log(f"Start analysing serie", "info", message_type='serie_start_analyse',
                            serie=serie_name)
            await cls._analyse_serie(serie_name)
            del cls._analyser_queue[0]
        cls._busy = False

    @classmethod
    async def add_to_queue(cls, serie_name):
        """
        Adds serie to queue when not already present.
        :param serie_name:
        :return:
        """
        if serie_name not in cls._analyser_queue:
            cls._analyser_queue.append(serie_name)

    @classmethod
    async def remove_from_queue(cls, serie_name):
        """
        Removes a serie by it's name when it's present in the queue
        :param serie_name:
        :return:
        """
        if serie_name in cls._analyser_queue:
            cls._analyser_queue.remove(serie_name)

    @classmethod
    def is_busy(cls):
        return cls._busy

    @classmethod
    def get_queue_size(cls):
        return len(cls._analyser_queue)

    @classmethod
    async def serie_in_queue(cls, serie_name):
        return serie_name in cls._analyser_queue

    @classmethod
    async def watch_queue(cls):
        """
        Loop for checking if queue needs work
        :return:
        """
        while not cls._shutdown:
            print(f"Watching Analyser queue")
            if not Analyser.is_busy() and Analyser.get_queue_size() and SiriDB.siridb_connected:
                cls._current_future = await Analyser.work_queue()
            await asyncio.sleep(5)


def start_analyser_worker(loop):
    """Switch to new event loop and run forever"""
    asyncio.set_event_loop(loop)
    loop.run_forever()
