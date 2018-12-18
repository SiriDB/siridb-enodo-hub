import asyncio


class Analyser:
    _analyse_queue = None
    _busy = None

    @classmethod
    async def prepare(cls):
        cls._analyse_queue = list()
        cls._busy = False

    @classmethod
    async def analyse_serie(cls, serie_name):
        cls._busy = True
        pass

    @classmethod
    async def work_queue(cls):
        pass

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
