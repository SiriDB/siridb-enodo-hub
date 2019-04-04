import asyncio
from unittest import TestCase

from lib.analyser.analyser import Analyser


class TestAnalyser(TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()

    def test_add_to_queue(self):
        async def async_test_add_to_queue():
            serie_name = 'serie_name_101'
            # Reset Analyser
            await Analyser.prepare()

            # Add serie_name to queue
            await Analyser.add_to_queue(serie_name)

            # Assert
            self.assertEqual(Analyser._analyse_queue, [serie_name])

        coro = asyncio.coroutine(async_test_add_to_queue)
        self.loop.run_until_complete(coro())

    def test_remove_from_queue(self):
        async def async_test_remove_from_queue():
            # Reset Analyser
            await Analyser.prepare()

            # Add series to queue
            await Analyser.add_to_queue('serie_name_1')
            await Analyser.add_to_queue('serie_name_2')
            await Analyser.add_to_queue('serie_name_3')

            # remove serie from queue
            await Analyser.remove_from_queue('serie_name_2')

            # Assert
            self.assertEqual(Analyser._analyse_queue, ['serie_name_1', 'serie_name_3'])

        coro = asyncio.coroutine(async_test_remove_from_queue)
        self.loop.run_until_complete(coro())

    def test_is_busy(self):
        async def async_test_is_busy():
            # Reset Analyser
            await Analyser.prepare()

            # Assert initial value
            self.assertFalse(Analyser.is_busy())

            # Set new value
            Analyser._busy = True

            # Assert new value
            self.assertTrue(Analyser.is_busy())

        coro = asyncio.coroutine(async_test_is_busy)
        self.loop.run_until_complete(coro())

    def test_get_queue_size(self):
        async def async_test_get_queue_size():
            # Reset Analyser
            await Analyser.prepare()

            # Assert initial state
            self.assertEqual(Analyser.get_queue_size(), 0)

            # Add series to queue
            await Analyser.add_to_queue('serie_1')
            await Analyser.add_to_queue('serie_2')
            await Analyser.add_to_queue('serie_3')
            await Analyser.add_to_queue('serie_4')
            await Analyser.add_to_queue('serie_5')

            # Assert new state
            self.assertEqual(Analyser.get_queue_size(), 5)

        coro = asyncio.coroutine(async_test_get_queue_size)
        self.loop.run_until_complete(coro())

    def test_serie_in_queue(self):
        async def async_test_serie_in_queue():
            serie_name = 'serie_name_101'
            # Reset Analyser
            await Analyser.prepare()

            # Assert initial state
            self.assertFalse(await Analyser.serie_in_queue(serie_name))

            # Add serie_name to queue
            await Analyser.add_to_queue(serie_name)

            # Assert
            self.assertTrue(await Analyser.serie_in_queue(serie_name))

        coro = asyncio.coroutine(async_test_serie_in_queue)
        self.loop.run_until_complete(coro())
