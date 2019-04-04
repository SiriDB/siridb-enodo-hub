import asyncio
from unittest import TestCase

from lib.serie.serie import Serie


class TestSerie(TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()

    def test_set_datapoints_counter_lock(self):
        async def async_test_set_datapoints_counter_lock():
            serie = Serie('test_serie', 234)
            await serie.set_datapoints_counter_lock(True)
            self.assertEqual(serie.datapoints_count_lock, True)
            await serie.set_datapoints_counter_lock(False)
            self.assertEqual(serie.datapoints_count_lock, False)

        coro = asyncio.coroutine(async_test_set_datapoints_counter_lock)
        self.loop.run_until_complete(coro())

    def test_get_datapoints_counter_lock(self):
        async def async_test_get_datapoints_counter_lock():
            serie = Serie('test_serie', 234)
            await serie.set_datapoints_counter_lock(True)
            self.assertEqual(await serie.get_datapoints_counter_lock(), True)
            await serie.set_datapoints_counter_lock(False)
            self.assertEqual(await serie.get_datapoints_counter_lock(), False)

        coro = asyncio.coroutine(async_test_get_datapoints_counter_lock)
        self.loop.run_until_complete(coro())

    def test_get_name(self):
        async def async_test_get_name():
            serie_name = 'test_serie123'
            serie = Serie(serie_name, 123456)
            self.assertEqual(await serie.get_name(), serie_name)

        coro = asyncio.coroutine(async_test_get_name)
        self.loop.run_until_complete(coro())

    def test_get_type(self):
        async def async_test_get_type():
            serie_type = 'microseconds'
            serie = Serie('test_serie123', 123456, serie_type)
            self.assertEqual(await serie.get_type(), serie_type)

        coro = asyncio.coroutine(async_test_get_type)
        self.loop.run_until_complete(coro())

    def test_get_datapoints_count(self):
        async def async_test_get_datapoints_count():
            serie_name = 'test_serie123'
            serie = Serie(serie_name, 123456)
            self.assertEqual(await serie.get_datapoints_count(), 123456)

        coro = asyncio.coroutine(async_test_get_datapoints_count)
        self.loop.run_until_complete(coro())

    def test_add_to_datapoints_count(self):
        async def async_test_add_to_datapoints_count():
            serie_name = 'test_serie123'
            serie = Serie(serie_name, 123456)
            await serie.add_to_datapoints_count(22)
            self.assertEqual(await serie.get_datapoints_count(), 123478)

        coro = asyncio.coroutine(async_test_add_to_datapoints_count)
        self.loop.run_until_complete(coro())

    def test_to_json(self):
        async def async_test_to_json():
            serie_name = 'test_serie123'
            serie = Serie(serie_name, 123456)
            expected_result = {'data_points': 123456, 'name': 'test_serie123', 'type': 'miliseconds'}
            self.assertEqual(await serie.to_dict(), expected_result)

        coro = asyncio.coroutine(async_test_to_json)
        self.loop.run_until_complete(coro())
