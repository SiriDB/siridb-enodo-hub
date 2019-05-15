import asyncio
from unittest import TestCase

from lib.config.config import Config
from lib.serie.serie import Serie
from lib.serie.seriemanager import SerieManager


class TestSerieManager(TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()

    def test_get_serie(self):
        async def async_test_get_serie():
            # Set new serie name and create Serie instrance
            serie_name = 'serie_01'
            serie = Serie(serie_name, 12)

            # Mock config class
            Config.names_enabled_series_for_analysis = []

            # Prepare SerieManger (clear/reset)
            await SerieManager.prepare()

            # Add serie to SerieManager
            SerieManager._series[serie_name] = serie

            # Check if returned serie is the same as inserted one
            returned_serie = await SerieManager.get_serie(serie_name)
            self.assertEqual(returned_serie, serie)

        coro = asyncio.coroutine(async_test_get_serie)
        self.loop.run_until_complete(coro())

    def test_get_monitored_series(self):
        async def async_test_get_monitored_series():
            # Set new serie name and create Serie instrance
            serie_name = 'serie_02'
            serie = Serie(serie_name, 50)

            # Mock config class
            Config.names_enabled_series_for_analysis = []

            # Prepare SerieManger (clear/reset)
            await SerieManager.prepare()

            # Add serie to SerieManager
            SerieManager._series[serie_name] = serie

            # Check if returned serie is the same as inserted one
            json_monitored_series = await SerieManager.get_series_to_dict()
            expected_value = [
                {'analysed': False, 'data_points': 50, 'name': 'serie_02', 'parameters': {}, 'type': 'ms'}]
            self.assertEqual(json_monitored_series, expected_value)

        coro = asyncio.coroutine(async_test_get_monitored_series)
        self.loop.run_until_complete(coro())

    def test_remove_serie(self):
        async def async_test_remove_serie():
            # Set new serie name and create Serie instance
            serie1 = Serie('Serie_001', 50)
            serie2 = Serie('Serie_002', 20)
            serie3 = Serie('Serie_003', 43)
            serie4 = Serie('Serie_004', 11)

            # Mock config class
            Config.names_enabled_series_for_analysis = []
            Config.enabled_series_for_analysis = {}

            # Prepare SerieManger (clear/reset)
            await SerieManager.prepare()

            # Add serie to SerieManager
            SerieManager._series['Serie_001'] = serie1
            SerieManager._series['Serie_002'] = serie2
            SerieManager._series['Serie_003'] = serie3
            SerieManager._series['Serie_004'] = serie4

            # Remove series
            await SerieManager.remove_serie('Serie_001')
            await SerieManager.remove_serie('Serie_003')

            # Check if returned serie is the same as inserted one
            json_monitored_series = await SerieManager.get_series_to_dict()
            expected_value = [
                {'analysed': False, 'data_points': 20, 'name': 'Serie_002', 'parameters': {}, 'type': 'ms'},
                {'analysed': False, 'data_points': 11, 'name': 'Serie_004', 'parameters': {}, 'type': 'ms'}]
            self.assertEqual(json_monitored_series, expected_value)

        coro = asyncio.coroutine(async_test_remove_serie)
        self.loop.run_until_complete(coro())

    def test_add_to_datapoint_counter(self):
        async def async_test_add_to_datapoint_counter():
            # Set new serie name and create Serie instrance
            serie_name = 'serie_02'
            serie = Serie(serie_name, 50)

            # Mock config class
            Config.names_enabled_series_for_analysis = []

            # Prepare SerieManger (clear/reset)
            await SerieManager.prepare()

            # Add serie to SerieManager
            SerieManager._series[serie_name] = serie

            # Add value to the counter
            await SerieManager.add_to_datapoint_counter(serie_name, 12)

            # Check if returned serie is the same as inserted one
            json_monitored_series = await SerieManager.get_series_to_dict()
            expected_value = [
                {'analysed': False, 'data_points': 62, 'name': 'serie_02', 'parameters': {}, 'type': 'ms'}]
            self.assertEqual(json_monitored_series, expected_value)

        coro = asyncio.coroutine(async_test_add_to_datapoint_counter)
        self.loop.run_until_complete(coro())