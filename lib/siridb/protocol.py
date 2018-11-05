import asyncio
import logging

from .package import Package


class SiriDBServerProtocol(asyncio.Protocol):

    def __init__(self, on_package_received):
        self._buffered_data = bytearray()
        self._data_package = None
        self._on_package_received = on_package_received

    def data_received(self, data):
        '''
        override asyncio.Protocol
        '''
        self._buffered_data.extend(data)
        while self._buffered_data:
            size = len(self._buffered_data)
            if self._data_package is None:
                if size < Package.struct_datapackage.size:
                    return None
                self._data_package = Package(self._buffered_data)
            if size < self._data_package.length:
                return None
            try:
                self._data_package.extract_data_from(self._buffered_data)
            except KeyError as e:
                logging.error('Unsupported package received: {}'.format(e))
            except Exception as e:
                logging.exception(e)
                # empty the byte-array to recover from this error
                self._buffered_data.clear()
            else:
                self._on_package_received(self._data_package.data)
            self._data_package = None
