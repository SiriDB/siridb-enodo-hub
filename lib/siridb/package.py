import struct
import qpack


class Package:

    __slots__ = ('pid', 'length', 'tipe', 'checkbit', 'data')

    struct_datapackage = struct.Struct('<IHBB')

    def __init__(self, barray):
        self.length, self.pid, self.tipe, self.checkbit = \
            self.__class__.struct_datapackage.unpack_from(barray, offset=0)
        self.length += self.__class__.struct_datapackage.size
        self.data = None

    def extract_data_from(self, barray):
        try:
            self.data = qpack.unpackb(
                barray[self.__class__.struct_datapackage.size:self.length],
                decode='utf-8')
        finally:
            del barray[:self.length]
