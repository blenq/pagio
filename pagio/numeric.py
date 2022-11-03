from codecs import decode
from decimal import Decimal
from itertools import repeat, islice
from struct import Struct

from .common import ushort_struct_unpack_from, ProtocolError


def txt_numeric_to_python(buf: memoryview):
    return Decimal(decode(buf))


numeric_header = Struct("!HhHH")
NUMERIC_NAN = 0xC000
NUMERIC_POS = 0x0000
NUMERIC_NEG = 0x4000
NUMERIC_PINF = 0xD000
NUMERIC_NINF = 0xF000


def bin_numeric_to_python(buf: memoryview):
    npg_digits, weight, sign, dscale = numeric_header.unpack_from(buf)

    if sign == NUMERIC_NAN:
        sign = 0
        exp = 'n'
        digits = ()
    elif sign == NUMERIC_PINF:
        sign = 0
        digits = ()
        exp = 'F'
    elif sign == NUMERIC_NINF:
        sign = 1
        digits = ()
        exp = 'F'
    else:
        if sign == NUMERIC_NEG:
            sign = 1
        elif sign != NUMERIC_POS:
            print(sign)
            raise Exception('Bad value')
        exp = (weight + 1 - npg_digits) * 4
        buf = buf[numeric_header.size:]

        def get_digits():
            for i in range(npg_digits):
                dg = ushort_struct_unpack_from(buf, i * 2)[0]
                if dg > 9999:
                    raise ValueError("Invalid value")
                # a postgres digit contains 4 decimal digits
                q, r = divmod(dg, 1000)
                yield q
                q, r = divmod(r, 100)
                yield q
                q, r = divmod(r, 10)
                yield q
                yield r

        digits = tuple(dg for dg in get_digits())
    return Decimal((sign, digits, exp))
