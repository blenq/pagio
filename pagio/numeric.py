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


def bin_numeric_to_python(buf: memoryview):
    npg_digits, weight, sign, dscale = numeric_header.unpack_from(buf)
    if weight < 0:
        raise ProtocolError("Invalid value for numeric weight.")

    if sign == NUMERIC_NAN:
        sign = 0
        exp = 'n'
        digits = []
    else:
        if sign == NUMERIC_NEG:
            sign = 1
        elif sign != NUMERIC_POS:
            raise Exception('Bad value')
        exp = -dscale

        ndigits = dscale + (weight + 1) * 4

        def get_digits():
            offset = numeric_header.size
            for _ in range(npg_digits):
                dg = ushort_struct_unpack_from(buf, offset)[0]
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
                offset += 2
            # yield zeroes until caller is done
            yield from repeat(0)

        digits = [dg for dg in islice(get_digits(), ndigits)]
    return Decimal((sign, digits, exp))