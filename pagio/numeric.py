""" Numeric conversions """
from codecs import decode
from decimal import Decimal
from struct import Struct
from typing import Tuple, Any, Generator

from . import const
from .common import ushort_struct_unpack_from, Format, ProtocolError


def get_struct_unpacker(fmt: str) -> Any:
    """ Creates a function to get a single value from a struct """

    _unpack = Struct(f"!{fmt}").unpack

    def unpack_struct(buf: memoryview) -> Any:
        return _unpack(buf)[0]

    return unpack_struct


# ======== int ============================================================== #

bin_int2_to_python = get_struct_unpacker("h")
bin_int_to_python = get_struct_unpacker("i")
bin_uint_to_python = get_struct_unpacker("I")
bin_int8_to_python = get_struct_unpacker("q")

# ======== float ============================================================ #


def float_to_pg(val: float) -> Tuple[int, str, float, int, Format]:
    """ Converts a python float to a PG parameter tuple """
    return const.FLOAT8OID, "d", val, 8, Format.BINARY


bin_float4_to_python = get_struct_unpacker("f")
bin_float8_to_python = get_struct_unpacker("d")

# ======== numeric ========================================================== #


def txt_numeric_to_python(buf: memoryview) -> Decimal:
    """ Converts a PG numeric text value to a Python Decimal """
    return Decimal(decode(buf))


numeric_header = Struct("!HhHH")
NUMERIC_NAN = 0xC000
NUMERIC_POS = 0x0000
NUMERIC_NEG = 0x4000
NUMERIC_PINF = 0xD000
NUMERIC_NINF = 0xF000


def bin_numeric_to_python(buf: memoryview) -> Decimal:
    """ Converts a PG numeric binary value to a Python Decimal """

    npg_digits, weight, sign, _ = numeric_header.unpack_from(buf)

    if sign == NUMERIC_NAN:
        return Decimal("NaN")
    if sign == NUMERIC_PINF:
        return Decimal("inf")
    if sign == NUMERIC_NINF:
        return Decimal("-inf")

    if sign == NUMERIC_NEG:
        sign = 1
    elif sign != NUMERIC_POS:
        raise Exception('Bad value')

    buf = buf[numeric_header.size:]

    def get_digits() -> Generator[int, None, None]:
        for i in range(npg_digits):
            pg_digit = ushort_struct_unpack_from(buf, i * 2)[0]
            if pg_digit > 9999:
                raise ValueError("Invalid value")
            # a postgres digit contains 4 decimal digits
            quot, rest = divmod(pg_digit, 1000)
            yield quot
            quot, rest = divmod(rest, 100)
            yield quot
            quot, rest = divmod(rest, 10)
            yield quot
            yield rest

    digits = tuple(get_digits())
    exp = (weight + 1 - npg_digits) * 4

    return Decimal((sign, digits, exp))

# ======== bool ============================================================= #


def bin_bool_to_python(buf: memoryview) -> bool:
    """ Converts PG binary bool value to Python bool. """
    if buf == b'\x01':
        return True
    if buf == b'\0':
        return False
    raise ProtocolError("Invalid value for bool")


def text_bool_to_python(buf: memoryview) -> bool:
    """ Converts PG textual bool value to Python bool. """
    if buf == b't':
        return True
    if buf == b'f':
        return False
    raise ProtocolError("Invalid value for bool")
