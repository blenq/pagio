""" Numeric conversions """
import struct
from codecs import decode
from decimal import Decimal
from struct import Struct
from typing import Tuple, Any, Generator, Union

from . import const
from .common import ushort_struct_unpack_from, Format, ProtocolError
from .text import default_to_pg


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


def int_to_pg(val: int) -> Tuple[int, str, Union[int, bytes], int, Format]:
    """ Convert a Python int to a PG int parameter """
    if -0x10000000 <= val <= 0x7FFFFFFF:
        return const.INT4OID, "i", val, 4, Format.BINARY
    if -0x1000000000000000 <= val <= 0x7FFFFFFFFFFFFFFF:
        return const.INT8OID, "q", val, 8, Format.BINARY
    return default_to_pg(val)


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
            [pg_digit] = ushort_struct_unpack_from(buf, i * 2)
            if pg_digit > 9999:
                raise ValueError("Invalid value")
            # a postgres digit contains 4 decimal digits
            quot, rest = divmod(pg_digit, 1000)
            yield quot
            quot, rest = divmod(rest, 100)
            yield quot
            yield from divmod(rest, 10)

    digits = tuple(get_digits())
    exp = (weight + 1 - npg_digits) * 4

    return Decimal((sign, digits, exp))


def numeric_to_pg(val: Decimal) -> Tuple[int, str, bytes, int, Format]:
    """ Converts a Python decimal to a binary PG numeric """

    pg_digits = []
    if val.is_finite():
        # Regular finite number

        sign, digits, exp = val.as_tuple()
        if exp < -0x3FFF:
            # outside PG range, fallback to text
            return default_to_pg(val)

        # PG scale can not be negative.
        pg_scale = 0 if exp > 0 else -exp

        pg_sign = NUMERIC_NEG if sign else NUMERIC_POS

        # A PostgreSQL numeric pg_digit is a number from 0 to 9999, and
        # represents 4 decimal digits.
        # "len(digits) + exp", i.e. the number of decimal digits plus the
        # exponent is the 10 based exponent of the first decimal digit.
        # pg_weight is 10000 based exponent of first pg_digit minus one
        q, r = divmod(len(digits) + exp, 4)
        pg_weight = q + bool(r) - 1
        if pg_weight < -0x8000 or pg_weight > 0x7FFF:
            # outside PG range
            return default_to_pg(val)

        # Fill array of pg_digits.
        # The pg_digits are aligned around the decimal point.
        # For example the value 12345.67 should be encoded as the three
        # pg_digits: 0001 2345 6700

        # First set up counter for first digit, when it is not aligned on a
        # 4 digit boundary.
        i = 4 - r if r else 0

        pg_digit = 0
        for digit in digits:
            # Create pg_digits from decimal digits
            pg_digit *= 10
            pg_digit += digit
            i += 1
            if i == 4:
                pg_digits.append(pg_digit)
                pg_digit = 0
                i = 0
        if i:
            # Halfway last pg_digit. The last decimal digit is not
            # aligned on a 4 digit boundary
            pg_digits.append(pg_digit * 10 ** (4 - i))
    else:
        # [-][s]NaN or (-|+)Infinite
        pg_weight = 0
        pg_scale = 0
        if val.is_nan():
            # All Python variants ("Nan", "-NaN", "sNaN", "-sNaN") are
            # converted to the single PG NaN
            pg_sign = NUMERIC_NAN
        elif val.is_signed():
            pg_sign = NUMERIC_NINF
        else:
            pg_sign = NUMERIC_PINF

    npg_digits = len(pg_digits)
    byte_val = struct.pack(
        "!HhHH" + npg_digits * "H",
        *(npg_digits, pg_weight, pg_sign, pg_scale, *pg_digits))
    len_val = len(byte_val)
    return const.NUMERICOID, f"{len_val}s", byte_val, len_val, Format.BINARY


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


def bool_to_pg(val: bool) -> Tuple[int, str, bool, int, Format]:
    """ Convert a Python bool to a PG bool parameter """
    return const.BOOLOID, "B", val, 1, Format.BINARY
