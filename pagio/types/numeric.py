""" Numeric conversions """
import struct
from codecs import decode
from decimal import Decimal
from struct import Struct
from typing import Tuple, Any, Generator, Union, List

from .. import const
from .array import PGArray, parse_unquoted
from ..common import ushort_struct, Format, ProtocolError
from ..const import (
    INT4ARRAYOID, BOOLARRAYOID, NUMERICARRAYOID, NUMRANGEOID, FLOAT8ARRAYOID,
    FLOAT4ARRAYOID,
)
from .conv_utils import simple_int
from .range import DiscreteRange, BasePGRange
from .text import default_to_pg


INT16_MAX = 0x7FFF
INT16_MIN = -0x8000
INT32_MAX = 0x7FFFFFFF
INT32_MIN = -0x80000000
INT64_MAX = 0x7FFFFFFFFFFFFFFF
INT64_MIN = -0x8000000000000000


def get_struct_unpacker(fmt: str) -> Any:
    """ Creates a function to get a single value from a struct """

    _unpack = Struct(f"!{fmt}").unpack

    def unpack_struct(conn, buf: memoryview) -> Any:
        return _unpack(buf)[0]

    return unpack_struct


# ======== int ============================================================== #

bin_int2_to_python = get_struct_unpacker("h")
bin_int_to_python = get_struct_unpacker("i")
bin_uint_to_python = get_struct_unpacker("I")
bin_int8_to_python = get_struct_unpacker("q")


def int_to_pg(val: int) -> Tuple[int, str, Union[int, bytes], int, Format]:
    """ Convert a Python int to a PG int parameter """
    if INT32_MIN <= val <= INT32_MAX:
        return const.INT4OID, "i", val, 4, Format.BINARY
    if INT64_MIN <= val <= INT64_MAX:
        return const.INT8OID, "q", val, 8, Format.BINARY
    return default_to_pg(val)


def txt_intvector_to_python(prot, buf: memoryview) -> List[int]:
    return [int(v) for v in decode(buf).split(' ')]


# ======== float ============================================================ #


def float_to_pg(val: float) -> Tuple[int, str, float, int, Format]:
    """ Converts a python float to a PG parameter tuple """
    return const.FLOAT8OID, "d", val, 8, Format.BINARY


def txt_float4_to_python(conn, buf: memoryview) -> float:
    val = float(buf)

    # force float4 to float8 conversion to get exactly the same value as the
    # binary converter and the C extension values
    bin_val = struct.pack("f", val)
    return struct.unpack("f", bin_val)[0]


bin_float4_to_python = get_struct_unpacker("f")
bin_float8_to_python = get_struct_unpacker("d")

# ======== numeric ========================================================== #


def txt_numeric_to_python(conn, buf: memoryview) -> Decimal:
    """ Converts a PG numeric text value to a Python Decimal """
    return Decimal(decode(buf))


numeric_header = Struct("!HhHH")
NUMERIC_NAN = 0xC000
NUMERIC_POS = 0x0000
NUMERIC_NEG = 0x4000
NUMERIC_PINF = 0xD000
NUMERIC_NINF = 0xF000


def bin_numeric_to_python(conn, buf: memoryview) -> Decimal:
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
            [pg_digit] = ushort_struct.unpack_from(buf, i * 2)
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

    # Use the relatively complex binary format. To determine if a Decimal is
    # in the range of a PostgreSQL numeric, it has to be analyzed anyway.
    # From there it is only a small step to creating the binary value.

    pg_digits = []
    if val.is_finite():
        # Regular numeric value

        sign, digits, exp = val.as_tuple()
        pg_sign = NUMERIC_NEG if sign else NUMERIC_POS
        if exp < -0x3FFF:
            # outside PG range, fallback to text
            return default_to_pg(val)

        # PG scale, is the number of digits after the decimal point. It can not
        # be negative.
        pg_scale = 0 if exp > 0 else -exp

        # A PostgreSQL numeric pg_digit is a number from 0 to 9999, and
        # represents 4 decimal digits.
        # "len(digits) + exp", i.e. the number of decimal digits plus the
        # exponent is the 10 based exponent of the first decimal digit.
        # pg_weight is 10000 based exponent of first pg_digit minus one
        q, r = divmod(len(digits) + exp, 4)
        pg_weight = q + bool(r) - 1
        if pg_weight > INT16_MAX:
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


class PGNumRange(BasePGRange[Decimal]):
    oid = NUMRANGEOID

    def _type_check(self, val: Decimal) -> Decimal:
        if not isinstance(val, Decimal):
            return Decimal(val)
        return val


# ======== bool ============================================================= #


def bin_bool_to_python(conn, buf: memoryview) -> bool:
    """ Converts PG binary bool value to Python bool. """
    if buf == b'\x01':
        return True
    if buf == b'\0':
        return False
    raise ProtocolError("Invalid value for bool")


def text_bool_to_python(conn, buf: memoryview) -> bool:
    """ Converts PG textual bool value to Python bool. """
    if buf == b't':
        return True
    if buf == b'f':
        return False
    raise ProtocolError("Invalid value for bool")


def bool_to_pg(val: bool) -> Tuple[int, str, bool, int, Format]:
    """ Convert a Python bool to a PG bool parameter """
    return const.BOOLOID, "B", val, 1, Format.BINARY


# ======== tid ============================================================== #

left_parens = ord('(')
comma = ord(',')
right_parens = ord(')')


def txt_tid_to_python(prot, buf: memoryview) -> Tuple[int, int]:
    if buf[0] != left_parens:
        raise ProtocolError("Invalid tid value.")
    pos = 1
    num1, new_pos = parse_unquoted(buf[pos:], [comma], prot, simple_int)
    pos += new_pos
    pos += 1
    num2, new_pos = parse_unquoted(buf[pos:], [right_parens], prot, simple_int)
    pos += new_pos
    pos += 1
    if pos != len(buf):
        raise ProtocolError("Invalid tid value.")
    return num1, num2


tid_struct = struct.Struct("!LH")


def bin_tid_to_python(prot, buf: memoryview) -> Tuple[int, int]:
    return tid_struct.unpack(buf)


# ======== arrays =========================================================== #


class PGInt4Array(PGArray):
    oid = INT4ARRAYOID


class PGBoolArray(PGArray):
    oid = BOOLARRAYOID


class PGNumericArray(PGArray):
    oid = NUMERICARRAYOID


class PGFloat8Array(PGArray):
    oid = FLOAT8ARRAYOID


class PGFloat4Array(PGArray):
    oid = FLOAT4ARRAYOID


class PGIntRange(DiscreteRange[int]):

    _min_value: int
    _max_value: int

    def increment(self, value: int) -> int:
        return value + 1

    def _type_check(self, val: int) -> int:
        val = int(val)
        if self._min_value <= val <= self._max_value:
            return val
        raise ValueError("Value out of range for int4 type.")


class PGInt4Range(PGIntRange):

    _min_value = INT32_MIN
    _max_value = INT32_MAX


class PGInt8Range(PGIntRange):

    _min_value = INT64_MIN
    _max_value = INT64_MAX
