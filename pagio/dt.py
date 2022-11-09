""" Date/time type conversion functions """
import sys
from codecs import decode
from datetime import date, datetime, time
import re
from typing import Union, Tuple

from .common import ProtocolError
from .numeric import bin_int_to_python, bin_int8_to_python


__all__ = [
    "txt_date_to_python", "bin_date_to_python", "txt_timestamp_to_python",
    "bin_timestamp_to_python",
]


def txt_date_to_python(buf: memoryview) -> Union[str, date]:
    """ Converts PG textual date value to Python date """
    date_str = decode(buf)
    if len(date_str) == 10:
        return date.fromisoformat(date_str)
    return date_str


DATE_OFFSET = 730120  # Postgres date offset
MIN_PG_ORDINAL = date.min.toordinal() - DATE_OFFSET
MAX_PG_ORDINAL = date.max.toordinal() - DATE_OFFSET
MAX_YEAR = date.max.year
MIN_YEAR = date.min.year


def _date_vals_from_int(julian_day: int) -> Tuple[int, int, int]:

    # julian day magic to retrieve day, month and year, inspired by postgres
    # server code
    julian = julian_day + 2483589
    quad, extra = divmod(julian, 146097)
    extra = extra * 4 + 3
    julian += 60 + quad * 3 + extra // 146097
    quad, julian = divmod(julian, 1461)
    year_val = julian * 4 // 1461
    julian = ((julian + 305) % 365 if year_val else (julian + 306) % 366) + 123
    year_val += quad * 4
    quad = julian * 2141 // 65536
    return year_val - 4800, (quad + 10) % 12 + 1,  julian - 7834 * quad // 256


def bin_date_to_python(buf: memoryview) -> Union[str, date]:
    """ Converts PG binary date value to Python date """

    pg_ordinal = bin_int_to_python(buf)

    if MIN_PG_ORDINAL <= pg_ordinal <= MAX_PG_ORDINAL:
        # within Python date range
        return date.fromordinal(pg_ordinal + DATE_OFFSET)
    if pg_ordinal == 0x7FFFFFFF:
        # special value "infinity" does not exist in Python
        return "infinity"
    if pg_ordinal == -0x80000000:
        # special value "-infinity" does not exist in Python
        return "-infinity"

    # Outside python date range, convert to a string identical to PG ISO text
    # format
    year, month, day = _date_vals_from_int(pg_ordinal)
    if year > 0:
        fmt = "{0}-{1:02}-{2:02}"
    else:
        # there is no year zero
        fmt = "{0:04}-{1:02}-{2:02} BC"
        year = -1 * (year - 1)
    return fmt.format(year, month, day)


if sys.version_info < (3, 11):
    def _from_isoformat(ts_str: str):
        if len(ts_str) not in (19, 23, 26):
            # Python only accepts zero, three or six microsecond digits
            ts_str += '0' * (26 - len(ts_str))
        return datetime.fromisoformat(ts_str)
else:
    _from_isoformat = datetime.fromisoformat


def txt_timestamp_to_python(buf: memoryview) -> Union[str, datetime]:
    """ Converts PG textual timestamp value in ISO format to Python datetime.
    """
    # String is in the form "YYYY[YY..]-MM-DD HH:MM:SS[.U{1,6}][ BC]
    # Python datetime range can only handle 4 digit year without 'BC' suffix
    ts_str = decode(buf)
    if len(ts_str) > 4 and ts_str[4] == '-' and ts_str[-1] != 'C':
        # within Python range
        return _from_isoformat(ts_str)
    return ts_str


USECS_PER_SEC = 1000000
USECS_PER_MINUTE = 60 * USECS_PER_SEC
USECS_PER_HOUR = 60 * USECS_PER_MINUTE
USECS_PER_DAY = 24 * USECS_PER_HOUR


def _time_vals_from_int(tm):
    hour, tm = divmod(tm, USECS_PER_HOUR)
    if tm < 0 or hour > 24:
        raise ProtocolError("Invalid time value")
    hour = hour % 24
    minute, tm = divmod(tm, USECS_PER_MINUTE)
    second, usec = divmod(tm, USECS_PER_SEC)
    return hour, minute, second, usec


def bin_timestamp_to_python(buf: memoryview) -> Union[str, datetime]:
    """ Converts PG binary timestamp value to Python datetime """
    value = bin_int8_to_python(buf)

    # special values
    if value == 0x7FFFFFFFFFFFFFFF:
        return 'infinity'
    if value == -0x8000000000000000:
        return '-infinity'
    pg_ordinal, tm = divmod(value, USECS_PER_DAY)
    hour, minute, sec, usec = _time_vals_from_int(tm)

    if MIN_PG_ORDINAL <= pg_ordinal <= MAX_PG_ORDINAL:
        return datetime.combine(
            date.fromordinal(pg_ordinal + DATE_OFFSET),
            time(hour, minute, sec, usec))

    # Outside python date range, convert to a string identical to PG ISO text
    # format
    year, month, day = _date_vals_from_int(pg_ordinal)

    if year < 1:
        # display value of negative year including correction for non
        # existing year 0
        year = -1 * year + 1
        bc_suffix = " BC"
    else:
        bc_suffix = ""

    # strip trailing millisecond zeroes
    usec = str(usec).rstrip("0") if usec else ""

    return "{0:04}-{1:02}-{2:02} {3:02}:{4:02}:{5:02}{6}{7}".format(
        year, month, day, hour, minute, sec, usec, bc_suffix)
