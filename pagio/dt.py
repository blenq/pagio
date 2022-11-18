""" Date/time type conversion functions """
import sys
from codecs import decode
from datetime import date, datetime, time, timedelta, timezone, tzinfo
import re
from typing import Union, Tuple, Optional

from .common import ProtocolError
from .numeric import bin_int_to_python, bin_int8_to_python


__all__ = [
    "txt_date_to_python", "bin_date_to_python", "txt_timestamp_to_python",
    "bin_timestamp_to_python", "txt_timestamptz_to_python",
    "bin_timestamptz_to_python",
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
    def _from_isoformat(ts_str: str) -> datetime:
        if len(ts_str) not in (19, 23, 26):
            # Python only accepts zero, three or six microsecond digits
            ts_str += '0' * (26 - len(ts_str))
        return datetime.fromisoformat(ts_str)
else:
    _from_isoformat = datetime.fromisoformat


timestamp_re = re.compile(
    r"(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?$",
    re.ASCII)


def txt_timestamp_to_python(buf: memoryview) -> Union[str, datetime]:
    """ Converts PG textual timestamp value in ISO format to Python datetime.
    """
    # String is in the form "YYYY[YY..]-MM-DD HH:MM:SS[.U{1,6}][ BC]
    # Python datetime range can only handle 4 digit year without 'BC' suffix
    ts_str = decode(buf)
    match = timestamp_re.match(ts_str)
    if match:
        usec = match.group(7)
        if usec is None:
            usec = 0
        else:
            usec = int(usec + "0" * (6 - len(usec)))
        try:
            return datetime(  # type: ignore
                *(int(g) for g in match.groups()[:6]), usec)  # type:ignore
        except ValueError:
            pass
    return ts_str


timestamptz_re = re.compile(
    r"(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?"
    r"([-+])(\d{2})(?::(\d{2})(?::(\d{2}))?)?$",
    re.ASCII)


def txt_timestamptz_to_python(buf: memoryview) -> Union[str, datetime]:
    """ Converts PG textual timestamp value in ISO format to Python datetime.
    """
    # String is in the form
    # "YYYY[YY..]-MM-DD HH:MM:SS[.U{1,6}](-+)HH[:MM[:SS]][ BC]"
    # Python datetime range can only handle 4 digit year without 'BC' suffix
    ts_str = decode(buf)
    match = timestamptz_re.match(ts_str)
    if match:
        usec = match.group(7)
        if usec is None:
            usec = 0
        else:
            # compensate for missing trailing zeroes
            usec = int(usec + "0" * (6 - len(usec)))

        try:
            tz_timedelta = timedelta(
                hours=int(match.group(9)), minutes=int(match.group(10) or 0),
                seconds=int(match.group(11) or 0))
            if match.group(8) == '-':
                tz_timedelta *= -1
            return datetime(  # type: ignore
                *(int(g) for g in match.groups()[:6]), usec,  # type: ignore
                tzinfo=timezone(tz_timedelta))
        except ValueError:
            pass
    return ts_str


USECS_PER_SEC = 1000000
USECS_PER_MINUTE = 60 * USECS_PER_SEC
USECS_PER_HOUR = 60 * USECS_PER_MINUTE
USECS_PER_DAY = 24 * USECS_PER_HOUR


def _time_vals_from_int(time_val: int) -> Tuple[int, int, int, int]:
    hour, time_val = divmod(time_val, USECS_PER_HOUR)
    if time_val < 0 or hour > 24:
        raise ProtocolError("Invalid time value")
    hour = hour % 24
    minute, time_val = divmod(time_val, USECS_PER_MINUTE)
    second, usec = divmod(time_val, USECS_PER_SEC)
    return hour, minute, second, usec


def bin_timestamp_to_python(buf: memoryview) -> Union[str, datetime]:
    """ Converts PG binary timestamp value to Python datetime """
    value = bin_int8_to_python(buf)

    # special values
    if value == 0x7FFFFFFFFFFFFFFF:
        return 'infinity'
    if value == -0x8000000000000000:
        return '-infinity'
    pg_ordinal, time_val = divmod(value, USECS_PER_DAY)
    hour, minute, sec, usec = _time_vals_from_int(time_val)

    if MIN_PG_ORDINAL <= pg_ordinal <= MAX_PG_ORDINAL:
        return datetime.combine(
            date.fromordinal(pg_ordinal + DATE_OFFSET),
            time(hour, minute, sec, usec))

    # Outside python date range, convert to a string identical to PG ISO text
    # format
    year, month, day = _date_vals_from_int(pg_ordinal)

    if year < 1:
        # display value of negative year including correction for non-existing
        # year 0
        year = -1 * year + 1
        bc_suffix = " BC"
    else:
        bc_suffix = ""

    # strip trailing millisecond zeroes
    usec_str = str(usec).rstrip("0") if usec else ""

    return (
        f"{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{sec:02}"
        f"{usec_str}{bc_suffix}")


def bin_timestamptz_to_python(
        buf: memoryview, tz_info: Optional[tzinfo]) -> Union[str, datetime]:
    """ Converts PG binary timestamp value to Python datetime """
    value = bin_int8_to_python(buf)

    # special values
    if value == 0x7FFFFFFFFFFFFFFF:
        return 'infinity'
    if value == -0x8000000000000000:
        return '-infinity'
    pg_ordinal, time_val = divmod(value, USECS_PER_DAY)
    hour, minute, sec, usec = _time_vals_from_int(time_val)

    if MIN_PG_ORDINAL <= pg_ordinal <= MAX_PG_ORDINAL:
        try:
            timestamp = datetime.combine(
                date.fromordinal(pg_ordinal + DATE_OFFSET),
                time(hour, minute, sec, usec), tzinfo=timezone.utc)
            if tz_info is not None:
                timestamp = timestamp.astimezone(tz_info)
            return timestamp
        except (OverflowError, ValueError):
            pass

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
    usec_str = f".{str(usec).rstrip('0')}" if usec else ""

    return (
        f"{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{sec:02}"
        f"{usec_str}+00{bc_suffix}")
