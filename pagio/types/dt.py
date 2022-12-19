""" Date/time type conversion functions """

from codecs import decode
from datetime import (
    date, datetime, time, timedelta, timezone, tzinfo as dt_tzinfo)
import re
from struct import Struct
from typing import Union, Tuple, Optional, Any

from .array import PGArray
from ..common import ProtocolError, Format
from ..const import (
    DATEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMEOID, TIMETZOID, INTERVALOID,
    TIMESTAMPARRAYOID, TIMESTAMPTZARRAYOID, DATEARRAYOID, TIMEARRAYOID,
    TSRANGEOID, TSTZRANGEOID,
)
from .numeric import bin_int_to_python, bin_int8_to_python
from .range import BasePGRange, DiscreteRange
from .text import default_to_pg


__all__ = [
    "txt_date_to_python", "bin_date_to_python", "txt_timestamp_to_python",
    "bin_timestamp_to_python", "txt_timestamptz_to_python",
    "bin_timestamptz_to_python", "txt_time_to_python", "bin_time_to_python",
    "time_to_pg", "txt_timetz_to_python", "bin_timetz_to_python",
    "PGTimestampArray", "PGTimestampTZArray", "PGDateArray", "PGTimeArray",
    "PGTimestampTZRange",
]

# ======== date ============================================================= #


def txt_date_to_python(conn, buf: memoryview) -> Union[str, date]:
    """ Converts PG textual date value to Python date """
    date_str = decode(buf)
    if conn._iso_dates and len(date_str) == 10:
        return date.fromisoformat(date_str)
    return date_str


USECS_PER_SEC = 1000000
USECS_PER_MINUTE = 60 * USECS_PER_SEC
USECS_PER_HOUR = 60 * USECS_PER_MINUTE
USECS_PER_DAY = 24 * USECS_PER_HOUR

DATE_OFFSET = 730120  # Postgres date offset
MIN_PG_ORDINAL = date.min.toordinal() - DATE_OFFSET
MAX_PG_ORDINAL = date.max.toordinal() - DATE_OFFSET
MIN_PG_TIMESTAMP = MIN_PG_ORDINAL * USECS_PER_DAY
MAX_PG_TIMESTAMP = (
    MAX_PG_ORDINAL * USECS_PER_DAY + 23 * USECS_PER_HOUR +
    59 * USECS_PER_MINUTE + 59 * USECS_PER_SEC + 999999)
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


def bin_date_to_python(conn, buf: memoryview) -> Union[str, date]:
    """ Converts PG binary date value to Python date """

    pg_ordinal = bin_int_to_python(conn, buf)

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


def date_to_pg(val: date) -> Tuple[int, str, int, int, Format]:
    """ Converts Python date to PG parameter """
    return DATEOID, "i", val.toordinal() - DATE_OFFSET, 4, Format.BINARY


class PGDateRange(DiscreteRange[date]):

    def _type_check(self, val: date) -> date:
        if isinstance(val, str):
            return date.fromisoformat(val)
        if isinstance(val, datetime):
            return val.date()
        if not isinstance(val, date):
            raise ValueError("Invalid value for PGDateRange item")
        return val

    def increment(self, value: date) -> date:
        return value + timedelta(days=1)


# ======== time ============================================================= #


time_re = re.compile(r"(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?$")


def time_vals_from_txt(time_str: str) -> Tuple[int, int, int, int]:
    match = time_re.match(time_str)
    if match is None:
        raise ProtocolError("Invalid PG time value.")
    usec = match.group(4)
    if usec is None:
        usec = 0
    else:
        usec = int(usec + "0" * (6 - len(usec)))
    hour = int(match.group(1))
    if hour == 24:
        hour = 0
    return hour, int(match.group(2)), int(match.group(3)), usec


def txt_time_to_python(conn, buf: memoryview) -> time:
    """ Converts PG textual time value to Python time """
    time_str = decode(buf)
    hour, minute, second, usec = time_vals_from_txt(time_str)
    try:
        return time(hour, minute, second, usec)
    except ValueError as ex:
        raise ProtocolError("Invalid PG time value") from ex


def _time_vals_from_int(time_val: int) -> Tuple[int, int, int, int]:
    hour, time_val = divmod(time_val, USECS_PER_HOUR)
    if time_val < 0 or hour > 24:
        raise ProtocolError("Invalid time value")
    hour = hour % 24
    minute, time_val = divmod(time_val, USECS_PER_MINUTE)
    second, usec = divmod(time_val, USECS_PER_SEC)
    return hour, minute, second, usec


def bin_time_to_python(conn, buf: memoryview) -> time:
    """ Converts PG binary time value to Python time """

    value = bin_int8_to_python(conn, buf)
    return time(*_time_vals_from_int(value))


timetz_struct_pack = Struct("!qi").pack
MIN_TZ_OFFSET_SECS = -16 * 60 * 60
MAX_TZ_OFFSET_SECS = 16 * 60 * 60


def time_to_pg(val: time) -> Tuple[int, str, Any, int, Format]:
    """ Converts Python time value to PG time parameter """

    pg_val: Union[int, bytes] = (
        val.hour * USECS_PER_HOUR + val.minute * USECS_PER_MINUTE +
        val.second * USECS_PER_SEC + val.microsecond)
    utc_offset = val.utcoffset()

    if utc_offset is None:
        oid = TIMEOID
        fmt = "q"
        val_len = 8
    else:
        offset_seconds = utc_offset.days * 86400 + utc_offset.seconds
        if not MIN_TZ_OFFSET_SECS < offset_seconds < MAX_TZ_OFFSET_SECS:
            # PG supports offset up to +/- 16 hours, bind as text
            return default_to_pg(val)
        oid = TIMETZOID
        fmt = "12s"
        val_len = 12
        pg_val = timetz_struct_pack(pg_val, -offset_seconds)
    return oid, fmt, pg_val, val_len, Format.BINARY

# ======== timetz =========================================================== #


timetz_re = re.compile(
    r"(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?"
    r"([-+])(\d{2})(?::(\d{2})(?::(\d{2}))?)?$")


def txt_timetz_to_python(conn, buf: memoryview) -> time:
    """ Converts PG textual timetz value to Python time with timezone """

    time_str = decode(buf)
    match = timetz_re.match(time_str)
    if match is None:
        raise ProtocolError("Invalid PG time value.")
    usec = match.group(4)
    if usec is None:
        usec = 0
    else:
        usec = int(usec + "0" * (6 - len(usec)))
    hour = int(match.group(1))
    if hour == 24:
        hour = 0
    tz_timedelta = timedelta(
        hours=int(match.group(6)), minutes=int(match.group(7) or 0),
        seconds=int(match.group(8) or 0))
    if match.group(5) == '-':
        tz_timedelta *= -1
    try:
        return time(  # type: ignore
            hour, *(int(g) for g in match.groups()[1:3]), usec,  # type: ignore
            tzinfo=timezone(tz_timedelta))
    except ValueError as ex:
        raise ProtocolError("Invalid PG time value") from ex


timetz_struct = Struct("!qi")


def bin_timetz_to_python(conn, buf: memoryview) -> time:
    """ Converts PG binary timetz value to Python time with timezone """
    time_val, tz_val = timetz_struct.unpack(buf)
    return time(
        *_time_vals_from_int(time_val),
        tzinfo=timezone(timedelta(seconds=-tz_val)))

# ======== datetime ========================================================= #


timestamp_re = re.compile(
    r"(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?$",
    re.ASCII)


def txt_timestamp_to_python(conn, buf: memoryview) -> Union[str, datetime]:
    """ Converts PG textual timestamp value in ISO format to Python datetime.
    """
    # String is in the form "YYYY[YY..]-MM-DD HH:MM:SS[.U{1,6}][ BC]
    # Python datetime range can only handle 4 digit year without 'BC' suffix
    ts_str = decode(buf)

    if not conn._iso_dates:
        return ts_str

    match = timestamp_re.match(ts_str)
    if not match:
        return ts_str

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


def txt_timestamptz_to_python(conn, buf: memoryview) -> Union[str, datetime]:
    """ Converts PG textual timestamp value in ISO format to Python datetime.
    """
    # String is in the form
    # "YYYY[YY..]-MM-DD HH:MM:SS[.U{1,6}](-+)HH[:MM[:SS]][ BC]"
    # Python datetime range can only handle 4 digit year without 'BC' suffix
    ts_str = decode(buf)
    if not conn._iso_dates:
        return ts_str
    tzinfo = conn._tzinfo

    match = timestamptz_re.match(ts_str)
    if match:
        usec = match.group(7)
        if usec is None:
            usec = 0
        else:
            # compensate for missing trailing zeroes
            usec = int(usec + "0" * (6 - len(usec)))

        try:
            if tzinfo is None:
                tz_delta = timedelta(
                    hours=int(match.group(9)), minutes=int(match.group(10) or 0),
                    seconds=int(match.group(11) or 0))
                if match.group(8) == '-':
                    tz_delta *= -1
                tzinfo = timezone(tz_delta)

            return datetime(  # type: ignore
                *(int(g) for g in match.groups()[:6]), usec,  # type: ignore
                tzinfo=tzinfo)
        except ValueError:
            pass
    return ts_str


def bin_timestamp_to_python(conn, buf: memoryview) -> Union[str, datetime]:
    """ Converts PG binary timestamp value to Python datetime """
    value = bin_int8_to_python(conn, buf)

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


def bin_timestamptz_to_python(conn, buf: memoryview) -> Union[str, datetime]:
    """ Converts PG binary timestamp value to Python datetime """
    value = bin_int8_to_python(conn, buf)

    # special values
    if value == 0x7FFFFFFFFFFFFFFF:
        return 'infinity'
    if value == -0x8000000000000000:
        return '-infinity'

    tzinfo = conn._tzinfo
    if MIN_PG_TIMESTAMP <= value <= MAX_PG_TIMESTAMP:
        # UTC value is within Python range
        pg_ordinal, time_val = divmod(value, USECS_PER_DAY)
        hour, minute, sec, usec = _time_vals_from_int(time_val)
        timestamp = datetime.combine(
            date.fromordinal(pg_ordinal + DATE_OFFSET),
            time(hour, minute, sec, usec), tzinfo=timezone.utc)
        if tzinfo is not None:
            try:
                timestamp = timestamp.astimezone(tzinfo)
            except (OverflowError, ValueError):
                pass
        return timestamp

    if tzinfo is not None and (
            value < MIN_PG_TIMESTAMP or value > MAX_PG_TIMESTAMP):
        # Edge case. UTC value is outside Python range, but with the session
        # timezone it might be just inside.
        # Apply the edge offset manually and test again
        if value < MIN_PG_TIMESTAMP:
            dt_to_use = datetime.min
        else:
            dt_to_use = datetime.max
        utc_offset = tzinfo.utcoffset(dt_to_use)
        utc_offset_usec = (
            utc_offset.days * USECS_PER_DAY +
            utc_offset.seconds * USECS_PER_SEC + utc_offset.microseconds)
        adj_value = value + utc_offset_usec

        if MIN_PG_TIMESTAMP <= adj_value <= MAX_PG_TIMESTAMP:
            # Adjusted value is within range. Use that with provided timezone
            pg_ordinal, time_val = divmod(adj_value, USECS_PER_DAY)
            hour, minute, sec, usec = _time_vals_from_int(time_val)
            return datetime.combine(
                date.fromordinal(pg_ordinal + DATE_OFFSET),
                time(hour, minute, sec, usec), tzinfo=tzinfo)

    # Outside python date range, convert to a string identical to PG ISO text
    # format
    pg_ordinal, time_val = divmod(value, USECS_PER_DAY)
    year, month, day = _date_vals_from_int(pg_ordinal)
    hour, minute, sec, usec = _time_vals_from_int(time_val)

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


def datetime_to_pg(val: datetime) -> Tuple[int, str, int, int, Format]:
    """ Converts Python datetime to PG timestamp(tz) parameter """

    if val.tzinfo is None:
        oid = TIMESTAMPOID
    else:
        oid = TIMESTAMPTZOID
        val = val.astimezone(timezone.utc)
    pg_val = (
        (val.toordinal() - DATE_OFFSET) * USECS_PER_DAY +
        val.hour * USECS_PER_HOUR + val.minute * USECS_PER_MINUTE +
        val.second * USECS_PER_SEC + val.microsecond)
    return oid, "q", pg_val, 8, Format.BINARY


class PGTimestampRange(BasePGRange[datetime]):
    oid = TSRANGEOID

    def _type_check(self, val: datetime) -> datetime:
        if not isinstance(val, datetime):
            raise ValueError("PGTimestampRange item must be a datetime")
        if val.tzinfo is not None:
            raise ValueError("PGTimestampRange item can not have a tzinfo.")
        return val


class PGTimestampTZRange(BasePGRange[datetime]):

    oid = TSTZRANGEOID

    def _type_check(self, val: datetime) -> datetime:
        if not isinstance(val, datetime):
            raise ValueError("PGTimestampTZRange item must be a datetime")
        if val.tzinfo is None:
            raise ValueError("Missing tzinfo for PGTimestampTZRange item")
        return val


# ======== interval ========================================================= #


def txt_interval_to_python(conn, buf: memoryview) -> Tuple[int, timedelta]:

    str_val = decode(buf)
    if conn._interval_style != "postgres":
        return str_val

    parts = str_val.split(" ")
    td_sign = 1
    if len(parts) % 2:
        time_str = parts[-1]
        if time_str.startswith("-"):
            td_sign = -1
            time_str = time_str[1:]
        elif time_str.startswith("+"):
            time_str = time_str[1:]
        hour, minute, second, usec = time_vals_from_txt(time_str)
        hour *= td_sign
        minute *= td_sign
        second *= td_sign
        usec *= td_sign
    else:
        hour = minute = second = usec = 0
    year = month = day = 0
    for num_str, unit in zip(parts[::2], parts[1::2]):
        if unit.startswith("y"):
            year = int(num_str)
        elif unit.startswith("mo"):
            month = int(num_str)
        elif unit.startswith("d"):
            day = int(num_str)
    return year * 12 + month, timedelta(
        day, hours=hour, minutes=minute, seconds=second, microseconds=usec)


interval_struct = Struct("!qii")


def bin_interval_to_python(conn, buf: memoryview):
    time_val, days, months = interval_struct.unpack(buf)
    return months, timedelta(days, microseconds=time_val)


def timedelta_to_pg(val: timedelta) -> Tuple[int, str, int, int, Format]:
    val = interval_struct.pack(
        val.seconds * USECS_PER_SEC + val.microseconds, val.days, 0)
    return INTERVALOID, "16s", val, 16, Format.BINARY


class PGTimestampArray(PGArray):
    oid = TIMESTAMPARRAYOID

    def _val_to_str(self, val: datetime) -> str:
        return val.isoformat()


class PGTimestampTZArray(PGTimestampArray):
    oid = TIMESTAMPTZARRAYOID


class PGDateArray(PGTimestampArray):
    oid = DATEARRAYOID


class PGTimeArray(PGTimestampArray):
    oid = TIMEARRAYOID
