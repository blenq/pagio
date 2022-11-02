from codecs import decode
from datetime import date

from .common import int_struct_unpack

__all__ = ["txt_date_to_python", "bin_date_to_python"]


def txt_date_to_python(buf: memoryview):
    date_str = decode(buf)
    if len(date_str) == 10:
        return date.fromisoformat(date_str)
    return date_str


DATE_OFFSET = 730120  # Postgres date offset
MIN_PG_ORDINAL = date.min.toordinal() - DATE_OFFSET
MAX_PG_ORDINAL = date.max.toordinal() - DATE_OFFSET
MAX_YEAR = date.max.year
MIN_YEAR = date.min.year


def _date_vals_from_int(jd):

    # julian day magic to retrieve day, month and year, inspired by postgres
    # server code
    julian = jd + 2483589
    quad, extra = divmod(julian, 146097)
    extra = extra * 4 + 3
    julian += 60 + quad * 3 + extra // 146097
    quad, julian = divmod(julian, 1461)
    y = julian * 4 // 1461
    julian = ((julian + 305) % 365 if y else (julian + 306) % 366) + 123
    y += quad * 4
    quad = julian * 2141 // 65536
    return y - 4800, (quad + 10) % 12 + 1,  julian - 7834 * quad // 256


def bin_date_to_python(buf: memoryview):
    pg_ordinal = int_struct_unpack(buf)[0]

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
