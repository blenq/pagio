#include "datetime.h"
#include "protocol.h"
#include "utils.h"
#include <datetime.h>

#if PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION < 10

#define _PyDateTime_HAS_TZINFO(o)  (((_PyDateTime_BaseTZInfo *)(o))->hastzinfo)

#define PyDateTime_DATE_GET_TZINFO(o) (_PyDateTime_HAS_TZINFO((o)) ? \
    ((PyDateTime_DateTime *)(o))->tzinfo : Py_None)

#endif

PyObject *Date;
PyObject *DateTime;
PyObject *Time;
PyObject *ZoneInfo;

static PyObject *astimezone;

// ===== utils ================================================================

static PyObject *
tz_from_secs(int seconds) {
    // return a fixed offset timezone, from a number of seconds
    PyObject *tz_offset, *tz;

    tz_offset = PyDelta_FromDSU(0, seconds, 0);
    if (tz_offset == NULL) {
        return NULL;
    }
    tz = PyTimeZone_FromOffset(tz_offset);
    Py_DECREF(tz_offset);
    return tz;
}


// ===== text parsers =========================================================
//
// Functions to parse textual datetime components
//

static int
read_date_text(char **pos, char *end, unsigned int *year, unsigned int *month,
    unsigned int *day)
{
    // Parse an ISO date, format: YYYY-MM-DD

    int num_read, chars_read;

    if (end - *pos < 10) {
        PyErr_SetString(PyExc_ValueError, "Invalid date value");
        return -1;
    }
    num_read = sscanf(*pos, "%4u-%2u-%2u%n", year, month, day, &chars_read);
    if (num_read != 3 || chars_read != 10) {
        PyErr_SetString(PyExc_ValueError, "Invalid date value");
        return -1;
    }
    *pos += chars_read;
    return 0;
}


static int
read_time_text(char **pos, char *end, unsigned int *hour, unsigned int *minute,
    unsigned int *second, unsigned int *usec)
{
    // Parse a time, format: HH:MM:SS
    int num_read, chars_read;

    if (end - *pos < 8) {
        PyErr_SetString(PyExc_ValueError, "Invalid time value");
        return -1;
    }
    num_read = sscanf(
        *pos, "%2u:%2u:%2u%n", hour, minute, second, &chars_read);
    if (num_read != 3 || chars_read != 8) {
        PyErr_SetString(PyExc_ValueError, "Invalid time value");
        return -1;
    }
    if (*hour == 24) {
        *hour = 0;
    }
    *pos += chars_read;
    *usec = 0;
    if (*pos < end - 1 && (*pos)[0] == '.' && isdigit((*pos)[1])) {
        int mul = 100000;
        *pos += 1;
        do {
            *usec += mul * ((*pos)[0] - '0');
            mul /= 10;
            *pos += 1;
        }
        while (mul && *pos < end && isdigit((*pos)[0]));
    }
    return 0;
}


static int
read_datetime_text(
    char **pos,
    char *end,
    unsigned int *year,
    unsigned int *month,
    unsigned int *day,
    unsigned int *hour,
    unsigned int *minute,
    unsigned int *second,
    unsigned int *usec)
{
    // Parse date and time, format: YYYY-MM-DD HH:MM:SS

    if (read_date_text(pos, end, year, month, day) == -1) {
        return -1;
    }
    if (*pos == end || (*pos)[0] != ' ') {
        PyErr_SetString(PyExc_ValueError, "Invalid timestamp value.");
        return -1;
    }
    *pos += 1;
    if (read_time_text(pos, end, hour, minute, second, usec) == -1) {
        return -1;
    }
    return 0;
}


static PyObject *
read_tz_offset_text(char **pos, char *end)
{
    // Parse TZ offset string into timezone, (-|+)HH[:MM[:SS]]
    char sign;
    int tz_secs, num_read, chars_read;
    unsigned int tz_part, mul;

    // Check minimum length
    if (end - *pos < 3) {
        PyErr_SetString(PyExc_ValueError, "Invalid timezone offset.");
        return NULL;
    }

    // get sign
    sign = (*pos)[0];
    if (sign == '+') {
        mul = 1;
    }
    else if (sign == '-') {
        mul = -1;
    }
    else {
        PyErr_SetString(PyExc_ValueError, "Invalid timezone offset.");
        return NULL;
    }

    // read hours
    *pos += 1;
    num_read = sscanf(*pos, "%2u%n", &tz_part, &chars_read);
    if (num_read != 1 || chars_read != 2) {
        PyErr_SetString(PyExc_ValueError, "Invalid timezone offset.");
        return NULL;
    }
    *pos += chars_read;
    tz_secs = mul * tz_part * 3600;

    if (end - *pos >= 3 && (*pos)[0] == ':') {
        // read optional minutes
        *pos += 1;
        num_read = sscanf(*pos, "%2u%n", &tz_part, &chars_read);
        if (num_read != 1 || chars_read != 2) {
            PyErr_SetString(PyExc_ValueError, "Invalid timezone offset.");
            return NULL;
        }
        *pos += chars_read;
        tz_secs += mul * tz_part * 60;

        if (end - *pos >= 3 && (*pos)[0] == ':') {
            // read optional seconds
            *pos += 1;
            num_read = sscanf(*pos, "%2u%n", &tz_part, &chars_read);
            if (num_read != 1 || chars_read != 2) {
                PyErr_SetString(PyExc_ValueError, "Invalid timezone offset.");
                return NULL;
            }
            *pos += chars_read;
            tz_secs += mul * tz_part;
        }
    }
    return tz_from_secs(tz_secs);
}


// ===== text result converters ===============================================
//
// Converter functions for PG textual result values
//


PyObject *
convert_pg_date_text(PPObject *self, char *buf, int len)
{
    // Text converter for PG date values

    if (!self->iso_dates || len != 10) {
        // Not an ISO date or outside Python range
        return PyUnicode_FromStringAndSize(buf, len);
    }
    unsigned int year, month, day;
    char *pos = buf;

    if (read_date_text(&pos, buf + len, &year, &month, &day) == -1) {
        return NULL;
    }
    if (pos != buf + len) {
        PyErr_SetString(PyExc_ValueError, "Invalid date value");
        return NULL;
    }
    return PyDate_FromDate(year, month, day);
}


PyObject *
convert_pg_time_text(PPObject *self, char *buf, int len)
{
    // Text converter for PG time values

    unsigned int hour, minute, second, usec=0;
    char *pos;

    pos = buf;

    // Parse time values
    if (read_time_text(
            &pos, buf + len, &hour, &minute, &second, &usec) == -1) {
        return NULL;
    }

    // Check trailing garbage
    if (pos != buf + len) {
        PyErr_SetString(PyExc_ValueError, "Invalid time value");
        return NULL;
    }

    // Create time
    return PyTime_FromTime(hour, minute, second, usec);
}


PyObject *
convert_pg_timetz_txt(PPObject *self, char *buf, int len)
{
    // Text converter for PG timetz values

    unsigned int hour, minute, second, usec;
    char *pos;
    PyObject *tz;

    pos = buf;

    // Parse time values
    if (read_time_text(
            &pos, buf + len, &hour, &minute, &second, &usec) == -1) {
        return NULL;
    }

    // Parse timezone
    tz = read_tz_offset_text(&pos, buf + len);
    if (tz == NULL) {
        return NULL;
    }

    // Check trailing garbage
    if (pos != buf + len) {
        PyErr_SetString(PyExc_ValueError, "Invalid timezone offset.");
        return NULL;
    }

    // Create time with timezone
    return PyDateTimeAPI->Time_FromTime(
        hour, minute, second, usec, tz, PyDateTimeAPI->TimeType);
}


PyObject *
convert_pg_timestamp_text(PPObject *self, char *buf, int len) {

    // Text converter for PG timestamp values

    if (!self->iso_dates || len < 19 || len > 26 || buf[4] != '-' ||
            buf[len - 1] == 'C') {
        // Not an ISO string or outside Python range
        return PyUnicode_FromStringAndSize(buf, len);
    }

    // PostgreSQL is using ISO dates, the year has 4 digits and the string
    // does not end with 'BC'. Can be converted to Python datetime.

    unsigned int year, month, day, hour, minute, second, usec;
    char *pos = buf;

    // Parse date and time
    if (read_datetime_text(
            &pos, buf + len, &year, &month, &day, &hour, &minute, &second,
            &usec) == -1) {
        return NULL;
    }

    // Check trailing garbage
    if (pos != buf + len) {
        PyErr_SetString(PyExc_ValueError, "Invalid timezone offset.");
        return NULL;
    }

    // Create Python datetime
    return PyDateTime_FromDateAndTime(
        year, month, day, hour, minute, second, usec);
}


PyObject *
convert_pg_timestamptz_text(PPObject *self, char *buf, int len)
{
    // Text converter for PG timestamptz values

    if (!self->iso_dates || len < 22 || buf[4] != '-' || buf[len - 1] == 'C') {
        // Not an iso string within Python range
        return PyUnicode_FromStringAndSize(buf, len);
    }
    // PostgreSQL is using ISO dates, the year has 4 digits and the string
    // does not end with 'BC'. Can be converted to Python datetime.

    unsigned int year, month, day, hour, minute, second, usec;
    PyObject *tz, *ts;
    char *pos = buf, *end = buf + len;

    // Parse date and time
    if (read_datetime_text(
            &pos, end, &year, &month, &day, &hour, &minute, &second, &usec
            ) == -1) {
        return NULL;
    }

    if (self->zone_info) {
        tz = self->zone_info;
        Py_INCREF(tz);
    }
    else {
        // Parse timezone
        tz = read_tz_offset_text(&pos, buf + len);
        if (tz == NULL) {
            return NULL;
        }

        // Check trailing garbage
        if (pos != end) {
            Py_DECREF(tz);
            PyErr_SetString(PyExc_ValueError, "Invalid timezone offset.");
            return NULL;
        }
    }

    ts = PyDateTimeAPI->DateTime_FromDateAndTime(
        year, month, day, hour, minute, second, usec, tz,
        PyDateTimeAPI->DateTimeType);
    Py_DECREF(tz);
    return ts;
}


// ===== binary extractors ====================================================
//
// Functions to extract datetime components from binary values
//

#define POSTGRES_EPOCH_JDATE 2451545

static void
date_vals_from_int(int jd, int *year, int *month, int *day)
{
    unsigned int julian, quad, extra;
    int y;

    /* julian day magic to retrieve day, month and year, shamelessly copied
     * from postgres server code */
    julian = jd + POSTGRES_EPOCH_JDATE;
    julian += 32044;
    quad = julian / 146097;
    extra = (julian - quad * 146097) * 4 + 3;
    julian += 60 + quad * 3 + extra / 146097;
    quad = julian / 1461;
    julian -= quad * 1461;
    y = julian * 4 / 1461;
    julian = ((y != 0) ? ((julian + 305) % 365) : ((julian + 306) % 366))
           + 123;
    y += quad * 4;
    *year = y - 4800;
    quad = julian * 2141 / 65536;
    *day = julian - 7834 * quad / 256;
    *month = (quad + 10) % 12 + 1;
}


#define USECS_PER_DAY       Py_LL(86400000000)
#define SECS_PER_DAY        86400
#define USECS_PER_HOUR      Py_LL(3600000000)
#define USECS_PER_MINUTE    Py_LL(60000000)
#define USECS_PER_SEC       Py_LL(1000000)
#define MAX_TZ_OFFSET_SECS  57600
#define MIN_TZ_OFFSET_SECS  -57600


static int
time_vals_from_int(uint64_t tm, int *hour, int *minute, int *second, int *usec)
{
    uint64_t hr;

    hr = tm / USECS_PER_HOUR;
    if (tm < 0 || hr > 24) {
        PyErr_SetString(PyExc_ValueError, "Invalid time value");
        return -1;
    }
    *hour = (int)hr % 24;
    tm -= hr * USECS_PER_HOUR;
    *minute = (int)(tm / USECS_PER_MINUTE);
    tm -= *minute * USECS_PER_MINUTE;
    *second = (int)(tm / USECS_PER_SEC);
    *usec = (int)(tm - *second * USECS_PER_SEC);
    return 0;
}


// ===== binary result converters =============================================
//
// Converter functions for PG binary result values
//

PyObject *
convert_pg_date_bin(PPObject *self, char *buf, int len) {
    // Binary converter for PG date values

    int pg_ordinal, year, month, day;

    if (len != 4) {
        PyErr_SetString(PyExc_ValueError, "Invalid binary date value.");
        return NULL;
    }

    // Parse date components
    pg_ordinal = unpack_int4(buf);
    date_vals_from_int(pg_ordinal, &year, &month, &day);

    if (year >= 1 && year <= 9999) {
        // Date within Python range, return date
        return PyDate_FromDate(year, month, day);
    }
    else if (pg_ordinal == INT32_MIN) {
        // special value
        return PyUnicode_FromString("-infinity");
    }
    else if (pg_ordinal == INT32_MAX) {
        // special value
        return PyUnicode_FromString("infinity");
    }
    else {
        // Value not in Python range, create string similar to Postgres ISO
        // format
        char *fmt;
        if (year > 0) {
            fmt = "%i-%02i-%02i";
        }
        else {
            fmt = "%04i-%02i-%02i BC";
            year = -1 * (year - 1);
        }
        return PyUnicode_FromFormat(fmt, year, month, day);
    }
}


PyObject *
convert_pg_time_bin(PPObject *self, char *buf, int len)
{
    // Binary converter for PG time values

    uint64_t val;
    int hour, minute, second, usec;

    if (len != 8) {
        PyErr_SetString(PyExc_ValueError, "Invalid binary timestamp value.");
        return NULL;
    }
    val = unpack_int8(buf);
    if (time_vals_from_int(val, &hour, &minute, &second, &usec) == -1) {
        return NULL;
    }
    return PyTime_FromTime(hour, minute, second, usec);
}


PyObject *
convert_pg_timetz_bin(PPObject *self, char *buf, int len)
{
    // Binary converter for PG timetz values

    uint64_t time_val;
    int hour, minute, second, usec, tz_secs;
    PyObject *tz;

    if (len != 12) {
        PyErr_SetString(PyExc_ValueError, "Invalid binary timestamp value.");
        return NULL;
    }
    time_val = unpack_int8(buf);
    if (time_vals_from_int(time_val, &hour, &minute, &second, &usec) == -1) {
        return NULL;
    }
    tz_secs = unpack_int4(buf + 8);
    tz = tz_from_secs(-tz_secs);
    if (tz == NULL) {
        return NULL;
    }
    // Create time with timezone
    return PyDateTimeAPI->Time_FromTime(
        hour, minute, second, usec, tz, PyDateTimeAPI->TimeType);
}


PyObject *
_convert_pg_timestamp_bin(PPObject *self, char *buf, int len, int use_tz) {
    // Convert a PG timestamp or timestamptz binary value to a python value

    int64_t val, time_val;
    int32_t pg_ordinal, year, month, day, hour, minute, second, usec;
    char *bc_str, *tz_str, usec_str[8];

    if (len != 8) {
        PyErr_SetString(PyExc_ValueError, "Invalid binary timestamp value.");
        return NULL;
    }
    val = unpack_int8(buf);

    // Check special values
    if (val == INT64_MAX) {
        return PyUnicode_FromString("infinity");
    }
    if (val == INT64_MIN) {
        return PyUnicode_FromString("-infinity");
    }

    // Split value into date and time parts
    pg_ordinal = (int32_t)(val / USECS_PER_DAY);
    time_val = val - pg_ordinal * USECS_PER_DAY;
    if (time_val < 0) {
        // compensate for negative time_val
        time_val += USECS_PER_DAY;
        pg_ordinal -= 1;
    }

    // Get date and time components
    date_vals_from_int(pg_ordinal, &year, &month, &day);
    if (time_vals_from_int(time_val, &hour, &minute, &second, &usec) < 0)
        return NULL;

    if (year >= 1 && year <= 9999) {
        // Within Python range
        if (use_tz) {
            // Timestamptz value, add timezone
            PyObject *utc_ts, *loc_ts;

            // PG value is in UTC, create Python datetime with UTC timezone
            utc_ts = PyDateTimeAPI->DateTime_FromDateAndTime(
                year, month, day, hour, minute, second, usec,
                PyDateTime_TimeZone_UTC, PyDateTimeAPI->DateTimeType);
            if (utc_ts == NULL) {
                return NULL;
            }
            if (self->zone_info == NULL) {
                // no known session timezone, return the UTC value.
                return utc_ts;
            }
            // Convert datetime to session timezone
            loc_ts = PyObject_CallMethodObjArgs(
                utc_ts, astimezone, self->zone_info, NULL);
            if (loc_ts == NULL) {
                if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
                    // Can happen when a timestamp close to the limits is
                    // converted to other timezone. Fallback to the UTC value.
                    PyErr_Clear();
                    return utc_ts;
                }
            }
            Py_DECREF(utc_ts);
            return loc_ts;
        }
        else {
            // Return naive datetime
            return PyDateTime_FromDateAndTime(
                year, month, day, hour, minute, second, usec);
        }
    }

    // Value is outside Python range, create string similar to PG ISO format

    if (use_tz) {
        tz_str = "+00";
    }
    else {
        tz_str = "";
    }

    if (year > 9999) {
        bc_str = "";
    }
    else {
        year = -1 * (year - 1);  /* There is no year zero */
        bc_str = " BC";
    }

    // strip trailing millisecond zeroes
    while (usec && usec % 10 == 0) {
        usec = usec / 10;
    }
    if (usec)
        sprintf(usec_str, ".%i", usec);
    else
        usec_str[0] = '\0';

    // Create Python string
    return PyUnicode_FromFormat(
        "%04i-%02i-%02i %02i:%02i:%02i%s%s%s", year, month, day, hour, minute,
        second, usec_str, tz_str, bc_str);
}


PyObject *
convert_pg_timestamp_bin(PPObject *self, char *buf, int len) {
    return _convert_pg_timestamp_bin(self, buf, len, 0);
}


PyObject *
convert_pg_timestamptz_bin(PPObject *self, char *buf, int len) {
    return _convert_pg_timestamp_bin(self, buf, len, 1);
}


// ===== Parameter converters =================================================
//
// Converter functions for date/time parameters
//

#define DATE_OFFSET 730120

static const int _days_before_month[] = {
    0, /* unused; this vector uses 1-based indexing */
    0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334
};

static int
get_py_ordinal(PyObject *param)
{
    // Compute day number, equal to Python ordinal. Algorithm is the same as
    // Python source.

    int prev_year, year, month, day, py_ordinal;

    year = PyDateTime_GET_YEAR(param);
    month = PyDateTime_GET_MONTH(param);
    day = PyDateTime_GET_DAY(param);

    prev_year = year - 1;
    // days from previous years + days from previous month + days of current
    // month
    py_ordinal = (
        prev_year * 365 + prev_year / 4 - prev_year / 100 + prev_year / 400 +
        _days_before_month[month] + day);

    // If current year is leap year and date is later than leap day, compensate
    // for leap day.
    if (month > 2 && year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        py_ordinal += 1;
    }
    return py_ordinal;
}


int
fill_date_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param)
{
    int py_ordinal;

    py_ordinal = get_py_ordinal(param);
    *oid = DATEOID;
    *p_fmt = 1;
    param_info->val.int4 = htobe32(py_ordinal - DATE_OFFSET);
    param_info->ptr = (char *)&param_info->val;
    param_info->len = 4;
    return 0;
}

static PyObject *utcoffset;

int
fill_time_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param)
{
    // Parameter converter for Python time values.

    int64_t time_val;
    PyObject *py_utc_offset;

    // Binary time value
    time_val = htobe64(
        PyDateTime_TIME_GET_HOUR(param) * USECS_PER_HOUR +
        PyDateTime_TIME_GET_MINUTE(param) * USECS_PER_MINUTE +
        PyDateTime_TIME_GET_SECOND(param) * USECS_PER_SEC +
        PyDateTime_TIME_GET_MICROSECOND(param)
    );

    // Get UTC offset. This will only be set if the time value has a tzinfo and
    // if that tzinfo is a fixed offset timezone.
    py_utc_offset = PyObject_CallMethodObjArgs(param, utcoffset, NULL);
    if (py_utc_offset == NULL) {
        return -1;
    }
    if (py_utc_offset == Py_None) {
        // No tz offset, use PG time type. Just a 64 bit integer
        param_info->val.int8 = time_val;
        param_info->ptr = (char *)&param_info->val;
        param_info->len = 8;
        *oid = TIMEOID;
    }
    else {
        // Time has a fixed offset, use PG timetz type. 64 bit integer timeval
        // and 32 bit integer offset seconds

        if (!PyDelta_Check(py_utc_offset)) {
            Py_DECREF(py_utc_offset);
            PyErr_SetString(
                PyExc_ValueError,
                "Time utcoffset should return a timedelta value");
            return -1;
        }
        int tz_secs, tz_days, tz_val;
        uint32_t utz_val;
        char *buf;

        // Get UTC offset in seconds, microseconds are not supported by PG in
        // offset. Ignore microseconds in offset.
        tz_secs = PyDateTime_DELTA_GET_SECONDS(py_utc_offset);
        tz_days = PyDateTime_DELTA_GET_DAYS(py_utc_offset);
        tz_val = tz_days * SECS_PER_DAY + tz_secs;
        if (tz_val <= MIN_TZ_OFFSET_SECS || tz_val >= MAX_TZ_OFFSET_SECS) {
            // Offset outside PG timetz timezone offset range, bind as text
            Py_DECREF(py_utc_offset);
            return fill_object_info(param_info, oid, p_fmt, param);
        }
        utz_val = htobe32((uint32_t)-tz_val);

        // Allocate value
        buf = PyMem_Malloc(12);
        if (buf == NULL) {
            Py_DECREF(py_utc_offset);
            PyErr_NoMemory();
            return -1;
        }

        // Copy time value and offset value in buffer
        memcpy(buf, &time_val, 8);
        memcpy(buf + 8, &utz_val, 4);

        // Set parameter values
        param_info->ptr = buf;
        param_info->flags = PARAM_NEEDS_FREE;
        param_info->len = 12;
        *oid = TIMETZOID;
    }
    Py_DECREF(py_utc_offset);
    *p_fmt = 1;
    return 0;
}


void
_fill_datetime_info(ParamInfo *param_info, PyObject *param)
{
    int py_ordinal;

    py_ordinal = get_py_ordinal(param);
    param_info->val.int8 = htobe64(
        (py_ordinal - DATE_OFFSET) * USECS_PER_DAY +
        PyDateTime_DATE_GET_HOUR(param) * USECS_PER_HOUR +
        PyDateTime_DATE_GET_MINUTE(param) * USECS_PER_MINUTE +
        PyDateTime_DATE_GET_SECOND(param) * USECS_PER_SEC +
        PyDateTime_DATE_GET_MICROSECOND(param)
        );
    param_info->ptr = (char *)&param_info->val;
    param_info->len = 8;
}


int
fill_datetime_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param)
{
    if (PyDateTime_DATE_GET_TZINFO(param) == Py_None) {
        // datetime without timezone
        _fill_datetime_info(param_info, param);
        *oid = TIMESTAMPOID;
    }
    else {
        // datetime with timezone, first convert to utc
        PyObject *pg_date = PyObject_CallMethodObjArgs(
            param, astimezone, PyDateTime_TimeZone_UTC, NULL);
        if (pg_date == NULL) {
            return -1;
        }
        _fill_datetime_info(param_info, pg_date);
        Py_DECREF(pg_date);
        *oid = TIMESTAMPTZOID;
    }
    *p_fmt = 1;
    return 0;
}


// ===== Init date/time functionality =========================================

int
init_datetime(void) {
    PyObject *zoneinfo_module;

    /* necessary to call PyDate API */
    PyDateTime_IMPORT;

    Date = (PyObject *)PyDateTimeAPI->DateType;
    Py_INCREF(Date);
    Time = (PyObject *)PyDateTimeAPI->TimeType;
    Py_INCREF(Time);
    DateTime = (PyObject *)PyDateTimeAPI->DateTimeType;
    Py_INCREF(DateTime);

    zoneinfo_module = PyImport_ImportModule("zoneinfo");
    if (zoneinfo_module == NULL && PyErr_ExceptionMatches(PyExc_ImportError)) {
        PyErr_Clear();
        zoneinfo_module = PyImport_ImportModule("backports.zoneinfo");
    }
    if (zoneinfo_module == NULL) {
        return -1;
    }
    ZoneInfo = PyObject_GetAttrString(zoneinfo_module, "ZoneInfo");
    Py_DECREF(zoneinfo_module);
    if (ZoneInfo == NULL) {
        return -1;
    }
    astimezone = PyUnicode_InternFromString("astimezone");
    if (astimezone == NULL) {
        return -1;
    }
    utcoffset = PyUnicode_InternFromString("utcoffset");
    if (utcoffset == NULL) {
        return -1;
    }
    return 0;
}