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

// ===== date =================================================================


PyObject *
convert_pg_date_text(PPObject *self, char *buf, int len) {
    if (self->iso_dates && len == 10) {
        int year, month, day, count;

        count = sscanf(buf, "%4d-%2d-%2d", &year, &month, &day);
        if (count != 3) {
            PyErr_SetString(PyExc_ValueError, "Invalid date value");
            return NULL;
        }
        return PyDate_FromDate(year, month, day);
    }
    return PyUnicode_FromStringAndSize(buf, len);
}


#define POSTGRES_EPOCH_JDATE 2451545
#define DATE_OFFSET 730120

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


PyObject *
convert_pg_date_bin(PPObject *self, char *buf, int len) {
    int pg_ordinal, year, month, day;

    if (len != 4) {
        PyErr_SetString(PyExc_ValueError, "Invalid binary date value.");
        return NULL;
    }
    pg_ordinal = unpack_int4(buf);
    date_vals_from_int(pg_ordinal, &year, &month, &day);

    if (year >= 1 && year <= 9999) {
        return PyDate_FromDate(year, month, day);
    }
    else if (pg_ordinal == INT32_MIN) {
        return PyUnicode_FromString("-infinity");
    }
    else if (pg_ordinal == INT32_MAX) {
        return PyUnicode_FromString("infinity");
    }
    else {
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

static const int _days_before_month[] = {
    0, /* unused; this vector uses 1-based indexing */
    0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334
};

static int
get_py_ordinal(PyObject *param)
{
    int prev_year, year, month, day, py_ordinal;

    year = PyDateTime_GET_YEAR(param);
    prev_year = year - 1;
    month = PyDateTime_GET_MONTH(param);
    day = PyDateTime_GET_DAY(param);

    py_ordinal = (
        prev_year * 365 + prev_year / 4 - prev_year / 100 + prev_year / 400 +
        _days_before_month[month] + day);
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

// ===== time =================================================================

PyObject *
convert_pg_time_text(PPObject *self, char *buf, int len) {
    unsigned int hour, minute, second, usec=0;
    int num_read;

    if (len < 8 || len == 9 || len > 15) {
        PyErr_SetString(PyExc_ValueError, "Invalid time value");
        return NULL;
    }
    num_read = sscanf(buf, "%2u:%2u:%2u", &hour, &minute, &second);
    if (num_read != 3) {
        PyErr_SetString(PyExc_ValueError, "Invalid time value");
        return NULL;
    }
    if (hour == 24) {
        hour = 0;
    }
    if (len > 8) {
        // read microseconds
        char *pos;
        int mul = 100000;

        if (buf[8] != '.') {
            PyErr_SetString(PyExc_ValueError, "Invalid time value");
            return NULL;
        }
        for (pos = buf + 9; mul && pos < buf + len; pos++) {
            unsigned int udigit;
            num_read = sscanf(pos, "%1u", &udigit);
            if (num_read != 1) {
                PyErr_SetString(PyExc_ValueError, "Invalid time value");
                return NULL;
            }
            usec += mul * udigit;
            mul /= 10;
        }
    }
    return PyTime_FromTime(hour, minute, second, usec);
}


#define USECS_PER_DAY       Py_LL(86400000000)
#define USECS_PER_HOUR      Py_LL(3600000000)
#define USECS_PER_MINUTE    Py_LL(60000000)
#define USECS_PER_SEC       Py_LL(1000000)


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


PyObject *
convert_pg_time_bin(PPObject *self, char *buf, int len)
{
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


int
fill_time_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param)
{
    param_info->val.int8 = htobe64(
        PyDateTime_TIME_GET_HOUR(param) * USECS_PER_HOUR +
        PyDateTime_TIME_GET_MINUTE(param) * USECS_PER_MINUTE +
        PyDateTime_TIME_GET_SECOND(param) * USECS_PER_SEC +
        PyDateTime_TIME_GET_MICROSECOND(param)
        );
    param_info->ptr = (char *)&param_info->val;
    param_info->len = 8;
    *oid = TIMEOID;
    *p_fmt = 1;
    return 0;
}


// ===== datetime =============================================================

PyObject *datetime_fromisoformat;

PyObject *
convert_pg_timestamp_text(PPObject *self, char *buf, int len) {

    if (!self->iso_dates || len < 19 || len > 26 || buf[4] != '-' ||
            buf[len - 1] == 'C') {
        return PyUnicode_FromStringAndSize(buf, len);
    }

    // PostgreSQL is using ISO dates, the year has 4 digits and the string
    // does not end with 'BC'. Can be converted to Python datetime.

    // fromisoformat before Python version 3.11 accepts only zero, three or
    // six microseconds digits. PG outputs any from 0 to 6
    if (len == 19 || len == 23 || len == 26) {
        // Parseable
        return PyObject_CallFunction(datetime_fromisoformat, "s#", buf, len);
    }
    // Add zero digits to the microseconds fraction
    char new_buf[26];

    memcpy(new_buf + 20, "000000", 6); // First copy zeroes
    memcpy(new_buf, buf, len);         // Copy original string
    return PyObject_CallFunction(datetime_fromisoformat, "s#", new_buf, 26);
}


PyObject *
convert_pg_timestamp_bin(PPObject *self, char *buf, int len) {
    int64_t val, time_val;
    int32_t pg_ordinal, year, month, day, hour, minute, second, usec;
    char *bc_str, usec_str[8];

    if (len != 8) {
        PyErr_SetString(PyExc_ValueError, "Invalid binary timestamp value.");
        return NULL;
    }
    val = unpack_int8(buf);
    if (val == INT64_MAX) {
        return PyUnicode_FromString("infinity");
    }
    if (val == INT64_MIN) {
        return PyUnicode_FromString("-infinity");
    }
    pg_ordinal = (int32_t)(val / USECS_PER_DAY);
    time_val = val - pg_ordinal * USECS_PER_DAY;
    if (time_val < 0) {
        time_val += USECS_PER_DAY;
        pg_ordinal -= 1;
    }
    date_vals_from_int(pg_ordinal, &year, &month, &day);
    if (time_vals_from_int(time_val, &hour, &minute, &second, &usec) < 0)
        return NULL;

    if (year >= 1 && year <= 9999) {
        return PyDateTime_FromDateAndTime(
            year, month, day, hour, minute, second, usec);
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
    return PyUnicode_FromFormat(
        "%04i-%02i-%02i %02i:%02i:%02i%s%s", year, month, day, hour, minute,
        second, usec_str, bc_str);
}


PyObject *
convert_iso_timestamptz_txt(PPObject *self, char *buf, int len)
{
    unsigned int year, month, day, minute, hour, second, usec = 0,
        tz_hour, tz_minute = 0, tz_second = 0;
    int num_read, pos;
    char *date_str, tz_sign;
    PyObject *dt = NULL, *td = NULL, *tz = NULL;

    if (!self->iso_dates || len < 22 || buf[4] != '-' || buf[len - 1] == 'C') {
        // Not an iso string within Python range
        return NULL;
    }
    date_str = PyMem_Malloc(len + 1);
    if (date_str == NULL) {
        return PyErr_NoMemory();
    }
    memcpy(date_str, buf, len);
    date_str[len] = '\0';
    num_read = sscanf(
        date_str, "%4u-%2u-%2u %2u:%2u:%2u", &year, &month, &day, &hour,
        &minute, &second);
    if (num_read != 6) {
        goto end;
    }
    if (year == 0) {
        goto end;
    }
    if (date_str[19] == '.') {
        int i;
        num_read = sscanf(date_str + 20, "%6u%n", &usec, &pos);
        if (num_read != 1) {
            goto end;
        }
        for (i = 6; i > pos; i--) {
            usec *= 10;
        }
        pos += 20;
    }
    else {
        pos = 19;
    }
    if (pos == len) {
        goto end;
    }
    tz_sign = date_str[pos];
    pos += 1;
    if (pos == len) {
        goto end;
    }
    num_read = sscanf(
        date_str + pos, "%2u:%2u:%2u", &tz_hour, &tz_minute, &tz_second);
    if (num_read == 0) {
        goto end;
    }
    if (tz_hour > 23 || tz_minute > 59 || tz_second > 59) {
        goto end;
    }
    if (pos + 3 * num_read - 1 != len) {
        goto end;
    }
    int tz_total_secs = tz_hour * 3600 + tz_minute * 60 + tz_second;
    if (tz_sign == '-') {
        tz_total_secs *= -1;
    }
    else if (tz_sign != '+') {
        goto end;
    }
    td = PyDelta_FromDSU(0, tz_total_secs, 0);
    if (td == NULL) {
        goto end;
    }
    tz = PyTimeZone_FromOffset(td);
    if (tz == NULL) {
        goto end;
    }
    dt = PyDateTimeAPI->DateTime_FromDateAndTime(
        year, month, day, hour, minute, second, usec, tz,
        PyDateTimeAPI->DateTimeType);

end:
    PyMem_Free(date_str);
    Py_XDECREF(td);
    Py_XDECREF(tz);
    return dt;
}


PyObject *
convert_pg_timestamptz_text(PPObject *self, char *buf, int len)
{
    PyObject *dt = convert_iso_timestamptz_txt(self, buf, len);
    if (dt == NULL) {
        if (PyErr_Occurred()) {
            return NULL;
        }
        return PyUnicode_FromStringAndSize(buf, len);
    }
    return dt;
}


PyObject *astimezone;

PyObject *
convert_pg_timestamptz_bin(PPObject *self, char *buf, int len)
{
    int64_t value, tm;
    int32_t pg_ordinal;
    int year, month, day, hour, minute, second, usec;
    char *bc_str, usec_str[8];

    if (len != 8) {
        PyErr_SetString(PyExc_ValueError, "Invalid binary timestamptz value.");
        return NULL;
    }
    value = unpack_int8(buf);

    // special values
    if (value == INT64_MIN) {
        return PyUnicode_FromString("-infinity");
    }
    if (value == INT64_MAX) {
        return PyUnicode_FromString("infinity");
    }

    // split into date and time
    pg_ordinal = (int32_t)(value / USECS_PER_DAY);
    tm = value - pg_ordinal * USECS_PER_DAY;
    if (tm < 0) {
        tm += USECS_PER_DAY;
        pg_ordinal -= 1;
    }

    // get date and time components
    date_vals_from_int(pg_ordinal, &year, &month, &day);
    if (time_vals_from_int(tm, &hour, &minute, &second, &usec) < 0)
        return NULL;

    if (year < 1) {
        year = -1 * (year - 1);  /* There is no year zero */
        bc_str = " BC";
    }
    else {
        if (year < 10000) {
            // timestamp is in Python range
            PyObject *utc_ts, *loc_ts;

            // Create datetime with UTC timezone
            utc_ts = PyDateTimeAPI->DateTime_FromDateAndTime(
                year, month, day, hour, minute, second, usec,
                PyDateTime_TimeZone_UTC, PyDateTimeAPI->DateTimeType);
            if (utc_ts == NULL) {
                return NULL;
            }
            if (self->zone_info == NULL) {
                // no known session timezone, return the value
                return utc_ts;
            }
            // convert datetime to session timezone
            loc_ts = PyObject_CallMethodObjArgs(
                utc_ts, astimezone, self->zone_info, NULL);
            Py_DECREF(utc_ts);
            if (loc_ts) {
                return loc_ts;
            }
            // Can happen when a timestamp close to the limits is converted to
            // other timezone.
            if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
                PyErr_Clear();
            }
            else {
                return NULL;
            }
        }
        bc_str = "";
    }
    // Timestamp is outside Python datetime range. Create string similar to
    // postgres.

    // strip trailing millisecond zeroes
    while (usec && usec % 10 == 0) {
        usec = usec / 10;
    }
    if (usec)
        sprintf(usec_str, ".%i", usec);
    else
        usec_str[0] = '\0';
    return PyUnicode_FromFormat(
        "%04i-%02i-%02i %02i:%02i:%02i%s+00%s", year, month, day, hour, minute,
        second, usec_str, bc_str);
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


PyObject *ZoneInfo;

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

    datetime_fromisoformat = PyObject_GetAttrString(
        (PyObject *)PyDateTimeAPI->DateTimeType, "fromisoformat");
    if (datetime_fromisoformat == NULL) {
        return -1;
    }
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
    return 0;
}